/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.expression

import org.apache.gluten.backendsapi.velox.{VeloxBackendSettings, VeloxRuleApi}
import org.apache.gluten.config.VeloxConfig
import org.apache.gluten.execution.GlutenPlan
import org.apache.gluten.execution.HashAggregateExecTransformer
import org.apache.gluten.execution.ProjectExecTransformer
import org.apache.gluten.execution.WindowExecTransformer
import org.apache.gluten.extension.RewriteTypedAggregatorStages
import org.apache.gluten.extension.columnar.validator.FallbackInjects
import org.apache.gluten.tags.UDFTest

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, DataFrame, Encoder, Encoders, GlutenQueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateMode, Final, Partial}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan, UnionExec}
import org.apache.spark.sql.execution.GlutenImplicits._
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, ObjectHashAggregateExec, TypedAggregateExpression}
import org.apache.spark.sql.execution.exchange.{ShuffleExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.expression.{UDFResolver, UserDefinedAggregateFunction}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, DataType, DoubleType, LongType, StructField, StructType}

import org.scalactic.source.Position

import java.nio.file.Paths

abstract class VeloxUdfSuite extends GlutenQueryTest with SQLHelper {

  protected val master: String

  private var _spark: SparkSession = _

  private lazy val bypassRegistration: Boolean =
    new SparkConf().getBoolean(VeloxConfig.NATIVE_UDF_BYPASS_REGISTRATION.key, false)

  private def testBypassRegistration(testName: String)(testFun: => Any)(implicit
      pos: Position): Unit = {
    if (bypassRegistration) {
      test(testName)(testFun)
    }
  }

  // This property is used for unit tests.
  val UDFLibPathProperty: String = "velox.udf.lib.path"

  protected lazy val udfLibPath: String =
    sys.props.get(UDFLibPathProperty) match {
      case Some(path) =>
        path
      case None =>
        throw new IllegalArgumentException(
          UDFLibPathProperty + s" cannot be null. You may set it by adding " +
            s"-D$UDFLibPathProperty=" +
            "/path/to/gluten/cpp/build/velox/udf/examples/libmyudf.so")
    }

  protected lazy val udfLibRelativePath: String =
    udfLibPath.split(",").map(p => Paths.get(p).getFileName.toString).mkString(",")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    if (_spark == null) {
      _spark = SparkSession
        .builder()
        .master(master)
        .config(sparkConf)
        .enableHiveSupport()
        .getOrCreate()
    }

    _spark.sparkContext.setLogLevel("warn")
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
      if (_spark != null) {
        try {
          _spark.sessionState.catalog.reset()
        } finally {
          _spark.stop()
          _spark = null
        }
      }
    } finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      doThreadPostAudit()
    }
  }

  override protected def spark = _spark

  protected def sparkConf: SparkConf = {
    new SparkConf()
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.adaptive.enabled", "false")
  }

  test("test native hive udf") {
    val tbl = "test_hive_udf_replacement"
    withTempPath {
      dir =>
        try {
          spark.sql(s"""
                       |CREATE EXTERNAL TABLE $tbl
                       |LOCATION 'file://$dir'
                       |AS select * from values (1, '1'), (2, '2'), (3, '3')
                       |""".stripMargin)

          // Check native hive udf has been registered.
          assert(
            UDFResolver.UDFNames.contains("org.apache.spark.sql.hive.execution.UDFStringString"))

          spark.sql("""
                      |CREATE TEMPORARY FUNCTION hive_string_string
                      |AS 'org.apache.spark.sql.hive.execution.UDFStringString'
                      |""".stripMargin)

          val offloadWithImplicitConversionDF =
            spark.sql(s"""SELECT hive_string_string(col1, 'a') FROM $tbl""")
          checkGlutenPlan[ProjectExecTransformer](offloadWithImplicitConversionDF)
          val offloadWithImplicitConversionResult = offloadWithImplicitConversionDF.collect()

          val offloadDF =
            spark.sql(s"""SELECT hive_string_string(col2, 'a') FROM $tbl""")
          checkGlutenPlan[ProjectExecTransformer](offloadDF)
          val offloadResult = offloadWithImplicitConversionDF.collect()

          // Unregister native hive udf to fallback.
          UDFResolver.UDFNames.remove("org.apache.spark.sql.hive.execution.UDFStringString")
          val fallbackDF =
            spark.sql(s"""SELECT hive_string_string(col2, 'a') FROM $tbl""")
          checkSparkPlan[ProjectExec](fallbackDF)
          val fallbackResult = fallbackDF.collect()
          assert(offloadWithImplicitConversionResult.sameElements(fallbackResult))
          assert(offloadResult.sameElements(fallbackResult))

          // Add an unimplemented udf to the map to test fallback of registered native hive udf.
          UDFResolver.UDFNames.add("org.apache.spark.sql.hive.execution.UDFIntegerToString")
          spark.sql("""
                      |CREATE TEMPORARY FUNCTION hive_int_to_string
                      |AS 'org.apache.spark.sql.hive.execution.UDFIntegerToString'
                      |""".stripMargin)
          val df = spark.sql(s"""select hive_int_to_string(col1) from $tbl""")
          checkSparkPlan[ProjectExec](df)
          checkAnswer(df, Seq(Row("1"), Row("2"), Row("3")))
        } finally {
          spark.sql(s"DROP TABLE IF EXISTS $tbl")
          spark.sql(s"DROP TEMPORARY FUNCTION IF EXISTS hive_string_string")
          spark.sql(s"DROP TEMPORARY FUNCTION IF EXISTS hive_int_to_string")
        }
    }
  }

  test("test native hive udaf") {
    val tbl = "test_hive_udaf_replacement"
    val udafClass = "test.org.apache.spark.sql.MyDoubleAvg"
    withTempPath {
      dir =>
        try {
          // Check native hive udaf has been registered.
          assert(UDFResolver.UDAFNames.contains(udafClass))

          spark.sql(s"""
                       |CREATE TEMPORARY FUNCTION my_double_avg
                       |AS '$udafClass'
                       |""".stripMargin)
          spark.sql(s"""
                       |CREATE EXTERNAL TABLE $tbl
                       |LOCATION 'file://$dir'
                       |AS select * from values (1, '1'), (2, '2'), (3, '3')
                       |""".stripMargin)
          val df = spark.sql(s"""select
                                |  my_double_avg(cast(col1 as double)),
                                |  my_double_avg(cast(col2 as double))
                                |  from $tbl
                                |""".stripMargin)
          val nativeImplicitConversionDF = spark.sql(s"""select
                                                        |  my_double_avg(col1),
                                                        |  my_double_avg(col2)
                                                        |  from $tbl
                                                        |""".stripMargin)
          val nativeResult = df.collect()
          val nativeImplicitConversionResult = nativeImplicitConversionDF.collect()

          UDFResolver.UDAFNames.remove(udafClass)
          val fallbackDF = spark.sql(s"""select
                                        |  my_double_avg(cast(col1 as double)),
                                        |  my_double_avg(cast(col2 as double))
                                        |  from $tbl
                                        |""".stripMargin)
          val fallbackResult = fallbackDF.collect()
          assert(nativeResult.sameElements(fallbackResult))
          assert(nativeImplicitConversionResult.sameElements(fallbackResult))
        } finally {
          UDFResolver.UDAFNames.add(udafClass)
          spark.sql(s"DROP TABLE IF EXISTS $tbl")
          spark.sql(s"DROP TEMPORARY FUNCTION IF EXISTS my_double_avg")
        }
    }
  }

  test("test native hive udaf in window") {
    val tbl = "test_hive_udaf_window"
    val udafClass = "test.org.apache.spark.sql.MyDoubleAvg"
    val query =
      s"""SELECT
         |  col1,
         |  my_double_avg(col1) OVER (
         |    PARTITION BY col1 % 2
         |    ORDER BY col1
         |    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS my_avg_window
         |FROM $tbl
         |ORDER BY col1
         |""".stripMargin

    withTempPath {
      dir =>
        try {
          assert(UDFResolver.UDAFNames.contains(udafClass))

          spark.sql(s"""
                       |CREATE TEMPORARY FUNCTION my_double_avg
                       |AS '$udafClass'
                       |""".stripMargin)
          spark.sql(s"""
                       |DROP TABLE IF EXISTS $tbl;
                       |""".stripMargin)
          spark.sql(s"""
                       |CREATE EXTERNAL TABLE $tbl
                       |LOCATION 'file://$dir'
                       |AS SELECT CAST(v AS FLOAT) AS col1
                       |FROM VALUES (1.0), (2.0), (3.0), (4.0) AS t(v)
                       |""".stripMargin)

          val offloadDF = spark.sql(query)
          checkGlutenPlan[WindowExecTransformer](offloadDF)
          checkAnswer(
            offloadDF,
            Seq(
              Row(1.0f, 101.0),
              Row(2.0f, 102.0),
              Row(3.0f, 102.0),
              Row(4.0f, 103.0)
            ))
          val offloadResult = offloadDF.collect()

          UDFResolver.UDAFNames.remove(udafClass)
          val fallbackDF = spark.sql(query)
          checkSparkPlan[WindowExec](fallbackDF)
          val fallbackResult = fallbackDF.collect()

          assert(offloadResult.sameElements(fallbackResult))
        } finally {
          UDFResolver.UDAFNames.add(udafClass)
          spark.sql(s"DROP TABLE IF EXISTS $tbl")
          spark.sql(s"DROP TEMPORARY FUNCTION IF EXISTS my_double_avg")
        }
    }
  }

  // Three uneven, nonempty partitions (1, 1 and 2 rows), so the final aggregate merges.
  private val typedAverageInput = Seq(1.0, 2.0, 3.0, 4.0)
  private val typedAverageNumPartitions = 3

  private def typedAverage(
      aggregator: Aggregator[Double, MyDoubleAvgBuffer, Double],
      input: Seq[Double]): DataFrame = {
    val session = spark
    import session.implicits._
    val ds =
      if (input.isEmpty) input.toDS()
      else spark.sparkContext.parallelize(input, typedAverageNumPartitions).toDS()
    ds.select(aggregator.toColumn.name("avg")).toDF()
  }

  private def checkOriginalTypedAggregates(plan: SparkPlan): Unit = {
    val aggregates = plan.collect { case a: BaseAggregateExec => a }
    assert(aggregates.nonEmpty, plan)
    aggregates.foreach {
      a =>
        assert(a.isInstanceOf[ObjectHashAggregateExec], plan)
        assert(a.aggregateExpressions.head.aggregateFunction.isInstanceOf[TypedAggregateExpression])
        a.aggregateExpressions.head.mode match {
          case Partial => assert(a.output.map(_.dataType) == Seq(BinaryType))
          case Final => assert(a.child.output.map(_.dataType) == Seq(BinaryType))
          case other => fail(s"Unexpected aggregate mode: $other")
        }
    }
  }

  // The intermediate type registered for MyDoubleAvgAggregator in MyUDAF.cc.
  private val averageIntermediate =
    StructType(Seq(StructField("a", DoubleType), StructField("b", LongType)))

  private def checkTypedAverageStages(
      df: DataFrame,
      native: Boolean,
      aggregator: Class[_] = classOf[MyDoubleAvgAggregator],
      nativeIntermediate: DataType = averageIntermediate): Unit = {
    val plan = getExecutedPlan(df)
    val aggregates = plan.collect { case a: BaseAggregateExec => a }
    assert(aggregates.size == 2, plan.last)
    def stage(mode: AggregateMode): BaseAggregateExec =
      aggregates.find(_.aggregateExpressions.map(_.mode) == Seq(mode)).get
    val partial = stage(Partial)
    val fin = stage(Final)
    val buffer = partial.output
    if (native) {
      assert(aggregates.forall(_.isInstanceOf[HashAggregateExecTransformer]), plan.last)
      aggregates.foreach {
        a =>
          a.aggregateExpressions.head.aggregateFunction match {
            case udaf: UserDefinedAggregateFunction =>
              assert(udaf.name == aggregator.getName)
            case other => fail(s"Unexpected aggregate function: $other")
          }
      }
    } else {
      assert(aggregates.forall(_.isInstanceOf[ObjectHashAggregateExec]), plan.last)
      assert(
        aggregates.forall(
          _.aggregateExpressions.head.aggregateFunction.isInstanceOf[TypedAggregateExpression]))
    }
    val bufferType = Seq(if (native) nativeIntermediate else BinaryType)
    val exchanges = plan.collect { case e: ShuffleExchangeLike => e }
    assert(buffer.map(_.dataType) == bufferType, plan.last)
    assert(exchanges.map(_.output.map(_.dataType)) == Seq(bufferType), plan.last)
    assert(fin.child.output.map(_.dataType) == bufferType, plan.last)
    assert(exchanges.map(_.output.map(_.exprId)) == Seq(buffer.map(_.exprId)), plan.last)
    assert(fin.child.output.map(_.exprId) == buffer.map(_.exprId))
    assert(
      fin.aggregateExpressions.head.aggregateFunction.inputAggBufferAttributes.map(_.exprId) ==
        buffer.map(_.exprId))
  }

  test("typed Aggregator.toColumn runs both aggregate stages in its native UDAF without AQE") {
    assert(
      spark.sparkContext
        .parallelize(typedAverageInput, typedAverageNumPartitions)
        .glom()
        .map(_.length)
        .collect()
        .toSeq == Seq(1, 1, 2))
    Seq("false", "true").foreach {
      aqe =>
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe) {
          val df = typedAverage(new MyDoubleAvgAggregator, typedAverageInput)
          checkAnswer(df, Row(102.5))
          checkTypedAverageStages(df, native = aqe == "false")
        }
    }
  }

  test("typed Aggregator.toColumn on an empty Dataset matches vanilla Spark") {
    var expected: Seq[Row] = Nil
    withSQLConf("spark.gluten.enabled" -> "false") {
      expected = typedAverage(new MyDoubleAvgAggregator, Seq.empty).collect().toSeq
    }
    Seq("false", "true").foreach {
      aqe =>
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe) {
          val df = typedAverage(new MyDoubleAvgAggregator, Seq.empty)
          checkAnswer(df, expected)
          checkTypedAverageStages(df, native = aqe == "false")
        }
    }
  }

  test("typed Aggregator.toColumn runs both aggregate stages on Spark if one cannot offload") {
    for (aqe <- Seq("false", "true"); mode <- Seq(Partial, Final)) {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe) {
        FallbackInjects.fallbackOn {
          case agg: ObjectHashAggregateExec => agg.aggregateExpressions.exists(_.mode == mode)
        } {
          val df = typedAverage(new MyDoubleAvgAggregator, typedAverageInput)
          checkAnswer(df, Row(102.5))
          checkTypedAverageStages(df, native = false)
        }
      }
    }
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      "spark.gluten.sql.columnar.wholeStage.fallback.threshold" -> "1") {
      val df = typedAverage(new MyDoubleAvgAggregator, typedAverageInput)
      checkAnswer(df, Row(102.5))
      checkTypedAverageStages(df, native = false)
    }
  }

  test("typed Aggregator.toColumn runs on Spark if ANSI mode falls back its stages") {
    Seq("false", "true").foreach {
      aqe =>
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe,
          SQLConf.ANSI_ENABLED.key -> "true",
          "spark.gluten.sql.ansiFallback.enabled" -> "true",
          "spark.gluten.sql.columnar.wholeStage.fallback.threshold" -> "-1"
        ) {
          val df = typedAverage(new MyDoubleAvgAggregator, typedAverageInput)
          checkAnswer(df, Row(102.5))
          checkTypedAverageStages(df, native = false)
        }
    }
  }

  test("typed Aggregator.toColumn runs its original stages on Spark if the query falls back") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      "spark.gluten.sql.columnar.query.fallback.threshold" -> "1") {
      val df = typedAverage(new MyDoubleAvgAggregator, typedAverageInput)
      checkAnswer(df, Row(102.5))
      checkTypedAverageStages(df, native = false)
      assert(!df.queryExecution.executedPlan.exists(_.isInstanceOf[GlutenPlan]))
      assert(
        df.fallbackSummary()
          .fallbackNodeToReason
          .flatMap(_.values)
          .exists(_.contains("Fallback policy is taking effect")))
    }
  }

  test("unregistered typed Aggregator.toColumn runs on Spark") {
    val df = typedAverage(new UnregisteredDoubleAvgAggregator, typedAverageInput)
    checkAnswer(df, Row(102.5))
    checkTypedAverageStages(df, native = false)
  }

  test("grouped typed Aggregator keeps both original aggregates on Spark") {
    val session = spark
    import session.implicits._
    val df = spark.sparkContext
      .parallelize(typedAverageInput, typedAverageNumPartitions)
      .toDS()
      .groupByKey(v => v.toLong % 2)
      .agg(new MyDoubleAvgAggregator().toColumn)
      .toDF()
    checkAnswer(df, Seq(Row(0L, 103.0), Row(1L, 102.0)))
    val aggregates = getExecutedPlan(df).collect { case a: BaseAggregateExec => a }
    assert(aggregates.size == 2)
    aggregates.foreach {
      a =>
        assert(a.isInstanceOf[ObjectHashAggregateExec])
        assert(a.groupingExpressions.nonEmpty)
        assert(a.aggregateExpressions.head.aggregateFunction.isInstanceOf[TypedAggregateExpression])
    }
  }

  test("typed Aggregator stages are not paired across a node other than the shuffle") {
    var plan: SparkPlan = null
    withSQLConf("spark.gluten.enabled" -> "false") {
      plan = typedAverage(new MyDoubleAvgAggregator, typedAverageInput).queryExecution.executedPlan
    }
    val withProject = plan.transformUp {
      case e: ShuffleExchangeExec => e.withNewChildren(Seq(ProjectExec(e.child.output, e.child)))
    }
    var dryRuns = 0
    val rule = RewriteTypedAggregatorStages(
      p => {
        dryRuns += 1
        p
      },
      isAdaptive = false)
    rule(plan)
    assert(dryRuns == 1)
    checkOriginalTypedAggregates(rule(withProject))
    assert(dryRuns == 1)
  }

  test("typed Aggregator stages shared under a union stay on Spark if the final cannot offload") {
    var plan: SparkPlan = null
    withSQLConf("spark.gluten.enabled" -> "false") {
      plan = typedAverage(new MyDoubleAvgAggregator, typedAverageInput).queryExecution.executedPlan
    }
    val rule = RewriteTypedAggregatorStages(VeloxRuleApi.offloadDryRun(spark), isAdaptive = false)
    FallbackInjects.fallbackOn {
      case agg: ObjectHashAggregateExec => agg.aggregateExpressions.exists(_.mode == Final)
    } {
      checkOriginalTypedAggregates(rule(UnionExec(Seq(plan, plan))))
    }
  }

  test(
    "typed Aggregator in a rewritten full outer join stays on Spark if the final cannot offload") {
    def query(): DataFrame = {
      val avg = typedAverage(new MyDoubleAvgAggregator, typedAverageInput)
      val ids = spark.range(0, 4, 1, typedAverageNumPartitions).toDF("id")
      ids.join(broadcast(avg), ids("id") < avg("avg"), "full_outer")
    }
    var expected: Seq[Row] = Nil
    withSQLConf("spark.gluten.enabled" -> "false") {
      expected = query().collect().toSeq
    }
    FallbackInjects.fallbackOn {
      case agg: ObjectHashAggregateExec => agg.aggregateExpressions.exists(_.mode == Final)
    } {
      val df = query()
      checkAnswer(df, expected)
      val plan = df.queryExecution.executedPlan
      checkOriginalTypedAggregates(plan)
      // Both join branches consume the aggregate, one of them through the reused exchange.
      val finals = plan.collect {
        case a: ObjectHashAggregateExec if a.aggregateExpressions.head.mode == Final => a
      }
      assert(finals.size == 2, plan)
      finals.foreach(f => assert(f.child.output.map(_.dataType) == Seq(BinaryType), plan))
    }
  }

  test("typed Aggregator.toColumn keeps a scalar registered intermediate type") {
    checkRegisteredIntermediate(new ScalarIntermediateSumAggregator, DoubleType)
  }

  test("typed Aggregator.toColumn keeps a one-field struct registered intermediate type") {
    checkRegisteredIntermediate(
      new OneFieldIntermediateSumAggregator,
      StructType(Seq(StructField("a", DoubleType))))
  }

  private def checkRegisteredIntermediate(
      aggregator: Aggregator[Double, SumBuffer, Double],
      intermediate: DataType): Unit = {
    def query(): DataFrame = {
      val session = spark
      import session.implicits._
      spark.sparkContext
        .parallelize(typedAverageInput, typedAverageNumPartitions)
        .toDS()
        .select(aggregator.toColumn.name("sum"))
        .toDF()
    }
    var expected: Seq[Row] = Nil
    withSQLConf("spark.gluten.enabled" -> "false") {
      expected = query().collect().toSeq
    }
    assert(expected == Seq(Row(10.0)))
    Seq("true", "false").foreach {
      validation =>
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          "spark.gluten.sql.enable.native.validation" -> validation) {
          val df = query()
          checkAnswer(df, expected)
          checkTypedAverageStages(df, native = true, aggregator.getClass, intermediate)
        }
    }
  }

  test("typed Aggregator.toColumn keeps a native intermediate with a null field") {
    // The first partition has no positive input, so its intermediate is (null, 2).
    def query(): DataFrame = {
      val session = spark
      import session.implicits._
      spark.sparkContext
        .parallelize(Seq(Seq(-1.0, -2.0), Seq(5.0)), 2)
        .flatMap(identity)
        .toDS()
        .select(new CountIfAnyPositiveAggregator().toColumn.name("count"))
        .toDF()
    }
    var expected: Seq[Row] = Nil
    withSQLConf("spark.gluten.enabled" -> "false") {
      expected = query().collect().toSeq
    }
    assert(expected == Seq(Row(3.0)))
    val df = query()
    checkAnswer(df, Row(3.0))
    checkTypedAverageStages(df, native = true, classOf[CountIfAnyPositiveAggregator])
  }

  test("typed Aggregator.toColumn prepared by explain runs after native aggregation is disabled") {
    Seq("false", "true").foreach {
      aqe =>
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe,
          "spark.gluten.sql.columnar.hashagg" -> "true") {
          val df = typedAverage(new MyDoubleAvgAggregator, typedAverageInput)
          df.explain()
          withSQLConf("spark.gluten.sql.columnar.hashagg" -> "false") {
            assert(df.collect().toSeq == Seq(Row(102.5)))
          }
          // Without AQE, explain() already planned the final, native plan.
          checkTypedAverageStages(df, native = aqe == "false")
        }
    }
  }

  test("test udf fallback in partition filter") {
    withTempPath {
      dir =>
        try {
          spark.sql("""
                      |CREATE TEMPORARY FUNCTION hive_int_to_string
                      |AS 'org.apache.spark.sql.hive.execution.UDFIntegerToString'
                      |""".stripMargin)

          spark.sql(s"""
                       |CREATE EXTERNAL TABLE t(i INT, p INT)
                       |LOCATION 'file://$dir'
                       |PARTITIONED BY (p)""".stripMargin)

          spark
            .range(0, 10, 1)
            .selectExpr("id as col")
            .createOrReplaceTempView("temp")

          for (part <- Seq(1, 2, 3, 4)) {
            spark.sql(s"""
                         |INSERT OVERWRITE TABLE t PARTITION (p=$part)
                         |SELECT col FROM temp""".stripMargin)
          }

          val df = spark.sql("SELECT i FROM t WHERE hive_int_to_string(p) = '4'")
          checkAnswer(df, (0 until 10).map(Row(_)))
        } finally {
          spark.sql("DROP TABLE IF EXISTS t")
          spark.sql("DROP VIEW IF EXISTS temp")
          spark.sql(s"DROP TEMPORARY FUNCTION IF EXISTS hive_string_string")
        }
    }
  }

  testBypassRegistration("native udf with a plain name is callable without a hive udf class") {
    // No CREATE TEMPORARY FUNCTION and no Java class: the session extension put the name in
    // Spark's registry via SparkInjector.injectFunction, which is the only writer for it.
    assert(
      spark.sessionState.functionRegistry
        .lookupFunction(FunctionIdentifier("myudf_plus_one"))
        .isDefined)

    val df = spark.sql("SELECT myudf_plus_one(col1) FROM VALUES (1L), (2L), (3L) AS t(col1)")
    checkGlutenPlan[ProjectExecTransformer](df)
    checkAnswer(df, Seq(Row(2L), Row(3L), Row(4L)))
  }

  testBypassRegistration("native udf with a plain name fails at analysis when gluten is disabled") {
    withSQLConf(("spark.gluten.enabled", "false")) {
      val e = intercept[Exception] {
        spark.sql("SELECT myudf_plus_one(col1) FROM VALUES (1L) AS t(col1)").collect()
      }
      // The injected function has no JVM implementation to fall back to, so the call is
      // rejected rather than silently returning a result from somewhere else.
      assert(e.getMessage.contains("myudf_plus_one"))
    }
  }

  testBypassRegistration("native udf with a named row argument is callable") {
    // given
    val query = "SELECT myudf_sum_row(named_struct('a', col1, 'b', col2)) " +
      "FROM VALUES (1L, 2L), (3L, 4L) AS t(col1, col2)"
    val expected = Seq(Row(3L), Row(7L))

    // when
    val df = spark.sql(query)

    // then
    checkGlutenPlan[ProjectExecTransformer](df)
    checkAnswer(df, expected)
  }

  testBypassRegistration("native udf returning a named row exposes its field names") {
    // given
    val query = "SELECT myudf_make_row(col1).b FROM VALUES (1L), (2L) AS t(col1)"
    val expected = Seq(Row(2L), Row(3L))

    // when
    val df = spark.sql(query)

    // then
    checkGlutenPlan[ProjectExecTransformer](df)
    checkAnswer(df, expected)
  }

  test("native udf names are not registered when bypass registration is disabled") {
    if (!bypassRegistration) {
      val queries = Seq(
        "myudf_plus_one" -> "SELECT myudf_plus_one(1L)",
        "myudf_sum_row" -> "SELECT myudf_sum_row(named_struct('a', 1L, 'b', 2L))",
        "myudf_make_row" -> "SELECT myudf_make_row(1L).b"
      )
      queries.foreach {
        case (name, query) =>
          assert(
            spark.sessionState.functionRegistry
              .lookupFunction(FunctionIdentifier(name))
              .isEmpty)
          val e = intercept[AnalysisException] {
            spark.sql(query)
          }
          assert(e.getMessage.contains(name))
      }
    }
  }

}

case class MyDoubleAvgBuffer(sum: Double, count: Long)

/** Registered as a native UDAF under its class name in cpp/velox/udf/examples/MyUDAF.cc. */
class MyDoubleAvgAggregator extends Aggregator[Double, MyDoubleAvgBuffer, Double] {
  override def zero: MyDoubleAvgBuffer = MyDoubleAvgBuffer(0.0, 0L)
  override def reduce(b: MyDoubleAvgBuffer, a: Double): MyDoubleAvgBuffer =
    MyDoubleAvgBuffer(b.sum + a, b.count + 1)
  override def merge(b1: MyDoubleAvgBuffer, b2: MyDoubleAvgBuffer): MyDoubleAvgBuffer =
    MyDoubleAvgBuffer(b1.sum + b2.sum, b1.count + b2.count)
  override def finish(r: MyDoubleAvgBuffer): Double = r.sum / r.count + 100.0
  override def bufferEncoder: Encoder[MyDoubleAvgBuffer] = Encoders.product[MyDoubleAvgBuffer]
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

class UnregisteredDoubleAvgAggregator extends MyDoubleAvgAggregator

case class SumBuffer(sum: Double)

/** Registered under its class name with a scalar double intermediate in MyUDAF.cc. */
class ScalarIntermediateSumAggregator extends Aggregator[Double, SumBuffer, Double] {
  override def zero: SumBuffer = SumBuffer(0.0)
  override def reduce(b: SumBuffer, a: Double): SumBuffer = SumBuffer(b.sum + a)
  override def merge(b1: SumBuffer, b2: SumBuffer): SumBuffer = SumBuffer(b1.sum + b2.sum)
  override def finish(r: SumBuffer): Double = r.sum
  override def bufferEncoder: Encoder[SumBuffer] = Encoders.product[SumBuffer]
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

/** Registered under its class name with a struct<a:double> intermediate in MyUDAF.cc. */
class OneFieldIntermediateSumAggregator extends ScalarIntermediateSumAggregator

case class CountIfAnyPositiveBuffer(positive: Boolean, count: Long)

/** Registered as a native UDAF under its class name in cpp/velox/udf/examples/MyUDAF.cc. */
class CountIfAnyPositiveAggregator extends Aggregator[Double, CountIfAnyPositiveBuffer, Double] {
  override def zero: CountIfAnyPositiveBuffer = CountIfAnyPositiveBuffer(positive = false, 0L)
  override def reduce(b: CountIfAnyPositiveBuffer, a: Double): CountIfAnyPositiveBuffer =
    CountIfAnyPositiveBuffer(b.positive || a > 0, b.count + 1)
  override def merge(
      b1: CountIfAnyPositiveBuffer,
      b2: CountIfAnyPositiveBuffer): CountIfAnyPositiveBuffer =
    CountIfAnyPositiveBuffer(b1.positive || b2.positive, b1.count + b2.count)
  override def finish(r: CountIfAnyPositiveBuffer): Double =
    if (r.positive) r.count.toDouble else 0.0
  override def bufferEncoder: Encoder[CountIfAnyPositiveBuffer] =
    Encoders.product[CountIfAnyPositiveBuffer]
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

@UDFTest
class VeloxUdfSuiteLocal extends VeloxUdfSuite {

  override val master: String = "local[2]"
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.files", udfLibPath)
      .set(VeloxBackendSettings.GLUTEN_VELOX_UDF_LIB_PATHS, udfLibRelativePath)
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
  }
}

// Set below environment variables and VM options to run this test:
// export SCALA_HOME=/usr/share/scala
// export SPARK_SCALA_VERSION=2.12
//
// VM options:
// -Dspark.test.home=${SPARK_HOME}
// -Dvelox.udf.lib.path=\
// /path/to/gluten/cpp/build/velox/udf/examples/libmyudf.so
@UDFTest
class VeloxUdfSuiteCluster extends VeloxUdfSuite {

  override val master: String = "local-cluster[2,2,1024]"

  private lazy val driverUdfLibPath =
    udfLibPath.split(",").map(p => Paths.get(p).toAbsolutePath.toUri.toString).mkString(",")

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.files", udfLibPath)
      .set(VeloxBackendSettings.GLUTEN_VELOX_DRIVER_UDF_LIB_PATHS, driverUdfLibPath)
      .set(VeloxBackendSettings.GLUTEN_VELOX_UDF_LIB_PATHS, udfLibRelativePath)
      .set("spark.executor.extraClassPath", sys.props("java.class.path"))
  }
}

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

import org.apache.gluten.backendsapi.velox.VeloxBackendSettings
import org.apache.gluten.config.VeloxConfig
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.execution.HashAggregateExecTransformer
import org.apache.gluten.execution.ProjectExecTransformer
import org.apache.gluten.execution.WindowExecTransformer
import org.apache.gluten.tags.{SkipTest, UDFTest}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{GlutenQueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.expression.UDFResolver
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, FloatType, IntegerType, LongType, MapType, StringType}

import java.nio.file.Paths

abstract class VeloxUdfSuite extends GlutenQueryTest with SQLHelper {

  protected val master: String

  private var _spark: SparkSession = _

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

  test("native udf with a plain name is callable without a hive udf class") {
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

  test("native udf with a plain name fails at analysis when gluten is disabled") {
    withSQLConf(("spark.gluten.enabled", "false")) {
      val e = intercept[Exception] {
        spark.sql("SELECT myudf_plus_one(col1) FROM VALUES (1L) AS t(col1)").collect()
      }
      // The injected function has no JVM implementation to fall back to, so the call is
      // rejected rather than silently returning a result from somewhere else.
      assert(e.getMessage.contains("myudf_plus_one"))
    }
  }

  // libmyudf declares myudf_map_cardinality as a RegistryUdfEntry: a name and nothing else. Its
  // signature, map(K,V) -> bigint, was never restated for Gluten, so the argument and return
  // types come from binding each call against what Velox holds for the name.
  test("native udf declared by name resolves its signature from the velox registry") {
    assert(UDFResolver.UDFNames.contains("myudf_map_cardinality"))

    val df = spark.sql(
      "SELECT myudf_map_cardinality(map(col1, col2, 'z', col2)) " +
        "FROM VALUES ('a', 1.0D), ('b', 2.0D) AS t(col1, col2)")
    checkGlutenPlan[ProjectExecTransformer](df)
    assert(df.schema.head.dataType == LongType)
    checkAnswer(df, Seq(Row(2L), Row(2L)))
  }

  test("native udf declared by name binds a type combination no entry would list") {
    // A nested value type: the kind of shape that a UdfEntry grid would have to spell out.
    val df = spark.sql(
      "SELECT myudf_map_cardinality(map(col1, array(col2, col2))) " +
        "FROM VALUES ('a', 1L), ('b', 2L) AS t(col1, col2)")
    checkGlutenPlan[ProjectExecTransformer](df)
    checkAnswer(df, Seq(Row(1L), Row(1L)))
  }

  // A by-name declaration whose Velox signature has no type variables is resolved to concrete
  // types once at registration, so it binds exactly like a UdfEntry -- including the cast that
  // udfAllowTypeConversion allows. Spark inserts no cast of its own here: UDFExpression does not
  // implement ExpectsInputTypes, so nothing in the analyzer coerces a Gluten UDF call.
  test("native udf declared by name binds an exact call with no conversion") {
    val df = spark.sql("SELECT myudf_registry_plus_one(col1) FROM VALUES (1L), (2L) AS t(col1)")
    checkGlutenPlan[ProjectExecTransformer](df)
    checkAnswer(df, Seq(Row(2L), Row(3L)))
  }

  // A function called by its own name is standing in for nothing, so it binds as it stands.
  test("native udf declared by name does not coerce its arguments") {
    val e = intercept[Exception] {
      spark.sql("SELECT myudf_registry_plus_one(col1) FROM VALUES (1), (2) AS t(col1)").collect()
    }
    assert(e.getMessage.contains("myudf_registry_plus_one"))
  }

  // Hive would have widened the argument itself, so a function standing in for a hive UDF binds
  // by Hive's implicit conversions. No config gates it: a registry-declared function states no
  // signature of its own, so there is nothing for the library or the session to opt into.
  test("a call standing in for a hive udf coerces by hive's rules") {
    val call = UDFResolver.getUdfExpression(
      "myudf_registry_plus_one",
      "myudf_registry_plus_one",
      allowHiveCoercion = true)(Seq(AttributeReference("c", IntegerType)()))
    assert(call.dataType == LongType)
    assert(call.children.head.isInstanceOf[Cast])
    assert(call.children.head.dataType == LongType)
  }

  // The shape this is for: an existing hive UDF whose native counterpart states no signature.
  // Reached the ordinary way, through a hive class name and CREATE TEMPORARY FUNCTION, with no
  // UdfEntry anywhere and bypassRegistration playing no part.
  //
  // Backed by a table rather than a VALUES list: a HiveSimpleUDF is still evaluable during
  // logical optimization, so ConvertToLocalRelation would fold the call away before Gluten sees
  // it. A by-name UDF is Unevaluable by then and does not have that problem.
  test("a hive udf declared by name offloads, and coerces as hive would") {
    val hiveClass = "org.apache.gluten.udf.RegistryHiveUDF"
    val tbl = "test_registry_hive_udf"
    withTempPath {
      dir =>
        try {
          assert(UDFResolver.UDFNames.contains(hiveClass))
          spark.sql(s"CREATE TEMPORARY FUNCTION registry_hive_udf AS '$hiveClass'")
          spark.sql(s"""
                       |CREATE EXTERNAL TABLE $tbl
                       |LOCATION 'file://$dir'
                       |AS SELECT * FROM VALUES ('a', 1), ('x', 2) AS t(s, n)
                       |""".stripMargin)

          // Matching types bind as they stand.
          val exact = spark.sql(s"SELECT registry_hive_udf(s, 'b') FROM $tbl")
          checkGlutenPlan[ProjectExecTransformer](exact)
          checkAnswer(exact, Seq(Row("a b"), Row("x b")))

          // An integer against a string argument: Hive would have converted it at evaluation, so
          // the offloaded call widens it and must agree with what the JVM UDF produces.
          val query = s"SELECT registry_hive_udf(n, 'b') FROM $tbl"
          val offloaded = spark.sql(query)
          checkGlutenPlan[ProjectExecTransformer](offloaded)
          val offloadedResult = offloaded.collect()

          UDFResolver.UDFNames.remove(hiveClass)
          try {
            val fallback = spark.sql(query)
            checkSparkPlan[ProjectExec](fallback)
            assert(offloadedResult.sameElements(fallback.collect()))
          } finally {
            UDFResolver.UDFNames.add(hiveClass)
          }
        } finally {
          spark.sql(s"DROP TABLE IF EXISTS $tbl")
          spark.sql("DROP TEMPORARY FUNCTION IF EXISTS registry_hive_udf")
        }
    }
  }

  // int -> bigint, which the previous two tests use, is permitted by Velox's own coercion rules
  // as well, so it cannot tell the two rule sets apart. numeric -> string can: Hive allows it and
  // a hive UDF taking a Text depends on it, while Velox's defaults are numeric widening only.
  test("hive coercion widens a numeric to a string, which velox's own rules would not") {
    val call = UDFResolver.getUdfExpression(
      "myudf_registry_string_string",
      "myudf_registry_string_string",
      allowHiveCoercion = true)(
      Seq(AttributeReference("n", IntegerType)(), AttributeReference("s", StringType)()))
    assert(call.dataType == StringType)
    assert(call.children.head.isInstanceOf[Cast])
    assert(call.children.head.dataType == StringType)
    assert(!call.children(1).isInstanceOf[Cast])
  }

  // map(K,V), bigint -> bigint: generic in the data, concrete in the control parameter. Only the
  // concrete position is widened; the generic ones bind as they are.
  test("hive coercion widens only the concrete position of a partly generic signature") {
    val args = Seq(
      AttributeReference("m", MapType(StringType, DoubleType))(),
      AttributeReference("n", IntegerType)())

    val call = UDFResolver.getUdfExpression(
      "myudf_map_size_plus",
      "myudf_map_size_plus",
      allowHiveCoercion = true)(args)
    assert(call.dataType == LongType)
    assert(!call.children.head.isInstanceOf[Cast], "the map position should bind as it is")
    assert(call.children(1).isInstanceOf[Cast])
    assert(call.children(1).dataType == LongType)

    // Without the hive rules the integer does not match and nothing binds.
    intercept[GlutenNotSupportException] {
      UDFResolver.getUdfExpression("myudf_map_size_plus", "m", allowHiveCoercion = false)(args)
    }
  }

  // Both positions are the same variable, so binding has to settle on a type both arguments
  // reach. Velox takes their least common supertype and widens the narrower one; the point of
  // pinning it is that this is binder behaviour Gluten depends on rather than implements.
  test("hive coercion binds a fully generic signature to the common supertype") {
    val call = UDFResolver.getUdfExpression("myudf_generic_pair", "p", allowHiveCoercion = true)(
      Seq(AttributeReference("a", IntegerType)(), AttributeReference("b", LongType)()))
    assert(call.dataType == LongType)
    assert(call.children.head.isInstanceOf[Cast])
    assert(call.children.head.dataType == LongType, "the narrower argument is widened")
    assert(!call.children(1).isInstanceOf[Cast], "the wider argument is left alone")
  }

  // Two signatures reachable at the same cost. Rather than picking one, Velox reports no winner
  // and the call does not bind, which for a hive UDF means falling back to the JVM.
  test("an ambiguous widening binds nothing rather than guessing") {
    val args = Seq(AttributeReference("a", IntegerType)(), AttributeReference("b", IntegerType)())
    intercept[GlutenNotSupportException] {
      UDFResolver.getUdfExpression("myudf_ambiguous", "amb", allowHiveCoercion = true)(args)
    }
  }

  // The aggregate side ranks candidates itself, since no Velox entry point returns the
  // intermediate type alongside the coercions. An integer reaches both of this aggregate's
  // signatures, real more cheaply than double, so the cheaper one has to win.
  test("hive coercion picks the cheapest of several aggregate signatures") {
    val udaf = UDFResolver.getUdafExpression("myudaf_avg_registry", allowHiveCoercion = true)(
      Seq(AttributeReference("v", IntegerType)()))
    assert(udaf.dataType == FloatType, "real is a cheaper widening than double")
    assert(udaf.children.head.isInstanceOf[Cast])
    assert(udaf.children.head.dataType == FloatType)
    // The buffer has to come from the same signature that won, not from another candidate.
    assert(udaf.aggBufferAttributes.map(_.dataType) == Seq(DoubleType, LongType))
  }

  // Hive only converts when both types are primitive -- implicitConvertible(TypeInfo, TypeInfo)
  // returns false unless both categories are PRIMITIVE. Velox's coercer would instead recurse and
  // widen array(int) to array(bigint), so that has to be refused.
  test("hive coercion does not widen inside a container") {
    intercept[GlutenNotSupportException] {
      UDFResolver.getUdfExpression("myudf_generic_pair", "p", allowHiveCoercion = true)(
        Seq(
          AttributeReference("a", ArrayType(IntegerType))(),
          AttributeReference("b", ArrayType(LongType))()))
    }
  }

  // VeloxToSubstraitType has no decimal case, so a short decimal would be reported over the wire
  // as its TypeKind, BIGINT -- the JVM would take this decimal-returning call for a
  // long-returning one and read the result as a long. Silently wrong is worse than not offloaded,
  // so a decimal in the reply has to decline instead.
  test("a decimal return type declines rather than reporting a long") {
    intercept[GlutenNotSupportException] {
      UDFResolver.getUdfExpression("myudf_decimal_return", "d", allowHiveCoercion = true)(
        Seq(AttributeReference("a", LongType)()))
    }
  }

  // The same limit applies to a coercion target, which travels in the same reply. Decimal is
  // therefore left out of the Hive rule table, so a decimal position binds nothing at all.
  test("a decimal argument is not a coercion target") {
    intercept[GlutenNotSupportException] {
      UDFResolver.getUdfExpression("myudf_decimal_return", "d", allowHiveCoercion = true)(
        Seq(AttributeReference("a", IntegerType)()))
    }
  }

  // Binding is exact first: a call that matches a signature as it stands must not pick up a cast
  // just because another signature is reachable by widening.
  test("an exact call binds without any widening") {
    val call = UDFResolver.getUdfExpression("myudf_ambiguous", "amb", allowHiveCoercion = true)(
      Seq(AttributeReference("a", LongType)(), AttributeReference("b", DoubleType)()))
    assert(call.dataType == LongType)
    assert(call.children.forall(c => !c.isInstanceOf[Cast]), s"unexpected cast in ${call.children}")
  }

  test("native udf declared by name rejects a call that binds to no signature") {
    // Only map(K,V) is registered in Velox. An array argument resolves to nothing, and since a
    // by-name udf has no JVM implementation behind it the call is rejected at analysis rather
    // than offloaded with a guessed type.
    val e = intercept[Exception] {
      spark.sql("SELECT myudf_map_cardinality(array(col1)) FROM VALUES (1L) AS t(col1)").collect()
    }
    // Pin the reason, so the test cannot pass on an unrelated analysis failure.
    val causes = Iterator.iterate(e: Throwable)(_.getCause).takeWhile(_ != null).toSeq
    assert(
      causes.exists {
        c =>
          c.isInstanceOf[GlutenNotSupportException] &&
          c.getMessage.contains("myudf_map_cardinality -> array<bigint>")
      },
      s"expected an unresolved-signature failure, got: ${causes.map(_.toString).mkString("; ")}"
    )
  }

  // libmyudaf declares myudaf_arbitrary the same way, and its Velox signature is T -> T with
  // intermediate T. A UDAF is only reachable from SQL through a hive UDAF class name, so the
  // resolution is driven directly here. Both the result type and the aggregation buffer come
  // from the signature that binds.
  test("native udaf declared by name resolves its return and intermediate types per call site") {
    assert(UDFResolver.UDAFNames.contains("myudaf_arbitrary"))

    Seq[DataType](
      LongType,
      StringType,
      MapType(StringType, ArrayType(LongType))
    ).foreach {
      argType =>
        val udaf = UDFResolver.getUdafExpression("myudaf_arbitrary")(
          Seq(AttributeReference("c", argType)()))
        // arbitrary(T) returns T and accumulates in T.
        assert(udaf.dataType == argType, s"return type for $argType")
        assert(
          udaf.aggBufferAttributes.map(_.dataType) == Seq(argType),
          s"aggregation buffer for $argType")
    }
  }

  test("native udaf declared by name exchanges partial state across a shuffle") {
    // libmyudaf also registers the aggregate under this hive UDAF class name, which is the only
    // way a query can reach a UDAF. A grouped aggregation splits into partial and final stages
    // around a shuffle, so the intermediate type resolved from the Velox signature is what the
    // two stages have to agree on -- the disagreement a restated intermediateType can introduce
    // is exactly what would fail here.
    val tbl = "test_registry_udaf_shuffle"
    val udafClass = "test.org.apache.spark.sql.MyDoubleSum"
    withTempPath {
      dir =>
        try {
          assert(UDFResolver.UDAFNames.contains(udafClass))

          spark.sql(s"""
                       |CREATE TEMPORARY FUNCTION my_arbitrary
                       |AS '$udafClass'
                       |""".stripMargin)
          spark.sql(s"""
                       |CREATE EXTERNAL TABLE $tbl
                       |LOCATION 'file://$dir'
                       |AS SELECT * FROM VALUES
                       |  ('a', 1.0D), ('a', 1.0D), ('b', 2.0D), ('b', 2.0D) AS t(k, v)
                       |""".stripMargin)

          val df = spark.sql(
            s"SELECT k, my_arbitrary(v) AS agg FROM $tbl GROUP BY k ORDER BY k")

          val plan = df.queryExecution.executedPlan
          val aggregates = plan.collect { case h: HashAggregateExecTransformer => h }
          assert(
            aggregates.size >= 2,
            s"expected a partial and a final native aggregate, got ${aggregates.size} in:\n$plan")
          assert(
            plan.exists(_.isInstanceOf[ShuffleExchangeLike]),
            s"expected the partial state to cross a shuffle in:\n$plan")

          // Every row in a group carries the same value, so which one arbitrary keeps does not
          // change the answer.
          checkAnswer(df, Seq(Row("a", 1.0d), Row("b", 2.0d)))
        } finally {
          spark.sql(s"DROP TABLE IF EXISTS $tbl")
          spark.sql("DROP TEMPORARY FUNCTION IF EXISTS my_arbitrary")
        }
    }
  }

  test("native udaf declared by name reports a call that binds to no signature") {
    // arbitrary takes one argument. A miss has to surface as an unsupported expression so the
    // aggregate falls back to the JVM, not as a silently wrong buffer schema.
    intercept[GlutenNotSupportException] {
      UDFResolver.getUdafExpression("myudaf_arbitrary")(
        Seq(AttributeReference("a", LongType)(), AttributeReference("b", LongType)()))
    }
  }
}

@UDFTest
class VeloxUdfSuiteLocal extends VeloxUdfSuite {

  override val master: String = "local[2]"
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.files", udfLibPath)
      .set(VeloxBackendSettings.GLUTEN_VELOX_UDF_LIB_PATHS, udfLibRelativePath)
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      // Off by default, so the by-name tests below have to opt in.
      .set(VeloxConfig.NATIVE_UDF_BYPASS_REGISTRATION.key, "true")
  }
}

// Set below environment variables and VM options to run this test:
// export SCALA_HOME=/usr/share/scala
// export SPARK_SCALA_VERSION=2.12
//
// VM options:
// -Dspark.test.home=${SPARK_HOME}
// -Dgluten.package.jar=\
// /path/to/gluten/package/target/gluten-package-${project.version}.jar
// -Dvelox.udf.lib.path=\
// /path/to/gluten/cpp/build/velox/udf/examples/libmyudf.so
@SkipTest
class VeloxUdfSuiteCluster extends VeloxUdfSuite {

  override val master: String = "local-cluster[2,2,1024]"

  val GLUTEN_JAR: String = "gluten.package.jar"

  private lazy val glutenJar = sys.props.get(GLUTEN_JAR) match {
    case Some(jar) => jar
    case None =>
      throw new IllegalArgumentException(
        GLUTEN_JAR + s" cannot be null. You may set it by adding " +
          s"-D$GLUTEN_JAR=" +
          "/path/to/gluten/package/target/gluten-package-${project.version}.jar")
  }

  private lazy val driverUdfLibPath =
    udfLibPath.split(",").map("file://" + _).mkString(",")

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.files", udfLibPath)
      .set(VeloxBackendSettings.GLUTEN_VELOX_DRIVER_UDF_LIB_PATHS, driverUdfLibPath)
      .set(VeloxBackendSettings.GLUTEN_VELOX_UDF_LIB_PATHS, udfLibRelativePath)
      .set("spark.driver.extraClassPath", glutenJar)
      .set("spark.executor.extraClassPath", glutenJar)
  }
}

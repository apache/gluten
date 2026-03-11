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
package org.apache.spark.sql

import org.apache.gluten.execution.ProjectExecTransformer

import org.apache.spark.SparkException
import org.apache.spark.sql.execution.ColumnarShuffleExchangeExec
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestData.TestData2
import org.apache.spark.sql.types.StringType

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import scala.util.Random

class GlutenDataFrameSuite extends DataFrameSuite with GlutenSQLTestsTrait {

  testGluten("repartitionByRange") {
    val partitionNum = 10
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> partitionNum.toString) {
      import testImplicits._
      val data1d = Random.shuffle(0.to(partitionNum - 1))
      val data2d = data1d.map(i => (i, data1d.size - i))

      checkAnswer(
        data1d
          .toDF("val")
          .repartitionByRange(data1d.size, $"val".asc)
          .select(spark_partition_id().as("id"), $"val"),
        data1d.map(i => Row(i, i)))

      checkAnswer(
        data1d
          .toDF("val")
          .repartitionByRange(data1d.size, $"val".desc)
          .select(spark_partition_id().as("id"), $"val"),
        data1d.map(i => Row(i, data1d.size - 1 - i)))

      checkAnswer(
        data1d
          .toDF("val")
          .repartitionByRange(data1d.size, lit(42))
          .select(spark_partition_id().as("id"), $"val"),
        data1d.map(i => Row(0, i)))

      checkAnswer(
        data1d
          .toDF("val")
          .repartitionByRange(data1d.size, lit(null), $"val".asc, rand())
          .select(spark_partition_id().as("id"), $"val"),
        data1d.map(i => Row(i, i)))

      // .repartitionByRange() assumes .asc by default if no explicit sort order is specified
      checkAnswer(
        data2d
          .toDF("a", "b")
          .repartitionByRange(data2d.size, $"a".desc, $"b")
          .select(spark_partition_id().as("id"), $"a", $"b"),
        data2d
          .toDF("a", "b")
          .repartitionByRange(data2d.size, $"a".desc, $"b".asc)
          .select(spark_partition_id().as("id"), $"a", $"b")
      )

      // at least one partition-by expression must be specified
      intercept[IllegalArgumentException] {
        data1d.toDF("val").repartitionByRange(data1d.size)
      }
      intercept[IllegalArgumentException] {
        data1d.toDF("val").repartitionByRange(data1d.size, Seq.empty: _*)
      }
    }
  }

  testGluten("distributeBy and localSort") {
    import testImplicits._
    val data = spark.sparkContext.parallelize((1 to 100).map(i => TestData2(i % 10, i))).toDF()

    /** partitionNum = 1 */
    var partitionNum = 1
    val original = testData.repartition(partitionNum)
    assert(original.rdd.partitions.length == partitionNum)

    // Distribute into one partition and order by. This partition should contain all the values.
    val df6 = data.repartition(partitionNum, $"a").sortWithinPartitions("b")
    // Walk each partition and verify that it is sorted ascending and not globally sorted.
    df6.rdd.foreachPartition {
      p =>
        var previousValue: Int = -1
        var allSequential: Boolean = true
        p.foreach {
          r =>
            val v: Int = r.getInt(1)
            if (previousValue != -1) {
              if (previousValue > v) throw new SparkException("Partition is not ordered.")
              if (v - 1 != previousValue) allSequential = false
            }
            previousValue = v
        }
        if (!allSequential) {
          throw new SparkException("Partition should contain all sequential values")
        }
    }

    /** partitionNum = 5 */
    partitionNum = 5
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> partitionNum.toString) {
      val df = original.repartition(partitionNum, $"key")
      assert(df.rdd.partitions.length == partitionNum)
      checkAnswer(original.select(), df.select())

      // Distribute and order by.
      val df4 = data.repartition(partitionNum, $"a").sortWithinPartitions($"b".desc)
      // Walk each partition and verify that it is sorted descending and does not contain all
      // the values.
      df4.rdd.foreachPartition {
        p =>
          // Skip empty partition
          if (p.hasNext) {
            var previousValue: Int = -1
            var allSequential: Boolean = true
            p.foreach {
              r =>
                val v: Int = r.getInt(1)
                if (previousValue != -1) {
                  if (previousValue < v) throw new SparkException("Partition is not ordered.")
                  if (v + 1 != previousValue) allSequential = false
                }
                previousValue = v
            }
            if (allSequential) throw new SparkException("Partition should not be globally ordered")
          }
      }
    }

    /** partitionNum = 10 */
    partitionNum = 10
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> partitionNum.toString) {
      val df2 = original.repartition(partitionNum, $"key")
      assert(df2.rdd.partitions.length == partitionNum)
      checkAnswer(original.select(), df2.select())
    }

    // Group by the column we are distributed by. This should generate a plan with no exchange
    // between the aggregates
    val df3 = testData.repartition($"key").groupBy("key").count()
    verifyNonExchangingAgg(df3)
    verifyNonExchangingAgg(
      testData
        .repartition($"key", $"value")
        .groupBy("key", "value")
        .count())

    // Grouping by just the first distributeBy expr, need to exchange.
    verifyExchangingAgg(
      testData
        .repartition($"key", $"value")
        .groupBy("key")
        .count())

    /** partitionNum = 2 */
    partitionNum = 2
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> partitionNum.toString) {
      // Distribute and order by with multiple order bys
      val df5 = data.repartition(partitionNum, $"a").sortWithinPartitions($"b".asc, $"a".asc)
      // Walk each partition and verify that it is sorted ascending
      df5.rdd.foreachPartition {
        p =>
          var previousValue: Int = -1
          var allSequential: Boolean = true
          p.foreach {
            r =>
              val v: Int = r.getInt(1)
              if (previousValue != -1) {
                if (previousValue > v) throw new SparkException("Partition is not ordered.")
                if (v - 1 != previousValue) allSequential = false
              }
              previousValue = v
          }
          if (allSequential) throw new SparkException("Partition should not be all sequential")
      }
    }
  }

  testGluten("reuse exchange") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2") {
      val df = spark.range(100).toDF()
      val join = df.join(df, "id")
      val plan = join.queryExecution.executedPlan
      checkAnswer(join, df)
      assert(collect(join.queryExecution.executedPlan) {
        // replace ShuffleExchangeExec
        case e: ColumnarShuffleExchangeExec => true
      }.size === 1)
      assert(collect(join.queryExecution.executedPlan) {
        case e: ReusedExchangeExec => true
      }.size === 1)
      val broadcasted = broadcast(join)
      val join2 = join.join(broadcasted, "id").join(broadcasted, "id")
      checkAnswer(join2, df)
      assert(collect(join2.queryExecution.executedPlan) {
        // replace ShuffleExchangeExec
        case e: ColumnarShuffleExchangeExec => true
      }.size == 1)
      assert(collect(join2.queryExecution.executedPlan) {
        case e: ReusedExchangeExec => true
      }.size == 4)
    }
  }

  // TODO: fix in spark-4.0
  /** Failed to check WholeStageCodegenExec, so we rewrite the UT. */
  // testGluten("SPARK-22520: support code generation for large CaseWhen") {
  //   import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
  //   val N = 30
  //   var expr1 = when(equalizer($"id", lit(0)), 0)
  //   var expr2 = when(equalizer($"id", lit(0)), 10)
  //   (1 to N).foreach {
  //     i =>
  //       expr1 = expr1.when(equalizer($"id", lit(i)), -i)
  //       expr2 = expr2.when(equalizer($"id", lit(i + 10)), i)
  //   }
  //   val df = spark.range(1).select(expr1, expr2.otherwise(0))
  //   checkAnswer(df, Row(0, 10) :: Nil)
  //   // We check WholeStageTransformer instead of WholeStageCodegenExec
  //   assert(df.queryExecution.executedPlan.find(_.isInstanceOf[WholeStageTransformer]).isDefined)
  // }
  import testImplicits._

  private lazy val person2: DataFrame = Seq(
    ("Bob", 16, 176),
    ("Alice", 32, 164),
    ("David", 60, 192),
    ("Amy", 24, 180)).toDF("name", "age", "height")

  testGluten("describe") {
    val describeResult = Seq(
      Row("count", "4", "4", "4"),
      Row("mean", null, "33.0", "178.0"),
      Row("stddev", null, "19.148542155126762", "11.547005383792516"),
      Row("min", "Alice", "16", "164"),
      Row("max", "David", "60", "192")
    )

    val emptyDescribeResult = Seq(
      Row("count", "0", "0", "0"),
      Row("mean", null, null, null),
      Row("stddev", null, null, null),
      Row("min", null, null, null),
      Row("max", null, null, null))

    val aggResult = Seq(
      Row("4", "33.0", "19.148542155126762", "16", "60")
    )

    def getSchemaAsSeq(df: DataFrame): Seq[String] = df.schema.map(_.name)

    Seq("true", "false").foreach {
      ansiEnabled =>
        withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled) {
          val describeAllCols = person2.describe()
          assert(getSchemaAsSeq(describeAllCols) === Seq("summary", "name", "age", "height"))
          checkAnswer(describeAllCols, describeResult)
          // All aggregate value should have been cast to string
          describeAllCols.collect().foreach {
            row =>
              row.toSeq.foreach {
                value =>
                  if (value != null) {
                    assert(
                      value.isInstanceOf[String],
                      "expected string but found " + value.getClass)
                  }
              }
          }

          val describeOneCol = person2.describe("age")
          assert(getSchemaAsSeq(describeOneCol) === Seq("summary", "age"))
          val aggOneCol = person2.agg(
            count("age").cast(StringType),
            avg("age").cast(StringType),
            stddev_samp("age").cast(StringType),
            min("age").cast(StringType),
            max("age").cast(StringType))
          checkAnswer(aggOneCol, aggResult)

          val describeNoCol = person2.select().describe()
          assert(getSchemaAsSeq(describeNoCol) === Seq("summary"))
          checkAnswer(describeNoCol, describeResult.map { case Row(s, _, _, _) => Row(s) })

          val emptyDescription = person2.limit(0).describe()
          assert(getSchemaAsSeq(emptyDescription) === Seq("summary", "name", "age", "height"))
          checkAnswer(emptyDescription, emptyDescribeResult)
        }
    }
  }

  testGluten("Allow leading/trailing whitespace in string before casting") {
    withSQLConf("spark.gluten.velox.castFromVarcharAddTrimNode" -> "true") {
      def checkResult(df: DataFrame, expectedResult: Seq[Row]): Unit = {
        checkAnswer(df, expectedResult)
        assert(
          find(df.queryExecution.executedPlan)(_.isInstanceOf[ProjectExecTransformer]).isDefined)
      }

      // scalastyle:off nonascii
      Seq(
        " 123",
        "123 ",
        " 123 ",
        "\u2000123\n\n\n",
        "123\r\r\r",
        "123\f\f\f",
        "123\u000C",
        "123\u0000")
        .toDF("col1")
        .createOrReplaceTempView("t1")
      // scalastyle:on nonascii
      val expectedIntResult = Row(123) :: Row(123) ::
        Row(123) :: Row(123) :: Row(123) :: Row(123) :: Row(123) :: Row(123) :: Nil
      var df = spark.sql("select cast(col1 as int) from t1")
      checkResult(df, expectedIntResult)
      df = spark.sql("select cast(col1 as long) from t1")
      checkResult(df, expectedIntResult)

      Seq(" 123.5", "123.5 ", " 123.5 ", "123.5\n\n\n", "123.5\r\r\r", "123.5\f\f\f", "123.5\u000C")
        .toDF("col1")
        .createOrReplaceTempView("t1")
      val expectedFloatResult = Row(123.5) :: Row(123.5) ::
        Row(123.5) :: Row(123.5) :: Row(123.5) :: Row(123.5) :: Row(123.5) :: Nil
      df = spark.sql("select cast(col1 as float) from t1")
      checkResult(df, expectedFloatResult)
      df = spark.sql("select cast(col1 as double) from t1")
      checkResult(df, expectedFloatResult)

      // scalastyle:off nonascii
      val rawData =
        Seq(" abc", "abc ", " abc ", "\u2000abc\n\n\n", "abc\r\r\r", "abc\f\f\f", "abc\u000C")
      // scalastyle:on nonascii
      rawData.toDF("col1").createOrReplaceTempView("t1")
      val expectedBinaryResult = rawData.map(d => Row(d.getBytes(StandardCharsets.UTF_8))).seq
      df = spark.sql("select cast(col1 as binary) from t1")
      checkResult(df, expectedBinaryResult)
    }
  }

  testGluten("SPARK-27439: Explain result should match collected result after view change") {
    withTempView("test", "test2", "tmp") {
      spark.range(10).createOrReplaceTempView("test")
      spark.range(5).createOrReplaceTempView("test2")
      spark.sql("select * from test").createOrReplaceTempView("tmp")
      val df = spark.sql("select * from tmp")
      spark.sql("select * from test2").createOrReplaceTempView("tmp")

      val captured = new ByteArrayOutputStream()
      Console.withOut(captured) {
        df.explain(extended = true)
      }
      checkAnswer(df, spark.range(10).toDF)
      val output = captured.toString
      assert(output.contains("""== Parsed Logical Plan ==
                               |'Project [*]
                               |+- 'UnresolvedRelation [tmp]""".stripMargin))
      assert(output.contains("""== Physical Plan ==
                               |*(1) ColumnarToRow
                               |+- ColumnarRange 0, 10, 1, 2, 10""".stripMargin))
    }
  }

  testGluten("getRows: binary") {
    val df = Seq(
      ("12".getBytes(StandardCharsets.UTF_8), "ABC.".getBytes(StandardCharsets.UTF_8)),
      ("34".getBytes(StandardCharsets.UTF_8), "12346".getBytes(StandardCharsets.UTF_8))
    ).toDF()
    val expectedAnswer =
      Seq(Seq("_1", "_2"), Seq("[31 32]", "[41 42 43 2E]"), Seq("[33 34]", "[31 32 33 34 36]"))
    assert(df.getRows(10, 20) === expectedAnswer)
  }

  // TODO: fix in spark-4.0
  // private def withExpr(newExpr: Expression): Column = new Column(newExpr)

  // def equalizer(expr: Expression, other: Any): Column = withExpr {
  //   val right = lit(other).expr
  //   if (expr == right) {
  //     logWarning(
  //       s"Constructing trivially true equals predicate, '$expr = $right'. " +
  //         "Perhaps you need to use aliases.")
  //   }
  //   EqualTo(expr, right)
  // }

  private def verifyNonExchangingAgg(df: DataFrame): Unit = {
    var atFirstAgg: Boolean = false
    df.queryExecution.executedPlan.foreach {
      case agg: HashAggregateExec =>
        atFirstAgg = !atFirstAgg
      case _ =>
        if (atFirstAgg) {
          fail("Should not have operators between the two aggregations")
        }
    }
  }

  private def verifyExchangingAgg(df: DataFrame): Unit = {
    var atFirstAgg: Boolean = false
    df.queryExecution.executedPlan.foreach {
      case _: HashAggregateExec =>
        if (atFirstAgg) {
          fail("Should not have back to back Aggregates")
        }
        atFirstAgg = true
      case _: ShuffleExchangeExec => atFirstAgg = false
      case _ =>
    }
  }
}

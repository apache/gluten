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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{HashAggregateExecBaseTransformer, HashAggregateExecTransformer}

import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

import java.lang.{Long => JLong}

import scala.util.Random

class GlutenDataFrameAggregateSuite extends DataFrameAggregateSuite with GlutenSQLTestsTrait {

  import testImplicits._

  // blackTestNameList is defined in ClickHouseNotSupport

  testGluten("count") {
    // agg with no input col
    assert(testData2.count() === testData2.rdd.map(_ => 1).count())

    checkAnswer(
      testData2.agg(count($"a"), sum_distinct($"a")), // non-partial
      Row(6, 6.0))
  }

  testGluten("null count") {
    checkAnswer(testData3.groupBy($"a").agg(count($"b")), Seq(Row(1, 0), Row(2, 1)))

    checkAnswer(testData3.groupBy($"a").agg(count($"a" + $"b")), Seq(Row(1, 0), Row(2, 1)))

    checkAnswer(
      testData3
        .agg(count($"a"), count($"b"), count(lit(1)), count_distinct($"a"), count_distinct($"b")),
      Row(2, 1, 2, 2, 1))

    // [wishlist] does not support sum distinct
//    checkAnswer(
//      testData3.agg(count($"b"), count_distinct($"b"), sum_distinct($"b")), // non-partial
//      Row(1, 1, 2)
//    )
  }

  testGluten("groupBy") {
    checkAnswer(testData2.groupBy("a").agg(sum($"b")), Seq(Row(1, 3), Row(2, 3), Row(3, 3)))
    checkAnswer(testData2.groupBy("a").agg(sum($"b").as("totB")).agg(sum($"totB")), Row(9))
    checkAnswer(testData2.groupBy("a").agg(count("*")), Row(1, 2) :: Row(2, 2) :: Row(3, 2) :: Nil)
    checkAnswer(
      testData2.groupBy("a").agg(Map("*" -> "count")),
      Row(1, 2) :: Row(2, 2) :: Row(3, 2) :: Nil)
    checkAnswer(
      testData2.groupBy("a").agg(Map("b" -> "sum")),
      Row(1, 3) :: Row(2, 3) :: Row(3, 3) :: Nil)

    val df1 = Seq(("a", 1, 0, "b"), ("b", 2, 4, "c"), ("a", 2, 3, "d"))
      .toDF("key", "value1", "value2", "rest")

    checkAnswer(df1.groupBy("key").min(), df1.groupBy("key").min("value1", "value2").collect())
    checkAnswer(df1.groupBy("key").min("value2"), Seq(Row("a", 0), Row("b", 4)))

    // [wishlist] does not support decimal
//    checkAnswer(
//      decimalData.groupBy("a").agg(sum("b")),
//      Seq(Row(new java.math.BigDecimal(1), new java.math.BigDecimal(3)),
//        Row(new java.math.BigDecimal(2), new java.math.BigDecimal(3)),
//        Row(new java.math.BigDecimal(3), new java.math.BigDecimal(3)))
//    )
//
//    val decimalDataWithNulls = spark.sparkContext.parallelize(
//      DecimalData(1, 1) ::
//        DecimalData(1, null) ::
//        DecimalData(2, 1) ::
//        DecimalData(2, null) ::
//        DecimalData(3, 1) ::
//        DecimalData(3, 2) ::
//        DecimalData(null, 2) :: Nil).toDF()
//    checkAnswer(
//      decimalDataWithNulls.groupBy("a").agg(sum("b")),
//      Seq(Row(new java.math.BigDecimal(1), new java.math.BigDecimal(1)),
//        Row(new java.math.BigDecimal(2), new java.math.BigDecimal(1)),
//        Row(new java.math.BigDecimal(3), new java.math.BigDecimal(3)),
//        Row(null, new java.math.BigDecimal(2)))
//    )
  }

  testGluten("average") {

    checkAnswer(testData2.agg(avg($"a"), mean($"a")), Row(2.0, 2.0))

    checkAnswer(
      testData2.agg(avg($"a"), sum_distinct($"a")), // non-partial and test deprecated version
      Row(2.0, 6.0) :: Nil)

    // [wishlist] does not support decimal
//    checkAnswer(
//      decimalData.agg(avg($"a")),
//      Row(new java.math.BigDecimal(2)))
//
//    checkAnswer(
//      decimalData.agg(avg($"a"), sum_distinct($"a")), // non-partial
//      Row(new java.math.BigDecimal(2), new java.math.BigDecimal(6)) :: Nil)
//
//    checkAnswer(
//      decimalData.agg(avg($"a" cast DecimalType(10, 2))),
//      Row(new java.math.BigDecimal(2)))
//    // non-partial
//    checkAnswer(
//      decimalData.agg(
//        avg($"a" cast DecimalType(10, 2)), sum_distinct($"a" cast DecimalType(10, 2))),
//      Row(new java.math.BigDecimal(2), new java.math.BigDecimal(6)) :: Nil)
  }

  ignoreGluten("SPARK-32038: NormalizeFloatingNumbers should work on distinct aggregate") {
    withTempView("view") {
      Seq(
        ("mithunr", Float.NaN),
        ("mithunr", Float.NaN),
        ("mithunr", Float.NaN),
        ("abellina", 1.0f),
        ("abellina", 2.0f)).toDF("uid", "score").createOrReplaceTempView("view")

      val df = spark.sql("select uid, count(distinct score) from view group by 1 order by 1 asc")
      checkAnswer(df, Row("abellina", 2) :: Row("mithunr", 1) :: Nil)
    }
  }

  testGluten("variance") {
    checkAnswer(
      testData2.agg(var_samp($"a"), var_pop($"a"), variance($"a")),
      Row(0.8, 2.0 / 3.0, 0.8))
    checkAnswer(testData2.agg(var_samp("a"), var_pop("a"), variance("a")), Row(0.8, 2.0 / 3.0, 0.8))
  }

  testGluten("aggregation with filter") {
    Seq(
      ("mithunr", 12.3f, 5.0f, true, 9.4f),
      ("mithunr", 15.5f, 4.0f, false, 19.9f),
      ("mithunr", 19.8f, 3.0f, false, 35.6f),
      ("abellina", 20.1f, 2.0f, true, 98.0f),
      ("abellina", 20.1f, 1.0f, true, 0.5f),
      ("abellina", 23.6f, 2.0f, true, 3.9f)
    )
      .toDF("uid", "time", "score", "pass", "rate")
      .createOrReplaceTempView("view")
    var df = spark.sql("select count(score) filter (where pass) from view group by time")
    checkAnswer(df, Row(1) :: Row(0) :: Row(0) :: Row(2) :: Row(1) :: Nil)

    df = spark.sql("select count(score) filter (where pass) from view")
    checkAnswer(df, Row(4) :: Nil)

    df = spark.sql("select count(score) filter (where rate > 20) from view group by time")
    checkAnswer(df, Row(0) :: Row(0) :: Row(1) :: Row(1) :: Row(0) :: Nil)

    df = spark.sql("select count(score) filter (where rate > 20) from view")
    checkAnswer(df, Row(2) :: Nil)
  }

  testGluten("extend with cast expression") {
    checkAnswer(
      decimalData.agg(
        sum($"a".cast("double")),
        avg($"b".cast("double")),
        count_distinct($"a"),
        count_distinct($"b")),
      Row(12.0, 1.5, 3, 2))
  }

  // This test is applicable to velox backend. For CH backend, the replacement is disabled.
  testGluten("use gluten hash agg to replace vanilla spark sort agg") {
    withTempView("t1") {
      withSQLConf((GlutenConfig.COLUMNAR_FORCE_HASHAGG_ENABLED.key, "false")) {
        Seq("A", "B", "C", "D").toDF("col1").createOrReplaceTempView("t1")
        // SortAggregateExec is expected to be used for string type input.
        val df = spark.sql("select max(col1) from t1")
        checkAnswer(df, Row("D") :: Nil)
        assert(find(df.queryExecution.executedPlan)(_.isInstanceOf[SortAggregateExec]).isDefined)
      }

      withSQLConf((GlutenConfig.COLUMNAR_FORCE_HASHAGG_ENABLED.key, "true")) {
        Seq("A", "B", "C", "D").toDF("col1").createOrReplaceTempView("t1")
        val df = spark.sql("select max(col1) from t1")
        checkAnswer(df, Row("D") :: Nil)
        // Sort agg is expected to be replaced by gluten's hash agg.
        assert(
          find(df.queryExecution.executedPlan)(
            _.isInstanceOf[HashAggregateExecBaseTransformer]).isDefined)
      }
    }
  }

  testGluten("mixed supported and unsupported aggregate functions") {
    withUserDefinedFunction(("udaf_sum", true)) {
      spark.udf.register(
        "udaf_sum",
        udaf(new Aggregator[JLong, JLong, JLong] {
          override def zero: JLong = 0
          override def reduce(b: JLong, a: JLong): JLong = a + b
          override def merge(b1: JLong, b2: JLong): JLong = b1 + b2
          override def finish(reduction: JLong): JLong = reduction
          override def bufferEncoder: Encoder[JLong] = Encoders.LONG
          override def outputEncoder: Encoder[JLong] = Encoders.LONG
        })
      )

      val df = spark.sql("SELECT a, udaf_sum(b), max(b) FROM testData2 group by a")
      checkAnswer(df, Row(1, 3, 2) :: Row(2, 3, 2) :: Row(3, 3, 2) :: Nil)
    }
  }

  // Ported from spark DataFrameAggregateSuite only with plan check changed.
  private def assertNoExceptions(c: Column): Unit = {
    for (
      (wholeStage, useObjectHashAgg) <-
        Seq((true, true), (true, false), (false, true), (false, false))
    ) {
      withSQLConf(
        (SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, wholeStage.toString),
        (SQLConf.USE_OBJECT_HASH_AGG.key, useObjectHashAgg.toString)) {

        val df = Seq(("1", 1), ("1", 2), ("2", 3), ("2", 4)).toDF("x", "y")

        // test case for HashAggregate
        val hashAggDF = df.groupBy("x").agg(c, sum("y"))
        hashAggDF.collect()
        val hashAggPlan = hashAggDF.queryExecution.executedPlan
        if (wholeStage) {
          assert(find(hashAggPlan) {
            case WholeStageCodegenExec(_: HashAggregateExec) => true
            // If offloaded, spark whole stage codegen takes no effect and a gluten hash agg is
            // expected to be used.
            case _: HashAggregateExecBaseTransformer => true
            case _ => false
          }.isDefined)
        } else {
          assert(
            stripAQEPlan(hashAggPlan).isInstanceOf[HashAggregateExec] ||
              stripAQEPlan(hashAggPlan).find {
                case _: HashAggregateExecBaseTransformer => true
                case _ => false
              }.isDefined)
        }

        // test case for ObjectHashAggregate and SortAggregate
        val objHashAggOrSortAggDF = df.groupBy("x").agg(c, collect_list("y"))
        objHashAggOrSortAggDF.collect()
        assert(stripAQEPlan(objHashAggOrSortAggDF.queryExecution.executedPlan).find {
          case _: HashAggregateExecBaseTransformer => true
          case _ => false
        }.isDefined)
      }
    }
  }

  testGluten(
    "SPARK-19471: AggregationIterator does not initialize the generated" +
      " result projection before using it") {
    Seq(
      monotonically_increasing_id(),
      spark_partition_id(),
      rand(Random.nextLong()),
      randn(Random.nextLong())
    ).foreach(assertNoExceptions)
  }

  testGluten("SPARK-22223: ObjectHashAggregate should not introduce unnecessary shuffle") {
    withSQLConf(SQLConf.USE_OBJECT_HASH_AGG.key -> "true") {
      val df = Seq(("1", "2", 1), ("1", "2", 2), ("2", "3", 3), ("2", "3", 4))
        .toDF("a", "b", "c")
        .repartition(col("a"))

      val objHashAggDF = df
        .withColumn("d", expr("(a, b, c)"))
        .groupBy("a", "b")
        .agg(collect_list("d").as("e"))
        .withColumn("f", expr("(b, e)"))
        .groupBy("a")
        .agg(collect_list("f").as("g"))
      val aggPlan = objHashAggDF.queryExecution.executedPlan

      val sortAggPlans = collect(aggPlan) { case sortAgg: SortAggregateExec => sortAgg }
      // SortAggregate will be retained due velox_collect_list
      assert(sortAggPlans.size == 4)

      val objHashAggPlans = collect(aggPlan) {
        case objHashAgg: ObjectHashAggregateExec => objHashAgg
      }
      assert(objHashAggPlans.isEmpty)

      val exchangePlans = collect(aggPlan) { case shuffle: ShuffleExchangeExec => shuffle }
      assert(exchangePlans.length == 1)
    }
  }

  Seq(true, false).foreach {
    value =>
      testGluten(s"SPARK-31620: agg with subquery (whole-stage-codegen = $value)") {
        withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> value.toString) {
          withTempView("t1", "t2") {
            sql("create temporary view t1 as select * from values (1, 2) as t1(a, b)")
            sql("create temporary view t2 as select * from values (3, 4) as t2(c, d)")

            // test without grouping keys
            checkAnswer(
              sql("select sum(if(c > (select a from t1), d, 0)) as csum from t2"),
              Row(4) :: Nil)

            // test with grouping keys
            checkAnswer(
              sql(
                "select c, sum(if(c > (select a from t1), d, 0)) as csum from " +
                  "t2 group by c"),
              Row(3, 4) :: Nil)

            // test with distinct
            checkAnswer(
              sql(
                "select avg(distinct(d)), sum(distinct(if(c > (select a from t1)," +
                  " d, 0))) as csum from t2 group by c"),
              Row(4, 4) :: Nil)

            // test subquery with agg
            checkAnswer(
              sql(
                "select sum(distinct(if(c > (select sum(distinct(a)) from t1)," +
                  " d, 0))) as csum from t2 group by c"),
              Row(4) :: Nil)

            // test SortAggregateExec
            // Converts to HashAggregateExecTransformer
            var df = sql("select max(if(c > (select a from t1), 'str1', 'str2')) as csum from t2")
            df.collect()
            assert(
              find(df.queryExecution.executedPlan)(
                _.isInstanceOf[HashAggregateExecTransformer]).isDefined)
            checkAnswer(df, Row("str1") :: Nil)

            // test ObjectHashAggregateExec
            // Converts to HashAggregateExecTransformer
            df =
              sql("select collect_list(d), sum(if(c > (select a from t1), d, 0)) as csum from t2")
            df.collect()
            assert(
              find(df.queryExecution.executedPlan)(
                _.isInstanceOf[HashAggregateExecTransformer]).isDefined)
            checkAnswer(df, Row(Array(4), 4) :: Nil)
          }
        }
      }
  }
}

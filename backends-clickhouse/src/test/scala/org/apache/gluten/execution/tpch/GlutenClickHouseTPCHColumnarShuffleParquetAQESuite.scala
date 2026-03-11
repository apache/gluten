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
package org.apache.gluten.execution.tpch

import org.apache.gluten.backendsapi.clickhouse.CHConfig
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution._
import org.apache.gluten.utils.Arm

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.execution.InputIteratorTransformer
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec

import java.io.File

import scala.io.Source

class GlutenClickHouseTPCHColumnarShuffleParquetAQESuite extends ParquetTPCHSuite {

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    import org.apache.gluten.backendsapi.clickhouse.CHConfig._

    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.gluten.sql.columnar.backend.ch.shuffle.hash.algorithm", "sparkMurmurHash3_32")
      .setCHConfig("enable_streaming_aggregating", true)
      .set(CHConfig.COLUMNAR_CH_SHUFFLE_SPILL_THRESHOLD.key, (1024 * 1024).toString)
  }

  final override val testCases: Seq[Int] = Seq(
    4, 6, 10, 12, 13, 16, 19, 20
  )

  final override val testCasesWithConfig: Map[Int, Seq[(String, String)]] =
    Map(
      7 -> Seq(
        ("spark.sql.shuffle.partitions", "2"),
        ("spark.sql.autoBroadcastJoinThreshold", "-1")),
      8 -> Seq(
        ("spark.sql.shuffle.partitions", "1"),
        ("spark.sql.autoBroadcastJoinThreshold", "-1")),
      14 -> Seq(
        ("spark.sql.shuffle.partitions", "1"),
        ("spark.sql.autoBroadcastJoinThreshold", "-1")),
      17 -> Seq(("spark.shuffle.sort.bypassMergeThreshold", "2")),
      18 -> Seq(("spark.shuffle.sort.bypassMergeThreshold", "2"))
    )
  setupTestCase()

  test("TPCH Q1") {
    customCheck(1) {
      df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
          case hashAggExec: HashAggregateExecBaseTransformer => hashAggExec
          case inputIteratorTransformer: InputIteratorTransformer => inputIteratorTransformer
        }
        assert(plans.size == 5)

        assert(plans(4).metrics("numFiles").value === 1)
        assert(plans(4).metrics("pruningTime").value === pruningTimeValueSpark)
        assert(plans(4).metrics("filesSize").value === 19230111)
        assert(plans(4).metrics("numOutputRows").value === 600572)

        assert(plans(3).metrics("numInputRows").value === 591673)
        assert(plans(3).metrics("numOutputRows").value === 4)
        assert(plans(3).metrics("outputVectors").value === 1)

        assert(plans(2).metrics("numInputRows").value === 8)
        assert(plans(2).metrics("numOutputRows").value === 8)

        // Execute Sort operator, it will read the data twice.
        assert(plans(1).metrics("numOutputRows").value === 8)
        assert(plans(1).metrics("outputVectors").value === 2)

        assert(plans.head.metrics("numInputRows").value === 4)
        assert(plans.head.metrics("numOutputRows").value === 4)
    }
  }

  test("Check the metrics values") {
    withSQLConf((GlutenConfig.COLUMNAR_SORT_ENABLED.key, "false")) {
      customCheck(1, native = false) {
        df =>
          assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
          val plans = collect(df.queryExecution.executedPlan) {
            case scanExec: BasicScanExecTransformer => scanExec
            case hashAggExec: HashAggregateExecBaseTransformer => hashAggExec
          }
          assert(plans.size == 3)

          assert(plans(2).metrics("numFiles").value === 1)
          assert(plans(2).metrics("pruningTime").value === pruningTimeValueSpark)
          assert(plans(2).metrics("filesSize").value === 19230111)

          assert(plans(1).metrics("numInputRows").value === 591673)
          assert(plans(1).metrics("numOutputRows").value === 4)
          assert(plans(1).metrics("outputVectors").value === 1)

          // Execute Sort operator, it will read the data twice.
          assert(plans.head.metrics("numOutputRows").value === 8)
          assert(plans.head.metrics("outputVectors").value === 2)
      }
    }
  }

  // TODO: there is a bug when using timestamp type as the partition column
  ignore("test timestamp as partition column") {
    spark.sql("""
                |create table part_by_timestamp (
                |  a int,
                |  b timestamp,
                |  c string,
                |  p timestamp
                |) using parquet
                |partitioned by (p);
                |""".stripMargin)

    // Insert some test rows.
    spark.sql("""
                |insert into table part_by_timestamp
                |values
                |(1, TIMESTAMP '2022-01-01 00:01:20', '2022-01-01 00:01:20',
                |TIMESTAMP '2022-01-01 00:01:20');
                |""".stripMargin)
    compareResultsAgainstVanillaSpark(
      "select a, b, to_timestamp(c), p from part_by_timestamp",
      customCheck = { _ => })
  }

  test("TPCH Q2") {
    customCheck(2) {
      df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
        val scanExec = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
        }
        assert(scanExec.size == 8)
    }
  }

  test("TPCH Q3") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      customCheck(3) {
        df =>
          assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
          val shjBuildLeft = collect(df.queryExecution.executedPlan) {
            case shj: ShuffledHashJoinExecTransformerBase if shj.joinBuildSide == BuildLeft => shj
          }
          assert(shjBuildLeft.size == 2)

          val inputIteratorTransformers = collect(df.queryExecution.executedPlan) {
            case inputIteratorTransformer: InputIteratorTransformer => inputIteratorTransformer
          }

          assert(inputIteratorTransformers.size == 4)

          assert(inputIteratorTransformers(3).metrics("numInputRows").value === 324322)
          assert(inputIteratorTransformers(3).metrics("numOutputRows").value === 324322)

          assert(inputIteratorTransformers(2).metrics("numInputRows").value === 72678)
          assert(inputIteratorTransformers(2).metrics("numOutputRows").value === 72678)

          assert(inputIteratorTransformers(1).metrics("numInputRows").value === 3111)
          assert(inputIteratorTransformers(1).metrics("numOutputRows").value === 3111)

          assert(inputIteratorTransformers.head.metrics("numInputRows").value === 15224)
          assert(inputIteratorTransformers.head.metrics("numOutputRows").value === 15224)
      }
    }
  }

  test("TPCH Q5") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      customCheck(5) {
        df =>
          assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
          val bhjRes = collect(df.queryExecution.executedPlan) {
            case bhj: BroadcastHashJoinExecTransformerBase => bhj
          }
          assert(bhjRes.isEmpty)
      }
    }
  }

  /**
   * TODO: With Spark 3.3, it can not support to use Spark Shuffle Manager and set
   * shuffle.partitions=1 at the same time, because OptimizeOneRowPlan rule will remove Sort
   * operator.
   */
  test("TPCH Q7 - with shuffle.partitions=1") {
    withSQLConf(
      ("spark.sql.shuffle.partitions", "1"),
      ("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      check(7)
    }
  }

  test("TPCH Q9") {
    check(9, compare = false)
  }

  test("TPCH Q11") {
    customCheck(11, compare = false) {
      df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
        val adaptiveSparkPlanExec = collectWithSubqueries(df.queryExecution.executedPlan) {
          case adaptive: AdaptiveSparkPlanExec => adaptive
        }
        assert(adaptiveSparkPlanExec.size == 2)
    }
  }

  test("TPCH Q15") {
    customCheck(15) {
      df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
        val adaptiveSparkPlanExec = collectWithSubqueries(df.queryExecution.executedPlan) {
          case adaptive: AdaptiveSparkPlanExec => adaptive
        }
        assert(adaptiveSparkPlanExec.size == 2)
    }
  }
  test("TPCH Q21") {
    customCheck(21) {
      df =>
        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
          case filterExec: FilterExecTransformerBase => filterExec
        }
        assert(plans(2).metrics("numInputRows").value === 600572)
        assert(plans(2).metrics("numOutputRows").value === 379809)

        assert(plans(3).metrics("numOutputRows").value === 600572)
    }
  }

  test("TPCH Q22") {
    customCheck(22) {
      df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
        val adaptiveSparkPlanExec = collectWithSubqueries(df.queryExecution.executedPlan) {
          case adaptive: AdaptiveSparkPlanExec => adaptive
        }
        assert(adaptiveSparkPlanExec.size == 2)
    }
  }

  test("Test 'spark.gluten.enabled' false") {
    withSQLConf((GlutenConfig.GLUTEN_ENABLED.key, "false")) {
      customCheck(2, native = false) {
        df =>
          val glutenPlans = collect(df.queryExecution.executedPlan) {
            case glutenPlan: GlutenPlan => glutenPlan
          }
          assert(glutenPlans.isEmpty)
      }
    }
  }

  test("collect_set") {
    val sql =
      """
        |select a, b from (
        |select n_regionkey as a, collect_set(if(n_regionkey=0, n_name, null))
        | as set from nation group by n_regionkey)
        |lateral view explode(set) as b
        |order by a, b
        |""".stripMargin
    runQueryAndCompare(sql)(checkGlutenPlan[CHHashAggregateExecTransformer])
  }

  test("test 'aggregate function collect_list'") {
    val df = runQueryAndCompare(
      "select l_orderkey,from_unixtime(l_orderkey, 'yyyy-MM-dd HH:mm:ss') " +
        "from lineitem order by l_orderkey desc limit 10"
    )(checkGlutenPlan[ProjectExecTransformer])
    checkLengthAndPlan(df, 10)
  }

  test("test max string") {
    withSQLConf(("spark.gluten.sql.columnar.force.hashagg", "true")) {
      val sql =
        """
          |SELECT
          |    l_returnflag,
          |    l_linestatus,
          |    max(l_comment)
          |FROM
          |    lineitem
          |WHERE
          |    l_shipdate <= date'1998-09-02' - interval 1 day
          |GROUP BY
          |    l_returnflag,
          |    l_linestatus
          |""".stripMargin
      runQueryAndCompare(sql) { df => }
    }
  }

  ignore("https://github.com/apache/gluten/issues/7726") {
    runQueryAndCompare(Arm.withResource(
      Source.fromFile(new File(s"$queryPath/tpch-schema-related/7726.sql"), "UTF-8"))(_.mkString)) {
      df =>
    }
  }
}

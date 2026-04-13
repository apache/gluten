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
package org.apache.gluten.execution

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}
import org.apache.spark.sql.GlutenTestUtils
import org.apache.spark.sql.execution.{ColumnarInputAdapter, CommandResultExec, InputIteratorTransformer}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, BroadcastQueryStageExec}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeLike
import org.apache.spark.sql.internal.SQLConf

class VeloxMetricsSuite extends VeloxWholeStageTransformerSuite with AdaptiveSparkPlanHelper {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark
      .range(100)
      .selectExpr("id as c1", "id % 3 as c2")
      .write
      .format("parquet")
      .saveAsTable("metrics_t1")

    spark
      .range(200)
      .selectExpr("id as c1", "id % 3 as c2")
      .write
      .format("parquet")
      .saveAsTable("metrics_t2")
  }

  override protected def afterAll(): Unit = {
    spark.sql("drop table metrics_t1")
    spark.sql("drop table metrics_t2")

    super.afterAll()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
  }

  test("test sort merge join metrics") {
    withSQLConf(
      GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // without preproject
      runQueryAndCompare(
        "SELECT * FROM metrics_t1 join metrics_t2 on metrics_t1.c1 = metrics_t2.c1"
      ) {
        df =>
          val smj = find(df.queryExecution.executedPlan) {
            case _: SortMergeJoinExecTransformer => true
            case _ => false
          }
          assert(smj.isDefined)
          val metrics = smj.get.metrics
          assert(metrics("numOutputRows").value == 100)
          assert(metrics("numOutputVectors").value > 0)
          assert(metrics("numOutputBytes").value > 0)
      }

      // with preproject
      runQueryAndCompare(
        "SELECT * FROM metrics_t1 join metrics_t2 on metrics_t1.c1 + 1 = metrics_t2.c1 + 1"
      ) {
        df =>
          val smj = find(df.queryExecution.executedPlan) {
            case _: SortMergeJoinExecTransformer => true
            case _ => false
          }
          assert(smj.isDefined)
          val metrics = smj.get.metrics
          assert(metrics("numOutputRows").value == 100)
          assert(metrics("numOutputVectors").value > 0)
          assert(metrics("streamPreProjectionCpuCount").value > 0)
          assert(metrics("bufferPreProjectionCpuCount").value > 0)
      }
    }
  }

  test("test shuffle hash join metrics") {
    withSQLConf(
      GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      // without preproject
      runQueryAndCompare(
        "SELECT * FROM metrics_t1 join metrics_t2 on metrics_t1.c1 = metrics_t2.c1"
      ) {
        df =>
          val smj = find(df.queryExecution.executedPlan) {
            case _: ShuffledHashJoinExecTransformer => true
            case _ => false
          }
          assert(smj.isDefined)
          val metrics = smj.get.metrics
          assert(metrics("numOutputRows").value == 100)
          assert(metrics("numOutputVectors").value > 0)
          assert(metrics("numOutputBytes").value > 0)
      }

      // with preproject
      runQueryAndCompare(
        "SELECT * FROM metrics_t1 join metrics_t2 on metrics_t1.c1 + 1 = metrics_t2.c1 + 1"
      ) {
        df =>
          val smj = find(df.queryExecution.executedPlan) {
            case _: ShuffledHashJoinExecTransformer => true
            case _ => false
          }
          assert(smj.isDefined)
          val metrics = smj.get.metrics
          assert(metrics("numOutputRows").value == 100)
          assert(metrics("numOutputVectors").value > 0)
          assert(metrics("streamPreProjectionCpuCount").value > 0)
          assert(metrics("buildPreProjectionCpuCount").value > 0)
      }
    }
  }

  test("Generate metrics") {
    runQueryAndCompare("SELECT explode(array(c1, c2, 1)) FROM metrics_t1") {
      df =>
        val generate = find(df.queryExecution.executedPlan) {
          case _: GenerateExecTransformer => true
          case _ => false
        }
        assert(generate.isDefined)
        val metrics = generate.get.metrics
        assert(metrics("numOutputRows").value == 300)
        assert(metrics("numOutputVectors").value > 0)
        assert(metrics("numOutputBytes").value > 0)
    }

    runQueryAndCompare(
      "SELECT c1, col FROM metrics_t1 LATERAL VIEW explode(array(c1, c2)) t AS col") {
      df =>
        val scan = find(df.queryExecution.executedPlan) {
          case _: FileSourceScanExecTransformer => true
          case _ => false
        }
        assert(scan.isDefined)
        val scanMetrics = scan.get.metrics
        assert(scanMetrics("rawInputRows").value > 0)

        val generate = find(df.queryExecution.executedPlan) {
          case _: GenerateExecTransformer => true
          case _ => false
        }
        assert(generate.isDefined)
        val genMetrics = generate.get.metrics
        assert(genMetrics("numOutputRows").value == 2 * scanMetrics("rawInputRows").value)
        assert(genMetrics.contains("loadLazyVectorTime"))
    }
  }

  test("Metrics of window") {
    runQueryAndCompare("SELECT c1, c2, sum(c2) over (partition by c1) as s FROM metrics_t1") {
      df =>
        val window = find(df.queryExecution.executedPlan) {
          case _: WindowExecTransformer => true
          case _ => false
        }
        assert(window.isDefined)
        val metrics = window.get.metrics
        assert(metrics("numOutputRows").value == 100)
        assert(metrics("outputVectors").value == 2)
    }
  }

  test("Metrics of noop filter's children") {
    runQueryAndCompare("SELECT c1, c2 FROM metrics_t1 where c1 < 50") {
      df =>
        val scan = find(df.queryExecution.executedPlan) {
          case _: FileSourceScanExecTransformer => true
          case _ => false
        }
        assert(scan.isDefined)
        val metrics = scan.get.metrics
        assert(metrics("rawInputRows").value == 100)
        assert(metrics("outputVectors").value == 1)
    }
  }

  test("Write metrics") {
    if (SparkShimLoader.getSparkVersion.startsWith("3.4")) {
      withSQLConf((GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")) {
        runQueryAndCompare(
          "Insert into table metrics_t1 values(1 , 2)"
        ) {
          df =>
            val plan =
              df.queryExecution.executedPlan.asInstanceOf[CommandResultExec].commandPhysicalPlan
            val write = find(plan) {
              case _: WriteFilesExecTransformer => true
              case _ => false
            }
            assert(write.isDefined)
            val metrics = write.get.metrics
            assert(metrics("physicalWrittenBytes").value > 0)
            assert(metrics("writeIONanos").value > 0)
            assert(metrics("numWrittenFiles").value == 1)
        }
      }
    }
  }

  test("File scan task input metrics") {
    createTPCHNotNullTables()

    @volatile var inputRecords = 0L
    val partTableRecords = spark.sql("select * from part").count()
    val itemTableRecords = spark.sql("select * from lineitem").count()
    val inputMetricsListener = new SparkListener {
      override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
        inputRecords += stageCompleted.stageInfo.taskMetrics.inputMetrics.recordsRead
      }
    }

    GlutenTestUtils.withListener(spark.sparkContext, inputMetricsListener) {
      _ =>
        val df = spark.sql("""
                             |select /*+ BROADCAST(part) */ * from part join lineitem
                             |on l_partkey = p_partkey
                             |""".stripMargin)
        df.count()
    }

    assert(inputRecords == (partTableRecords + itemTableRecords))
  }

  test("Metrics for input iterator of broadcast exchange") {
    createTPCHNotNullTables()
    val partTableRecords = spark.sql("select * from part").count()

    // Repartition to make sure we have multiple tasks executing the join.
    spark
      .sql("select * from lineitem")
      .repartition(2)
      .createOrReplaceTempView("lineitem")

    Seq("true", "false").foreach {
      adaptiveEnabled =>
        withSQLConf("spark.sql.adaptive.enabled" -> adaptiveEnabled) {
          val sqlStr =
            """
              |select /*+ BROADCAST(part) */ * from part join lineitem
              |on l_partkey = p_partkey
              |""".stripMargin

          runQueryAndCompare(sqlStr) {
            df =>
              val inputIterator = find(df.queryExecution.executedPlan) {
                case InputIteratorTransformer(ColumnarInputAdapter(child)) =>
                  child.isInstanceOf[BroadcastQueryStageExec] || child
                    .isInstanceOf[BroadcastExchangeLike]
                case _ => false
              }
              assert(inputIterator.isDefined)
              val metrics = inputIterator.get.metrics
              assert(metrics("numOutputRows").value == partTableRecords)
          }
        }
    }
  }

  test("Velox cache metrics") {
    val df = spark.sql(s"SELECT * FROM metrics_t1")
    val scans = collect(df.queryExecution.executedPlan) {
      case scan: FileSourceScanExecTransformer => scan
    }
    df.collect()
    assert(scans.length === 1)
    val metrics = scans.head.metrics
    assert(metrics("storageReadBytes").value > 0)
    assert(metrics("ramReadBytes").value == 0)
  }

  test("Velox datasource metrics") {
    val df = spark.sql(s"SELECT * FROM metrics_t1")
    val scans = collect(df.queryExecution.executedPlan) {
      case scan: FileSourceScanExecTransformer => scan
    }
    df.collect()
    assert(scans.length === 1)
    val metrics = scans.head.metrics
    assert(metrics("dataSourceReadTime").value > 0)
    assert(metrics("dataSourceAddSplitTime").value > 0)
  }

  test("test nested loop join metrics") {
    withSQLConf() {
      runQueryAndCompare(
        "select /*+ BROADCAST(t2) */ c1, c2 from (select c1  from metrics_t1 where c1 = 50) t1 ," +
          "(select c2 from metrics_t2) t2;"
      ) {
        df =>
          val join = find(df.queryExecution.executedPlan) {
            case _: BroadcastNestedLoopJoinExecTransformer => true
            case _ => false
          }
          assert(join.isDefined)
          val metrics = join.get.metrics
          assert(metrics("nestedLoopJoinBuildInputRows").value == 200)
          assert(metrics("nestedLoopJoinProbeInputRows").value == 1)
          assert(metrics("numOutputRows").value == 200 * 1)
      }
    }
  }
}

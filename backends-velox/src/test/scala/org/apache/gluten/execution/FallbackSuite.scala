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

import org.apache.gluten.config.{GlutenConfig, VeloxConfig}

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.{ColumnarBroadcastExchangeExec, ColumnarShuffleExchangeExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, AQEShuffleReadExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.utils.GlutenSuiteUtils

class FallbackSuite extends VeloxWholeStageTransformerSuite with AdaptiveSparkPlanHelper {
  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      // The gluten ui event test suite expects the spark ui to be enabled.
      .set(GlutenConfig.GLUTEN_UI_ENABLED.key, "true")
      .set("spark.ui.enabled", "true")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark
      .range(100)
      .selectExpr("cast(id % 3 as int) as c1", "id as c2")
      .write
      .format("parquet")
      .saveAsTable("tmp1")
    spark
      .range(100)
      .selectExpr("cast(id % 9 as int) as c1")
      .write
      .format("parquet")
      .saveAsTable("tmp2")
    spark
      .range(100)
      .selectExpr("cast(id % 3 as int) as c1", "cast(id % 9 as int) as c2")
      .write
      .format("parquet")
      .saveAsTable("tmp3")
    // ORC files are written with DECIMAL(38, 18) (Hive's native storage precision).
    // tmp4/tmp5 declare DECIMAL(20, 0) pointing to the same ORC files,
    // so the reader must handle a precision/scale mismatch.
    spark
      .range(100)
      .selectExpr(
        "cast(id as decimal(38, 18)) as c1",
        "cast(id % 3 as int) as c2",
        "cast(id % 9 as timestamp) as c3")
      .write
      .format("orc")
      .saveAsTable("tmp4_wide")
    spark
      .range(100)
      .selectExpr(
        "cast(id as decimal(38, 18)) as c1",
        "cast(id % 3 as int) as c2",
        "cast(id % 5 as timestamp) as c3")
      .write
      .format("orc")
      .saveAsTable("tmp5_wide")
    val loc4 = spark
      .sql("DESCRIBE FORMATTED tmp4_wide")
      .filter("col_name = 'Location'")
      .select("data_type")
      .collect()(0)
      .getString(0)
    val loc5 = spark
      .sql("DESCRIBE FORMATTED tmp5_wide")
      .filter("col_name = 'Location'")
      .select("data_type")
      .collect()(0)
      .getString(0)
    spark.sql(
      s"CREATE TABLE tmp4 (c1 DECIMAL(20, 0), c2 INT, c3 TIMESTAMP) USING ORC LOCATION '$loc4'")
    spark.sql(
      s"CREATE TABLE tmp5 (c1 DECIMAL(20, 0), c2 INT, c3 TIMESTAMP) USING ORC LOCATION '$loc5'")
  }

  override protected def afterAll(): Unit = {
    spark.sql("drop table tmp1")
    spark.sql("drop table tmp2")
    spark.sql("drop table tmp3")
    spark.sql("drop table tmp4_wide")
    spark.sql("drop table tmp5_wide")
    spark.sql("drop table tmp4")
    spark.sql("drop table tmp5")

    super.afterAll()
  }

  private def collectColumnarToRow(plan: SparkPlan): Int = {
    collect(plan) { case v: VeloxColumnarToRowExec => v }.size
  }

  private def collectColumnarShuffleExchange(plan: SparkPlan): Int = {
    collect(plan) { case c: ColumnarShuffleExchangeExec => c }.size
  }

  private def collectShuffleExchange(plan: SparkPlan): Int = {
    collect(plan) { case c: ShuffleExchangeExec => c }.size
  }

  test("fallback with shuffle manager") {
    withSQLConf(GlutenConfig.COLUMNAR_SHUFFLE_ENABLED.key -> "false") {
      runQueryAndCompare("select c1, count(*) from tmp1 group by c1") {
        df =>
          val plan = df.queryExecution.executedPlan

          assert(collectColumnarShuffleExchange(plan) == 0)
          assert(collectShuffleExchange(plan) == 1)

          val wholeQueryColumnarToRow = collectColumnarToRow(plan)
          assert(wholeQueryColumnarToRow == 2)
      }
    }
  }

  test("fallback with collect") {
    withSQLConf(GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key -> "1") {
      runQueryAndCompare("SELECT count(*) FROM tmp1") {
        df =>
          val columnarToRow = collectColumnarToRow(df.queryExecution.executedPlan)
          assert(columnarToRow == 1)
      }
    }
  }

  test("offload BroadcastExchange and fall back BHJ") {
    withSQLConf(
      GlutenConfig.COLUMNAR_BROADCAST_JOIN_ENABLED.key -> "false"
    ) {
      runQueryAndCompare(
        """
          |SELECT java_method('java.lang.Integer', 'sum', tmp1.c1, tmp2.c1) FROM tmp1
          |LEFT JOIN tmp2 on tmp1.c1 = tmp2.c1 limit 10
          |""".stripMargin
      ) {
        df =>
          val plan = df.queryExecution.executedPlan
          val columnarBhj = find(plan) {
            case _: BroadcastHashJoinExecTransformerBase => true
            case _ => false
          }
          assert(columnarBhj.isEmpty)

          val vanillaBhj = find(plan) {
            case _: BroadcastHashJoinExec => true
            case _ => false
          }
          assert(vanillaBhj.isDefined)

          val columnarBroadcastExchange = find(plan) {
            case _: ColumnarBroadcastExchangeExec => true
            case _ => false
          }
          assert(columnarBroadcastExchange.isDefined)
      }
    }
  }

  test("fallback final aggregate of collect_list") {
    withSQLConf(
      GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key -> "1",
      GlutenConfig.COLUMNAR_FALLBACK_IGNORE_ROW_TO_COLUMNAR.key -> "false",
      GlutenConfig.EXPRESSION_BLACK_LIST.key -> "element_at"
    ) {
      runQueryAndCompare(
        "SELECT sum(ele) FROM (SELECT c1, element_at(collect_list(c2), 1) as ele FROM tmp3 " +
          "GROUP BY c1)") {
        df =>
          val columnarToRow = collectColumnarToRow(df.queryExecution.executedPlan)
          assert(columnarToRow == 1)
      }
    }
  }

  // Elements in velox_collect_set's output set may be in different order. This is a benign bug
  // until we can exactly align with vanilla Spark.
  ignore("fallback final aggregate of collect_set") {
    withSQLConf(
      GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key -> "1",
      GlutenConfig.COLUMNAR_FALLBACK_IGNORE_ROW_TO_COLUMNAR.key -> "false",
      GlutenConfig.EXPRESSION_BLACK_LIST.key -> "element_at"
    ) {
      runQueryAndCompare(
        "SELECT sum(ele) FROM (SELECT c1, element_at(collect_set(c2), 1) as ele FROM tmp3 " +
          "GROUP BY c1)") {
        df =>
          val columnarToRow = collectColumnarToRow(df.queryExecution.executedPlan)
          assert(columnarToRow == 1)
      }
    }
  }

  test("fallback with AQE read") {
    runQueryAndCompare(
      """
        |select java_method('java.lang.Integer', 'sum', c1, c1), * from (
        |select /*+ repartition */ cast(c1 as int) as c1 from tmp1
        |)
        |""".stripMargin
    ) {
      df =>
        val aqeRead = find(df.queryExecution.executedPlan) {
          case _: AQEShuffleReadExec => true
          case _ => false
        }
        assert(aqeRead.isDefined)
    }
  }

  test("Do not fallback eagerly with ColumnarToRowExec") {
    withSQLConf(GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key -> "1") {
      runQueryAndCompare("select count(*) from tmp1") {
        df =>
          assert(
            collect(df.queryExecution.executedPlan) {
              case h: HashAggregateExecTransformer => h
            }.size == 2,
            df.queryExecution.executedPlan)
      }

      runQueryAndCompare("select c1, count(*) from tmp1 group by c1") {
        df =>
          assert(
            collect(df.queryExecution.executedPlan) {
              case h: HashAggregateExecTransformer => h
            }.size == 2,
            df.queryExecution.executedPlan)
      }
    }
  }

  test("Prefer to use Gluten plan in fallback policy") {
    withSQLConf(GlutenConfig.COLUMNAR_QUERY_FALLBACK_THRESHOLD.key -> "1") {
      runQueryAndCompare("SELECT * FROM tmp1 WHERE c1 > 0") {
        df =>
          val plan = df.queryExecution.executedPlan
          assert(collect(plan) { case f: FileSourceScanExecTransformer => f }.size == 1)
          assert(collect(plan) { case f: FilterExecTransformer => f }.size == 1)
      }
    }
  }

  test("test ignore row to columnar") {
    Seq("true", "false").foreach {
      ignoreRowToColumnar =>
        withSQLConf(
          GlutenConfig.COLUMNAR_FALLBACK_IGNORE_ROW_TO_COLUMNAR.key -> ignoreRowToColumnar,
          GlutenConfig.EXPRESSION_BLACK_LIST.key -> "collect_set",
          GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key -> "1"
        ) {
          runQueryAndCompare("SELECT c1, collect_set(c2) FROM tmp1 GROUP BY c1") {
            df =>
              val plan = df.queryExecution.executedPlan
              // fallback if not ignore row to columnar
              assert(collect(plan) {
                case g: GlutenPlan => g
              }.nonEmpty == ignoreRowToColumnar.toBoolean)
          }
        }
    }
  }

  test("fallback with smj") {
    val sql = "SELECT /*+ SHUFFLE_MERGE(tmp1) */ * FROM tmp1 join tmp2 on tmp1.c1 = tmp2.c1"
    withSQLConf(
      GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_SHUFFLED_HASH_JOIN_ENABLED.key -> "false") {
      runQueryAndCompare(sql) {
        df =>
          val plan = df.queryExecution.executedPlan
          assert(collect(plan) { case smj: SortMergeJoinExec => smj }.size == 1)
      }
    }
    withSQLConf(
      GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false",
      GlutenConfig.COLUMNAR_SORTMERGEJOIN_ENABLED.key -> "false") {
      runQueryAndCompare(sql) {
        df =>
          val plan = df.queryExecution.executedPlan
          assert(collect(plan) { case smj: SortMergeJoinExec => smj }.size == 1)
      }
    }
  }

  testWithMinSparkVersion("fallback with index based schema evolution", "3.4") {
    val query = "SELECT c2 FROM test"
    Seq("parquet", "orc").foreach {
      format =>
        Seq("true", "false").foreach {
          parquetUseColumnNames =>
            Seq("true", "false").foreach {
              orcUseColumnNames =>
                withSQLConf(
                  VeloxConfig.PARQUET_USE_COLUMN_NAMES.key -> parquetUseColumnNames,
                  VeloxConfig.ORC_USE_COLUMN_NAMES.key -> orcUseColumnNames
                ) {
                  withTable("test") {
                    spark
                      .range(100)
                      .selectExpr("to_timestamp_ntz(from_unixtime(id % 3)) as c1", "id as c2")
                      .write
                      .format(format)
                      .saveAsTable("test")

                    runQueryAndCompare(query) {
                      df =>
                        val plan = df.queryExecution.executedPlan
                        assert(collect(plan) { case g: GlutenPlan => g }.nonEmpty)
                    }
                  }
                }
            }
        }
    }
  }

  test("fallback on spilt with unsupported regex") {
    runQueryAndCompare("SELECT split(cast(c1 as string), '(?<=\\\\}),(?=\\\\{)') from tmp1") {
      df =>
        val columnarToRow = collectColumnarToRow(df.queryExecution.executedPlan)
        assert(columnarToRow == 1)
    }
  }

  test("get correct fallback reason on nodes without logicalLink") {
    withSQLConf(GlutenConfig.COLUMNAR_SORT_ENABLED.key -> "false") {
      GlutenSuiteUtils.withFallbackEventListener(spark.sparkContext) {
        events =>
          val df = spark.sql("""
                               |SELECT
                               |  c1,
                               |  c2,
                               |  ROW_NUMBER() OVER (PARTITION BY c1 ORDER BY c2) as row_num,
                               |  RANK() OVER (PARTITION BY c1 ORDER BY c2) as rank_num
                               |FROM tmp1
                               |
                               |""".stripMargin)
          df.collect()
          GlutenSuiteUtils.waitUntilEmpty(spark.sparkContext)
          val sort = find(df.queryExecution.executedPlan) {
            _.isInstanceOf[SortExec]
          }
          assert(sort.isDefined)
          val fallbackReasons = events.flatMap(_.fallbackNodeToReason.values)
          assert(fallbackReasons.nonEmpty)
          assert(
            fallbackReasons.forall(
              _.contains("[FallbackByUserOptions] Validation failed on node Sort")))
      }
    }
  }

  test("fallback when nested loop join has unsupported expression") {
    GlutenSuiteUtils.withFallbackEventListener(spark.sparkContext) {
      events =>
        val df = spark.sql("""
                             |select tmp1.c1, tmp1.c2 from tmp1
                             |left join tmp2 on (
                             |  tmp1.c1 = regexp_extract(tmp2.c1, '(?<=@)[^.]+(?=\.)', 0)
                             |  or tmp2.c1 > 10
                             |)
                             |""".stripMargin)
        df.collect()
        GlutenSuiteUtils.waitUntilEmpty(spark.sparkContext)

        val nestedLoopJoin = find(df.queryExecution.executedPlan) {
          _.isInstanceOf[BroadcastNestedLoopJoinExec]
        }
        assert(nestedLoopJoin.isDefined)
        val fallbackReasons = events.flatMap(_.fallbackNodeToReason.values)
        assert(fallbackReasons.nonEmpty)
        assert(fallbackReasons.forall(_.contains("regexp_extract due to Pattern")))
    }
  }

  test("fallback when join post filter has unsupported expression") {
    GlutenSuiteUtils.withFallbackEventListener(spark.sparkContext) {
      events =>
        val df = spark.sql("""
                             |select tmp1.c1, tmp1.c2 from tmp1
                             |left join tmp2
                             |on tmp1.c1 = tmp2.c1
                             |and tmp1.c2 not rlike '^[\\u4e00-\\u9fa5]{2,10}[0-9]+$'
                             |""".stripMargin)
        df.collect()
        GlutenSuiteUtils.waitUntilEmpty(spark.sparkContext)

        val broadcastHashJoin = find(df.queryExecution.executedPlan) {
          _.isInstanceOf[BroadcastHashJoinExec]
        }
        assert(broadcastHashJoin.isDefined)
        val fallbackReasons = events.flatMap(_.fallbackNodeToReason.values)
        assert(fallbackReasons.nonEmpty)
        assert(fallbackReasons.forall(_.contains("rlike due to Pattern")))
    }
  }

  test("no fallback event emitted for vanilla Spark execution with gluten disabled") {
    // Regression test: before the fix, GlutenQueryExecutionListener would post a
    // GlutenPlanFallbackEvent even when spark.gluten.enabled=false (e.g. the vanilla baseline run
    // inside runQueryAndCompare). All nodes would appear as fallback with the generic reason
    // "Gluten does not touch it or does not support it".
    withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
      GlutenSuiteUtils.withFallbackEventListener(spark.sparkContext) {
        events =>
          // Execute a query with gluten disabled — this mimics what runQueryAndCompare does for
          // the vanilla baseline run. No GlutenPlanFallbackEvent should be emitted at all.
          spark.sql("SELECT c1, count(*) FROM tmp1 GROUP BY c1").collect()
          GlutenSuiteUtils.waitUntilEmpty(spark.sparkContext)
          assert(
            events.isEmpty,
            s"Expected no GlutenPlanFallbackEvent for vanilla Spark execution, " +
              s"but got ${events.size} event(s). " +
              s"First event fallback reasons: ${events.headOption.map(_.fallbackNodeToReason)}"
          )
      }
    }
  }

  test("For decimal-key joins, if one side falls back to Spark, force fallback the other side") {
    // ORC files are written with DECIMAL(38, 18) (Hive's native storage precision).
    // The metastore tables tmp4/tmp5 declare DECIMAL(20, 0) and point to the
    // same ORC files, so the reader must handle a precision/scale mismatch.
    // Selecting only c2 (INT) -> native FileSourceScanExecTransformer.
    // Selecting c3 (TIMESTAMP) in addition -> native validation fails ->
    //   vanilla FileSourceScanExec.

    // -- SortMergeJoin ------------------------------------------------------------------

    val sql1 = "SELECT /*+ MERGE(tmp4) */ tmp4.c2 AS 4c2, tmp4.c3 AS 4c3, " +
      "tmp5.c2 AS 5c2, tmp5.c3 AS 5c3 FROM tmp4 JOIN tmp5 ON tmp4.c1 = tmp5.c1"
    withSQLConf(
      GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false",
      GlutenConfig.COLUMNAR_SHUFFLED_HASH_JOIN_ENABLED.key -> "false") {
      checkAnswer(
        spark.sql(sql1),
        spark.sql(
          "SELECT tmp4_wide.c2 AS 4c2, tmp4_wide.c3 AS 4c3, " +
            "tmp5_wide.c2 AS 5c2, tmp5_wide.c3 AS 5c3 " +
            "FROM tmp4_wide JOIN tmp5_wide ON tmp4_wide.c1 = tmp5_wide.c1")
      )
    }

    val sql2 = "SELECT /*+ MERGE(tmp4) */ tmp4.c2 AS 4c2, tmp4.c3 AS 4c3, " +
      "tmp5.c2 AS 5c2 FROM tmp4 JOIN tmp5 ON tmp4.c1 = tmp5.c1"
    withSQLConf(
      GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false",
      GlutenConfig.COLUMNAR_SHUFFLED_HASH_JOIN_ENABLED.key -> "false") {
      checkAnswer(
        spark.sql(sql2),
        spark.sql(
          "SELECT tmp4_wide.c2 AS 4c2, tmp4_wide.c3 AS 4c3, " +
            "tmp5_wide.c2 AS 5c2 " +
            "FROM tmp4_wide JOIN tmp5_wide ON tmp4_wide.c1 = tmp5_wide.c1")
      )
    }

    val sql3 = "SELECT /*+ MERGE(tmp4) */ tmp4.c2 AS 4c2, " +
      "tmp5.c2 AS 5c2, tmp5.c3 AS 5c3 FROM tmp4 JOIN tmp5 ON tmp4.c1 = tmp5.c1"
    withSQLConf(
      GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false",
      GlutenConfig.COLUMNAR_SHUFFLED_HASH_JOIN_ENABLED.key -> "false") {
      checkAnswer(
        spark.sql(sql3),
        spark.sql(
          "SELECT tmp4_wide.c2 AS 4c2, " +
            "tmp5_wide.c2 AS 5c2, tmp5_wide.c3 AS 5c3 " +
            "FROM tmp4_wide JOIN tmp5_wide ON tmp4_wide.c1 = tmp5_wide.c1")
      )
    }

    // -- ShuffledHashJoin ---------------------------------------------------------------

    val sql4 = "SELECT /*+ SHUFFLE_HASH(tmp4) */ tmp4.c2 AS 4c2, tmp4.c3 AS 4c3, " +
      "tmp5.c2 AS 5c2, tmp5.c3 AS 5c3 FROM tmp4 JOIN tmp5 ON tmp4.c1 = tmp5.c1"
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      checkAnswer(
        spark.sql(sql4),
        spark.sql(
          "SELECT tmp4_wide.c2 AS 4c2, tmp4_wide.c3 AS 4c3, " +
            "tmp5_wide.c2 AS 5c2, tmp5_wide.c3 AS 5c3 " +
            "FROM tmp4_wide JOIN tmp5_wide ON tmp4_wide.c1 = tmp5_wide.c1")
      )
    }

    val sql5 = "SELECT /*+ SHUFFLE_HASH(tmp4) */ tmp4.c2 AS 4c2, tmp4.c3 AS 4c3, " +
      "tmp5.c2 AS 5c2 FROM tmp4 JOIN tmp5 ON tmp4.c1 = tmp5.c1"
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      checkAnswer(
        spark.sql(sql5),
        spark.sql(
          "SELECT tmp4_wide.c2 AS 4c2, tmp4_wide.c3 AS 4c3, " +
            "tmp5_wide.c2 AS 5c2 " +
            "FROM tmp4_wide JOIN tmp5_wide ON tmp4_wide.c1 = tmp5_wide.c1")
      )
    }

    val sql6 = "SELECT /*+ SHUFFLE_HASH(tmp4) */ tmp4.c2 AS 4c2, " +
      "tmp5.c2 AS 5c2, tmp5.c3 AS 5c3 FROM tmp4 JOIN tmp5 ON tmp4.c1 = tmp5.c1"
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      checkAnswer(
        spark.sql(sql6),
        spark.sql(
          "SELECT tmp4_wide.c2 AS 4c2, " +
            "tmp5_wide.c2 AS 5c2, tmp5_wide.c3 AS 5c3 " +
            "FROM tmp4_wide JOIN tmp5_wide ON tmp4_wide.c1 = tmp5_wide.c1")
      )
    }

    // -- BroadcastHashJoin --------------------------------------------------------------

    val sql7 = "SELECT tmp4.c2 AS 4c2, tmp4.c3 AS 4c3, " +
      "tmp5.c2 AS 5c2, tmp5.c3 AS 5c3 FROM tmp4 JOIN tmp5 ON tmp4.c1 = tmp5.c1"
    checkAnswer(
      spark.sql(sql7),
      spark.sql(
        "SELECT tmp4_wide.c2 AS 4c2, tmp4_wide.c3 AS 4c3, " +
          "tmp5_wide.c2 AS 5c2, tmp5_wide.c3 AS 5c3 " +
          "FROM tmp4_wide JOIN tmp5_wide ON tmp4_wide.c1 = tmp5_wide.c1")
    )

    val sql8 = "SELECT tmp4.c2 AS 4c2, tmp4.c3 AS 4c3, " +
      "tmp5.c2 AS 5c2 FROM tmp4 JOIN tmp5 ON tmp4.c1 = tmp5.c1"
    checkAnswer(
      spark.sql(sql8),
      spark.sql(
        "SELECT tmp4_wide.c2 AS 4c2, tmp4_wide.c3 AS 4c3, " +
          "tmp5_wide.c2 AS 5c2 " +
          "FROM tmp4_wide JOIN tmp5_wide ON tmp4_wide.c1 = tmp5_wide.c1")
    )

    val sql9 = "SELECT tmp4.c2 AS 4c2, " +
      "tmp5.c2 AS 5c2, tmp5.c3 AS 5c3 FROM tmp4 JOIN tmp5 ON tmp4.c1 = tmp5.c1"
    checkAnswer(
      spark.sql(sql9),
      spark.sql(
        "SELECT tmp4_wide.c2 AS 4c2, " +
          "tmp5_wide.c2 AS 5c2, tmp5_wide.c3 AS 5c3 " +
          "FROM tmp4_wide JOIN tmp5_wide ON tmp4_wide.c1 = tmp5_wide.c1")
    )
  }
}

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
package org.apache.spark.sql.execution

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{ShuffledHashJoinExecTransformerBase, SortMergeJoinExecTransformer}
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.{GlutenQueryTest, Row}
import org.apache.spark.sql.catalyst.plans.{FullOuter, LeftOuter, RightOuter}
import org.apache.spark.sql.test.SharedSparkSession

class GlutenSortMergeJoinStreamedSideSuite extends GlutenQueryTest with SharedSparkSession {
  private val optimizeKey =
    "spark.gluten.sql.columnar.backend.velox.sortMergeJoin.optimizeStreamedSide"

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
    .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
    .set("spark.memory.offHeap.enabled", "true")
    .set("spark.memory.offHeap.size", "1g")
    .set("spark.ui.enabled", "false")
    .set("spark.sql.adaptive.enabled", "false")
    .set("spark.sql.autoBroadcastJoinThreshold", "-1")
    .set("spark.sql.shuffle.partitions", "2")
    .set(GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key, "false")

  private def withInputs(f: => Unit): Unit = {
    assume(BackendTestUtils.isVeloxBackendLoaded())
    withTempPath {
      path =>
        withTempView("small_merge_input", "large_merge_input") {
          val small = new java.io.File(path, "small").getAbsolutePath
          val large = new java.io.File(path, "large").getAbsolutePath
          spark.range(4).selectExpr("IF(id = 3, NULL, id) as k", "id as v").write.parquet(small)
          spark
            .range(20)
            .selectExpr(
              "CASE WHEN id = 18 THEN NULL WHEN id = 19 THEN 4 ELSE id % 2 END as k",
              "id + 10 as v")
            .write
            .parquet(large)
          spark.read.parquet(small).createOrReplaceTempView("small_merge_input")
          spark.read.parquet(large).createOrReplaceTempView("large_merge_input")
          f
        }
    }
  }

  private val query =
    """
      |SELECT /*+ MERGE(s, l) */ s.k, s.v, l.v
      |FROM small_merge_input s JOIN large_merge_input l
      |ON s.k = l.k AND l.v < s.v + 15
      |ORDER BY l.v
      |""".stripMargin

  test("native merge join streams the larger input and preserves duplicate-key results") {
    withInputs {
      Seq(false, true).foreach {
        enabled =>
          withSQLConf(optimizeKey -> enabled.toString) {
            val df = spark.sql(query)
            val plan = df.queryExecution.executedPlan
            val joins = plan.collect { case smj: SortMergeJoinExecTransformer => smj }
            assert(joins.size == 1, plan.treeString)
            val smj = joins.head
            val expectedLeft = if (enabled) "l" else "s"
            val expectedRight = if (enabled) "s" else "l"
            assert(smj.streamedKeys.head.references.head.qualifier.last == expectedLeft)
            assert(smj.bufferedKeys.head.references.head.qualifier.last == expectedRight)
            checkAnswer(df, (10L until 16L).map(v => Row(v % 2, v % 2, v)))
          }
      }
    }
  }

  Seq(
    ("LEFT OUTER", LeftOuter),
    ("RIGHT OUTER", RightOuter),
    ("FULL OUTER", FullOuter)).foreach {
    case (sqlType, originalType) =>
      test(s"native $sqlType merge join preserves unmatched rows and nulls when swapped") {
        withInputs {
          val matched = (10L until 16L).map(v => Row(v % 2, v % 2, v % 2, v))
          val unmatchedLeft = Seq(Row(2L, 2L, null, null), Row(null, 3L, null, null))
          val unmatchedRight = (16L until 30L).map {
            v =>
              val key: java.lang.Long = if (v == 28) null else if (v == 29) 4L else v % 2
              Row(null, null, key, v)
          }
          val expected = (matched ++
            (if (originalType != RightOuter) unmatchedLeft else Nil) ++
            (if (originalType != LeftOuter) unmatchedRight else Nil)).sortBy {
            row =>
              (
                if (row.isNullAt(1)) Long.MinValue else row.getLong(1),
                if (row.isNullAt(3)) Long.MinValue else row.getLong(3))
          }
          Seq(false, true).foreach {
            enabled =>
              withSQLConf(optimizeKey -> enabled.toString) {
                val df = spark.sql(s"""
                                      |SELECT /*+ MERGE(s, l) */ s.k, s.v, l.k, l.v
                                      |FROM small_merge_input s $sqlType JOIN large_merge_input l
                                      |ON s.k = l.k AND l.v < s.v + 15
                                      |ORDER BY s.v, l.v
                                      |""".stripMargin)
                val plan = df.queryExecution.executedPlan
                val joins = plan.collect { case smj: SortMergeJoinExecTransformer => smj }
                assert(joins.size == 1, plan.treeString)
                val smj = joins.head
                assert(smj.joinType == originalType)
                assert(smj.streamedKeys.head.references.head.qualifier.last ==
                  (if (enabled) "l" else "s"))
                assert(smj.bufferedKeys.head.references.head.qualifier.last ==
                  (if (enabled) "s" else "l"))
                checkAnswer(df, expected)
              }
          }
        }
      }
  }

  test("native merge join swaps computed keys with the correct input metrics") {
    withInputs {
      withSQLConf(optimizeKey -> "true") {
        // Avoid the range exchange's sampling execution when asserting metrics.
        val df = spark.sql(query
          .replace("s.k = l.k", "s.k + 1 = l.k + 1")
          .replace("ORDER BY l.v", ""))
        val joins = df.queryExecution.executedPlan.collect {
          case smj: SortMergeJoinExecTransformer => smj
        }
        assert(joins.size == 1)
        assert(joins.head.switchChildren)
        // Collect once so the metrics describe one execution.
        assert(df.collect().toSeq.sortBy(_.getLong(2)) ==
          (10L until 16L).map(v => Row(v % 2, v % 2, v)))
        assert(joins.head.metrics("numOutputRows").value == 6)
      }
    }
  }

  test("native merge join swaps scan inputs without an exchange") {
    assume(BackendTestUtils.isVeloxBackendLoaded())
    withSQLConf(optimizeKey -> "true") {
      withTable("small_merge_bucketed", "large_merge_bucketed") {
        spark.range(2).selectExpr("id as k", "id as v")
          .write.bucketBy(1, "k").sortBy("k").saveAsTable("small_merge_bucketed")
        spark.range(12).selectExpr("id % 2 as k", "id + 10 as v")
          .write.bucketBy(1, "k").sortBy("k").saveAsTable("large_merge_bucketed")
        val df = spark.sql(query
          .replace("small_merge_input", "small_merge_bucketed")
          .replace("large_merge_input", "large_merge_bucketed"))
        val plan = df.queryExecution.executedPlan
        val joins = plan.collect { case smj: SortMergeJoinExecTransformer => smj }
        assert(joins.size == 1, plan.treeString)
        assert(joins.head.columnarInputRDDs.isEmpty, plan.treeString)
        assert(joins.head.streamedKeys.head.references.head.qualifier.last == "l")
        checkAnswer(df, (10L until 16L).map(v => Row(v % 2, v % 2, v)))
      }
    }
  }

  test("streamed side optimization preserves the forced native hash join path") {
    withInputs {
      withSQLConf(
        optimizeKey -> "true",
        GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "true") {
        val df = spark.sql(query)
        val plan = df.queryExecution.executedPlan
        assert(plan.collect { case _: ShuffledHashJoinExecTransformerBase => 1 }.size == 1)
        assert(plan.collect { case _: SortMergeJoinExecTransformer => 1 }.isEmpty)
        checkAnswer(df, (10L until 16L).map(v => Row(v % 2, v % 2, v)))
      }
    }
  }
}

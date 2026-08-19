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

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.SparkPlan

/**
 * Tests for [[GlutenStrideExecTransformer]] - the example Gluten-native custom operator.
 *
 * The operator keeps every N-th row within each input batch (indices 0, stride, 2*stride, ...). It
 * has no Spark logical-plan equivalent, so it cannot be exercised via SQL. Instead the tests
 * construct the physical plan node directly and drive execution via [[SparkPlan.executeCollect()]],
 * which triggers the full Velox pipeline.
 */
class GlutenStrideExecSuite extends VeloxWholeStageTransformerSuite {

  override protected val resourcePath: String = "N/A"
  override protected val fileFormat: String = "N/A"

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set("spark.memory.offHeap.size", "512m")
      // Single partition keeps tests deterministic - no per-partition counter reset.
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.default.parallelism", "1")

  // ---------------------------------------------------------------------------
  // Helper: wrap `child` in a GlutenStrideExecTransformer, execute it, and
  // return the collected Long values from the first (id) column.
  // ---------------------------------------------------------------------------
  private def strideIds(child: SparkPlan, stride: Long): Seq[Long] = {
    val strider = GlutenStrideExecTransformer(stride = stride, child = child)
    strider.executeCollect().map(_.getLong(0)).toSeq
  }

  // ---------------------------------------------------------------------------
  // Correctness tests
  // ---------------------------------------------------------------------------

  test("stride=1 returns all rows unchanged") {
    withTable("stride_t") {
      spark.range(0, 6).write.format("parquet").saveAsTable("stride_t")
      val child = spark.table("stride_t").queryExecution.executedPlan
      val result = strideIds(child, stride = 1L)
      assert(
        result.sorted == Seq(0L, 1L, 2L, 3L, 4L, 5L),
        s"stride=1 should return all 6 rows, got: $result")
    }
  }

  test("stride=2 returns every other row") {
    withTable("stride_t") {
      // Write exactly 5 rows so the stride-2 result is deterministic within one batch
      spark.range(0, 5).write.format("parquet").saveAsTable("stride_t")
      val child = spark.table("stride_t").orderBy("id").queryExecution.executedPlan
      val result = strideIds(child, stride = 2L)
      // Sorted input: 0,1,2,3,4 -> indices 0,2,4 -> values 0,2,4
      assert(
        result == Seq(0L, 2L, 4L),
        s"stride=2 over 5 rows should give [0,2,4], got: $result")
    }
  }

  test("stride=3 returns every third row") {
    withTable("stride_t") {
      // 9 rows sorted -> indices 0,3,6 -> values 0,3,6
      spark.range(0, 9).write.format("parquet").saveAsTable("stride_t")
      val child = spark.table("stride_t").orderBy("id").queryExecution.executedPlan
      val result = strideIds(child, stride = 3L)
      assert(
        result == Seq(0L, 3L, 6L),
        s"stride=3 over 9 rows should give [0,3,6], got: $result")
    }
  }

  test("stride > row count returns only the first row") {
    withTable("stride_t") {
      spark.range(0, 5).write.format("parquet").saveAsTable("stride_t")
      val child = spark.table("stride_t").orderBy("id").queryExecution.executedPlan
      val result = strideIds(child, stride = 100L)
      assert(
        result == Seq(0L),
        s"stride > row count should keep only row at index 0, got: $result")
    }
  }

  test("stride applied to single-row input returns that single row") {
    withTable("stride_t") {
      spark.range(42L, 43L).write.format("parquet").saveAsTable("stride_t")
      val child = spark.table("stride_t").queryExecution.executedPlan
      val result = strideIds(child, stride = 5L)
      assert(
        result == Seq(42L),
        s"single-row input should be returned unchanged, got: $result")
    }
  }

  test("stride applied to empty input returns empty result") {
    withTable("stride_t") {
      spark.range(0L, 0L).write.format("parquet").saveAsTable("stride_t")
      val child = spark.table("stride_t").queryExecution.executedPlan
      val result = strideIds(child, stride = 2L)
      assert(
        result.isEmpty,
        s"empty input should produce empty output, got: $result")
    }
  }

  // ---------------------------------------------------------------------------
  // Plan-structure test: GlutenStrideExecTransformer appears in the plan tree
  // ---------------------------------------------------------------------------

  test("GlutenStrideExecTransformer appears in the plan tree string") {
    withTable("stride_t") {
      spark.range(0, 4).write.format("parquet").saveAsTable("stride_t")
      val child = spark.table("stride_t").queryExecution.executedPlan
      val strider = GlutenStrideExecTransformer(stride = 2L, child = child)
      val planStr = strider.treeString
      assert(
        planStr.contains("GlutenStride"),
        s"Expected 'GlutenStride' in plan tree but got:\n$planStr")
    }
  }

  // ---------------------------------------------------------------------------
  // Chained plan: GlutenStrideExecTransformer wrapping an already-filtered scan
  // ---------------------------------------------------------------------------

  test("stride after filter produces correct subset") {
    withTable("stride_t") {
      // rows 0..9 -> filter keeps even rows: 0,2,4,6,8 -> stride=2 -> indices 0,2,4 -> values 0,4,8
      spark.range(0, 10).write.format("parquet").saveAsTable("stride_t")
      val filteredPlan = spark
        .table("stride_t")
        .filter("id % 2 = 0")
        .orderBy("id")
        .queryExecution
        .executedPlan
      val result = strideIds(filteredPlan, stride = 2L)
      // Even values sorted: 0,2,4,6,8 -> keep indices 0,2,4 -> 0,4,8
      assert(
        result == Seq(0L, 4L, 8L),
        s"stride=2 after even-filter mismatch: $result")
    }
  }

  // ---------------------------------------------------------------------------
  // Argument validation
  // ---------------------------------------------------------------------------

  test("stride=0 is rejected at construction time") {
    withTable("stride_t") {
      spark.range(0, 3).write.format("parquet").saveAsTable("stride_t")
      val child = spark.table("stride_t").queryExecution.executedPlan
      val ex = intercept[IllegalArgumentException] {
        GlutenStrideExecTransformer(stride = 0L, child = child)
      }
      assert(ex.getMessage.contains("stride"), s"Unexpected error message: ${ex.getMessage}")
    }
  }
}

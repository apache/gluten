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
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.execution.{ApplyResourceProfileExec, SparkPlan}
import org.apache.spark.sql.internal.SQLConf

/**
 * Tests for GlutenAutoAdjustStageResourceProfile when CPU/GPU hybrid execution is enabled.
 *
 * The SparkSession for this suite is created with ENABLE_HYBRID_EXECUTION=true (a static config),
 * so all tests run in that context.
 *
 * When hybridExecution is enabled together with cudf, the rule inserts an ApplyResourceProfileExec
 * node for stages whose WholeStageTransformer is fully cudf-tagged. In Spark testing mode the
 * profile is always the default one, but the wrapper node itself must be present in the plan tree.
 */
@Experimental
class HybridExecutionResourceProfileSuite extends VeloxWholeStageTransformerSuite {

  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  import testImplicits._

  private val tbl = "hybrid_rp_tbl"
  private val leftTable = "hybrid_rp_left"
  private val rightTable = "hybrid_rp_right"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.shuffle.partitions", "2")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.memory.offHeap.size", "2g")
      .set(GlutenConfig.AUTO_ADJUST_STAGE_RESOURCE_PROFILE_ENABLED.key, "true")
      .set(GlutenConfig.ENABLE_HYBRID_EXECUTION.key, "true")
      // Custom resource names to verify they are read correctly
      .set(GlutenConfig.HYBRID_EXECUTION_CPU_RESOURCE_NAME.key, "mycpu")
      .set(GlutenConfig.HYBRID_EXECUTION_GPU_RESOURCE_NAME.key, "mygpu")
      .set(GlutenConfig.HYBRID_EXECUTION_GPU_RESOURCE_AMOUNT_PER_TASK.key, "0.5")
      .set(VeloxConfig.CUDF_ENABLE_VALIDATION.key, "false")
      .set(VeloxConfig.CUDF_ENABLE_TABLE_SCAN.key, "false")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark.range(100).selectExpr("cast(id % 5 as int) as k", "id as v").write
      .mode("overwrite").format("parquet").saveAsTable(tbl)

    Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "val").write
      .mode("overwrite").format("parquet").saveAsTable(leftTable)

    Seq((1, "x"), (2, "y"), (4, "z")).toDF("id", "val").write
      .mode("overwrite").format("parquet").saveAsTable(rightTable)
  }

  override def afterAll(): Unit = {
    try {
      spark.sql(s"DROP TABLE IF EXISTS $tbl")
      spark.sql(s"DROP TABLE IF EXISTS $leftTable")
      spark.sql(s"DROP TABLE IF EXISTS $rightTable")
    } finally {
      super.afterAll()
    }
  }

  private def collectApplyResourceProfileExec(plan: SparkPlan): Seq[ApplyResourceProfileExec] = {
    collect(plan) { case a: ApplyResourceProfileExec => a }
  }

  /**
   * With hybridExecution=true and cudf enabled, GlutenAutoAdjustStageResourceProfile should insert
   * an ApplyResourceProfileExec for a fully-cudf stage (e.g. a simple aggregation). In testing mode
   * Spark always falls back to the default resource profile, but the wrapper node must still appear
   * in the executed plan.
   */
  test("GPU resource profile wrapper is inserted for a fully-cudf aggregate stage") {
    withSQLConf(
      GlutenConfig.COLUMNAR_CUDF_ENABLED.key -> "true",
      SQLConf.ANSI_ENABLED.key -> "false"
    ) {
      val df = sql(s"SELECT k, count(*) FROM $tbl GROUP BY k")
      df.collect()
      val plan = df.queryExecution.executedPlan
      val nodes = collectApplyResourceProfileExec(plan)
      assert(
        nodes.nonEmpty,
        "Expected ApplyResourceProfileExec to be inserted when hybrid execution is enabled " +
          "for a fully-cudf-offloaded stage")
    }

    withSQLConf(
      GlutenConfig.COLUMNAR_CUDF_ENABLED.key -> "true",
      GlutenConfig.GPU_ONLY_OFFLOAD_JOIN_STAGE.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.ANSI_ENABLED.key -> "false"
    ) {
      val df = sql(
        s"""
           |SELECT l.id, l.val, r.val
           |FROM $leftTable l
           |JOIN $rightTable r ON l.id = r.id
           |""".stripMargin)
      df.collect()
      val plan = df.queryExecution.executedPlan
      val nodes = collectApplyResourceProfileExec(plan)
      assert(
        nodes.nonEmpty,
        "Expected ApplyResourceProfileExec for a cudf join stage with hybrid execution enabled")
    }
  }
}

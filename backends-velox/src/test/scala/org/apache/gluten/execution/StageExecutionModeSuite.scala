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
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.ColumnarShuffleExchangeExec
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, ColumnarAQEShuffleReadExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf

class StageExecutionModeSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  import testImplicits._

  private val leftTable = "stage_mode_left"
  private val rightTable = "stage_mode_right"

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark.sql(s"DROP TABLE IF EXISTS $leftTable")
    spark.sql(s"DROP TABLE IF EXISTS $rightTable")

    Seq(
      (1, "left-1"),
      (2, "left-2"),
      (3, "left-3"))
      .toDF("id", "left_value")
      .write
      .mode("overwrite")
      .format("parquet")
      .saveAsTable(leftTable)

    Seq(
      (1, "right-1"),
      (2, "right-2"),
      (4, "right-4"))
      .toDF("id", "right_value")
      .write
      .mode("overwrite")
      .format("parquet")
      .saveAsTable(rightTable)
  }

  override def afterAll(): Unit = {
    try {
      spark.sql(s"DROP TABLE IF EXISTS $leftTable")
      spark.sql(s"DROP TABLE IF EXISTS $rightTable")
    } finally {
      super.afterAll()
    }
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.shuffle.partitions", "2")
      .set(VeloxConfig.CUDF_ENABLE_VALIDATION.key, "false")
      .set(VeloxConfig.CUDF_ENABLE_TABLE_SCAN.key, "false")
  }

  /**
   * Collects all ColumnarAQEShuffleReadExec nodes from the executed plan of `df`, irrespective of
   * plan nesting.
   */
  private def collectColumnarAQEReaders(
      df: org.apache.spark.sql.DataFrame): Seq[ColumnarAQEShuffleReadExec] = {
    val plan = getExecutedPlan(df)
    plan.collect { case r: ColumnarAQEShuffleReadExec => r }
  }

  test("CPU shuffle mapper and GPU shuffle reader with AQE") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      GlutenConfig.COLUMNAR_CUDF_ENABLED.key -> "true",
      SQLConf.ANSI_ENABLED.key -> "false"
    ) {
      val df = sql(
        s"""
           |SELECT l.id, l.left_value, r.right_value
           |FROM $leftTable l
           |JOIN $rightTable r
           |  ON l.id = r.id
           |""".stripMargin)

      checkAnswer(
        df,
        Seq(
          Row(1, "left-1", "right-1"),
          Row(2, "left-2", "right-2")))

      val readers = collectColumnarAQEReaders(df)
      assert(readers.nonEmpty, "Expected at least one ColumnarAQEShuffleReadExec")

      readers.foreach {
        reader =>
          val canonicalized = reader.canonicalized
          // canonicalized plan before applying query stage optimizer rules.
          assert(canonicalized.children.forall(_.isInstanceOf[ShuffleExchangeExec]))
          assert(
            reader.executionMode == MockGPUStageMode,
            s"Expected GPU AQE shuffle reader, but got ${reader.executionMode}")
      }

      val shuffleStages: Seq[ShuffleQueryStageExec] = readers.map(_.delegate).map {
        case a: AQEShuffleReadExec =>
          assert(a.child.isInstanceOf[ShuffleQueryStageExec])
          a.child.asInstanceOf[ShuffleQueryStageExec]
        case s: ShuffleQueryStageExec => s
        case _ =>
          throw new IllegalArgumentException("Unexpected child of ColumnarAQEShuffleReadExec")
      }

      shuffleStages.foreach {
        shuffleStage =>
          assert(shuffleStage.shuffle.isInstanceOf[ColumnarShuffleExchangeExec])
          val exchange = shuffleStage.shuffle.asInstanceOf[ColumnarShuffleExchangeExec]
          assert(
            !exchange.mapperStageMode.contains(MockGPUStageMode),
            s"Expected CPU mapper stage, but got ${exchange.mapperStageMode}")
      }
    }
  }

  test("with gpuOnlyOffloadJoinStage=true, join stage is offloaded to GPU") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      GlutenConfig.COLUMNAR_CUDF_ENABLED.key -> "true",
      GlutenConfig.GPU_ONLY_OFFLOAD_JOIN_STAGE.key -> "true",
      SQLConf.ANSI_ENABLED.key -> "false"
    ) {
      // A join query: the join stage should still be offloaded to GPU.
      val df = sql(
        s"""
           |SELECT l.id, l.left_value, r.right_value
           |FROM $leftTable l
           |JOIN $rightTable r
           |  ON l.id = r.id
           |""".stripMargin)

      checkAnswer(
        df,
        Seq(
          Row(1, "left-1", "right-1"),
          Row(2, "left-2", "right-2")))

      val readers = collectColumnarAQEReaders(df)
      assert(readers.nonEmpty, "Expected at least one ColumnarAQEShuffleReadExec for a join query")
      readers.foreach {
        reader =>
          assert(
            reader.executionMode == MockGPUStageMode,
            s"Join stage should be GPU-offloaded, but got ${reader.executionMode}")
      }
    }
  }

  test("with gpuOnlyOffloadJoinStage=true, non-join stage is NOT offloaded to GPU") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      GlutenConfig.COLUMNAR_CUDF_ENABLED.key -> "true",
      GlutenConfig.GPU_ONLY_OFFLOAD_JOIN_STAGE.key -> "true",
      SQLConf.ANSI_ENABLED.key -> "false"
    ) {
      // A pure aggregation query (no join): the stage must NOT be GPU-offloaded.
      val df = sql(s"SELECT id, count(*) FROM $leftTable GROUP BY id")
      df.collect()

      val readers = collectColumnarAQEReaders(df)
      assert(readers.isEmpty, "Expected no ColumnarAQEShuffleReadExec")
    }
  }
}

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

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.ColumnarShuffleExchangeExec
import org.apache.spark.sql.execution.adaptive.{ColumnarAQEShuffleReadExec, ShuffleQueryStageExec}
import org.apache.spark.sql.internal.SQLConf

class StageExecutionModeSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  import testImplicits._

  test("CPU shuffle mapper and GPU shuffle reader with AQE") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      GlutenConfig.COLUMNAR_CUDF_ENABLED.key -> "true",
      VeloxConfig.CUDF_ENABLE_VALIDATION.key -> "false",
      "spark.sql.shuffle.partitions" -> "2"
    ) {

      withTempView("cpu_scan_left", "cpu_scan_right") {
        Seq((1, "left-1"), (2, "left-2"), (3, "left-3"))
          .toDF("id", "left_value")
          .createOrReplaceTempView("cpu_scan_left")

        Seq((1, "right-1"), (2, "right-2"), (4, "right-4"))
          .toDF("id", "right_value")
          .createOrReplaceTempView("cpu_scan_right")

        val df = sql(
          """
            |SELECT l.id, l.left_value, r.right_value
            |FROM cpu_scan_left l
            |JOIN cpu_scan_right r
            |  ON l.id = r.id
            |""".stripMargin)

        checkAnswer(
          df,
          Seq(
            Row(1, "left-1", "right-1"),
            Row(2, "left-2", "right-2")))

        val plan = getExecutedPlan(df)

        val shuffleReaders = plan.collect {
          case reader: ColumnarAQEShuffleReadExec => reader
        }

        assert(shuffleReaders.nonEmpty)

        shuffleReaders.foreach {
          reader =>
            assert(
              reader.executionMode == MockGPUStageMode,
              s"Expected GPU AQE shuffle reader, but got ${reader.executionMode}")
        }

        val shuffleStages = plan.collect {
          case stage: ShuffleQueryStageExec => stage
        }

        val exchanges = shuffleStages.flatMap {
          _.plan.collect {
            case exchange: ColumnarShuffleExchangeExec => exchange
          }
        }

        assert(exchanges.nonEmpty)

        exchanges.foreach {
          exchange =>
            assert(
              exchange.mapperStageMode.contains(CPUStageMode),
              s"Expected CPU mapper stage, but got ${exchange.mapperStageMode}")
        }
      }
    }
  }
}

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

import org.apache.gluten.execution.{BroadcastHashJoinExecTransformerBase, FileSourceScanExecTransformerBase, HashAggregateExecBaseTransformer, ProjectExecTransformer}
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.sql.execution.ColumnarShuffleExchangeExec
import org.apache.spark.sql.functions.sum

/** Verifies that the Bolt Spark 4 test profile executes native operators. */
class GlutenBoltBackendSmokeSuite extends GlutenSQLTestsTrait {

  private def withParquetData(body: DataFrame => Unit): Unit = {
    assert(BackendTestUtils.isBoltBackendLoaded())
    withTempPath {
      path =>
        withSQLConf("spark.gluten.enabled" -> "false") {
          spark.range(8).selectExpr("id", "id % 2 AS k", "concat('row-', id) AS label")
            .write.parquet(path.getCanonicalPath)
        }
        body(spark.read.parquet(path.getCanonicalPath))
    }
  }

  for (adaptive <- Seq(false, true)) {
    test(s"Bolt native Parquet scan and expressions with AQE=$adaptive") {
      withSQLConf(
        "spark.sql.ansi.enabled" -> "false",
        "spark.sql.adaptive.enabled" -> adaptive.toString) {
        withParquetData {
          input =>
            val result = input.filter("id >= 6").selectExpr("id + 3 AS shifted", "upper(label)")
            checkAnswer(result, Seq(Row(9L, "ROW-6"), Row(10L, "ROW-7")))
            val plan = getExecutedPlan(result)
            assert(
              plan.exists(_.isInstanceOf[FileSourceScanExecTransformerBase]),
              result.queryExecution)
            assert(plan.exists(_.isInstanceOf[ProjectExecTransformer]), result.queryExecution)
        }
      }
    }

    test(s"Bolt native shuffle and aggregation with AQE=$adaptive") {
      withSQLConf(
        "spark.sql.ansi.enabled" -> "false",
        "spark.sql.adaptive.enabled" -> adaptive.toString) {
        withParquetData {
          input =>
            val result = input.repartition(2, input("k")).groupBy("k").agg(sum("id"))
            checkAnswer(result, Seq(Row(0L, 12L), Row(1L, 16L)))
            val plan = getExecutedPlan(result)
            assert(
              plan.exists(_.isInstanceOf[HashAggregateExecBaseTransformer]),
              result.queryExecution)
            assert(plan.exists(_.isInstanceOf[ColumnarShuffleExchangeExec]), result.queryExecution)
            assert(
              plan.exists(_.isInstanceOf[FileSourceScanExecTransformerBase]),
              result.queryExecution)
        }
      }
    }
  }

  test("Bolt native broadcast join") {
    withSQLConf("spark.sql.ansi.enabled" -> "false") {
      withParquetData {
        input =>
          val result = input.join(spark.range(2).selectExpr("id AS k"), "k")
            .groupBy("k").agg(sum("id"))
          checkAnswer(result, Seq(Row(0L, 12L), Row(1L, 16L)))
          assert(
            getExecutedPlan(result).exists(_.isInstanceOf[BroadcastHashJoinExecTransformerBase]),
            result.queryExecution)
      }
    }
  }
}

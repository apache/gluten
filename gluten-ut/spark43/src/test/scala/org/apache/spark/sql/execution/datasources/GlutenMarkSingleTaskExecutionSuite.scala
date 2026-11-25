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
package org.apache.spark.sql.execution.datasources

import org.apache.gluten.execution.FileSourceScanExecTransformer

import org.apache.spark.sql.{GlutenSQLTestsTrait, Row}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.internal.SQLConf

class GlutenMarkSingleTaskExecutionSuite
  extends MarkSingleTaskExecutionSuite
  with GlutenSQLTestsTrait {

  testGluten("bucketed scan does not run in a single task") {
    val bucketed = "single_task_bucketed"
    withTable(bucketed) {
      spark.range(0, 2).selectExpr("id as col").write.bucketBy(2, "col").saveAsTable(bucketed)
      withSQLConf(
        SQLConf.SINGLE_TASK_EXECUTION_ENABLED.key -> "true",
        SQLConf.SINGLE_TASK_EXECUTION_MIN_NUM_FILES.key -> "0",
        SQLConf.SINGLE_TASK_EXECUTION_MIN_NUM_BYTES.key -> "0",
        SQLConf.SINGLE_TASK_EXECUTION_LOCAL_TABLE_SCAN_MIN_ROWS.key -> "0",
        SQLConf.SINGLE_TASK_EXECUTION_MAX_NUM_FILES.key -> "4"
      ) {
        val df = sql(s"select col, count(1) as c from $bucketed group by col")
        checkAnswer(df, Seq(Row(0, 1), Row(1, 1)))
        // A scan that runs in a single task falls back to the vanilla FileSourceScanExec, so the
        // bucketed scan must be offloaded.
        val plan = df.queryExecution.executedPlan
        assert(collect(plan) { case s: FileSourceScanExecTransformer => s }.nonEmpty, plan)
        assert(collect(plan) { case s: FileSourceScanExec => s }.isEmpty, plan)
      }
    }
  }
}

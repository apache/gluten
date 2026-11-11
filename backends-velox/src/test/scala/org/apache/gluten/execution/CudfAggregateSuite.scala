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

import org.apache.gluten.tags.CudfTest

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.adaptive.ColumnarAQEShuffleReadExec

/** GPU regression coverage for Gluten-specific aggregate intermediate rows. */
@CudfTest
class CudfAggregateSuite extends VeloxWholeStageTransformerSuite {

  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.gluten.sql.columnar.cudf", "true")
      .set("spark.gluten.sql.columnar.backend.velox.cudf.allowCpuFallback", "false")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "4g")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()
  }

  test("cuDF supports row_constructor_with_null for average states") {
    // Three nullable AVG states mirror the row-construction shape in TPC-H q1
    // without introducing the separate decimal or legacy-cast paths.
    val query =
      """
        |SELECT
        |  l_returnflag,
        |  avg(CASE WHEN l_returnflag = 'R' THEN CAST(1 AS DOUBLE) END) AS avg_r,
        |  avg(CASE WHEN l_returnflag = 'A' THEN CAST(2 AS DOUBLE) END) AS avg_a,
        |  avg(CASE WHEN l_returnflag = 'N' THEN CAST(3 AS DOUBLE) END) AS avg_n
        |FROM lineitem
        |GROUP BY l_returnflag
        |""".stripMargin

    runQueryAndCompare(query) {
      df =>
        val plan = df.queryExecution.executedPlan
        val aggregates = collect(plan) { case aggregate: HashAggregateExecTransformer => aggregate }
        assert(aggregates.nonEmpty, s"expected an offloaded hash aggregate, got:\n$plan")

        val readers =
          getExecutedPlan(df).collect { case reader: ColumnarAQEShuffleReadExec => reader }
        assert(
          readers.exists(_.executionMode == MockGPUStageMode),
          s"expected a cuDF-tagged aggregate stage, got:\n$plan")
    }
  }
}

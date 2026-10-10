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
package org.apache.spark.sql.execution.joins

import org.apache.gluten.execution.BroadcastHashJoinExecTransformer

import org.apache.spark.SparkConf
import org.apache.spark.sql.GlutenSQLTestsTrait
import org.apache.spark.sql.execution.ColumnarBroadcastExchangeExec

class GlutenParallelBroadcastJoinSuite extends GlutenSQLTestsTrait {

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.adaptive.enabled", "false")
    .set("spark.gluten.sql.columnar.backend.velox.IOThreads", "0")
    .set("spark.gluten.velox.broadcast.build.targetBytesPerThread", "8KB")

  for (buildOnDriver <- Seq(false, true)) {
    testGluten(s"parallel broadcast hash table build with IO disabled, driver=$buildOnDriver") {
      withSQLConf(
        "spark.gluten.sql.columnar.backend.velox.driverSideBroadcastHashTableBuild" ->
          buildOnDriver.toString,
        "spark.gluten.velox.minTableRowsForParallelJoinBuild" -> "1"
      ) {
        withTable("parallel_broadcast_build") {
          spark.range(4096)
            .selectExpr("id", "sha2(cast(id as string), 256) AS value")
            .write.saveAsTable("parallel_broadcast_build")

          val result = spark.sql("""
                                   |SELECT /*+ BROADCAST(b) */ p.id, b.value
                                   |FROM parallel_broadcast_build p JOIN parallel_broadcast_build b
                                   |ON p.id = b.id
                                   |""".stripMargin)
          val expected = spark.table("parallel_broadcast_build").collect().toSeq
          checkAnswer(result, expected)

          val plan = result.queryExecution.executedPlan
          assert(plan.collect { case j: BroadcastHashJoinExecTransformer => j }.nonEmpty)
          val exchanges = plan.collect { case e: ColumnarBroadcastExchangeExec => e }
          assert(exchanges.size == 1)
          assert(exchanges.head.metrics("buildThreads").value > 1)
          if (buildOnDriver) {
            assert(exchanges.head.metrics("serializedHashTableSize").value > 0)
          }
        }
      }
    }
  }
}

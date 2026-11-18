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

import org.apache.gluten.GlutenBroadcastValueStreamTestSupport
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{BroadcastHashJoinExecTransformerBase, TakeOrderedAndProjectExecTransformer}

import org.apache.spark.sql.{GlutenSQLTestsTrait, Row}
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.task.TaskResources

class GlutenBroadcastValueStreamSuite
  extends GlutenSQLTestsTrait
  with GlutenBroadcastValueStreamTestSupport {
  import testImplicits._

  testGluten("broadcast expression join preserves batch schema") {
    withTempPath {
      dir =>
        val leftPath = s"${dir.getCanonicalPath}/left"
        val rightPath = s"${dir.getCanonicalPath}/right"
        Seq((1, 10), (2, 20), (3, 30), (4, 40), (2, 5))
          .toDF("id", "value")
          .repartition(2)
          .write
          .parquet(leftPath)
        Seq(0, 1, 2).toDF("id").write.parquet(rightPath)

        withTempView("top_n_left", "top_n_right") {
          spark.read.parquet(leftPath).createOrReplaceTempView("top_n_left")
          spark.read.parquet(rightPath).createOrReplaceTempView("top_n_right")

          for (cudfEnabled <- Seq(false, true); buildOnce <- Seq(true, false)) {
            withClue(s"cuDF=$cudfEnabled, buildOnce=$buildOnce: ") {
              withSQLConf(
                // Exercise CPU execution with both cuDF broadcast settings on CPU CI.
                SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
                SQLConf.SHUFFLE_PARTITIONS.key -> "2",
                SQLConf.FILES_OPEN_COST_IN_BYTES.key -> "134217728",
                GlutenConfig.COLUMNAR_CUDF_ENABLED.key -> cudfEnabled.toString,
                "spark.gluten.velox.buildHashTableOncePerExecutor.enabled" -> buildOnce.toString,
                "spark.gluten.velox.offHeapBroadcastBuildRelation.enabled" -> "false"
              ) {
                // Exercise both broadcast expression-key projection and the exchange before TopN.
                val df = sql("""
                               |SELECT /*+ BROADCAST(r) */ l.id, SUM(l.value) AS total
                               |FROM top_n_left l JOIN top_n_right r ON l.id = r.id + 1
                               |GROUP BY l.id
                               |ORDER BY total DESC, l.id
                               |LIMIT 2
                               |""".stripMargin)
                val plan = getExecutedPlan(df)
                assert(plan.exists(_.isInstanceOf[BroadcastHashJoinExecTransformerBase]))
                assert(plan.exists(_.isInstanceOf[TakeOrderedAndProjectExecTransformer]))
                checkAnswer(df, Seq(Row(3, 30L), Row(2, 25L)))

                val exchanges = plan.collect { case e: ColumnarBroadcastExchangeExec => e }
                assert(exchanges.size == 1)
                val exchange = exchanges.head
                val relation = exchange.doExecuteBroadcast[BuildSideRelation]().value
                val outputTypes = exchange.output.map(_.dataType)
                // Broadcast preprojection appends the expression key in either cuDF mode.
                val expectedColumns = outputTypes.size + (if (buildOnce) 1 else 0)

                TaskResources.runUnsafe {
                  // The iterator recycles loaded batches and closes its serializer when drained.
                  val batches = relation.asReadOnlyCopy().deserialized
                  var rows = 0
                  while (batches.hasNext) {
                    val batch = batches.next()
                    assert(batch.numCols() == expectedColumns)
                    val loaded = loadBroadcastBatch(batch)
                    val types = (0 until loaded.numCols()).map(i => loaded.column(i).dataType())
                    assert(types.take(outputTypes.size) == outputTypes)
                    rows += loaded.numRows()
                  }
                  assert(rows == 3)
                }
              }
            }
          }
        }
    }
  }
}

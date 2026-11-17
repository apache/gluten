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
package org.apache.spark.sql.kafka010

import org.apache.gluten.execution.{MicroBatchScanExecTransformer, ProjectExecTransformerBase, RowToColumnarExecBase}

import org.apache.spark.sql.GlutenSQLTestsTrait
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.execution.datasources.v2.MicroBatchScanExec

class GlutenKafkaHybridSourceSuite extends KafkaSourceTest with GlutenSQLTestsTrait {
  import testImplicits._

  testGluten("Kafka scan fallback preserves native projection and restores native scanning") {
    val scanEnabled = "spark.gluten.sql.columnar.kafkaScan"
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 2)
    val values = 0L until 12L
    testUtils.sendMessages(topic, values.map(_.toString).toArray)
    var expectedProjects: Option[Seq[Seq[String]]] = None

    def checkScan(nativeScan: Boolean): Unit = {
      val input = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(CAST(value AS STRING) AS BIGINT) + 1 AS value")

      testStream(input)(
        makeSureGetOffsetCalled,
        CheckAnswer(values.map(_ + 1L): _*),
        AssertOnQuery {
          query =>
            val plan = query.lastExecution.executedPlan
            withClue(plan.treeString) {
              val nativeScans = plan.collect { case scan: MicroBatchScanExecTransformer => scan }
              val sparkScans = plan.collect { case scan: MicroBatchScanExec => scan }
              val projects = plan.collect { case project: ProjectExecTransformerBase => project }
              val conversions = plan.collect { case r2c: RowToColumnarExecBase => r2c }
              assert(nativeScans.size == (if (nativeScan) 1 else 0))
              assert(sparkScans.size == (if (nativeScan) 0 else 1))
              assert(projects.nonEmpty, "The projection must remain native in both modes")
              assert(!plan.exists(_.isInstanceOf[ProjectExec]))
              if (nativeScan) {
                assert(conversions.isEmpty)
              } else {
                assert(conversions.size == 1)
                assert(conversions.head.child.exists(_.isInstanceOf[MicroBatchScanExec]))
                assert(projects.exists(_.child.exists(_.isInstanceOf[RowToColumnarExecBase])))
              }
              val projectExpressions = projects.map(_.list.map(_.sql))
              expectedProjects match {
                case Some(expected) => assert(projectExpressions == expected)
                case None => expectedProjects = Some(projectExpressions)
              }
            }
            true
        }
      )
    }

    withSQLConf("spark.gluten.enabled" -> "true", scanEnabled -> "true") {
      checkScan(nativeScan = true)
      withSQLConf(scanEnabled -> "false") {
        checkScan(nativeScan = false)
      }
      assert(spark.conf.get(scanEnabled) == "true")
      checkScan(nativeScan = true)
    }
  }
}

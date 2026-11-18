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

import org.apache.gluten.backendsapi.velox.VeloxListenerApi
import org.apache.gluten.execution.{BroadcastHashJoinExecTransformer, VeloxWholeStageTransformerSuite}

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.ColumnarBroadcastExchangeExec

class GlutenParallelBroadcastJoinSuite extends VeloxWholeStageTransformerSuite {

  override protected val resourcePath: String = "placeholder"
  override protected val fileFormat: String = "parquet"

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.adaptive.enabled", "false")
    .set("spark.gluten.sql.columnar.backend.velox.IOThreads", "0")
    .set("spark.gluten.velox.broadcast.build.targetBytesPerThread", "8KB")
    .set("spark.gluten.sql.columnar.backend.velox.broadcastHashTableBuildThreads", "4")

  private val threadsKey =
    "spark.gluten.sql.columnar.backend.velox.broadcastHashTableBuildThreads"
  private val taskSlotsKey = "spark.gluten.numTaskSlotsPerExecutor"

  test("broadcast build threads: resolve process defaults without modifying SparkConf") {
    val conf = new SparkConf(false)
      .setMaster("yarn")
      .set("spark.driver.cores", "6")
      .set("spark.executor.cores", "16")
      .set("spark.task.cpus", "2")
      .set(taskSlotsKey, "8")
    val original = conf.getAll.toMap

    assert(VeloxListenerApi.parseConf(conf, isDriver = true)(threadsKey) == "6")
    assert(conf.getAll.toMap == original)
    assert(VeloxListenerApi.parseConf(conf, isDriver = false)(threadsKey) == "8")
    assert(conf.getAll.toMap == original)
  }

  test("broadcast build threads: explicit thread count takes precedence on both " +
    "driver and executor") {
    val conf = new SparkConf(false)
      .set("spark.driver.cores", "6")
      .set(taskSlotsKey, "8")
      .set(threadsKey, "3")

    Seq(true, false).foreach {
      isDriver => assert(VeloxListenerApi.parseConf(conf, isDriver)(threadsKey) == "3")
    }
    assert(conf.get(threadsKey) == "3")
  }

  test("broadcast build threads: missing resource settings and spark.master default " +
    "to one thread") {
    Seq(true, false).foreach {
      isDriver =>
        assert(VeloxListenerApi.parseConf(new SparkConf(false), isDriver)(threadsKey) == "1")
    }
  }

  test(
    "broadcast build threads: driver defaults to one core independently of executor task slots") {
    val conf = new SparkConf(false).set(taskSlotsKey, "8")
    assert(VeloxListenerApi.parseConf(conf, isDriver = true)(threadsKey) == "1")
  }

  test("broadcast build threads: local pool uses master cores instead of task slots") {
    val masters = Seq(
      "local" -> 1,
      "local[6]" -> 6,
      "local[6,2]" -> 6,
      "local[*]" -> Runtime.getRuntime.availableProcessors(),
      "local[*,2]" -> Runtime.getRuntime.availableProcessors()
    )
    masters.foreach {
      case (master, cores) =>
        val conf = new SparkConf(false)
          .setMaster(master)
          .set("spark.driver.cores", "3")
          .set("spark.task.cpus", "2")
          .set(taskSlotsKey, "2")
        assert(VeloxListenerApi.parseConf(conf, isDriver = true)(threadsKey) == cores.toString)
    }
  }

  test("broadcast build threads: explicit thread count takes precedence in local mode") {
    val conf = new SparkConf(false).setMaster("local[6]").set(threadsKey, "3")
    assert(VeloxListenerApi.parseConf(conf, isDriver = true)(threadsKey) == "3")
  }

  test("broadcast build threads: zero task slots still provide at least one build thread") {
    val conf = new SparkConf(false).set(taskSlotsKey, "0")
    assert(VeloxListenerApi.parseConf(conf, isDriver = false)(threadsKey) == "1")
  }

  test(s"parallel broadcast hash table build with IO disabled") {
    Seq(true, false).foreach {
      buildOnDriver =>
        withSQLConf(
          "spark.gluten.sql.columnar.backend.velox.driverSideBroadcastHashTableBuild" ->
            buildOnDriver.toString,
          "spark.gluten.velox.minTableRowsForParallelJoinBuild" -> "1"
        ) {
          withTable("parallel_broadcast_build") {
            spark.range(4096)
              .selectExpr("id", "sha2(cast(id as string), 256) AS value")
              .write.saveAsTable("parallel_broadcast_build")

            val result =
              spark.sql("""
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

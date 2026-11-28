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

import org.apache.gluten.config.{GlutenConfig, GlutenCoreConfig}
import org.apache.gluten.memory.memtarget.spark.TreeMemoryConsumers

import org.apache.spark.SparkConf
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.task.TaskResources

class GlutenAutoAdjustStageResourceProfileSuite extends SharedSparkSession {

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .setMaster("local[2]")
      .set("spark.ui.enabled", "false")
      .set("spark.task.cpus", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "4g")
      .set("spark.memory.storageFraction", "0.5")

  test("native and isolation off-heap budgets follow executor resources with stale SQLConf") {
    val mib = 1024L * 1024L
    val taskOffHeapKey = GlutenCoreConfig.COLUMNAR_TASK_OFFHEAP_SIZE_IN_BYTES.key
    val conservativeKey = GlutenCoreConfig.COLUMNAR_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES.key

    // Model the budgets propagated from the application's original 256 MiB off-heap setting.
    withSQLConf(taskOffHeapKey -> (128 * mib).toString, conservativeKey -> (64 * mib).toString) {
      val budgets = SQLExecution.withNewExecutionId(spark.range(1).queryExecution) {
        spark.sparkContext
          .parallelize(Seq(0), 1)
          .map {
            _ =>
              TaskResources.onTaskStart()
              val taskConf = SQLConf.get
              val propagated = taskConf.getConfString(taskOffHeapKey)
              val nativeConf =
                GlutenConfig.getNativeSessionConf("velox", Map(taskOffHeapKey -> propagated))
              val isolatedRoot = TreeMemoryConsumers.factory(MemoryMode.OFF_HEAP).isolatedRoot()
              val granted = isolatedRoot.borrow(1024 * mib)
              isolatedRoot.repay(granted)
              (propagated.toLong, nativeConf(taskOffHeapKey).toLong, granted)
          }
          .collect()
      }
      assert(budgets.toSeq == Seq((128 * mib, 2048 * mib, 1024 * mib)))
    }
  }
}

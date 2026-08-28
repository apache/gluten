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

import org.apache.gluten.config.GlutenCoreConfig

import org.apache.spark.SparkConf
import org.apache.spark.resource.{ExecutorResourceRequests, ResourceProfile, TaskResourceRequests}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.SparkResourceUtil

import org.scalatest.funsuite.AnyFunSuite

class GlutenAutoAdjustStageResourceProfileSuite extends AnyFunSuite {

  private val MIB = 1024L * 1024L

  private def clusterConf(offHeap: String): SparkConf = new SparkConf(false)
    .set("spark.master", "yarn")
    .set("spark.executor.cores", "4")
    .set("spark.task.cpus", "1")
    .set("spark.memory.offHeap.enabled", "true")
    .set("spark.memory.offHeap.size", offHeap)

  private def profileWith(cores: Int, taskCpus: Int, offHeap: Option[String]): ResourceProfile = {
    val ereqs = new ExecutorResourceRequests()
    ereqs.cores(cores)
    offHeap.foreach(ereqs.offHeapMemory)
    val treqs = new TaskResourceRequests()
    treqs.cpus(taskCpus)
    new ResourceProfile(ereqs.requests, treqs.requests)
  }

  test("updateResourceSetting converts the profile's MiB amount to bytes") {
    // A ResourceProfile records executor memory in MiB, while the configs written here are declared
    // as bytesConf(ByteUnit.BYTE). Writing the amount verbatim shrank the off-heap budget by 2^20,
    // so 20g became 20480 bytes.
    val rp = profileWith(cores = 4, taskCpus = 1, offHeap = Some("20g"))
    SQLConf.withExistingConf(new SQLConf) {
      GlutenAutoAdjustStageResourceProfile.updateResourceSetting(rp, clusterConf("20g"))
      val conf = SQLConf.get
      assert(conf.getConfString(GlutenCoreConfig.NUM_TASK_SLOTS_PER_EXECUTOR.key) == "4")
      assert(
        conf.getConfString(GlutenCoreConfig.COLUMNAR_OFFHEAP_SIZE_IN_BYTES.key) ==
          (20480L * MIB).toString)
      assert(
        conf.getConfString(GlutenCoreConfig.COLUMNAR_TASK_OFFHEAP_SIZE_IN_BYTES.key) ==
          (20480L * MIB / 4).toString)
    }
  }

  test("updateResourceSetting reads the exact off-heap size for the default profile") {
    // The default profile's amount is the conf value truncated to MiB, so going through the profile
    // would shrink a size that is not a whole number of MiB. spark.memory.offHeap.size=1536k must
    // stay at 1572864, the value GlutenPlugin already wrote at driver init.
    val sparkConf = clusterConf("1536k")
    val rp = ResourceProfile.getOrCreateDefaultProfile(sparkConf)
    SQLConf.withExistingConf(new SQLConf) {
      GlutenAutoAdjustStageResourceProfile
        .updateResourceSetting(rp, sparkConf, isDefaultProfile = true)
      assert(
        SQLConf.get.getConfString(GlutenCoreConfig.COLUMNAR_OFFHEAP_SIZE_IN_BYTES.key) == "1572864")
    }
  }

  test("updateResourceSetting agrees with getTaskSlots in local mode") {
    // A profile reports spark.executor.cores, 1 by default, while GlutenPlugin resolves local[4] to
    // four slots. Deriving the count from the profile there made every task believe it owned the
    // whole off-heap budget.
    val sparkConf = new SparkConf(false)
      .set("spark.master", "local[4]")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "20g")
    val rp = profileWith(cores = 1, taskCpus = 1, offHeap = Some("20g"))
    SQLConf.withExistingConf(new SQLConf) {
      GlutenAutoAdjustStageResourceProfile.updateResourceSetting(rp, sparkConf)
      val conf = SQLConf.get
      val expectedSlots = SparkResourceUtil.getTaskSlots(sparkConf)
      assert(expectedSlots == 4)
      assert(conf.getConfString(GlutenCoreConfig.NUM_TASK_SLOTS_PER_EXECUTOR.key) == "4")
      assert(
        conf.getConfString(GlutenCoreConfig.COLUMNAR_TASK_OFFHEAP_SIZE_IN_BYTES.key) ==
          (20480L * MIB / 4).toString)
    }
  }

  test("updateResourceSetting floors the slot count at one") {
    // spark.task.cpus greater than the executor cores is a combination Spark rejects later with a
    // dedicated message, but the division below would throw first on a zero quotient.
    val rp = profileWith(cores = 1, taskCpus = 2, offHeap = Some("20g"))
    SQLConf.withExistingConf(new SQLConf) {
      GlutenAutoAdjustStageResourceProfile.updateResourceSetting(rp, clusterConf("20g"))
      val conf = SQLConf.get
      assert(conf.getConfString(GlutenCoreConfig.NUM_TASK_SLOTS_PER_EXECUTOR.key) == "1")
      assert(
        conf.getConfString(GlutenCoreConfig.COLUMNAR_TASK_OFFHEAP_SIZE_IN_BYTES.key) ==
          (20480L * MIB).toString)
    }
  }

  test("updateResourceSetting rejects a non-positive task cpus") {
    // The profile's own task cpus wins when present, as the two tests above rely on, so the profile
    // here carries none and the conf fallback decides.
    val ereqs = new ExecutorResourceRequests()
    ereqs.cores(4)
    ereqs.offHeapMemory("20g")
    val rp = new ResourceProfile(ereqs.requests, Map.empty)
    val sparkConf = clusterConf("20g").set("spark.task.cpus", "0")
    SQLConf.withExistingConf(new SQLConf) {
      val e = intercept[IllegalArgumentException](
        GlutenAutoAdjustStageResourceProfile.updateResourceSetting(rp, sparkConf))
      assert(e.getMessage.contains("spark.task.cpus should be positive"))
    }
  }
}

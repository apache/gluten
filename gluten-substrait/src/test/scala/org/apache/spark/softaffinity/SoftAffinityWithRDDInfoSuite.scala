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
package org.apache.spark.softaffinity

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.softaffinity.{AffinityManager, SoftAffinityManager}
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.storage.{RDDInfo, StorageLevel}

object FakeSoftAffinityManager extends AffinityManager {
  override lazy val usingSoftAffinity: Boolean = true

  override lazy val minOnTargetHosts: Int = 1

  override lazy val detectDuplicateReading: Boolean = true

  override lazy val duplicateReadingMaxCacheItems: Int = 1
}

class SoftAffinityWithRDDInfoSuite extends QueryTest with SharedSparkSession with PredicateHelper {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(GlutenConfig.GLUTEN_SOFT_AFFINITY_ENABLED.key, "true")
    .set(GlutenConfig.GLUTEN_SOFT_AFFINITY_DUPLICATE_READING_DETECT_ENABLED.key, "true")
    .set(GlutenConfig.GLUTEN_SOFT_AFFINITY_REPLICATIONS_NUM.key, "2")
    .set(GlutenConfig.GLUTEN_SOFT_AFFINITY_MIN_TARGET_HOSTS.key, "2")
    .set(GlutenConfig.SOFT_AFFINITY_LOG_LEVEL.key, "INFO")
    .set("spark.ui.enabled", "false")

  test("Soft Affinity Scheduler with duplicate reading detection") {
    val addEvent0 = SparkListenerExecutorAdded(
      System.currentTimeMillis(),
      "0",
      new ExecutorInfo("host-0", 3, null))
    val addEvent1 = SparkListenerExecutorAdded(
      System.currentTimeMillis(),
      "1",
      new ExecutorInfo("host-1", 3, null))
    val removedEvent0 = SparkListenerExecutorRemoved(System.currentTimeMillis(), "0", "")
    val removedEvent1 = SparkListenerExecutorRemoved(System.currentTimeMillis(), "1", "")
    val rdd1 = new RDDInfo(1, "", 3, StorageLevel.NONE, false, Seq.empty)
    val rdd2 = new RDDInfo(2, "", 3, StorageLevel.NONE, false, Seq.empty)
    var stage1 = new StageInfo(1, 0, "", 1, Seq(rdd1, rdd2), Seq.empty, "", resourceProfileId = 0)
    val stage1SubmitEvent = SparkListenerStageSubmitted(stage1)
    val stage1EndEvent = SparkListenerStageCompleted(stage1)
    val taskEnd1 = SparkListenerTaskEnd(
      1,
      0,
      "",
      org.apache.spark.Success,
      // this is little tricky here for 3.2 compatibility, we use -1 for partition id.
      new TaskInfo(1, 1, 1, 1L, "0", "host-0", TaskLocality.ANY, false),
      null,
      null
    )
    val files = Seq(
      SparkShimLoader.getSparkShims.generatePartitionedFile(
        InternalRow.empty,
        "fakePath0",
        0,
        100,
        Array("host-3")),
      SparkShimLoader.getSparkShims.generatePartitionedFile(
        InternalRow.empty,
        "fakePath0",
        100,
        200,
        Array("host-3"))
    ).toArray
    val filePartition = FilePartition(-1, files)
    val softAffinityListener = new SoftAffinityListener()
    softAffinityListener.onExecutorAdded(addEvent0)
    softAffinityListener.onExecutorAdded(addEvent1)
    SoftAffinityManager.updatePartitionMap(filePartition, 1)
    assert(SoftAffinityManager.rddPartitionInfoMap.size == 1)
    softAffinityListener.onStageSubmitted(stage1SubmitEvent)
    softAffinityListener.onTaskEnd(taskEnd1)
    assert(SoftAffinityManager.duplicateReadingInfos.size == 1)
    // check location (executor 0) of dulicate reading is returned.
    val locations = SoftAffinity.getFilePartitionLocations(filePartition)

    assertResult(Set("executor_host-0_0")) {
      locations.toSet
    }
    softAffinityListener.onStageCompleted(stage1EndEvent)
    // stage 1 completed, check all middle status is cleared.
    assert(SoftAffinityManager.rddPartitionInfoMap.size == 0)
    assert(SoftAffinityManager.stageInfoMap.size == 0)
    softAffinityListener.onExecutorRemoved(removedEvent0)
    softAffinityListener.onExecutorRemoved(removedEvent1)
    // executor 0 is removed, return empty.
    assert(SoftAffinityManager.askExecutors(filePartition).isEmpty)
  }

  test("Duplicate reading detection limits middle states count") {
    // This test simulate the case listener bus stucks. We need to make sure the middle states
    // count would not exceed the configed threshold.
    val files = Seq(
      SparkShimLoader.getSparkShims.generatePartitionedFile(
        InternalRow.empty,
        "fakePath0",
        0,
        100,
        Array("host-3")),
      SparkShimLoader.getSparkShims.generatePartitionedFile(
        InternalRow.empty,
        "fakePath0",
        100,
        200,
        Array("host-3"))
    ).toArray
    val filePartition = FilePartition(-1, files)
    FakeSoftAffinityManager.updatePartitionMap(filePartition, 1)
    assert(FakeSoftAffinityManager.rddPartitionInfoMap.size == 1)
    val filePartition1 = FilePartition(-1, files)
    FakeSoftAffinityManager.updatePartitionMap(filePartition1, 2)
    assert(FakeSoftAffinityManager.rddPartitionInfoMap.size == 1)
    assert(FakeSoftAffinityManager.stageInfoMap.size <= 1)
  }
}

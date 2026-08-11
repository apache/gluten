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
import org.apache.gluten.execution.{ColumnarToRowExecBase, CudfTag, GlutenPlan, WholeStageTransformer}
import org.apache.gluten.logging.LogLevelUtil

import org.apache.spark.SparkConf
import org.apache.spark.annotation.Experimental
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{CPUS_PER_TASK, EXECUTOR_CORES, MEMORY_OFFHEAP_SIZE}
import org.apache.spark.resource.{ExecutorResourceRequest, ResourceProfile, ResourceProfileManager, TaskResourceRequest}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{GlutenAutoAdjustStageResourceProfile => GlutenResourceProfile}
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.command.{DataWritingCommandExec, ExecutedCommandExec}
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.{SparkResourceUtil, SparkTestUtil}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * This rule dynamically adjusts the resource profile of each AQE query stage. It handles three
 * cases:
 *
 *   1. CPU/GPU hybrid execution: if every `WholeStageTransformer` in the stage is fully
 *      cuDF-offloaded, the stage is assigned a GPU resource profile so that Spark schedules its
 *      tasks on GPU-equipped executors.
 *   2. Whole-stage fallback: if a stage contains no native Gluten operators (or only
 *      columnar-to-row conversion nodes), heap memory is increased and off-heap memory is reduced,
 *      since the stage runs entirely on the JVM.
 *   3. Partial fallback: if the ratio of fallen (non-Gluten) nodes in a stage exceeds
 *      `spark.gluten.auto.adjustStageResources.fallenNode.ratio.threshold`, heap memory is
 *      increased and off-heap memory is decreased proportionally.
 *
 * * Note: Case 2 and 3 are not applied to final (non-Exchange) stages yet.
 */
@Experimental
case class GlutenAutoAdjustStageResourceProfile(glutenConf: GlutenConfig, spark: SparkSession)
  extends Rule[SparkPlan]
  with LogLevelUtil {

  lazy val sparkConf = spark.sparkContext.getConf

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!glutenConf.enableAutoAdjustStageResourceProfile) {
      return plan
    }
    if (!SQLConf.get.adaptiveExecutionEnabled) {
      return plan
    }
    if (!sparkConf.getBoolean(GlutenCoreConfig.SPARK_OFFHEAP_ENABLED_KEY, defaultValue = false)) {
      return plan
    }
    // Starting here, the resource profile may differ between stages. Configure resource settings
    // using the default profile to prevent any impact from the previous stage. If a new resource
    // profile is applied, the settings will be updated accordingly.
    GlutenResourceProfile.updateResourceSetting(
      ResourceProfile.getOrCreateDefaultProfile(sparkConf),
      sparkConf,
      isDefaultProfile = true)

    val rpManager = spark.sparkContext.resourceProfileManager
    val defaultRP = rpManager.defaultResourceProfile

    // initial resource profile config as default resource profile
    val taskResource = mutable.Map.empty[String, TaskResourceRequest] ++= defaultRP.taskResources
    val executorResource =
      mutable.Map.empty[String, ExecutorResourceRequest] ++= defaultRP.executorResources

    if (glutenConf.enableColumnarCudf && glutenConf.enableHybridExecution) {
      val transformers = plan.collect { case t: WholeStageTransformer => t }
      if (
        transformers.nonEmpty && transformers.forall {
          t => t.offloadCuda || t.getTagValue(CudfTag.CudfTestingTag).getOrElse(false)
        }
      ) {
        return GlutenResourceProfile.setResourceProfileForGpu(
          plan,
          executorResource,
          taskResource,
          rpManager,
          sparkConf,
          glutenConf)
      }
    }

    if (!plan.isInstanceOf[Exchange]) {
      // todo: support set resource profile for final stage
      return plan
    }
    val planNodes = GlutenResourceProfile.collectStagePlan(plan)
    if (planNodes.isEmpty) {
      return plan
    }
    log.info(s"detailPlanNodes ${planNodes.map(_.nodeName).mkString("Array(", ", ", ")")}")

    val memoryRequest = executorResource.get(ResourceProfile.MEMORY)
    val offheapRequest = executorResource.get(ResourceProfile.OFFHEAP_MEM)
    logInfo(s"default memory request $memoryRequest")
    logInfo(s"default offheap request $offheapRequest")

    // case 1: whole stage fallback to vanilla spark in such case we increase the heap
    //
    // one stage is considered as fallback if all node is not GlutenPlan
    // or all GlutenPlan node is C2R node.
    val wholeStageFallback = planNodes
      .filter(_.isInstanceOf[GlutenPlan])
      .count(!_.isInstanceOf[ColumnarToRowExecBase]) == 0
    if (wholeStageFallback) {
      val newMemoryAmount = memoryRequest.get.amount * glutenConf.autoAdjustStageRPHeapRatio
      val newExecutorMemory =
        new ExecutorResourceRequest(ResourceProfile.MEMORY, newMemoryAmount.toLong)
      executorResource.put(ResourceProfile.MEMORY, newExecutorMemory)

      val newExecutorOffheap =
        new ExecutorResourceRequest(ResourceProfile.OFFHEAP_MEM, offheapRequest.get.amount / 10)
      executorResource.put(ResourceProfile.OFFHEAP_MEM, newExecutorOffheap)

      return GlutenResourceProfile.applyNewResourceProfile(
        plan,
        executorResource,
        taskResource,
        rpManager,
        sparkConf)
    }

    // case 2: check whether fallback exists and decide whether increase heap memory
    // and decrease offheap memory.
    val fallenNodeCnt = planNodes.count(p => !p.isInstanceOf[GlutenPlan])
    val totalCount = planNodes.size

    if (1.0 * fallenNodeCnt / totalCount >= glutenConf.autoAdjustStageFallenNodeThreshold) {
      val newMemoryAmount = memoryRequest.get.amount * glutenConf.autoAdjustStageRPHeapRatio
      val newExecutorMemory =
        new ExecutorResourceRequest(ResourceProfile.MEMORY, newMemoryAmount.toLong)
      executorResource.put(ResourceProfile.MEMORY, newExecutorMemory)

      val newOffHeapMemoryAmount =
        offheapRequest.get.amount * glutenConf.autoAdjustStageRPOffHeapRatio
      val newExecutorOffheap =
        new ExecutorResourceRequest(ResourceProfile.OFFHEAP_MEM, newOffHeapMemoryAmount.toLong)
      executorResource.put(ResourceProfile.OFFHEAP_MEM, newExecutorOffheap)

      return GlutenResourceProfile.applyNewResourceProfile(
        plan,
        executorResource,
        taskResource,
        rpManager,
        sparkConf)
    }
    plan
  }
}

object GlutenAutoAdjustStageResourceProfile extends Logging {
  // collect all plan nodes belong to this stage including child query stage
  // but exclude query stage child
  def collectStagePlan(plan: SparkPlan): ArrayBuffer[SparkPlan] = {

    def collectStagePlan(plan: SparkPlan, planNodes: ArrayBuffer[SparkPlan]): Unit = {
      if (plan.isInstanceOf[DataWritingCommandExec] || plan.isInstanceOf[ExecutedCommandExec]) {
        // todo: support set final stage's resource profile
        return
      }
      planNodes += plan
      if (plan.isInstanceOf[QueryStageExec]) {
        return
      }
      plan.children.foreach(collectStagePlan(_, planNodes))
    }

    val planNodes = new ArrayBuffer[SparkPlan]()
    collectStagePlan(plan, planNodes)
    planNodes
  }

  private def getFinalResourceProfile(
      rpManager: ResourceProfileManager,
      newRP: ResourceProfile): ResourceProfile = {
    // Just for test
    // ResourceProfiles are only supported on YARN and Kubernetes with dynamic allocation enabled
    if (SparkTestUtil.isTesting) {
      return rpManager.defaultResourceProfile
    }
    val maybeEqProfile = rpManager.getEquivalentProfile(newRP)
    if (maybeEqProfile.isDefined) {
      maybeEqProfile.get
    } else {
      // register new resource profile here
      rpManager.addResourceProfile(newRP)
      newRP
    }
  }

  /**
   * Reflects resource changes in some configurations that will be passed to the native side.
   *
   * The values are written into the active SQLConf. On the driver, outside a task and outside
   * SQLConf#withExistingConf, that is the session's own conf, so the writes are visible to every
   * thread using the session and outlive the query that triggered them.
   */
  def updateResourceSetting(
      rp: ResourceProfile,
      sparkConf: SparkConf,
      isDefaultProfile: Boolean = false): Unit = {
    // Resource profiles never take effect in local mode, where a profile reports
    // spark.executor.cores (1 by default) rather than the local[N] thread count that
    // SparkResourceUtil and GlutenPlugin resolve. Defer to the shared resolver there so the rule
    // and the plugin agree on the slot count; elsewhere the profile's own values are authoritative.
    val taskSlots = if (SparkResourceUtil.isLocalMaster(sparkConf)) {
      SparkResourceUtil.getTaskSlots(sparkConf)
    } else {
      val coresPerExecutor = rp.getExecutorCores.getOrElse(sparkConf.get(EXECUTOR_CORES))
      val coresPerTask = rp.getTaskCpus.getOrElse(sparkConf.get(CPUS_PER_TASK))
      require(coresPerTask > 0, s"${CPUS_PER_TASK.key} should be positive, but was $coresPerTask")
      // Floor at one slot so the division below cannot throw on a combination Spark itself rejects
      // later with a dedicated message.
      Math.max(coresPerExecutor / coresPerTask, 1)
    }
    val conf = SQLConf.get
    conf.setConfString(GlutenCoreConfig.NUM_TASK_SLOTS_PER_EXECUTOR.key, taskSlots.toString)
    // A resource profile records executor memory amounts in MiB, while the two configs written
    // below are declared as bytesConf(ByteUnit.BYTE). The unmodified default profile carries the
    // same off-heap size the conf does, only truncated to MiB, so read the conf directly there to
    // keep this in step with what GlutenPlugin wrote at driver init.
    val offHeapSize = if (isDefaultProfile) {
      sparkConf.get(MEMORY_OFFHEAP_SIZE)
    } else {
      rp.executorResources
        .get(ResourceProfile.OFFHEAP_MEM)
        .map(request => SparkResourceUtil.mibToBytes(request.amount))
        .getOrElse(sparkConf.get(MEMORY_OFFHEAP_SIZE))
    }
    conf.setConfString(GlutenCoreConfig.COLUMNAR_OFFHEAP_SIZE_IN_BYTES.key, offHeapSize.toString)
    conf.setConfString(
      GlutenCoreConfig.COLUMNAR_TASK_OFFHEAP_SIZE_IN_BYTES.key,
      (offHeapSize / taskSlots).toString)
  }

  def applyNewResourceProfile(
      plan: SparkPlan,
      executorResource: mutable.Map[String, ExecutorResourceRequest],
      taskResource: mutable.Map[String, TaskResourceRequest],
      rpManager: ResourceProfileManager,
      sparkConf: SparkConf): SparkPlan = {
    val rp = new ResourceProfile(executorResource.toMap, taskResource.toMap)
    val finalRP = getFinalResourceProfile(rpManager, rp)
    updateResourceSetting(finalRP, sparkConf)

    plan match {
      case shuffle: Exchange =>
        logInfo(s"Apply resource profile $finalRP for plan ${shuffle.child.nodeName}")
        // Wrap the plan with ApplyResourceProfileExec so that we can apply new ResourceProfile
        val wrapperPlan = ApplyResourceProfileExec(shuffle.child, finalRP)
        shuffle.withNewChildren(Seq(wrapperPlan))
      case other =>
        logInfo(s"Apply resource profile $finalRP for plan ${other.nodeName}")
        ApplyResourceProfileExec(other, finalRP)
    }
  }

  def setResourceProfileForGpu(
      plan: SparkPlan,
      executorResource: mutable.Map[String, ExecutorResourceRequest],
      taskResource: mutable.Map[String, TaskResourceRequest],
      rpManager: ResourceProfileManager,
      sparkConf: SparkConf,
      glutenConf: GlutenConfig): SparkPlan = {
    val cpuResourceName = glutenConf.cpuResourceName
    val gpuResourceName = glutenConf.gpuResourceName

    executorResource.remove(glutenConf.cpuResourceName)
    taskResource.remove(cpuResourceName)

    executorResource.put(gpuResourceName, new ExecutorResourceRequest(gpuResourceName, 1))
    // The gpu task resource limits how many tasks can be launched in one executor.
    taskResource.put(
      gpuResourceName,
      new TaskResourceRequest(gpuResourceName, glutenConf.gpuResourceAmountPerTask))

    applyNewResourceProfile(
      plan,
      executorResource,
      taskResource,
      rpManager,
      sparkConf)
  }
}

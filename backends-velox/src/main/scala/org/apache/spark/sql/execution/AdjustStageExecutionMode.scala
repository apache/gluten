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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution._
import org.apache.gluten.logging.LogLevelUtil

import org.apache.spark.annotation.Experimental
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.AdjustStageExecutionMode.{adjustExecutionMode, unsetTag}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, ColumnarAQEShuffleReadExec, ShuffleQueryStageExec}
import org.apache.spark.util.SparkTestUtil

// For ShuffleStage, the resource profile is set to ColumnarShuffleExchangeExec.inputColumnarRDD.
@Experimental
case class AdjustStageExecutionMode(
    glutenConf: GlutenConfig,
    spark: SparkSession,
    isAdaptiveContext: Boolean)
  extends Rule[SparkPlan]
  with LogLevelUtil {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!isAdaptiveContext) {
      return plan
    }

    if (glutenConf.enableColumnarCudf) {
      return adjustExecutionModeForGPU(plan)
    }

    plan
  }

  private def adjustExecutionModeForGPU(plan: SparkPlan): SparkPlan = {
    val transformers = plan.collect { case t: WholeStageTransformer => t }
    if (transformers.isEmpty) {
      return plan
    }
    if (transformers.size > 1) {
      logWarning(s"Not offloading GPU because multiple WholeStageTransformer exist. Remove tags.")
      unsetTag(plan, CudfTag.CudfTag)
      return plan
    }

    val transformer = transformers.head
    if (transformer.isCudf) {
      val gpuStageMode = if (SparkTestUtil.isTesting) {
        // Only unset for transformer.
        transformer.unsetTagValue(CudfTag.CudfTag)
        MockGPUStageMode
      } else {
        GPUStageMode
      }

      adjustExecutionMode(plan, gpuStageMode)
    } else {
      plan
    }
  }
}

object AdjustStageExecutionMode extends Logging {
  def adjustExecutionMode(plan: SparkPlan, stageExecutionMode: StageExecutionMode): SparkPlan = {
    logInfo(s"Adjust ${plan.nodeName} to ${stageExecutionMode.name}.")
    plan match {
      // TODO: support BroadcastQueryStageExec.
      case aqeShuffleRead @ AQEShuffleReadExec(s @ ShuffleQueryStageExec(_, _, _), _)
          if s.shuffle.isInstanceOf[ColumnarShuffleExchangeExec] =>
        ColumnarAQEShuffleReadExec(
          Left(aqeShuffleRead),
          stageExecutionMode)
      case queryStageExec: ShuffleQueryStageExec
          if queryStageExec.shuffle.isInstanceOf[ColumnarShuffleExchangeExec] =>
        ColumnarAQEShuffleReadExec(
          Right(queryStageExec),
          stageExecutionMode)
      case shuffle: ColumnarShuffleExchangeExec =>
        shuffle
          .copy(mapperStageMode = Some(stageExecutionMode))
          .withNewChildren(Seq(adjustExecutionMode(shuffle.child, stageExecutionMode)))
      case resizeBatches: VeloxResizeBatchesExec =>
        VeloxResizeBatchesExec(
          adjustExecutionMode(resizeBatches.child, stageExecutionMode),
          Some(stageExecutionMode))
      case _ =>
        plan.withNewChildren(plan.children.map(adjustExecutionMode(_, stageExecutionMode)))
    }
  }

  def unsetTag[T](plan: SparkPlan, tag: TreeNodeTag[T]): Unit = {
    plan.foreach {
      case t: TransformSupport =>
        t.unsetTagValue(tag)
      case _ =>
    }
  }
}

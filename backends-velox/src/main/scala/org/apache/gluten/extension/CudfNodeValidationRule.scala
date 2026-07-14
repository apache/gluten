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
package org.apache.gluten.extension

import org.apache.gluten.config.{GlutenConfig, VeloxConfig}
import org.apache.gluten.cudf.VeloxCudfPlanValidatorJniWrapper
import org.apache.gluten.execution._
import org.apache.gluten.extension.CudfNodeValidationRule.{setStageExecutionModeForShuffle, setTagForWholeStageTransformer}

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.SparkTestUtil

// Add the node name prefix 'Cudf' to GlutenPlan when can offload to cudf
case class CudfNodeValidationRule(glutenConf: GlutenConfig) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!glutenConf.enableColumnarCudf) {
      return plan
    }
    val taggedPlan = plan.transformUp {
      case transformer: WholeStageTransformer =>
        setTagForWholeStageTransformer(transformer)
        transformer
    }

    if (!SQLConf.get.adaptiveExecutionEnabled) {
      // Set mapper and reducer stage execution mode for Shuffle.
      setStageExecutionModeForShuffle(taggedPlan, supportsCudf = false)._1
    } else {
      // Will be handled by AdjustStageExecutionMode rule.
      taggedPlan
    }
  }
}

object CudfNodeValidationRule {
  def setTagForWholeStageTransformer(transformer: WholeStageTransformer): Unit = {
    if (!VeloxConfig.get.cudfEnableTableScan) {
      val hasLeaf = transformer.exists {
        case _: LeafTransformSupport => true
        case _ => false
      }
      if (!hasLeaf && VeloxConfig.get.cudfEnableValidation) {
        if (
          VeloxCudfPlanValidatorJniWrapper.validate(
            transformer.substraitPlan.toProtobuf.toByteArray)
        ) {
          transformer.foreach {
            case _: LeafTransformSupport =>
            case t: TransformSupport =>
              t.setTagValue(CudfTag.CudfTag, true)
            case _ =>
          }
          transformer.setTagValue(CudfTag.CudfTag, true)
        }
      } else {
        transformer.setTagValue(CudfTag.CudfTag, !hasLeaf)
      }
    } else {
      transformer.setTagValue(CudfTag.CudfTag, true)
    }
  }

  // supportsCudf is the first parent WholeStageTransformer's `isCudf` for plan.
  // For WholeStageTransformer, it calls the child's setStageExecutionModeForShuffle with its
  // `isCudf` for the child shuffle reader,
  // and returns its `isCudf` for the parent shuffle writer.
  def setStageExecutionModeForShuffle(
      plan: SparkPlan,
      supportsCudf: Boolean): (SparkPlan, Boolean) = {
    def getStageExecutionMode(supportsCudf: Boolean): StageExecutionMode = {
      if (supportsCudf) {
        if (SparkTestUtil.isTesting) {
          MockGPUStageMode
        } else {
          GPUStageMode
        }
      } else {
        CPUStageMode
      }
    }

    plan match {
      case shuffle @ ColumnarShuffleExchangeExec(
            _,
            VeloxResizeBatchesExec(child, _),
            _,
            _,
            _,
            _,
            _) =>
        // `supportsCudf` is not decided yet for shuffle writer.
        val (newChild, mapperStageSupportsCudf) =
          setStageExecutionModeForShuffle(child, false)
        val mapperStageMode = getStageExecutionMode(mapperStageSupportsCudf)
        val reducerStageMode = getStageExecutionMode(supportsCudf)
        (
          shuffle.copy(
            child = VeloxResizeBatchesExec(newChild, Some(mapperStageMode)),
            mapperStageMode = Some(mapperStageMode),
            reducerStageMode = Some(reducerStageMode)),
          supportsCudf)
      case shuffle: ColumnarShuffleExchangeExec =>
        val (newChild, mapperStageSupportsCudf) =
          setStageExecutionModeForShuffle(shuffle.child, false)
        val mapperStageMode = getStageExecutionMode(mapperStageSupportsCudf)
        val reducerStageMode = getStageExecutionMode(supportsCudf)
        (
          shuffle.copy(
            child = newChild,
            mapperStageMode = Some(mapperStageMode),
            reducerStageMode = Some(reducerStageMode)),
          supportsCudf)
      case resizeBatches: VeloxResizeBatchesExec =>
        val (newChild, _) =
          setStageExecutionModeForShuffle(resizeBatches.child, supportsCudf)
        (VeloxResizeBatchesExec(newChild, Some(getStageExecutionMode(supportsCudf))), supportsCudf)
      case wst: WholeStageTransformer =>
        val (newChild, _) = setStageExecutionModeForShuffle(wst.child, wst.isCudf)
        (wst.withNewChildren(Seq(newChild)), wst.isCudf)
      case other =>
        val newChildren =
          other.children.map(child => setStageExecutionModeForShuffle(child, supportsCudf)._1)
        (other.withNewChildren(newChildren), supportsCudf)
    }
  }
}

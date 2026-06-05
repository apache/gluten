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

import org.apache.gluten.config.VeloxConfig
import org.apache.gluten.execution.VeloxResizeBatchesExec

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarShuffleExchangeExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, ShuffleQueryStageExec}

/**
 * Try to append [[VeloxResizeBatchesExec]] for shuffle input and output to make the batch sizes in
 * good shape.
 */
case class AppendBatchResizeForShuffleInputAndOutput(isAdaptiveContext: Boolean)
  extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (VeloxConfig.get.enableColumnarCudf) {
      return plan
    }

    val resizeBatchesShuffleInputEnabled = VeloxConfig.get.veloxResizeBatchesShuffleInput
    val resizeBatchesShuffleOutputEnabled = VeloxConfig.get.veloxResizeBatchesShuffleOutput
    if (!resizeBatchesShuffleInputEnabled && !resizeBatchesShuffleOutputEnabled) {
      return plan
    }

    val range = VeloxConfig.get.veloxResizeBatchesShuffleInputOutputRange
    val preferredBatchBytes = VeloxConfig.get.veloxPreferredBatchBytes

    val newPlan = if (resizeBatchesShuffleInputEnabled) {
      addResizeBatchesForShuffleInput(plan, range.min, range.max, preferredBatchBytes)
    } else {
      plan
    }

    val resultPlan = if (isAdaptiveContext && resizeBatchesShuffleOutputEnabled) {
      addResizeBatchesForShuffleOutput(newPlan, range.min, range.max, preferredBatchBytes)
    } else {
      newPlan
    }

    resultPlan
  }

  private def addResizeBatchesForShuffleInput(
      plan: SparkPlan,
      min: Int,
      max: Int,
      preferredBatchBytes: Long): SparkPlan = {
    plan.transformUp {
      case shuffle: ColumnarShuffleExchangeExec
          if shuffle.shuffleWriterType.requiresResizingShuffleInput =>
        val appendBatches =
          VeloxResizeBatchesExec(shuffle.child, min, max, preferredBatchBytes)
        shuffle.withNewChildren(Seq(appendBatches))
    }
  }

  private def addResizeBatchesForShuffleOutput(
      plan: SparkPlan,
      min: Int,
      max: Int,
      preferredBatchBytes: Long): SparkPlan = {
    plan match {
      case s: ShuffleQueryStageExec if requiresResizingShuffleOutput(s) =>
        VeloxResizeBatchesExec(s, min, max, preferredBatchBytes)
      case a @ AQEShuffleReadExec(s @ ShuffleQueryStageExec(_, _, _), _)
          if requiresResizingShuffleOutput(s) =>
        VeloxResizeBatchesExec(a, min, max, preferredBatchBytes)
      case other =>
        other.mapChildren(addResizeBatchesForShuffleOutput(_, min, max, preferredBatchBytes))
    }
  }

  private def requiresResizingShuffleOutput(s: ShuffleQueryStageExec): Boolean = {
    s.shuffle match {
      case c: ColumnarShuffleExchangeExec
          if c.shuffleWriterType.requiresResizingShuffleOutput =>
        true
      case _ => false
    }
  }
}

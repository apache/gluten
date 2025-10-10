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

import org.apache.gluten.config.BoltConfig
import org.apache.gluten.execution.BoltResizeBatchesExec

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarShuffleExchangeExecBase, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

/**
 * Try to append [[BoltResizeBatchesExec]] for shuffle input and output to make the batch sizes in
 * good shape.
 */
case class AppendBatchResizeForShuffleInputAndOutput(isAdaptiveContext: Boolean)
  extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    val resizeBatchesShuffleInputEnabled = BoltConfig.get.boltResizeBatchesShuffleInput
    val resizeBatchesShuffleOutputEnabled = BoltConfig.get.boltResizeBatchesShuffleOutput
    if (!resizeBatchesShuffleInputEnabled && !resizeBatchesShuffleOutputEnabled) {
      return plan
    }

    val range = BoltConfig.get.boltResizeBatchesShuffleInputOutputRange
    val preferredBatchBytes = BoltConfig.get.boltPreferredBatchBytes

    val newPlan = if (resizeBatchesShuffleInputEnabled) {
      addResizeBatchesForShuffleInput(plan, range.min, range.max, preferredBatchBytes)
    } else {
      plan
    }

    if (isAdaptiveContext && resizeBatchesShuffleOutputEnabled) {
      addResizeBatchesForShuffleOutput(newPlan, range.min, range.max, preferredBatchBytes)
    } else {
      newPlan
    }
  }

  private def addResizeBatchesForShuffleInput(
      plan: SparkPlan,
      min: Int,
      max: Int,
      preferredBatchBytes: Long): SparkPlan = {
    plan.transformUp {
      case shuffle: ColumnarShuffleExchangeExecBase
          if shuffle.shuffleWriterType.requiresResizingShuffleInput =>
        val appendBatches =
          BoltResizeBatchesExec(shuffle.child, min, max, preferredBatchBytes)
        shuffle.withNewChildren(Seq(appendBatches))
    }
  }

  private def addResizeBatchesForShuffleOutput(
      plan: SparkPlan,
      min: Int,
      max: Int,
      preferredBatchBytes: Long): SparkPlan = {
    plan match {
      case s @ ShuffleQueryStageExec(_, c: ColumnarShuffleExchangeExecBase, _)
          if c.shuffleWriterType.requiresResizingShuffleOutput =>
        BoltResizeBatchesExec(s, min, max, preferredBatchBytes)
      case s @ ShuffleQueryStageExec(
            _,
            ReusedExchangeExec(_, c: ColumnarShuffleExchangeExecBase),
            _)
          if c.shuffleWriterType.requiresResizingShuffleOutput =>
        BoltResizeBatchesExec(s, min, max, preferredBatchBytes)
      case a @ AQEShuffleReadExec(
            s @ ShuffleQueryStageExec(_, c: ColumnarShuffleExchangeExecBase, _),
            _)
          if c.shuffleWriterType.requiresResizingShuffleOutput =>
        BoltResizeBatchesExec(a, min, max, preferredBatchBytes)
      case a @ AQEShuffleReadExec(
            s @ ShuffleQueryStageExec(
              _,
              ReusedExchangeExec(_, c: ColumnarShuffleExchangeExecBase),
              _),
            _)
          if c.shuffleWriterType.requiresResizingShuffleOutput =>
        BoltResizeBatchesExec(a, min, max, preferredBatchBytes)
      case other =>
        other.mapChildren(addResizeBatchesForShuffleOutput(_, min, max, preferredBatchBytes))
    }
  }
}

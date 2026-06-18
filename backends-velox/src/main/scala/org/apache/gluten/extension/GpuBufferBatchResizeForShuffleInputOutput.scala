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

import org.apache.gluten.config.{HashShuffleWriterType, VeloxConfig}
import org.apache.gluten.execution.{GpuResizeBufferColumnarBatchExec, VeloxResizeBatchesExec}

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarShuffleExchangeExec, ColumnarShuffleExchangeExecBase, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

/**
 * Try to append [[GpuBufferBatchResizeForShuffleInputOutput]] for shuffle input and output to make
 * the batch sizes in good shape.
 */
case class GpuBufferBatchResizeForShuffleInputOutput() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!VeloxConfig.get.enableColumnarCudf) {
      return plan
    }
    val range = VeloxConfig.get.veloxResizeBatchesShuffleInputOutputRange
    val preferredBatchBytes = VeloxConfig.get.veloxPreferredBatchBytes
    val batchSize = VeloxConfig.get.cudfBatchSize
    val prefetchBatchBytes = VeloxConfig.get.cudfShuffleMaxPrefetchBytes
    plan.transformUp {
      case shuffle: ColumnarShuffleExchangeExec
          if shuffle.shuffleWriterType == HashShuffleWriterType &&
            VeloxConfig.get.veloxResizeBatchesShuffleInput =>
        val appendBatches =
          VeloxResizeBatchesExec(shuffle.child, range.min, range.max, preferredBatchBytes)
        shuffle.withNewChildren(Seq(appendBatches))
      case a @ AQEShuffleReadExec(
            ShuffleQueryStageExec(_, _: ColumnarShuffleExchangeExecBase, _),
            _) =>
        GpuResizeBufferColumnarBatchExec(a, batchSize, prefetchBatchBytes)
      case a @ AQEShuffleReadExec(
            ShuffleQueryStageExec(_, ReusedExchangeExec(_, _: ColumnarShuffleExchangeExecBase), _),
            _) =>
        GpuResizeBufferColumnarBatchExec(a, batchSize, prefetchBatchBytes)
      // Since it's transformed in a bottom to up order, so we may first encounter
      // ShuffeQueryStageExec, which is transformed to VeloxResizeBatchesExec(ShuffeQueryStageExec),
      // then we see AQEShuffleReadExec
      case a @ AQEShuffleReadExec(
            GpuResizeBufferColumnarBatchExec(
              s @ ShuffleQueryStageExec(_, _: ColumnarShuffleExchangeExecBase, _),
              _,
              _),
            _) =>
        GpuResizeBufferColumnarBatchExec(a.copy(child = s), batchSize, prefetchBatchBytes)
      case a @ AQEShuffleReadExec(
            GpuResizeBufferColumnarBatchExec(
              s @ ShuffleQueryStageExec(
                _,
                ReusedExchangeExec(_, _: ColumnarShuffleExchangeExecBase),
                _),
              _,
              _),
            _) =>
        GpuResizeBufferColumnarBatchExec(a.copy(child = s), batchSize, prefetchBatchBytes)
      case s @ ShuffleQueryStageExec(_, _: ColumnarShuffleExchangeExecBase, _) =>
        GpuResizeBufferColumnarBatchExec(s, batchSize, prefetchBatchBytes)
      case s @ ShuffleQueryStageExec(
            _,
            ReusedExchangeExec(_, _: ColumnarShuffleExchangeExecBase),
            _) =>
        GpuResizeBufferColumnarBatchExec(s, batchSize, prefetchBatchBytes)
    }
  }
}

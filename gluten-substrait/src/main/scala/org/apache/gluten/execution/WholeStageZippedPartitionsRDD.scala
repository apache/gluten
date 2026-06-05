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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.metrics.{GlutenTimeMetric, IMetrics}

import org.apache.spark.{Partition, SparkConf, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

private[gluten] class ZippedPartitionsPartition(
    override val index: Int,
    val inputColumnarRDDPartitions: Seq[Partition])
  extends Partition {}

class WholeStageZippedPartitionsRDD(
    @transient private val sc: SparkContext,
    var rdds: ColumnarInputRDDsWrapper,
    sparkConf: SparkConf,
    resCtx: WholeStageTransformContext,
    pipelineTime: SQLMetric,
    updateNativeMetrics: IMetrics => Unit,
    materializeInput: Boolean)
  extends RDD[ColumnarBatch](sc, rdds.getDependencies) {

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    GlutenTimeMetric.millis(pipelineTime) {
      _ =>
        val partitions = split.asInstanceOf[ZippedPartitionsPartition].inputColumnarRDDPartitions
        val inputIterators: Seq[Iterator[ColumnarBatch]] = rdds.getIterators(partitions, context)
        BackendsApiManager.getIteratorApiInstance
          .genFinalStageIterator(
            context,
            inputIterators,
            sparkConf,
            resCtx.root,
            pipelineTime,
            updateNativeMetrics,
            split.index,
            materializeInput,
            resCtx.enableCudf,
            resCtx.supportsValueStreamDynamicFilter
          )
    }
  }

  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](rdds.getPartitionLength) {
      i => new ZippedPartitionsPartition(i, rdds.getPartitions(i))
    }
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdds = null
  }
}

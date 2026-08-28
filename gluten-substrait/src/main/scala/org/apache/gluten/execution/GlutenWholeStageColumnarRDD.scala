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
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.extension.ApplyStageInputStatsRule
import org.apache.gluten.metrics.{GlutenTimeMetric, IMetrics}
import org.apache.gluten.substrait.rel.SplitInfo

import org.apache.spark.{Partition, SparkContext, SparkException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.utils.SparkInputMetricsUtil.InputMetricsWrapper
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters.asScalaBufferConverter

trait BaseGlutenPartition extends Partition with InputPartition {
  def plan: Array[Byte]
}

case class GlutenPartition(
    index: Int,
    plan: Array[Byte],
    splitInfos: Array[SplitInfo] = Array.empty[SplitInfo],
    files: Array[String] =
      Array.empty[String] // touched files, for implementing UDF input_file_name
) extends BaseGlutenPartition {

  override def preferredLocations(): Array[String] =
    splitInfos.flatMap(_.preferredLocations().asScala)
}

case class FirstZippedPartitionsPartition(
    index: Int,
    inputPartition: Partition,
    inputColumnarRDDPartitions: Seq[Partition] = Seq.empty)
  extends Partition

class GlutenWholeStageColumnarRDD(
    @transient sc: SparkContext,
    @transient private val inputPartitions: Seq[Partition],
    var rdds: ColumnarInputRDDsWrapper,
    pipelineTime: SQLMetric,
    updateInputMetrics: InputMetricsWrapper => Unit,
    updateNativeMetrics: IMetrics => Unit,
    enableCudf: Boolean = false,
    wsContext: WholeStageTransformContext = null)
  extends RDD[ColumnarBatch](sc, rdds.getDependencies) {

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    GlutenTimeMetric.millis(pipelineTime) {
      _ =>
        if (GlutenConfig.get.enablePassStageInputStats && wsContext != null) {
          ApplyStageInputStatsRule.setStageInputStatsToInputNode(
            wsContext,
            split.index,
            rdds.getPartitionLength)
        }
        val (inputPartition, inputColumnarRDDPartitions) = castNativePartition(split)
        val inputIterators = rdds.getIterators(inputColumnarRDDPartitions, context)
        BackendsApiManager.getIteratorApiInstance.genFirstStageIterator(
          inputPartition,
          context,
          pipelineTime,
          updateInputMetrics,
          updateNativeMetrics,
          split.index,
          inputIterators,
          enableCudf,
          wsContext
        )
    }
  }

  private def castNativePartition(split: Partition): (BaseGlutenPartition, Seq[Partition]) = {
    split match {
      case FirstZippedPartitionsPartition(_, g: BaseGlutenPartition, p) => (g, p)
      case _ => throw new SparkException(s"[BUG] Not a NativeSubstraitPartition: $split")
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    castNativePartition(split)._1.preferredLocations()
  }

  override protected def getPartitions: Array[Partition] = {
    rdds.getPartitionLengthOption.foreach {
      inputPartitionCount =>
        require(
          inputPartitionCount == inputPartitions.size,
          s"Whole-stage partition count ${inputPartitions.size} does not match " +
            s"input RDD partition count $inputPartitionCount"
        )
    }
    inputPartitions.zipWithIndex
      .map {
        case (partition, i) => FirstZippedPartitionsPartition(i, partition, rdds.getPartitions(i))
      }
      .toArray[Partition]
  }

  override protected def clearDependencies(): Unit = {
    super.clearDependencies()
    rdds = null
  }
}

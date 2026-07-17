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

import org.apache.gluten.execution.{CPUStageMode, StageExecutionMode}

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.{ShuffleHandle, ShuffleManager, ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.shuffle.sort.{ColumnarShuffleManager, SortShuffleManager}
import org.apache.spark.sql.execution.ShuffledColumnarBatchRDD.getReader
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.metric.SQLColumnarShuffleReadMetricsReporter
import org.apache.spark.sql.vectorized.ColumnarBatch

/** The [[Partition]] used by [[ShuffledColumnarBatchRDD]]. */
final private case class ShuffledColumnarBatchRDDPartition(index: Int, spec: ShufflePartitionSpec)
  extends Partition

/** [[ShuffledColumnarBatchRDD]] is the columnar version of [[org.apache.spark.rdd.ShuffledRDD]]. */
class ShuffledColumnarBatchRDD(
    var dependency: ShuffleDependency[Int, ColumnarBatch, ColumnarBatch],
    metrics: Map[String, SQLMetric],
    partitionSpecs: Array[ShufflePartitionSpec],
    executionMode: StageExecutionMode)
  extends RDD[ColumnarBatch](dependency.rdd.context, Nil) {

  override val partitioner: Option[Partitioner] =
    if (partitionSpecs.forall(_.isInstanceOf[CoalescedPartitionSpec])) {
      val indices = partitionSpecs.map(_.asInstanceOf[CoalescedPartitionSpec].startReducerIndex)
      // TODO this check is based on assumptions of callers' behavior but is sufficient for now.
      if (indices.toSet.size == partitionSpecs.length) {
        Some(new CoalescedPartitioner(dependency.partitioner, indices))
      } else {
        None
      }
    } else {
      None
    }

  if (SQLConf.get.fetchShuffleBlocksInBatch) {
    dependency.rdd.context
      .setLocalProperty(SortShuffleManager.FETCH_SHUFFLE_BLOCKS_IN_BATCH_ENABLED_KEY, "true")
  }

  def this(
      dependency: ShuffleDependency[Int, ColumnarBatch, ColumnarBatch],
      metrics: Map[String, SQLMetric],
      executionMode: StageExecutionMode = CPUStageMode) = {
    this(
      dependency,
      metrics,
      Array.tabulate(dependency.partitioner.numPartitions)(i => CoalescedPartitionSpec(i, i + 1)),
      executionMode
    )
  }

  override def getDependencies: Seq[Dependency[_]] = List(dependency)

  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](partitionSpecs.length) {
      i => ShuffledColumnarBatchRDDPartition(i, partitionSpecs(i))
    }
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    partition.asInstanceOf[ShuffledColumnarBatchRDDPartition].spec match {
      case CoalescedPartitionSpec(startReducerIndex, endReducerIndex, _) =>
        // TODO order by partition size.
        startReducerIndex.until(endReducerIndex).flatMap {
          reducerIndex => tracker.getPreferredLocationsForShuffle(dependency, reducerIndex)
        }
      case CoalescedMapperPartitionSpec(startMapIndex, endMapIndex, numReducers) =>
        tracker.getMapLocation(dependency, startMapIndex, endMapIndex)
      case PartialReducerPartitionSpec(_, startMapIndex, endMapIndex, _) =>
        tracker.getMapLocation(dependency, startMapIndex, endMapIndex)

      case PartialMapperPartitionSpec(mapIndex, _, _) =>
        tracker.getMapLocation(dependency, mapIndex, mapIndex + 1)
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val tempMetrics = context.taskMetrics().createTempShuffleReadMetrics()
    // `SQLShuffleReadMetricsReporter` will update its own metrics for SQL exchange operator,
    // as well as the `tempMetrics` for basic shuffle metrics.
    val sqlMetricsReporter = new SQLColumnarShuffleReadMetricsReporter(tempMetrics, metrics)

    val reader = split.asInstanceOf[ShuffledColumnarBatchRDDPartition].spec match {
      case CoalescedPartitionSpec(startReducerIndex, endReducerIndex, _) =>
        getReader(
          SparkEnv.get.shuffleManager,
          dependency.shuffleHandle,
          startReducerIndex,
          endReducerIndex,
          context,
          sqlMetricsReporter,
          executionMode)

      case PartialReducerPartitionSpec(reducerIndex, startMapIndex, endMapIndex, _) =>
        getReader(
          SparkEnv.get.shuffleManager,
          dependency.shuffleHandle,
          startMapIndex,
          endMapIndex,
          reducerIndex,
          reducerIndex + 1,
          context,
          sqlMetricsReporter,
          executionMode)

      case PartialMapperPartitionSpec(mapIndex, startReducerIndex, endReducerIndex) =>
        getReader(
          SparkEnv.get.shuffleManager,
          dependency.shuffleHandle,
          mapIndex,
          mapIndex + 1,
          startReducerIndex,
          endReducerIndex,
          context,
          sqlMetricsReporter,
          executionMode)

      case CoalescedMapperPartitionSpec(startMapIndex, endMapIndex, numReducers) =>
        getReader(
          SparkEnv.get.shuffleManager,
          dependency.shuffleHandle,
          startMapIndex,
          endMapIndex,
          0,
          numReducers,
          context,
          sqlMetricsReporter,
          executionMode)
    }
    reader.read().asInstanceOf[Iterator[Product2[Int, ColumnarBatch]]].map {
      case (_, batch: ColumnarBatch) =>
        sqlMetricsReporter.incBatchesRecordsRead(batch.numRows())
        batch
    }
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    dependency = null
  }
}

object ShuffledColumnarBatchRDD {
  private def getReader[K, C](
      shuffleManager: ShuffleManager,
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter,
      executionMode: StageExecutionMode): ShuffleReader[K, C] = {
    shuffleManager match {
      case columnarShuffleManager: ColumnarShuffleManager =>
        columnarShuffleManager.getReader(
          handle,
          startMapIndex,
          endMapIndex,
          startPartition,
          endPartition,
          context,
          metrics,
          executionMode)
      case _ =>
        shuffleManager.getReader(
          handle,
          startMapIndex,
          endMapIndex,
          startPartition,
          endPartition,
          context,
          metrics)
    }
  }

  private def getReader[K, C](
      shuffleManager: ShuffleManager,
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter,
      executionMode: StageExecutionMode): ShuffleReader[K, C] = {
    getReader[K, C](
      shuffleManager,
      handle,
      0,
      Int.MaxValue,
      startPartition,
      endPartition,
      context,
      metrics,
      executionMode)
  }
}

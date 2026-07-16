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
package org.apache.spark.sql.execution.adaptive

import org.apache.gluten.execution.StageExecutionMode

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.mutable.ArrayBuffer

/**
 * A wrapper of AQEShuffleReadExec. It is used to wrap the AQEShuffleReadExec or
 * ShuffleQueryStageExec if executionMode is set by the planner.
 *
 * @param delegate
 *   The AQEShuffleReadExec or ShuffleQueryStageExec.
 * @param executionMode
 *   The execution mode of the current AQE stage.
 */
case class ColumnarAQEShuffleReadExec(
    delegate: Either[AQEShuffleReadExec, ShuffleQueryStageExec],
    executionMode: StageExecutionMode) extends UnaryExecNode {

  private val isAQEShuffleRead = delegate.isLeft

  private val aqeReader: AQEShuffleReadExec = {
    if (isAQEShuffleRead) {
      delegate.left.get
    } else {
      ColumnarAQEShuffleReadExec.wrapQueryStageWithDummyPartitionSpecs(
        delegate.right.get)
    }
  }

  override def child: SparkPlan = aqeReader.child

  override def output: Seq[Attribute] = child.output

  private def shuffleStage = {
    val method = classOf[AQEShuffleReadExec].getDeclaredMethod("shuffleStage")
    method.setAccessible(true)
    method.invoke(aqeReader).asInstanceOf[Option[ShuffleQueryStageExec]]
  }

  private def isCoalescedSpec(spec: ShufflePartitionSpec) = {
    val method = classOf[AQEShuffleReadExec].getDeclaredMethod("isCoalescedSpec")
    method.setAccessible(true)
    method.invoke(aqeReader, spec).asInstanceOf[Boolean]
  }

  @transient private lazy val partitionDataSizes: Option[Seq[Long]] = {
    val mapStats = shuffleStage.get.mapStats
    if (!aqeReader.isLocalRead && mapStats.isDefined) {
      Some(aqeReader.partitionSpecs.zipWithIndex.map {
        case (p: CoalescedPartitionSpec, partition) =>
          if (!isAQEShuffleRead) {
            // The partition specs are dummy and `p.datasSize` is not set.
            // Get the data size from mapStats.
            mapStats.get.bytesByPartitionId(partition)
          } else {
            assert(p.dataSize.isDefined)
            p.dataSize.get
          }
        case (p: PartialReducerPartitionSpec, _) => p.dataSize
        case (p, _) => throw new IllegalStateException(s"unexpected $p")
      })
    } else {
      None
    }
  }

  private def sendDriverMetrics(): Unit = {
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    val driverAccumUpdates = ArrayBuffer.empty[(Long, Long)]

    val numPartitionsMetric = metrics("numPartitions")
    numPartitionsMetric.set(aqeReader.partitionSpecs.length)
    driverAccumUpdates += (numPartitionsMetric.id -> aqeReader.partitionSpecs.length.toLong)

    if (aqeReader.hasSkewedPartition) {
      val skewedSpecs =
        aqeReader.partitionSpecs.collect { case p: PartialReducerPartitionSpec => p }

      val skewedPartitions = metrics("numSkewedPartitions")
      val skewedSplits = metrics("numSkewedSplits")

      val numSkewedPartitions = skewedSpecs.map(_.reducerIndex).distinct.length
      val numSplits = skewedSpecs.length

      skewedPartitions.set(numSkewedPartitions)
      driverAccumUpdates += (skewedPartitions.id -> numSkewedPartitions)

      skewedSplits.set(numSplits)
      driverAccumUpdates += (skewedSplits.id -> numSplits)
    }

    if (aqeReader.hasCoalescedPartition) {
      val numCoalescedPartitionsMetric = metrics("numCoalescedPartitions")
      val x = aqeReader.partitionSpecs.count(isCoalescedSpec)
      numCoalescedPartitionsMetric.set(x)
      driverAccumUpdates += numCoalescedPartitionsMetric.id -> x
    }

    partitionDataSizes.foreach {
      dataSizes =>
        val partitionDataSizeMetrics = metrics("partitionDataSize")
        driverAccumUpdates ++= dataSizes.map(partitionDataSizeMetrics.id -> _)
        // Set sum value to "partitionDataSize" metric.
        partitionDataSizeMetrics.set(dataSizes.sum)
    }

    SQLMetrics.postDriverMetricsUpdatedByValue(sparkContext, executionId, driverAccumUpdates.toSeq)
  }

  private lazy val shuffleRDD: RDD[_] = {
    shuffleStage match {
      case Some(stage) =>
        sendDriverMetrics()
        stage.shuffle match {
          case columnarShuffle: ColumnarShuffleExchangeExec =>
            columnarShuffle.getShuffleRDD(aqeReader.partitionSpecs.toArray, executionMode)
          case _ =>
            stage.shuffle.getShuffleRDD(aqeReader.partitionSpecs.toArray)
        }
      case _ =>
        throw new IllegalStateException("operating on canonicalized plan")
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    shuffleRDD.asInstanceOf[RDD[InternalRow]]
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    shuffleRDD.asInstanceOf[RDD[ColumnarBatch]]
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarAQEShuffleReadExec = {
    if (isAQEShuffleRead) {
      copy(delegate =
        Left(delegate.left.get.withNewChildren(Seq(newChild)).asInstanceOf[AQEShuffleReadExec]))
    } else {
      copy(delegate =
        Right(
          delegate.right.get.withNewChildren(Seq(newChild)).asInstanceOf[ShuffleQueryStageExec]))
    }
  }
}

object ColumnarAQEShuffleReadExec {
  private def wrapQueryStageWithDummyPartitionSpecs(
      queryStageExec: ShuffleQueryStageExec)
      : AQEShuffleReadExec = {
    // Create CoalescedPartitionSpec for each partition.
    val partitionSpecs =
      Array.tabulate(queryStageExec.shuffle.numPartitions)(i => CoalescedPartitionSpec(i, i + 1))
    AQEShuffleReadExec(queryStageExec, partitionSpecs)
  }
}

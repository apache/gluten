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
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A wrapper of AQEShuffleReadExec. It is used to wrap the AQEShuffleReadExec or
 * ShuffleQueryStageExec if executionMode is set by the planner.
 *
 * @param delegate
 *   AQEShuffleReadExec, ShuffleQueryStageExec, or (during canonicalization) ShuffleExchange.
 * @param executionMode
 *   The execution mode of the current AQE stage.
 */
case class ColumnarAQEShuffleReadExec(
    delegate: SparkPlan,
    executionMode: StageExecutionMode) extends UnaryExecNode {

  override def nodeName: String = s"ColumnarAQEShuffleRead(${executionMode.name})"

  override def supportsColumnar: Boolean = true

  override def child: SparkPlan = delegate match {
    case AQEShuffleReadExec(c, _) => c
    case _ => delegate
  }

  override def output: Seq[Attribute] = delegate.output

  override lazy val outputPartitioning: Partitioning = delegate.outputPartitioning

  override protected def stringArgs: Iterator[Any] = {
    delegate match {
      case a: AQEShuffleReadExec => a.stringArgs
      case _ => super.stringArgs
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarAQEShuffleReadExec = {
    delegate match {
      case a: AQEShuffleReadExec => copy(delegate = a.withNewChildren(Seq(newChild)))
      case _ => copy(delegate = newChild)
    }
  }

  private lazy val aqeReader: AQEShuffleReadExec = {
    delegate match {
      case a: AQEShuffleReadExec => a
      case s: ShuffleQueryStageExec =>
        // Wrap ShuffleQueryStageExec with dummy PartitionSpecs by creating CoalescedPartitionSpec
        // for each partition.
        val partitionSpecs =
          Array.tabulate(s.shuffle.numPartitions)(i => CoalescedPartitionSpec(i, i + 1))
        AQEShuffleReadExec(s, partitionSpecs)
      case _ =>
        // The child is Exchange during canonicalization.
        throw new IllegalStateException(
          s"Cannot get aqeReader from delegate node ${delegate.nodeName}.")
    }
  }

  @transient override lazy val metrics: Map[String, SQLMetric] = aqeReader.metrics

  private def shuffleStage = {
    val method = classOf[AQEShuffleReadExec].getDeclaredMethod("shuffleStage")
    method.setAccessible(true)
    method.invoke(aqeReader).asInstanceOf[Option[ShuffleQueryStageExec]]
  }

  private def sendDriverMetrics(): Unit = {
    val method = classOf[AQEShuffleReadExec].getDeclaredMethod("sendDriverMetrics")
    method.setAccessible(true)
    method.invoke(aqeReader)
  }

  private lazy val shuffleRDD: RDD[_] = {
    shuffleStage match {
      case Some(stage) =>
        // Only send driver metrics if it's a wrapper for AQEShuffleRead.
        if (delegate.isInstanceOf[AQEShuffleReadExec]) {
          sendDriverMetrics()
        }
        stage.shuffle match {
          case columnarShuffle: ColumnarShuffleExchangeExec =>
            columnarShuffle.getShuffleRDD(aqeReader.partitionSpecs.toArray, executionMode)
          case _ =>
            throw new IllegalStateException("shuffle stage is not a ColumnarShuffleExchangeExec")
        }
      case _ =>
        throw new IllegalStateException("operating on canonicalized plan")
    }
  }

  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    shuffleRDD.asInstanceOf[RDD[ColumnarBatch]]
  }
}

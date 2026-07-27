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
 *   The AQEShuffleReadExec or ShuffleQueryStageExec.
 * @param executionMode
 *   The execution mode of the current AQE stage.
 */
case class ColumnarAQEShuffleReadExec(
    delegate: Either[AQEShuffleReadExec, ShuffleQueryStageExec],
    executionMode: StageExecutionMode) extends UnaryExecNode {

  override def nodeName: String = s"ColumnarAQEShuffleRead(${executionMode.name})"

  private val isAQEShuffleRead = delegate.isLeft

  private val aqeReader: AQEShuffleReadExec = {
    if (isAQEShuffleRead) {
      delegate.left.get
    } else {
      // Wrap ShuffleQueryStageExe with dummy PartitionSpecs.
      val queryStageExec = delegate.right.get
      // Create CoalescedPartitionSpec for each partition.
      val partitionSpecs =
        Array.tabulate(queryStageExec.shuffle.numPartitions)(i => CoalescedPartitionSpec(i, i + 1))
      AQEShuffleReadExec(queryStageExec, partitionSpecs)
    }
  }

  override def supportsColumnar: Boolean = true

  override def child: SparkPlan = aqeReader.child

  override def output: Seq[Attribute] = aqeReader.child.output

  override lazy val outputPartitioning: Partitioning = aqeReader.outputPartitioning

  override def stringArgs: Iterator[Any] = aqeReader.stringArgs

  @transient override lazy val metrics: Map[String, SQLMetric] = aqeReader.metrics

  private def isCoalescedSpec(spec: ShufflePartitionSpec) = {
    val method = classOf[AQEShuffleReadExec].getDeclaredMethod("isCoalescedSpec")
    method.setAccessible(true)
    method.invoke(aqeReader, spec).asInstanceOf[Boolean]
  }

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
        if (isAQEShuffleRead) {
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

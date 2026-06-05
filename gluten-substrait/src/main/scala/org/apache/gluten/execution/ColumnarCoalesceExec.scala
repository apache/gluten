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
import org.apache.gluten.extension.columnar.transition.Convention

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition, UnknownPartitioning}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class ColumnarCoalesceExec(numPartitions: Int, child: SparkPlan)
  extends UnaryExecNode
  with ValidatablePlan {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = {
    if (numPartitions == 1) SinglePartition else UnknownPartitioning(numPartitions)
  }

  override def batchType(): Convention.BatchType = BackendsApiManager.getSettings.primaryBatchType

  override def rowType(): Convention.RowType = Convention.RowType.None

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException()
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (numPartitions == 1 && child.executeColumnar().getNumPartitions < 1) {
      // Make sure we don't output an RDD with 0 partitions, when claiming that we have a
      // `SinglePartition`.
      new ColumnarCoalesceExec.EmptyRDDWithPartitions(sparkContext, numPartitions)
    } else {
      child.executeColumnar().coalesce(numPartitions, shuffle = false)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarCoalesceExec =
    copy(child = newChild)

}

object ColumnarCoalesceExec {
  class EmptyRDDWithPartitions(@transient private val sc: SparkContext, numPartitions: Int)
    extends RDD[ColumnarBatch](sc, Nil) {

    override def getPartitions: Array[Partition] =
      Array.tabulate(numPartitions)(i => EmptyPartition(i))

    override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
      Iterator.empty
    }
  }

  case class EmptyPartition(index: Int) extends Partition
}

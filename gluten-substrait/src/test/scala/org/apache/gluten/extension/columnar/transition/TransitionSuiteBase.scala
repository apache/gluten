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
package org.apache.gluten.extension.columnar.transition

import org.apache.gluten.execution.{ColumnarToColumnarExec, GlutenColumnarToColumnarTransition, GlutenPlan}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{BinaryExecNode, ColumnarToRowTransition, LeafExecNode, RowToColumnarTransition, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch

trait TransitionSuiteBase {}

object TransitionSuiteBase {
  case class BatchLeaf(override val batchType: Convention.BatchType)
    extends LeafExecNode
    with GlutenPlan {
    override def rowType(): Convention.RowType = Convention.RowType.None

    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()

    override def output: Seq[Attribute] = List.empty
  }

  case class BatchUnary(override val batchType: Convention.BatchType, override val child: SparkPlan)
    extends UnaryExecNode
    with GlutenPlan {
    override def rowType(): Convention.RowType = Convention.RowType.None

    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
      copy(child = newChild)

    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()

    override def output: Seq[Attribute] = child.output
  }

  case class BatchBinary(
      override val batchType: Convention.BatchType,
      override val left: SparkPlan,
      override val right: SparkPlan)
    extends BinaryExecNode
    with GlutenPlan {
    override def rowType(): Convention.RowType = Convention.RowType.None

    override protected def withNewChildrenInternal(
        newLeft: SparkPlan,
        newRight: SparkPlan): SparkPlan = copy(left = newLeft, right = newRight)

    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()

    override def output: Seq[Attribute] = left.output ++ right.output
  }

  case class RowLeaf(override val rowType: Convention.RowType)
    extends LeafExecNode
    with GlutenPlan {
    override def batchType(): Convention.BatchType = Convention.BatchType.None

    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()

    override def output: Seq[Attribute] = List.empty
  }

  case class RowUnary(override val rowType: Convention.RowType, override val child: SparkPlan)
    extends UnaryExecNode
    with GlutenPlan {
    override def batchType(): Convention.BatchType = Convention.BatchType.None

    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
      copy(child = newChild)

    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()

    override def output: Seq[Attribute] = child.output
  }

  case class RowBinary(
      override val rowType: Convention.RowType,
      override val left: SparkPlan,
      override val right: SparkPlan)
    extends BinaryExecNode
    with GlutenPlan {
    override def batchType(): Convention.BatchType = Convention.BatchType.None

    override protected def withNewChildrenInternal(
        newLeft: SparkPlan,
        newRight: SparkPlan): SparkPlan = copy(left = newLeft, right = newRight)

    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()

    override def output: Seq[Attribute] = left.output ++ right.output
  }

  case class RowToBatch(
      fromRowType: Convention.RowType,
      toBatchType: Convention.BatchType,
      override val child: SparkPlan)
    extends RowToColumnarTransition
    with GlutenPlan {
    override def batchType(): Convention.BatchType = toBatchType
    override def rowType(): Convention.RowType = Convention.RowType.None
    override def requiredChildConvention(): Seq[ConventionReq] = {
      List(ConventionReq.ofRow(ConventionReq.RowType.Is(fromRowType)))
    }

    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
      copy(child = newChild)
    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = child.output
  }

  case class BatchToRow(
      fromBatchType: Convention.BatchType,
      toRowType: Convention.RowType,
      override val child: SparkPlan)
    extends ColumnarToRowTransition
    with GlutenPlan {
    override def batchType(): Convention.BatchType = Convention.BatchType.None
    override def rowType(): Convention.RowType = toRowType
    override def requiredChildConvention(): Seq[ConventionReq] = {
      List(ConventionReq.ofBatch(ConventionReq.BatchType.Is(fromBatchType)))
    }

    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
      copy(child = newChild)
    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = child.output
  }

  case class BatchToBatch(
      override val from: Convention.BatchType,
      override val to: Convention.BatchType,
      override val child: SparkPlan)
    extends ColumnarToColumnarExec(child)
    with GlutenColumnarToColumnarTransition {
    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
      copy(child = newChild)
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override protected def mapIterator(in: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] =
      throw new UnsupportedOperationException()
  }

  case class RowToRow(
      from: Convention.RowType,
      to: Convention.RowType,
      override val child: SparkPlan)
    extends UnaryExecNode
    with GlutenPlan {
    override def batchType(): Convention.BatchType = Convention.BatchType.None
    override def rowType(): Convention.RowType = to
    override def requiredChildConvention(): Seq[ConventionReq] = {
      List(ConventionReq.ofRow(ConventionReq.RowType.Is(from)))
    }
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = child.output
    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
      copy(child = newChild)
  }
}

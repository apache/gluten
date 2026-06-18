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
import org.apache.gluten.expression.ExpressionConverter
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.{JoinParams, SubstraitContext}
import org.apache.gluten.substrait.rel.RelBuilder
import org.apache.gluten.utils.SubstraitUtil

import org.apache.spark.{Dependency, NarrowDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.joins.BaseJoinExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

import io.substrait.proto.CrossRel

import java.io.{IOException, ObjectOutputStream}

/**
 * A bridge to connect left/right children and [[CartesianProductExecTransformer]] to avoid wrapping
 * them into one [[WholeStageTransformer]]. The reason is that, [[WholeStageTransformer]] always
 * assume the input partitions are same, but it is not true for `CartesianProductExec`.
 */
case class ColumnarCartesianProductBridge(child: SparkPlan) extends UnaryExecNode with GlutenPlan {
  override def output: Seq[Attribute] = child.output
  override def batchType(): Convention.BatchType = BackendsApiManager.getSettings.primaryBatchType
  override def rowType(): Convention.RowType = Convention.RowType.None
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException()
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = child.executeColumnar()
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

case class CartesianProductExecTransformer(
    left: SparkPlan,
    right: SparkPlan,
    condition: Option[Expression])
  extends BaseJoinExec
  with TransformSupport {

  override def joinType: JoinType = Inner

  override def leftKeys: Seq[Expression] = Nil

  override def rightKeys: Seq[Expression] = Nil

  protected lazy val substraitJoinType: CrossRel.JoinType =
    SubstraitUtil.toCrossRelSubstrait(joinType)

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance.genNestedLoopJoinTransformerMetrics(sparkContext)

  override def metricsUpdater(): MetricsUpdater = {
    BackendsApiManager.getMetricsApiInstance.genNestedLoopJoinTransformerMetricsUpdater(metrics)
  }

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val leftPlanContext = left.asInstanceOf[TransformSupport].transform(context)
    val (inputLeftRelNode, inputLeftOutput) =
      (leftPlanContext.root, leftPlanContext.outputAttributes)

    val rightPlanContext = right.asInstanceOf[TransformSupport].transform(context)
    val (inputRightRelNode, inputRightOutput) =
      (rightPlanContext.root, rightPlanContext.outputAttributes)

    val expressionNode =
      condition.map {
        SubstraitUtil.toSubstraitExpression(_, inputLeftOutput ++ inputRightOutput, context)
      }

    val extensionNode =
      JoinUtils.createExtensionNode(inputLeftOutput ++ inputRightOutput, validation = false)

    val operatorId = context.nextOperatorId(this.nodeName)
    val joinParams = new JoinParams
    joinParams.postProjectionNeeded = false
    if (condition.isDefined) {
      joinParams.isWithCondition = true
    }

    val currRel = RelBuilder.makeCrossRel(
      inputLeftRelNode,
      inputRightRelNode,
      substraitJoinType,
      expressionNode.orNull,
      extensionNode,
      context,
      operatorId
    )

    context.registerJoinParam(operatorId, joinParams)

    TransformContext(output, currRel)
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (
      !BackendsApiManager.getSettings.supportCartesianProductExecWithCondition() &&
      condition.nonEmpty
    ) {
      return ValidationResult.failed(
        "CartesianProductExecTransformer with condition is not supported in this backend.")
    }

    if (!BackendsApiManager.getSettings.supportCartesianProductExec()) {
      return ValidationResult.failed("Cartesian product is not supported in this backend")
    }
    val substraitContext = new SubstraitContext
    val expressionNode = condition.map {
      expr =>
        ExpressionConverter
          .replaceWithExpressionTransformer(expr, left.output ++ right.output)
          .doTransform(substraitContext)
    }
    val extensionNode =
      JoinUtils.createExtensionNode(left.output ++ right.output, validation = true)

    val currRel = RelBuilder.makeCrossRel(
      null,
      null,
      substraitJoinType,
      expressionNode.orNull,
      extensionNode,
      substraitContext,
      substraitContext.nextOperatorId(this.nodeName)
    )
    doNativeValidation(substraitContext, currRel)
  }

  override def output: Seq[Attribute] = left.output ++ right.output

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): CartesianProductExecTransformer =
    copy(left = newLeft, right = newRight)

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    val leftRDD = getColumnarInputRDDs(left).head
    val rightRDD = getColumnarInputRDDs(right).head
    new CartesianColumnarBatchRDD(sparkContext, leftRDD, rightRDD) :: Nil
  }
}

/** The [[Partition]] used by [[CartesianColumnarBatchRDD]]. */
class CartesianColumnarBatchRDDPartition(
    idx: Int,
    @transient private val rdd1: RDD[_],
    @transient private val rdd2: RDD[_],
    s1Index: Int,
    s2Index: Int
) extends Partition {
  var s1: Partition = rdd1.partitions(s1Index)
  var s2: Partition = rdd2.partitions(s2Index)
  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = {
    // Update the reference to parent split at the time of task serialization
    s1 = rdd1.partitions(s1Index)
    s2 = rdd2.partitions(s2Index)
    oos.defaultWriteObject()
  }
}

/**
 * [[CartesianColumnarBatchRDD]] is the columnar version of [[org.apache.spark.rdd.CartesianRDD]].
 */
class CartesianColumnarBatchRDD(
    sc: SparkContext,
    var rdd1: RDD[ColumnarBatch],
    var rdd2: RDD[ColumnarBatch])
  extends RDD[ColumnarBatch](sc, Nil) {
  private val numPartitionsInRdd2 = rdd2.partitions.length

  override def getPartitions: Array[Partition] = {
    // create the cross product split
    val array = new Array[Partition](rdd1.partitions.length * rdd2.partitions.length)
    for (s1 <- rdd1.partitions; s2 <- rdd2.partitions) {
      val idx = s1.index * numPartitionsInRdd2 + s2.index
      array(idx) = new CartesianColumnarBatchRDDPartition(idx, rdd1, rdd2, s1.index, s2.index)
    }
    array
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val currSplit = split.asInstanceOf[CartesianColumnarBatchRDDPartition]
    (rdd1.preferredLocations(currSplit.s1) ++ rdd2.preferredLocations(currSplit.s2)).distinct
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {

    /**
     * Cartesian RDD returns both left and right RDD iterators. Due to the override method
     * signature, it is not possible to return Seq from here. see [getIterators] in
     * [[ColumnarInputRDDsWrapper]]
     */
    throw new IllegalStateException("Never reach here")
  }

  override def getDependencies: Seq[Dependency[_]] = List(
    new NarrowDependency(rdd1) {
      def getParents(id: Int): Seq[Int] = List(id / numPartitionsInRdd2)
    },
    new NarrowDependency(rdd2) {
      def getParents(id: Int): Seq[Int] = List(id % numPartitionsInRdd2)
    }
  )

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
  }

  def getIterators(
      split: CartesianColumnarBatchRDDPartition,
      context: TaskContext): Seq[Iterator[ColumnarBatch]] = {
    rdd1.iterator(split.s1, context) ::
      rdd2.iterator(split.s2, context) :: Nil
  }
}

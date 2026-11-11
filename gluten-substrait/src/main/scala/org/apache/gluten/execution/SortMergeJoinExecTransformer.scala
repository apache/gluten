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
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.{JoinParams, SubstraitContext}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.joins.BaseJoinExec
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.protobuf.{Any, StringValue}
import io.substrait.proto.JoinRel

trait MergeJoinLikeExecTransformer
  extends BaseJoinExec
  with TransformSupport
  with ColumnarShuffledJoin {}
abstract class SortMergeJoinExecTransformerBase(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isSkewJoin: Boolean = false,
    projectList: Seq[NamedExpression] = null)
  extends MergeJoinLikeExecTransformer {
  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genSortMergeJoinTransformerMetrics(sparkContext)

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genSortMergeJoinTransformerMetricsUpdater(metrics)

  protected def needSwitchChildren: Boolean = false

  lazy val (bufferedKeys, streamedKeys, bufferedPlan, streamedPlan) =
    if (needSwitchChildren) (leftKeys, rightKeys, left, right)
    else (rightKeys, leftKeys, right, left)

  override def verboseStringWithOperatorId(): String = {
    val joinCondStr = if (condition.isDefined) {
      s"${condition.get}"
    } else "None"
    s"""
       |(${ExplainUtils.getOpId(this)}) $nodeName
       |${ExplainUtils.generateFieldString("Left keys", leftKeys)}
       |${ExplainUtils.generateFieldString("Right keys", rightKeys)}
       |${ExplainUtils.generateFieldString("Join condition", joinCondStr)}
    """.stripMargin
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    requiredOrders(leftKeys) :: requiredOrders(rightKeys) :: Nil

  override def outputOrdering: Seq[SortOrder] = joinType match {
    // For inner like join, orders of both sides keys should be kept.
    case _: InnerLike =>
      val leftKeyOrdering = getKeyOrdering(leftKeys, left.outputOrdering)
      val rightKeyOrdering = getKeyOrdering(rightKeys, right.outputOrdering)
      leftKeyOrdering.zip(rightKeyOrdering).map {
        case (lKey, rKey) =>
          // Also add the right key and its `sameOrderExpressions`
          val sameOrderExpressions = ExpressionSet(lKey.sameOrderExpressions ++ rKey.children)
          SortOrder(lKey.child, Ascending, sameOrderExpressions.toSeq)
      }
    // For left and right outer joins, the output is ordered by the streamed input's join keys.
    case LeftOuter => getKeyOrdering(leftKeys, left.outputOrdering)
    case RightOuter => getKeyOrdering(rightKeys, right.outputOrdering)
    // There are null rows in both streams, so there is no order.
    case FullOuter => Nil
    case LeftExistence(_) => getKeyOrdering(leftKeys, left.outputOrdering)
    case x =>
      throw new IllegalArgumentException(
        s"${getClass.getSimpleName} should not take $x as the JoinType")
  }

  private def getKeyOrdering(
      keys: Seq[Expression],
      childOutputOrdering: Seq[SortOrder]): Seq[SortOrder] = {
    val requiredOrdering = requiredOrders(keys)
    if (SortOrder.orderingSatisfies(childOutputOrdering, requiredOrdering)) {
      keys.zip(childOutputOrdering).map {
        case (key, childOrder) =>
          val sameOrderExpressionsSet = ExpressionSet(childOrder.children) - key
          SortOrder(key, Ascending, sameOrderExpressionsSet.toSeq)
      }
    } else {
      requiredOrdering
    }
  }

  private def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] = {
    // This must be ascending in order to agree with the `keyOrdering` defined in `doExecute()`.
    keys.map(SortOrder(_, Ascending))
  }

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    getColumnarInputRDDs(streamedPlan) ++ getColumnarInputRDDs(bufferedPlan)
  }

  def genJoinParameters(): Any = {
    val (isSMJ, isNullAwareAntiJoin) = (1, 0)
    // Start with "JoinParameters:"
    val joinParametersStr = new StringBuffer("JoinParameters:")
    // isSMJ: 0 for SHJ, 1 for SMJ
    // isNullAwareAntiJoin: 0 for false, 1 for true
    joinParametersStr
      .append("isSMJ=")
      .append(isSMJ)
      .append("\n")
      .append("isNullAwareAntiJoin=")
      .append(isNullAwareAntiJoin)
      .append("\n")
      .append("isExistenceJoin=")
      .append(if (joinType.isInstanceOf[ExistenceJoin]) 1 else 0)
      .append("\n")
    val message = StringValue
      .newBuilder()
      .setValue(joinParametersStr.toString)
      .build()
    BackendsApiManager.getTransformerApiInstance.packPBMessage(message)
  }

  // Direct output order of substrait join operation
  protected lazy val substraitJoinType = joinType match {
    case _: InnerLike =>
      JoinRel.JoinType.JOIN_TYPE_INNER
    case FullOuter =>
      JoinRel.JoinType.JOIN_TYPE_OUTER
    case LeftOuter =>
      if (needSwitchChildren) JoinRel.JoinType.JOIN_TYPE_RIGHT
      else JoinRel.JoinType.JOIN_TYPE_LEFT
    case RightOuter =>
      if (needSwitchChildren) JoinRel.JoinType.JOIN_TYPE_LEFT
      else JoinRel.JoinType.JOIN_TYPE_RIGHT
    case LeftSemi =>
      JoinRel.JoinType.JOIN_TYPE_LEFT_SEMI
    case LeftAnti =>
      JoinRel.JoinType.JOIN_TYPE_LEFT_ANTI
    case _ =>
      // TODO: Support cross join with Cross Rel
      // TODO: Support existence join
      JoinRel.JoinType.UNRECOGNIZED
  }

  override protected def doValidateInternal(): ValidationResult = {
    val substraitContext = new SubstraitContext
    // Firstly, need to check if the Substrait plan for this operator can be successfully generated.
    if (substraitJoinType == JoinRel.JoinType.UNRECOGNIZED) {
      return ValidationResult
        .failed(s"Found unsupported join type of $joinType for substrait: $substraitJoinType")
    }
    val relNode = JoinUtils.createJoinRel(
      streamedKeys,
      bufferedKeys,
      condition,
      substraitJoinType,
      needSwitchChildren,
      joinType,
      genJoinParameters(),
      null,
      null,
      streamedPlan.output,
      bufferedPlan.output,
      substraitContext,
      substraitContext.nextOperatorId(this.nodeName),
      validation = true
    )
    // Then, validate the generated plan in native engine.
    doNativeValidation(substraitContext, relNode)
  }

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val streamedPlanContext = streamedPlan.asInstanceOf[TransformSupport].transform(context)
    val (inputStreamedRelNode, inputStreamedOutput) =
      (streamedPlanContext.root, streamedPlanContext.outputAttributes)

    val bufferedPlanContext = bufferedPlan.asInstanceOf[TransformSupport].transform(context)
    val (inputBuildRelNode, inputBuildOutput) =
      (bufferedPlanContext.root, bufferedPlanContext.outputAttributes)

    // Get the operator id of this Join.
    val operatorId = context.nextOperatorId(this.nodeName)

    val joinParams = new JoinParams
    if (JoinUtils.preProjectionNeeded(streamedKeys)) {
      joinParams.streamPreProjectionNeeded = true
    }
    if (JoinUtils.preProjectionNeeded(bufferedKeys)) {
      joinParams.buildPreProjectionNeeded = true
    }

    val joinRel = JoinUtils.createJoinRel(
      streamedKeys,
      bufferedKeys,
      condition,
      substraitJoinType,
      needSwitchChildren,
      joinType,
      genJoinParameters(),
      inputStreamedRelNode,
      inputBuildRelNode,
      inputStreamedOutput,
      inputBuildOutput,
      context,
      operatorId
    )

    context.registerJoinParam(operatorId, joinParams)
    TransformContext(output, joinRel)
  }
}

/** Performs a sort merge join of two child relations. */
case class SortMergeJoinExecTransformer(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isSkewJoin: Boolean = false,
    projectList: Seq[NamedExpression] = null,
    switchChildren: Boolean = false)
  extends SortMergeJoinExecTransformerBase(
    leftKeys,
    rightKeys,
    joinType,
    condition,
    left,
    right,
    isSkewJoin,
    projectList) {

  override protected def needSwitchChildren: Boolean = switchChildren

  override def stringArgs: Iterator[scala.Any] = super.stringArgs.toSeq.dropRight(1).iterator

  override protected def doValidateInternal(): ValidationResult = {
    super.doValidateInternal()
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): SortMergeJoinExecTransformer =
    copy(left = newLeft, right = newRight)
}

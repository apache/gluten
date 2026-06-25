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
import org.apache.gluten.utils.SubstraitUtil

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, InnerLike, JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{ExplainUtils, SparkPlan}
import org.apache.spark.sql.execution.joins.BaseJoinExec
import org.apache.spark.sql.execution.metric.SQLMetric

import com.google.protobuf.Any
import io.substrait.proto.CrossRel

abstract class BroadcastNestedLoopJoinExecTransformer(
    left: SparkPlan,
    right: SparkPlan,
    buildSide: BuildSide,
    joinType: JoinType,
    condition: Option[Expression])
  extends BaseJoinExec
  with TransformSupport {

  def joinBuildSide: BuildSide = buildSide

  override def simpleStringWithNodeId(): String = {
    val opId = ExplainUtils.getOpId(this)
    s"$nodeName $joinType $joinBuildSide ($opId)".trim
  }

  override def leftKeys: Seq[Expression] = Nil
  override def rightKeys: Seq[Expression] = Nil

  private lazy val substraitJoinType: CrossRel.JoinType =
    SubstraitUtil.toCrossRelSubstrait(joinType)

  // Unique ID for the build side.
  lazy val buildBroadcastTableId: String = buildPlan.id.toString

  // Hint substrait to switch the left and right,
  // since we assume always build right side in substrait.
  private lazy val needSwitchChildren: Boolean = buildSide match {
    case BuildLeft => true
    case BuildRight => false
  }

  protected lazy val (buildPlan, streamedPlan) = if (needSwitchChildren) {
    (left, right)
  } else {
    (right, left)
  }

  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance.genNestedLoopJoinTransformerMetrics(sparkContext)

  override def metricsUpdater(): MetricsUpdater = {
    BackendsApiManager.getMetricsApiInstance.genNestedLoopJoinTransformerMetricsUpdater(metrics)
  }

  override def output: Seq[Attribute] =
    JoinUtils.getDirectJoinOutputSeq(joinType, left.output, right.output, getClass.getSimpleName)

  override def outputPartitioning: Partitioning = buildSide match {
    case BuildLeft =>
      joinType match {
        case _: InnerLike => right.outputPartitioning
        case RightOuter => right.outputPartitioning
        case x =>
          throw new IllegalArgumentException(
            s"BroadcastNestedLoopJoin should not take $x as the JoinType with building left side")
      }
    case BuildRight =>
      joinType match {
        case _: InnerLike => left.outputPartitioning
        case LeftOuter | ExistenceJoin(_) => left.outputPartitioning
        case x =>
          throw new IllegalArgumentException(
            s"BroadcastNestedLoopJoin should not take $x as the JoinType with building right side")
      }
  }

  def genJoinParameters(): Any = Any.getDefaultInstance

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val streamedPlanContext = streamedPlan.asInstanceOf[TransformSupport].transform(context)
    val (inputStreamedRelNode, inputStreamedOutput) =
      (streamedPlanContext.root, streamedPlanContext.outputAttributes)

    val buildPlanContext = buildPlan.asInstanceOf[TransformSupport].transform(context)
    val (inputBuildRelNode, inputBuildOutput) =
      (buildPlanContext.root, buildPlanContext.outputAttributes)

    val operatorId = context.nextOperatorId(this.nodeName)
    val joinParams = new JoinParams
    if (condition.isDefined) {
      joinParams.isWithCondition = true
    }

    val crossRel = JoinUtils.createCrossRel(
      substraitJoinType,
      condition,
      inputStreamedRelNode,
      inputBuildRelNode,
      inputStreamedOutput,
      inputBuildOutput,
      context,
      operatorId,
      genJoinParameters()
    )

    context.registerJoinParam(operatorId, joinParams)

    val projectRelPostJoinRel = JoinUtils.createProjectRelPostJoinRel(
      needSwitchChildren,
      joinType,
      streamedPlan.output,
      buildPlan.output,
      context,
      operatorId,
      crossRel,
      inputStreamedOutput,
      inputBuildOutput
    )
    TransformContext(output, projectRelPostJoinRel)
  }

  def validateJoinTypeAndBuildSide(): ValidationResult = {
    val result = joinType match {
      case _: InnerLike | LeftOuter | RightOuter => ValidationResult.succeeded
      case ExistenceJoin(_) =>
        ValidationResult.succeeded
      case _ =>
        ValidationResult.failed(s"$joinType join is not supported with BroadcastNestedLoopJoin")
    }

    if (!result.ok()) {
      return result
    }

    (joinType, buildSide) match {
      case (LeftOuter, BuildLeft) | (RightOuter, BuildRight) | (ExistenceJoin(_), BuildLeft) =>
        ValidationResult.failed(s"$joinType join is not supported with $buildSide")
      case _ =>
        ValidationResult.succeeded
    }
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (substraitJoinType == CrossRel.JoinType.UNRECOGNIZED) {
      return ValidationResult.failed(
        s"$joinType join is not supported with BroadcastNestedLoopJoin")
    }

    val validateResult = validateJoinTypeAndBuildSide()
    if (!validateResult.ok()) {
      return validateResult
    }

    val substraitContext = new SubstraitContext

    val crossRel = JoinUtils.createCrossRel(
      substraitJoinType,
      condition,
      null,
      null,
      streamedPlan.output,
      buildPlan.output,
      substraitContext,
      substraitContext.nextOperatorId(this.nodeName),
      genJoinParameters(),
      validation = true
    )
    doNativeValidation(substraitContext, crossRel)
  }
}

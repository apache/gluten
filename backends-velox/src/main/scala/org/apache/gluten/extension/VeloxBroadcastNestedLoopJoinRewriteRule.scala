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
package org.apache.gluten.extension

import org.apache.gluten.config.VeloxConfig

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, IsNull, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.{FullOuter, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.physical.IdentityBroadcastMode
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan, UnionExec}
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, BroadcastExchangeLike, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec
import org.apache.spark.sql.types.BooleanType

/**
 * Rewrites `BroadcastNestedLoopJoinExec(FullOuter)` into a union of two nested-loop joins that
 * Velox already supports natively:
 *   1. left outer join to produce matches plus unmatched streamed-side rows
 *   2. an outer join with a synthetic non-null marker on the opposite side to identify unmatched
 *      broadcast-side rows without relying on data columns being non-null
 */
case class VeloxBroadcastNestedLoopJoinRewriteRule() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case bnlj: BroadcastNestedLoopJoinExec
        if bnlj.joinType == FullOuter && shouldRewriteFullOuter(bnlj) && conditionOffloadable(
          bnlj) && broadcastSideRelocatable(bnlj) =>
      rewriteFullOuter(bnlj)
  }

  /**
   * The rewrite reuses the original broadcast side in two roles at once: [[rewriteFullOuter]] keeps
   * it as the build (broadcast) side of `branchA`, while [[buildUnmatchedBroadcastSide]] calls
   * [[unwrapBroadcast]] on it and consumes the unwrapped subtree as a normal STREAMED input of
   * `branchB`. That is only safe when the broadcast side can be cleanly re-materialized as a
   * partitioned plan. Reject the rewrite otherwise, e.g. for the MERGE cardinality-check join (`ON
   * t.pk > s.pk` with an `autoBroadcastJoinThreshold = -1` broadcast of a reused `Union` source):
   * there the broadcast side does not unwrap to a plain partitioned subtree, so after the rewrite a
   * `ColumnarBroadcastExchangeExec` ends up in `branchB`'s streamed slot and is executed via
   * `ColumnarInputAdapter.doExecuteColumnar -> executeColumnar()`, which the broadcast exchange
   * does not support, crashing with `[INTERNAL_ERROR] ... has column support mismatch`.
   *
   * A broadcast side is considered relocatable only when:
   *   - it is an exclusively-owned broadcast, i.e. NOT a [[ReusedExchangeExec]] (a reused/shared
   *     exchange must not be turned into a streamed input); and
   *   - its unwrapped payload does not itself contain a nested broadcast, which would otherwise
   *     leak into the streamed position of `branchB`.
   */
  private def broadcastSideRelocatable(bnlj: BroadcastNestedLoopJoinExec): Boolean = {
    val broadcastSide = bnlj.buildSide match {
      case BuildLeft => bnlj.left
      case BuildRight => bnlj.right
    }
    isCleanRelocatableBroadcast(broadcastSide)
  }

  private def isCleanRelocatableBroadcast(plan: SparkPlan): Boolean = plan match {
    case stage: BroadcastQueryStageExec => isCleanRelocatableBroadcast(stage.plan)
    case _: ReusedExchangeExec => false
    case exchange: BroadcastExchangeLike => !containsBroadcast(exchange.child)
    case _ => false
  }

  private def containsBroadcast(plan: SparkPlan): Boolean =
    plan.exists {
      case _: BroadcastExchangeLike => true
      case _: BroadcastQueryStageExec => true
      case _: ReusedExchangeExec => true
      case _ => false
    }

  private def shouldRewriteFullOuter(bnlj: BroadcastNestedLoopJoinExec): Boolean = {
    val threshold = VeloxConfig.get.broadcastNestedLoopJoinFullOuterRewriteThreshold
    bnlj.logicalLink.collect {
      case join: Join =>
        val leftSize = join.left.stats.sizeInBytes
        val rightSize = join.right.stats.sizeInBytes
        leftSize >= 0 && rightSize >= 0 && leftSize <= threshold && rightSize <= threshold
    }.getOrElse(false)
  }

  private def extractChildLogicalSizes(
      bnlj: BroadcastNestedLoopJoinExec): Option[(BigInt, BigInt)] =
    for {
      leftLogical <- bnlj.left.logicalLink
      rightLogical <- bnlj.right.logicalLink
    } yield (leftLogical.stats.sizeInBytes, rightLogical.stats.sizeInBytes)

  private def conditionOffloadable(bnlj: BroadcastNestedLoopJoinExec): Boolean =
    bnlj.condition.exists {
      cond =>
        cond.references.exists(bnlj.left.outputSet.contains) &&
        cond.references.exists(bnlj.right.outputSet.contains)
    }

  private def rewriteFullOuter(bnlj: BroadcastNestedLoopJoinExec): SparkPlan = {
    val matchesAndStreamedUnmatched = bnlj.buildSide match {
      case BuildRight =>
        projectToOutput(
          BroadcastNestedLoopJoinExec(
            bnlj.left,
            bnlj.right,
            BuildRight,
            LeftOuter,
            bnlj.condition),
          bnlj.output)
      case BuildLeft =>
        projectToOutput(
          BroadcastNestedLoopJoinExec(
            bnlj.right,
            bnlj.left,
            BuildRight,
            LeftOuter,
            bnlj.condition),
          bnlj.output)
    }

    val unmatchedBroadcastRows = bnlj.buildSide match {
      case BuildRight =>
        buildUnmatchedBroadcastSide(
          unmatchedSide = bnlj.right,
          otherSide = bnlj.left,
          unmatchedSideIsLeft = false,
          condition = bnlj.condition,
          output = bnlj.output)
      case BuildLeft =>
        buildUnmatchedBroadcastSide(
          unmatchedSide = bnlj.left,
          otherSide = bnlj.right,
          unmatchedSideIsLeft = true,
          condition = bnlj.condition,
          output = bnlj.output)
    }

    val union = UnionExec(Seq(matchesAndStreamedUnmatched, unmatchedBroadcastRows))
    ProjectExec(
      union.output.zip(bnlj.output).map {
        case (childAttr, targetAttr) =>
          Alias(childAttr, targetAttr.name)(
            exprId = targetAttr.exprId,
            qualifier = targetAttr.qualifier,
            explicitMetadata = Some(targetAttr.metadata))
      },
      union
    )
  }

  private def buildUnmatchedBroadcastSide(
      unmatchedSide: SparkPlan,
      otherSide: SparkPlan,
      unmatchedSideIsLeft: Boolean,
      condition: Option[Expression],
      output: Seq[Attribute]): SparkPlan = {
    val unmatchedSideBase = unwrapBroadcast(unmatchedSide)
    val otherSideBase = unwrapBroadcast(otherSide)
    val markerAttr =
      AttributeReference("__gluten_bnlj_matched_build_side", BooleanType, nullable = false)()
    val markedOtherSide = ProjectExec(
      otherSideBase.output.map(attr => aliasTo(attr, attr)) :+
        Alias(Literal.TrueLiteral, markerAttr.name)(exprId = markerAttr.exprId),
      otherSideBase)
    val unmatchedJoin = if (unmatchedSideIsLeft) {
      BroadcastNestedLoopJoinExec(
        unmatchedSideBase,
        ensureBroadcast(markedOtherSide),
        BuildRight,
        LeftOuter,
        condition)
    } else {
      BroadcastNestedLoopJoinExec(
        ensureBroadcast(markedOtherSide),
        unmatchedSideBase,
        BuildLeft,
        RightOuter,
        condition)
    }
    val unmatchedOnly = FilterExec(IsNull(markerAttr), unmatchedJoin)
    val projected = if (unmatchedSideIsLeft) {
      output.zipWithIndex.map {
        case (targetAttr, idx) if idx < unmatchedSideBase.output.size =>
          aliasTo(unmatchedSideBase.output(idx), targetAttr)
        case (targetAttr, _) =>
          Alias(Literal.create(null, targetAttr.dataType), targetAttr.name)(
            exprId = targetAttr.exprId,
            qualifier = targetAttr.qualifier,
            explicitMetadata = Some(targetAttr.metadata))
      }
    } else {
      output.zipWithIndex.map {
        case (targetAttr, idx) if idx < otherSide.output.size =>
          Alias(Literal.create(null, targetAttr.dataType), targetAttr.name)(
            exprId = targetAttr.exprId,
            qualifier = targetAttr.qualifier,
            explicitMetadata = Some(targetAttr.metadata))
        case (targetAttr, idx) =>
          aliasTo(unmatchedSideBase.output(idx - otherSide.output.size), targetAttr)
      }
    }
    ProjectExec(projected, unmatchedOnly)
  }

  private def unwrapBroadcast(plan: SparkPlan): SparkPlan = plan match {
    case stage: BroadcastQueryStageExec => unwrapBroadcast(stage.plan)
    case reused: ReusedExchangeExec => unwrapBroadcast(reused.child)
    case exchange: BroadcastExchangeLike => exchange.child
    case other => other
  }

  private def ensureBroadcast(plan: SparkPlan): SparkPlan = plan match {
    case exchange: BroadcastExchangeLike => exchange
    case other => BroadcastExchangeExec(IdentityBroadcastMode, other)
  }

  private def projectToOutput(child: SparkPlan, output: Seq[Attribute]): ProjectExec = {
    val sourceByExprId = child.output.map(attr => attr.exprId -> attr).toMap
    ProjectExec(
      output.map(targetAttr => aliasTo(sourceByExprId(targetAttr.exprId), targetAttr)),
      child)
  }

  private def aliasTo(childAttr: Attribute, targetAttr: Attribute): NamedExpression = {
    Alias(childAttr, targetAttr.name)(
      exprId = targetAttr.exprId,
      qualifier = targetAttr.qualifier,
      explicitMetadata = Some(targetAttr.metadata))
  }
}

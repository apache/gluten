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

import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, ExtractSingleColumnNullAwareAntiJoin}
import org.apache.spark.sql.catalyst.plans.logical.{HintInfo, JoinHint}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}

/**
 * Strategy to capture join keys and context from logical plan before Spark's JoinSelection
 * transforms them. This strategy runs early in the planning phase to preserve the original join
 * keys and join context information before any transformations like rewriteKeyExpr.
 */
case class GlutenJoinKeysCapture() extends SparkStrategy {

  def apply(plan: LogicalPlan): Seq[SparkPlan] = {

    if (!plan.isInstanceOf[Join]) {
      return Nil
    }

    plan match {

      case ExtractEquiJoinKeys(
            joinType,
            leftKeys,
            rightKeys,
            condition,
            _,
            left,
            right,
            hint) =>
        // Set broadcast join context for the build side
        // This information will be used by BroadcastExchangeExec to build hash table
        val buildRight = chooseBuildRight(joinType, left, right, hint)
        val buildPlan = if (buildRight) right else left

        val contextInfo = BroadcastJoinContextInfo(
          joinType = joinType,
          buildRight = buildRight,
          isNullAwareAntiJoin = false,
          condition = condition,
          originalLeftKeys = leftKeys,
          originalRightKeys = rightKeys,
          buildOutputSet = buildPlan.outputSet
        )

        // Append context to both sides - support multiple joins using the same table
        val leftContexts = left.getTagValue(BroadcastJoinContextTag.BROADCAST_JOIN_CONTEXT)
          .getOrElse(Seq.empty)
        left.setTagValue(
          BroadcastJoinContextTag.BROADCAST_JOIN_CONTEXT,
          leftContexts :+ contextInfo)

        val rightContexts = right.getTagValue(BroadcastJoinContextTag.BROADCAST_JOIN_CONTEXT)
          .getOrElse(Seq.empty)
        right.setTagValue(
          BroadcastJoinContextTag.BROADCAST_JOIN_CONTEXT,
          rightContexts :+ contextInfo)

        Nil

      case j @ ExtractSingleColumnNullAwareAntiJoin(leftKeys, rightKeys) =>
        // Set broadcast join context for null-aware anti join
        val buildRight = true
        val buildPlan = j.right

        val contextInfo = BroadcastJoinContextInfo(
          joinType = j.joinType,
          buildRight = buildRight,
          isNullAwareAntiJoin = true,
          condition = j.condition,
          originalLeftKeys = leftKeys,
          originalRightKeys = rightKeys,
          buildOutputSet = buildPlan.outputSet
        )

        // Append context to both sides - support multiple joins using the same table
        val leftContexts = j.left.getTagValue(BroadcastJoinContextTag.BROADCAST_JOIN_CONTEXT)
          .getOrElse(Seq.empty)
        j.left.setTagValue(
          BroadcastJoinContextTag.BROADCAST_JOIN_CONTEXT,
          leftContexts :+ contextInfo)

        val rightContexts = j.right.getTagValue(BroadcastJoinContextTag.BROADCAST_JOIN_CONTEXT)
          .getOrElse(Seq.empty)
        j.right.setTagValue(
          BroadcastJoinContextTag.BROADCAST_JOIN_CONTEXT,
          rightContexts :+ contextInfo)

        Nil

      // For non-equi-join or other plan nodes, return Nil.
      case _ => Nil
    }
  }

  /**
   * Determine if we can build on the right side for this join type. This is a simplified version -
   * actual logic may be more complex.
   */
  private def chooseBuildRight(
      joinType: org.apache.spark.sql.catalyst.plans.JoinType,
      left: LogicalPlan,
      right: LogicalPlan,
      hint: JoinHint): Boolean = {
    import org.apache.spark.sql.catalyst.plans._
    val hintBuildRight = hintedBuildRight(hint)
    val canBuildLeft = joinType match {
      case _: InnerLike => true
      case RightOuter => true
      case _ => false
    }
    val canBuildRight = joinType match {
      case _: InnerLike => true
      case LeftOuter => true
      case LeftSemi => true
      case LeftAnti => true
      case ExistenceJoin(_) => true
      case _ => false
    }

    if (hintBuildRight.contains(true) && canBuildRight) {
      true
    } else if (hintBuildRight.contains(false) && canBuildLeft) {
      false
    } else if (canBuildRight && !canBuildLeft) {
      true
    } else if (canBuildLeft && !canBuildRight) {
      false
    } else if (canBuildLeft && canBuildRight) {
      right.stats.sizeInBytes <= left.stats.sizeInBytes
    } else {
      true
    }
  }

  private def hintedBuildRight(hint: JoinHint): Option[Boolean] = {
    def isBroadcast(hintInfo: HintInfo): Boolean = {
      hintInfo.strategy.exists(_.toString.equalsIgnoreCase("BROADCAST"))
    }

    val broadcastLeft = hint.leftHint.exists(isBroadcast)
    val broadcastRight = hint.rightHint.exists(isBroadcast)

    if (broadcastLeft && !broadcastRight) {
      Some(false)
    } else if (broadcastRight && !broadcastLeft) {
      Some(true)
    } else {
      None
    }
  }
}

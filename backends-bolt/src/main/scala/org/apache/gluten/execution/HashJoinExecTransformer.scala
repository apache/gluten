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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.vectorized.ColumnarBatch

import io.substrait.proto.JoinRel

case class ShuffledHashJoinExecTransformer(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isSkewJoin: Boolean)
  extends ShuffledHashJoinExecTransformerBase(
    leftKeys,
    rightKeys,
    joinType,
    buildSide,
    condition,
    left,
    right,
    isSkewJoin) {

  override protected lazy val substraitJoinType: JoinRel.JoinType = joinType match {
    case _: InnerLike =>
      JoinRel.JoinType.JOIN_TYPE_INNER
    case FullOuter =>
      JoinRel.JoinType.JOIN_TYPE_OUTER
    case LeftOuter =>
      if (needSwitchChildren) {
        JoinRel.JoinType.JOIN_TYPE_RIGHT
      } else {
        JoinRel.JoinType.JOIN_TYPE_LEFT
      }
    case RightOuter =>
      if (needSwitchChildren) {
        JoinRel.JoinType.JOIN_TYPE_LEFT
      } else {
        JoinRel.JoinType.JOIN_TYPE_RIGHT
      }
    case LeftSemi | ExistenceJoin(_) =>
      if (needSwitchChildren) {
        JoinRel.JoinType.JOIN_TYPE_RIGHT_SEMI
      } else {
        JoinRel.JoinType.JOIN_TYPE_LEFT_SEMI
      }
    case LeftAnti =>
      JoinRel.JoinType.JOIN_TYPE_LEFT_ANTI
    case _ =>
      JoinRel.JoinType.UNRECOGNIZED
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): ShuffledHashJoinExecTransformer =
    copy(left = newLeft, right = newRight)
}

case class BroadcastHashJoinExecTransformer(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isNullAwareAntiJoin: Boolean)
  extends BroadcastHashJoinExecTransformerBase(
    leftKeys,
    rightKeys,
    joinType,
    buildSide,
    condition,
    left,
    right,
    isNullAwareAntiJoin) {

  override protected lazy val substraitJoinType: JoinRel.JoinType = joinType match {
    case _: InnerLike =>
      JoinRel.JoinType.JOIN_TYPE_INNER
    case FullOuter =>
      JoinRel.JoinType.JOIN_TYPE_OUTER
    case LeftOuter | RightOuter =>
      // The right side is required to be used for building hash table in Substrait plan.
      // Therefore, for RightOuter Join, the left and right relations are exchanged and the
      // join type is reverted.
      JoinRel.JoinType.JOIN_TYPE_LEFT
    case LeftSemi | ExistenceJoin(_) =>
      JoinRel.JoinType.JOIN_TYPE_LEFT_SEMI
    case LeftAnti =>
      JoinRel.JoinType.JOIN_TYPE_LEFT_ANTI
    case _ =>
      // TODO: Support cross join with Cross Rel
      JoinRel.JoinType.UNRECOGNIZED
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): BroadcastHashJoinExecTransformer =
    copy(left = newLeft, right = newRight)

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    val streamedRDD = getColumnarInputRDDs(streamedPlan)
    val broadcast = buildPlan.executeBroadcast[BuildSideRelation]()
    val broadcastRDD = BoltBroadcastBuildSideRDD(sparkContext, broadcast)
    // FIXME: Do we have to make build side a RDD?
    streamedRDD :+ broadcastRDD
  }
}

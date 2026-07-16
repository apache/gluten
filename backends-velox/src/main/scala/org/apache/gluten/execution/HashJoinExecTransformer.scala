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

import org.apache.gluten.config.VeloxConfig
import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.GlutenDriverEndpoint
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.{BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.{ColumnarBuildSideRelation, SerializedHashTableBroadcastRelation, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.unsafe.UnsafeColumnarBuildSideRelation
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

  // Unique ID for the build side
  lazy val buildBroadcastTableId: String = buildPlan.id.toString

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
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    if (executionId != null) {
      GlutenDriverEndpoint.collectResources(executionId, buildBroadcastTableId)
    } else {
      logWarning(
        s"Cannot trace broadcast table data $buildBroadcastTableId" +
          s" because execution id is null." +
          s" Will clean up until expire time.")
    }

    val broadcast = buildPlan.executeBroadcast[BuildSideRelation]()
    val bloomFilterPushdownSize = if (VeloxConfig.get.hashProbeDynamicFilterPushdownEnabled) {
      VeloxConfig.get.hashProbeBloomFilterPushdownMaxSize
    } else {
      -1
    }

    val (filterBuildColumns: Array[String], filterPropagatesNulls: Boolean) = condition match {
      case Some(expr) =>
        val buildOutputSet = buildPlan.outputSet
        val cols: Array[String] = expr.references.toSeq.collect {
          case a: Attribute if buildOutputSet.contains(a) =>
            ConverterUtils.genColumnNameWithExprId(a)
        }.toArray
        val propagates = SparkShimLoader.getSparkShims.isNullIntolerant(expr)
        (cols, propagates)
      case None =>
        (Array.empty[String], false)
    }

    val context =
      BroadcastHashJoinContext(
        buildKeyExprs,
        substraitJoinType,
        buildSide == BuildRight,
        condition.isDefined,
        joinType.isInstanceOf[ExistenceJoin],
        buildPlan.output,
        filterBuildColumns,
        filterPropagatesNulls,
        buildBroadcastTableId,
        isNullAwareAntiJoin,
        bloomFilterPushdownSize,
        metrics.get("buildHashTableTime"),
        metrics.get("serializeHashTableTime"),
        metrics.get("deserializeHashTableTime"),
        metrics.get("serializedHashTableSize")
      )

    // Check the type of broadcast relation to determine the approach
    val broadcastRDD = broadcast.value match {
      case serializedRelation: SerializedHashTableBroadcastRelation =>
        joinParamsForMetrics.foreach(_.usesDriverSideSerializedHashTable = true)
        // Hash table was already built and serialized in BroadcastExchangeExec.
        // Reuse the existing broadcast variable (no re-broadcast).
        logInfo(
          s"Using pre-built serialized hash table from BroadcastExchangeExec " +
            s"for $buildBroadcastTableId")

        val rdd = VeloxSerializedBroadcastRDD(sparkContext, broadcast, context)

        // Update bloom filter metrics
        val (bloomFilterSize, dynamicFiltersProduced) = rdd.getBloomFilterMetrics
        metrics.get("bloomFilterBlocksByteSize").foreach(_.set(bloomFilterSize))
        metrics.get("hashProbeDynamicFiltersProduced").foreach(_.set(dynamicFiltersProduced))

        // Update size metric from the pre-built hash table
        val (_, sizeInBytes, _, _) = serializedRelation.getMetrics
        metrics.get("serializedHashTableSize").foreach(_.set(sizeInBytes))

        rdd

      case columnar: ColumnarBuildSideRelation =>
        joinParamsForMetrics.foreach(_.usesDriverSideSerializedHashTable = false)
        // Legacy path: ColumnarBuildSideRelation from BroadcastExchangeExec.
        // Hash table is built on each executor from broadcast data.
        val canOffload = columnar.offload

        if (!canOffload) {
          logWarning(
            s"Build side cannot be offloaded for $buildBroadcastTableId, " +
              "falling back to executor-side build")
        } else {
          logInfo(s"Using executor-side broadcast hash table build for $buildBroadcastTableId")
        }
        VeloxBroadcastBuildSideRDD(sparkContext, broadcast, context)

      case unsafe: UnsafeColumnarBuildSideRelation =>
        joinParamsForMetrics.foreach(_.usesDriverSideSerializedHashTable = false)
        // Similar to ColumnarBuildSideRelation
        val canOffload = unsafe.isOffload

        if (!canOffload) {
          logWarning(
            s"Build side cannot be offloaded for $buildBroadcastTableId, " +
              "falling back to executor-side build")
        } else {
          logInfo(s"Using executor-side broadcast hash table build for $buildBroadcastTableId")
        }
        VeloxBroadcastBuildSideRDD(sparkContext, broadcast, context)

      case other =>
        joinParamsForMetrics.foreach(_.usesDriverSideSerializedHashTable = false)
        // Fallback for unknown types
        logWarning(
          s"Unknown broadcast relation type: ${other.getClass.getName}, " +
            "using executor-side build")
        VeloxBroadcastBuildSideRDD(sparkContext, broadcast, context)
    }

    // FIXME: Do we have to make build side a RDD?
    streamedRDD :+ broadcastRDD
  }
}

case class BroadcastHashJoinContext(
    buildSideJoinKeys: Seq[Expression],
    substraitJoinType: JoinRel.JoinType,
    buildRight: Boolean,
    hasMixedFiltCondition: Boolean,
    isExistenceJoin: Boolean,
    buildSideStructure: Seq[Attribute],
    filterBuildColumns: Array[String],
    filterPropagatesNulls: Boolean,
    buildHashTableId: String,
    isNullAwareAntiJoin: Boolean = false,
    bloomFilterPushdownSize: Long,
    buildHashTableTimeMetric: Option[SQLMetric] = None,
    serializeHashTableTimeMetric: Option[SQLMetric] = None,
    deserializeHashTableTimeMetric: Option[SQLMetric] = None,
    serializedHashTableSizeMetric: Option[SQLMetric] = None) {
  def droppedDuplicates: Boolean = {
    !hasMixedFiltCondition && (
      substraitJoinType == JoinRel.JoinType.JOIN_TYPE_LEFT_SEMI ||
        substraitJoinType == JoinRel.JoinType.JOIN_TYPE_LEFT_ANTI
    )
  }
}

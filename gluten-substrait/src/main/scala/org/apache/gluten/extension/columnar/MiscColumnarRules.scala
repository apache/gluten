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
package org.apache.gluten.extension.columnar

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.extension.columnar.transition.{ColumnarToRowLike, Convention, Transitions}
import org.apache.gluten.extension.columnar.transition.Convention.BatchType
import org.apache.gluten.utils.PlanUtil

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortOrder}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftSemi}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, BroadcastExchangeLike, ShuffleExchangeLike}
import org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec
import org.apache.spark.sql.internal.SQLConf

object MiscColumnarRules {

  // Replaces all SubqueryBroadcastExec used by sub-queries with ColumnarSubqueryBroadcastExec.
  // This prevents query execution from being failed by fallen-back SubqueryBroadcastExec with
  // child plan with columnar output (e.g., an adaptive Spark plan that yields final plan that
  // is full-offloaded). ColumnarSubqueryBroadcastExec is both compatible with row-based and
  // columnar child plan so is always functional.
  case class RewriteSubqueryBroadcast() extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = {
      val out = plan.transformWithSubqueries {
        case p =>
          // Since https://github.com/apache/gluten/pull/1851.
          //
          // When AQE is on, the AQE sub-query cache should already be filled with
          // row-based SubqueryBroadcastExec for reusing. Thus we are doing the same
          // memorize-and-reuse work here for the replaced columnar version.
          val reuseRemoved = removeReuses(p)
          val replaced = replace(reuseRemoved)
          replaced
      }
      out
    }

    private def removeReuses(p: SparkPlan): SparkPlan = {
      val out = p.transformExpressions {
        case pe: ExecSubqueryExpression =>
          val newPlan = pe.plan match {
            case ReusedSubqueryExec(s: SubqueryBroadcastExec) =>
              // Remove ReusedSubqueryExec. We will re-create reuses in subsequent method
              // #replace.
              //
              // We assume only meeting reused sub-queries in AQE execution. When AQE is off,
              // Spark adds reuses only after applying columnar rules by preparation rule
              // ReuseExchangeAndSubquery.
              assert(s.child.isInstanceOf[AdaptiveSparkPlanExec])
              s
            case other =>
              other
          }
          pe.withNewPlan(newPlan)
      }
      out
    }

    private def replace(p: SparkPlan): SparkPlan = {
      val out = p.transformExpressions {
        case pe: ExecSubqueryExpression =>
          val newPlan = pe.plan match {
            case s: SubqueryBroadcastExec =>
              val columnarSubqueryBroadcast = toColumnarSubqueryBroadcast(s)
              val maybeReused = columnarSubqueryBroadcast.child match {
                case a: AdaptiveSparkPlanExec if SQLConf.get.subqueryReuseEnabled =>
                  val cached = a.context.subqueryCache.get(columnarSubqueryBroadcast.canonicalized)
                  if (cached.nonEmpty) {
                    // Reuse the one in cache.
                    ReusedSubqueryExec(cached.get)
                  } else {
                    // Place columnar sub-query broadcast into cache, then return it.
                    a.context.subqueryCache
                      .update(columnarSubqueryBroadcast.canonicalized, columnarSubqueryBroadcast)
                    columnarSubqueryBroadcast
                  }
                case _ =>
                  // We are not in AQE.
                  columnarSubqueryBroadcast
              }
              maybeReused
            case other => other
          }
          pe.withNewPlan(newPlan)
      }
      out
    }

    private def toColumnarBroadcastExchange(
        exchange: BroadcastExchangeExec): ColumnarBroadcastExchangeExec = {
      val newChild =
        Transitions.toBatchPlan(exchange.child, BackendsApiManager.getSettings.primaryBatchType)
      ColumnarBroadcastExchangeExec(exchange.mode, newChild)
    }

    private def toColumnarSubqueryBroadcast(
        from: SubqueryBroadcastExec): ColumnarSubqueryBroadcastExec = {
      val newChild = from.child match {
        case exchange: BroadcastExchangeExec =>
          toColumnarBroadcastExchange(exchange)
        case aqe: AdaptiveSparkPlanExec =>
          // Keeps the child if its is AQE even if its supportsColumnar == false.
          // ColumnarSubqueryBroadcastExec is compatible with both row-based
          // and columnar inputs.
          aqe
        case other => other
      }
      val SubqueryBroadcastExec(name, indices, buildKeys, _) = from
      ColumnarSubqueryBroadcastExec(name, indices, buildKeys, newChild)
    }
  }

  // Remove topmost columnar-to-row otherwise AQE throws error.
  // See: org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec#newQueryStage
  //
  // The rule is basically a workaround because of the limited compatibility between Spark's AQE
  // and columnar API.
  case class RemoveTopmostColumnarToRow(session: SparkSession, isAdaptiveContext: Boolean)
    extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = {
      if (!isAdaptiveContext) {
        // The rule only applies in AQE. If AQE is off the topmost C2R will be strictly required
        // by Spark.
        return plan
      }
      plan match {
        // See: org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec#newQueryStage
        case ColumnarToRowLike(child: ShuffleExchangeLike) => child
        case ColumnarToRowLike(child: BroadcastExchangeLike) => child
        // See: org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec#getFinalPhysicalPlan
        //  BroadQueryStageExec could be inside a C2R which may cause check failures. E.g.,
        //  org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec#doExecuteBroadcast
        // Note: To avoid the similar issue with AQE=off, we don't remove the C2R on
        //  ShuffleQueryStageExec. Also there is not check like the one for BroadcastQueryStageExec
        //  so it's safe to keep it.
        case ColumnarToRowLike(child: BroadcastQueryStageExec) => child
        case other => other
      }
    }
  }

  // `InMemoryTableScanExec` internally supports ColumnarToRow.
  case class RemoveGlutenTableCacheColumnarToRow(session: SparkSession) extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = plan.transformDown {
      case ColumnarToRowLike(child) if PlanUtil.isGlutenTableCache(child) =>
        child
    }
  }

  // Remove unnecessary bnlj like sql:
  //   ``` select l.* from l left semi join r; ```
  // The result always is left table.
  case class RemoveBroadcastNestedLoopJoin() extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
      case BroadcastNestedLoopJoinExec(
            left: SparkPlan,
            right: SparkPlan,
            buildSide: BuildSide,
            joinType: JoinType,
            condition: Option[Expression]) if condition.isEmpty && joinType == LeftSemi =>
        buildSide match {
          case BuildLeft => right
          case BuildRight => left
        }
    }
  }

  // Because of the hard-coded C2R removal code in
  // org.apache.spark.sql.execution.columnar.InMemoryRelation.convertToColumnarIfPossible
  // from Spark, This rule can be used when we have to make sure the columnar query plan
  // inside the C2R is recognizable by the user-specified columnar batch serializer.
  case class PreventBatchTypeMismatchInTableCache(
      isCalledByTableCachePlaning: Boolean,
      allowedBatchTypes: Set[BatchType])
    extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = {
      if (!isCalledByTableCachePlaning) {
        return plan
      }
      plan match {
        case c2r @ ColumnarToRowLike(columnarPlan: SparkPlan)
            if !allowedBatchTypes.contains(Convention.get(columnarPlan).batchType) =>
          // If the output batch type of 'columnarPlan' is not allowed (usually because it's not
          // supported by a user-specified columnar batch serializer),
          // We add a transparent row-based unary node to prevent the C2R from being removed by
          // Spark code in
          // org.apache.spark.sql.execution.columnar.InMemoryRelation.convertToColumnarIfPossible.
          ColumnarToRowRemovalGuard(c2r)
        case other => other
      }
    }
  }
}

// Having this unary node on the top of the query plan would prevent the c2r from being
// removed by Spark code in
// org.apache.spark.sql.execution.columnar.InMemoryRelation.convertToColumnarIfPossible.
case class ColumnarToRowRemovalGuard(c2r: SparkPlan) extends UnaryExecNode {
  override def supportsColumnar: Boolean = false
  override protected def doExecute(): RDD[InternalRow] = c2r.execute()
  override def doExecuteBroadcast[T](): Broadcast[T] = c2r.executeBroadcast()
  override def output: Seq[Attribute] = c2r.output
  override def outputPartitioning: Partitioning = c2r.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = c2r.outputOrdering
  override def child: SparkPlan = c2r
  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarToRowRemovalGuard =
    copy(c2r = newChild)
}

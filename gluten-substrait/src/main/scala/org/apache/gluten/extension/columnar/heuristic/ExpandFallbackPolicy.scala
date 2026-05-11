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
package org.apache.gluten.extension.columnar.heuristic

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.GlutenPlan
import org.apache.gluten.extension.columnar.{FallbackTag, FallbackTags}
import org.apache.gluten.extension.columnar.FallbackTags.add
import org.apache.gluten.extension.columnar.transition.{BackendTransitions, ColumnarToRowLike, RowToColumnarLike}
import org.apache.gluten.utils.PlanUtil

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.exchange.Exchange






// format: off
/**
 * Note, this rule should only fallback to row-based plan if there is no harm.
 * The follow case should be handled carefully
 *
 * 1. A BHJ and the previous broadcast exchange is columnar
 *    We should still make the BHJ columnar, otherwise it will fail if
 *    the vanilla BHJ accept a columnar broadcast exchange, e.g.,
 *
 *    Scan                Scan
 *      \                  |
 *        \     Columnar Broadcast Exchange
 *          \       /
 *             BHJ
 *              |
 *       VeloxColumnarToRow
 *              |
 *           Project (unsupport columnar)
 *
 * 2. The previous shuffle exchange stage is a columnar shuffle exchange
 *    We should use VeloxColumnarToRow rather than vanilla Spark ColumnarToRowExec, e.g.,
 *
 *             Scan
 *              |
 *    Columnar Shuffle Exchange
 *              |
 *       VeloxColumnarToRow
 *              |
 *           Project (unsupport columnar)
 *
 * @param isAdaptiveContext If is inside AQE
 * @param originalPlan The vanilla SparkPlan without apply gluten transform rules
 */
// format: on
case class ExpandFallbackPolicy(isAdaptiveContext: Boolean, originalPlan: SparkPlan)
  extends Rule[SparkPlan] {
  import ExpandFallbackPolicy._

  private def countTransitionCost(plan: SparkPlan): Int = {
    val ignoreRowToColumnar = GlutenConfig.get.fallbackIgnoreRowToColumnar
    var transitionCost = 0
    def countFallbackInternal(plan: SparkPlan): Unit = {
      plan match {
        case _: QueryStageExec => // Another stage.
        case _: CommandResultExec | _: ExecutedCommandExec => // ignore
        // we plan exchange to columnar exchange in columnar rules and the exchange does not
        // support columnar, so the output columnar is always false in AQE postStageCreationRules
        case ColumnarToRowLike(s: Exchange) if isAdaptiveContext =>
          countFallbackInternal(s)
        case u: UnaryExecNode
            if !PlanUtil.isGlutenColumnarOp(u) && PlanUtil.isGlutenTableCache(u.child) =>
          // Vanilla Spark plan will call `InMemoryTableScanExec.convertCachedBatchToInternalRow`
          // which is a kind of `ColumnarToRowExec`.
          transitionCost = transitionCost + 1
          countFallbackInternal(u.child)
        case ColumnarToRowLike(p: GlutenPlan) =>
          logDebug(s"Find a columnar to row for gluten plan:\n$p")
          transitionCost = transitionCost + 1
          countFallbackInternal(p)
        case RowToColumnarLike(child) =>
          if (!ignoreRowToColumnar) {
            transitionCost = transitionCost + 1
          }
          countFallbackInternal(child)
        case leafPlan: LeafExecNode if PlanUtil.isGlutenTableCache(leafPlan) =>
        case leafPlan: LeafExecNode if !PlanUtil.isGlutenColumnarOp(leafPlan) =>
          // Possible fallback for leaf node.
          transitionCost = transitionCost + 1
        case p => p.children.foreach(countFallbackInternal)
      }
    }
    countFallbackInternal(plan)
    transitionCost
  }

  // format: off
  /**
   * When making a stage fall back, it's possible that we need a ColumnarToRow to adapt to last
   * stage's columnar output. So we need to evaluate the cost, i.e., the number of required
   * ColumnarToRow between entirely fallback stage and last stage(s). Thus, we can avoid possible
   * performance degradation caused by fallback policy.
   *
   * Spark plan before applying fallback policy:
   *
   *        ColumnarExchange
   *  ----------- | --------------- last stage
   *    HashAggregateTransformer
   *              |
   *        ColumnarToRow
   *              |
   *           Project
   *
   * To illustrate the effect if cost is not taken into account, here is spark plan
   * after applying whole stage fallback policy (threshold = 1):
   *
   *        ColumnarExchange
   *  -----------  | --------------- last stage
   *         ColumnarToRow
   *               |
   *         HashAggregate
   *               |
   *            Project
   *
   *  So by considering the cost, the fallback policy will not be applied.
   */
  // format: on
  private def countStageFallbackTransitionCost(plan: SparkPlan): Int = {
    var stageFallbackTransitionCost = 0

    /**
     * 1) Find a Gluten plan whose child is InMemoryTableScanExec. Then, increase
     * stageFallbackTransitionCost if InMemoryTableScanExec is gluten's table cache and decrease
     * stageFallbackTransitionCost if not. 2) Find a Gluten plan whose child is QueryStageExec.
     * Then, increase stageFallbackTransitionCost if the last query stage's plan is GlutenPlan and
     * decrease stageFallbackTransitionCost if not.
     */
    def countStageFallbackTransitionCostInternal(plan: SparkPlan): Unit = {
      plan match {
        case glutenPlan: GlutenPlan =>
          val leaves = glutenPlan.collectLeaves()
          leaves
            .filter(_.isInstanceOf[InMemoryTableScanExec])
            .foreach {
              // For this case, table cache will internally execute ColumnarToRow if
              // we make the stage fall back.
              case tableCache if PlanUtil.isGlutenTableCache(tableCache) =>
                stageFallbackTransitionCost = stageFallbackTransitionCost + 1
              case _ =>
            }
          leaves
            .filter(_.isInstanceOf[QueryStageExec])
            .foreach {
              case stage: QueryStageExec
                  if PlanUtil.isGlutenColumnarOp(stage.plan) ||
                    // For TableCacheQueryStageExec since spark 3.5.
                    PlanUtil.isGlutenTableCache(stage) =>
                stageFallbackTransitionCost = stageFallbackTransitionCost + 1
              case _ =>
            }
        case _ => plan.children.foreach(countStageFallbackTransitionCostInternal)
      }
    }
    countStageFallbackTransitionCostInternal(plan)
    stageFallbackTransitionCost
  }

  private def fallback(plan: SparkPlan): FallbackInfo = {
    val fallbackThreshold = if (isAdaptiveContext) {
      GlutenConfig.get.wholeStageFallbackThreshold
    } else if (plan.find(_.isInstanceOf[AdaptiveSparkPlanExec]).isDefined) {
      // if we are here, that means we are now at `QueryExecution.preparations` and
      // AQE is actually not applied. We do nothing for this case, and later in
      // AQE we can check `wholeStageFallbackThreshold`.
      return FallbackInfo.DO_NOT_FALLBACK()
    } else {
      // AQE is not applied, so we use the whole query threshold to check if should fallback
      GlutenConfig.get.queryFallbackThreshold
    }
    if (fallbackThreshold < 0) {
      return FallbackInfo.DO_NOT_FALLBACK()
    }

    val transitionCost = countTransitionCost(plan)
    val fallbackTransitionCost = if (isAdaptiveContext) {
      countStageFallbackTransitionCost(plan)
    } else {
      0
    }
    val netTransitionCost = transitionCost - fallbackTransitionCost
    if (netTransitionCost >= fallbackThreshold) {
      FallbackInfo(
        Some(
          s"Fallback policy is taking effect, net transition cost: $netTransitionCost, " +
            s"cost: $transitionCost, fallback cost: $fallbackTransitionCost, " +
            s"threshold: $fallbackThreshold"),
        netTransitionCost
      )
    } else {
      FallbackInfo(netTransitionCost = netTransitionCost)
    }
  }

  private def fallbackToRowBasedPlan(glutenPlan: SparkPlan, outputsColumnar: Boolean): SparkPlan = {
    // Propagate fallback reason to vanilla SparkPlan
    glutenPlan.foreach {
      case _: GlutenPlan =>
      case p: SparkPlan if FallbackTags.nonEmpty(p) && p.logicalLink.isDefined =>
        originalPlan
          .find(_.logicalLink.exists(_.fastEquals(p.logicalLink.get)))
          .filterNot(FallbackTags.nonEmpty)
          .foreach(origin => FallbackTags.add(origin, FallbackTags.get(p)))
      case _ =>
    }

    val planWithTransitions = BackendTransitions.insert(originalPlan, outputsColumnar)
    planWithTransitions
  }

  private def countTransitionCostForVanillaSparkPlan(plan: SparkPlan): Int = {
    // Vanilla Spark should only contains `ColumnarToRowExec`
    plan.collect { case c: ColumnarToRowExec => c }.size
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    // By default, the outputsColumnar is always false.
    // The outputsColumnar will be true if it is a cached plan and we are going to
    // cache columnar batch using Gluten columnar serializer. So we should add a
    // Gluten RowToColumnar.
    val outputsColumnar = plan.supportsColumnar
    val fallbackInfo = fallback(plan)
    if (fallbackInfo.shouldFallback) {
      // If the transition cost of vanilla Spark plan is not smaller than Gluten plan,
      // then we prefer to use Gluten even if Gluten plan contains `ColumnarToRow`.
      // For example, use Gluten parquet scan rather than vanilla Spark parquet scan:
      //  Scan Parquet
      //       |
      //  ColumnarToRow
      val vanillaSparkPlan = fallbackToRowBasedPlan(plan, outputsColumnar)
      val vanillaSparkTransitionCost = countTransitionCostForVanillaSparkPlan(vanillaSparkPlan)
      if (
        GlutenConfig.get.fallbackPreferColumnar &&
        fallbackInfo.netTransitionCost <= vanillaSparkTransitionCost
      ) {
        plan
      } else {
        addFallbackTagsRecursively(
          vanillaSparkPlan,
          FallbackTag.Exclusive(fallbackInfo.reason.getOrElse("Unknown reason")))
        FallbackNode(vanillaSparkPlan)
      }
    } else {
      plan
    }
  }

  case class FallbackInfo(reason: Option[String] = None, netTransitionCost: Int = 0) {
    def shouldFallback: Boolean = reason.isDefined
  }

  object FallbackInfo {
    def DO_NOT_FALLBACK(): FallbackInfo = FallbackInfo()
  }
}

object ExpandFallbackPolicy {
  private def addFallbackTagsRecursively[T](plan: TreeNode[_], t: T)(implicit
      converter: FallbackTag.Converter[T]): Unit = {
    plan.foreach {
      case _: GlutenPlan => // ignore
      case other: TreeNode[_] => add(other, t)
    }
  }
}

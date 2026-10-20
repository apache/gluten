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

import org.apache.gluten.config.BoltConfig
import org.apache.gluten.execution._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.physical.ClusteredDistribution
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.EXCHANGE
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType}

/**
 * To transform regular aggregation to intermediate aggregation that internally enables
 * optimizations such as flushing and abandoning.
 */
case class FlushableHashAggregateRule(session: SparkSession) extends Rule[SparkPlan] {
  import FlushableHashAggregateRule._
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!BoltConfig.get.enableBoltFlushablePartialAggregation) {
      return plan
    }
    plan.transformUpWithPruning(_.containsPattern(EXCHANGE)) {
      case s: ShuffleExchangeLike =>
        // If an exchange follows a hash aggregate in which all functions are in partial mode,
        // then it's safe to convert the hash aggregate to flushable hash aggregate.
        val out = s.withNewChildren(
          List(
            replaceEligibleAggregates(s.child) {
              agg =>
                FlushableHashAggregateExecTransformer(
                  agg.requiredChildDistributionExpressions,
                  agg.groupingExpressions,
                  agg.aggregateExpressions,
                  agg.aggregateAttributes,
                  agg.initialInputBufferOffset,
                  agg.resultExpressions,
                  agg.child
                )
            }
          )
        )
        out
    }
  }

  private def aggregatesNotSupportFlush(aggExprs: Seq[AggregateExpression]): Boolean = {
    if (BoltConfig.get.floatingPointMode == "loose") {
      return false
    }

    def isFloatingPointType(dataType: DataType): Boolean = {
      dataType == DoubleType || dataType == FloatType
    }

    def isUnsupportedAggregation(aggExpr: AggregateExpression): Boolean = {
      aggExpr.aggregateFunction match {
        case Sum(child, _) if isFloatingPointType(child.dataType) => true
        case Average(child, _) if isFloatingPointType(child.dataType) => true
        case _ => false
      }
    }

    aggExprs.exists(isUnsupportedAggregation)
  }

  private def replaceEligibleAggregates(plan: SparkPlan)(
      func: RegularHashAggregateExecTransformer => SparkPlan): SparkPlan = {
    def transformDown: SparkPlan => SparkPlan = {
      case agg: RegularHashAggregateExecTransformer
          if !agg.aggregateExpressions.forall(p => p.mode == Partial || p.mode == PartialMerge) =>
        // Not a intermediate agg. Skip.
        agg
      case agg: RegularHashAggregateExecTransformer
          if isAggInputAlreadyDistributedWithAggKeys(agg) =>
        // Data already grouped by aggregate keys, Skip.
        agg
      case agg: RegularHashAggregateExecTransformer
          if aggregatesNotSupportFlush(agg.aggregateExpressions) =>
        agg
      case agg: RegularHashAggregateExecTransformer =>
        func(agg)
      case p if !canPropagate(p) => p
      case other => other.withNewChildren(other.children.map(transformDown))
    }

    val out = transformDown(plan)
    out
  }

  private def canPropagate(plan: SparkPlan): Boolean = plan match {
    case _: ProjectExecTransformer => true
    case _: BoltResizeBatchesExec => true
    case _ => false
  }
}

object FlushableHashAggregateRule {

  /**
   * If child output already partitioned by aggregation keys (this function returns true), we
   * usually avoid the optimization converting to flushable aggregation.
   *
   * For example, if input is hash-partitioned by keys (a, b) and aggregate node requests "group by
   * a, b, c", then the aggregate should NOT flush as the grouping set (a, b, c) will be created
   * only on a single partition among the whole cluster. Spark's planner may use this information to
   * perform optimizations like doing "partial_count(a, b, c)" directly on the output data.
   */
  private def isAggInputAlreadyDistributedWithAggKeys(
      agg: HashAggregateExecTransformer): Boolean = {
    if (agg.groupingExpressions.isEmpty) {
      // Empty grouping set () should not be satisfied by any partitioning patterns.
      //   E.g.,
      //   (a, b) satisfies (a, b, c)
      //   (a, b) satisfies (a, b)
      //   (a, b) doesn't satisfy (a)
      //   (a, b) doesn't satisfy ()
      return false
    }
    val distribution = ClusteredDistribution(agg.groupingExpressions)
    agg.child.outputPartitioning.satisfies(distribution)
  }
}

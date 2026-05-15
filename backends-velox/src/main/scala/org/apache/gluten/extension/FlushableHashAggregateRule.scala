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
import org.apache.gluten.execution._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.EXCHANGE
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType}

import scala.collection.mutable

/**
 * To transform regular aggregation to intermediate aggregation that internally enables
 * optimizations such as flushing and abandoning.
 */
case class FlushableHashAggregateRule(session: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!VeloxConfig.get.enableVeloxFlushablePartialAggregation) {
      return plan
    }
    val protectedAggs = collectProtectedOneDistinctPartialMergeAggs(plan)
    plan.transformUpWithPruning(_.containsPattern(EXCHANGE)) {
      case s: ShuffleExchangeLike =>
        // If an exchange follows a hash aggregate in which all functions are in partial mode,
        // then it's safe to convert the hash aggregate to flushable hash aggregate.
        val out = s.withNewChildren(
          List(replaceEligibleAggregates(s.child, protectedAggs))
        )
        out
    }
  }

  private def aggregatesNotSupportFlush(aggExprs: Seq[AggregateExpression]): Boolean = {
    if (VeloxConfig.get.floatingPointMode == "loose") {
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

  /**
   * Walks the plan downward, applying func to each RegularHashAggregateExecTransformer or
   * SortHashAggregateExecTransformer that is eligible for flushable conversion. An aggregate is
   * eligible when all expressions are Partial/PartialMerge, it is not the protected PartialMerge
   * aggregate directly below a distinct-partial aggregate, and no aggregate function disallows
   * flushing.
   */
  private def replaceEligibleAggregates(
      plan: SparkPlan,
      protectedAggs: mutable.Map[Int, HashAggregateExecTransformer]): SparkPlan = {
    def toFlushableAgg(agg: HashAggregateExecTransformer): FlushableHashAggregateExecTransformer = {
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

    def transformDown: SparkPlan => SparkPlan = {
      case agg: RegularHashAggregateExecTransformer
          if !agg.aggregateExpressions.forall(p => p.mode == Partial || p.mode == PartialMerge) =>
        // Not an intermediate agg. Skip.
        agg
      case agg: RegularHashAggregateExecTransformer
          if protectedAggs.contains(agg.id) =>
        // This is the PartialMerge aggregate directly below a distinct-partial aggregate in
        // Spark's one-distinct pipeline. Keep it non-flushable so the distinct step continues to
        // see globally de-duplicated (grouping + distinct) keys.
        agg
      case agg: RegularHashAggregateExecTransformer
          if aggregatesNotSupportFlush(agg.aggregateExpressions) =>
        // Aggregate uses a function that is unsafe to flush. Skip.
        agg
      case agg: RegularHashAggregateExecTransformer =>
        // All guards passed; replace with the flushable variant.
        toFlushableAgg(agg)
      case agg: SortHashAggregateExecTransformer
          if !agg.aggregateExpressions.forall(p => p.mode == Partial || p.mode == PartialMerge) =>
        // Not an intermediate agg. Skip.
        agg
      case agg: SortHashAggregateExecTransformer if protectedAggs.contains(agg.id) =>
        // See the RegularHashAggregateExecTransformer branch above.
        agg
      case agg: SortHashAggregateExecTransformer
          if aggregatesNotSupportFlush(agg.aggregateExpressions) =>
        // Aggregate uses a function that is unsafe to flush. Skip.
        agg
      case agg: SortHashAggregateExecTransformer =>
        // All guards passed; replace with the flushable variant.
        toFlushableAgg(agg)
      case exchange: ShuffleExchangeLike =>
        // Stop at the next exchange. This rule is applied from an exchange boundary and should not
        // continue rewriting into a different shuffle region.
        exchange
      case other => other.withNewChildren(other.children.map(transformDown))
    }

    val out = transformDown(plan)
    out
  }

  /**
   * Collect the PartialMerge aggregates that must stay regular in Spark's one-distinct aggregation
   * pipeline.
   *
   * Example plan shape:
   *
   * RegularHashAggregateExecTransformer [k] [count(distinct v)] // finalAggregate +-
   * RegularHashAggregateExecTransformer [k] [count(distinct v)] // partialDistinctAggregate +-
   * RegularHashAggregateExecTransformer [k, v] [count(...)] // partialMergeAggregate +-
   * ColumnarExchange hashpartitioning(k, v, 200) +- RegularHashAggregateExecTransformer [k, v]
   * [count(...)] // partialAggregate +- ...
   *
   * We walk every aggregate node and, when we encounter the `partialDistinctAggregate`, we record
   * its child `partialMergeAggregate` as protected.
   *
   * That `partialMergeAggregate` must stay regular. It is the step that materializes the
   * de-duplicated `(k, v)` stream consumed by the distinct-partial aggregate above it. If it
   * flushes, duplicate `(k, v)` keys may be reintroduced within one partition and the distinct
   * aggregation pipeline would no longer see the shape Spark planned for.
   */
  private def collectProtectedOneDistinctPartialMergeAggs(
      plan: SparkPlan): mutable.Map[Int, HashAggregateExecTransformer] = {
    val protectedAggs = mutable.HashMap.empty[Int, HashAggregateExecTransformer]
    plan.foreach {
      case agg: HashAggregateExecTransformer =>
        findProtectedPartialMergeAgg(agg).foreach {
          protectedAgg => protectedAggs.put(protectedAgg.id, protectedAgg)
        }
      case _ =>
    }
    protectedAggs
  }

  /** If this aggregate is the distinct-partial stage, return its child PartialMerge aggregate. */
  private def findProtectedPartialMergeAgg(
      distinctPartialAgg: HashAggregateExecTransformer): Option[HashAggregateExecTransformer] = {
    if (
      !distinctPartialAgg.aggregateExpressions.exists(
        expr => expr.isDistinct && expr.mode == Partial)
    ) {
      return None
    }

    for {
      partialMergeAgg <- asAggregate(distinctPartialAgg.child)
      if partialMergeAgg.aggregateExpressions.forall(_.mode == PartialMerge)
    } yield partialMergeAgg
  }

  private def asAggregate(plan: SparkPlan): Option[HashAggregateExecTransformer] = plan match {
    case agg: HashAggregateExecTransformer => Some(agg)
    case _ => None
  }
}

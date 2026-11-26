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

import org.apache.gluten.execution.HashAggregateExecBaseTransformer
import org.apache.gluten.extension.columnar.FallbackTags

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, AggregateMode, Final, Partial}
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, ObjectHashAggregateExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.expression.{UDFResolver, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.DataType

/**
 * Replaces a typed Aggregator registered as a native UDAF with that UDAF in both the partial and
 * final aggregates of a global batch aggregation, so that they exchange the native intermediate
 * result instead of Spark's binary buffer. The rewritten pair cannot run in Spark, so it is kept
 * only if `offloadDryRun` on the whole plan puts that final and that partial in Velox, connected by
 * their shuffle; otherwise the original pair stays on Spark.
 *
 * Under AQE the two aggregates are planned in different stages, after the rewrite would have to be
 * made, so the original pair always stays on Spark.
 */
case class RewriteTypedAggregatorStages(
    offloadDryRun: SparkPlan => SparkPlan,
    isAdaptive: Boolean)
  extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (isAdaptive) {
      return plan
    }
    val rewrites = plan.collect {
      case TypedAggregatorStages(agg, exchange, partial, typed, udaf, intermediateType) =>
        // One column of the registered intermediate type, so that a null intermediate stays
        // apart from one with null fields.
        val intermediate = AttributeReference("agg_inter", intermediateType)()
        val singleColumnUdaf = udaf.copy(aggBufferAttributes = Seq(intermediate))
        val rewritten = rewrite(agg, exchange, partial, typed, singleColumnUdaf)
        fallbackReason(plan, agg, rewritten, singleColumnUdaf) match {
          case None => Some(agg -> rewritten)
          case Some(reason) =>
            FallbackTags.add(agg, reason)
            FallbackTags.add(partial, reason)
            None
        }
    }.flatten
    rewrites.foldLeft(plan) {
      case (p, (agg, rewritten)) => p.transformDown { case node if node eq agg => rewritten }
    }
  }

  private object TypedAggregatorStages {
    def unapply(plan: SparkPlan): Option[(
        ObjectHashAggregateExec,
        ShuffleExchangeLike,
        ObjectHashAggregateExec,
        AggregateFunction,
        UserDefinedAggregateFunction,
        DataType)] = plan match {
      case agg: ObjectHashAggregateExec if isGlobalBatch(agg) =>
        agg.child match {
          case exchange: ShuffleExchangeLike if exchange.outputPartitioning == SinglePartition =>
            exchange.child match {
              case partial: ObjectHashAggregateExec if isGlobalBatch(partial) =>
                for {
                  (typed, udaf, intermediateType) <- typedAggregatorUdaf(agg, Final)
                  (partialTyped, _, _) <- typedAggregatorUdaf(partial, Partial)
                  if partialTyped == typed
                } yield (agg, exchange, partial, typed, udaf, intermediateType)
              case _ => None
            }
          case _ => None
        }
      case _ => None
    }

    private def isGlobalBatch(agg: ObjectHashAggregateExec): Boolean =
      agg.groupingExpressions.isEmpty && !agg.isStreaming
  }

  private def typedAggregatorUdaf(
      agg: ObjectHashAggregateExec,
      mode: AggregateMode): Option[(AggregateFunction, UserDefinedAggregateFunction, DataType)] =
    agg.aggregateExpressions match {
      case Seq(e) if e.mode == mode =>
        UDFResolver.TypedAggregatorUdaf.unapply(e.aggregateFunction).map {
          case (udaf, intermediateType) => (e.aggregateFunction, udaf, intermediateType)
        }
      case _ => None
    }

  private def rewrite(
      agg: ObjectHashAggregateExec,
      exchange: ShuffleExchangeLike,
      partial: ObjectHashAggregateExec,
      typed: AggregateFunction,
      udaf: UserDefinedAggregateFunction): SparkPlan = {
    def withUdaf(exprs: Seq[AggregateExpression]): Seq[AggregateExpression] =
      exprs.map(e => if (e.aggregateFunction == typed) e.copy(aggregateFunction = udaf) else e)
    def withBuffer[T <: NamedExpression](exprs: Seq[T], from: Seq[Attribute], to: Seq[T]): Seq[T] =
      exprs.flatMap(e => if (from.exists(_.exprId == e.exprId)) to else Seq(e))

    val newPartial = partial.copy(
      aggregateExpressions = withUdaf(partial.aggregateExpressions),
      aggregateAttributes =
        withBuffer(
          partial.aggregateAttributes,
          typed.aggBufferAttributes,
          udaf.aggBufferAttributes),
      resultExpressions = withBuffer(
        partial.resultExpressions,
        typed.inputAggBufferAttributes,
        udaf.inputAggBufferAttributes)
    )
    newPartial.copyTagsFrom(partial)
    val newAgg = agg.copy(
      aggregateExpressions = withUdaf(agg.aggregateExpressions),
      child = exchange.withNewChildren(Seq(newPartial)))
    newAgg.copyTagsFrom(agg)
    newAgg
  }

  private def fallbackReason(
      plan: SparkPlan,
      agg: SparkPlan,
      rewritten: SparkPlan,
      udaf: UserDefinedAggregateFunction): Option[String] = {
    def stages(p: SparkPlan): Seq[BaseAggregateExec] =
      p.collect {
        case a: BaseAggregateExec if a.aggregateExpressions.exists(_.aggregateFunction == udaf) => a
      }

    val candidate = plan.transformDown { case node if node eq agg => rewritten }
    if (stages(candidate).size != 2) {
      return Some("the aggregates of a typed Aggregator appear more than once in the plan")
    }
    val offloaded = offloadDryRun(candidate.clone())
    val connected = stages(offloaded) match {
      case Seq(fin: HashAggregateExecBaseTransformer, partial: HashAggregateExecBaseTransformer) =>
        fin.aggregateExpressions.map(_.mode) == Seq(Final) &&
        partial.aggregateExpressions.map(_.mode) == Seq(Partial) &&
        partial.output.map(_.exprId) == udaf.inputAggBufferAttributes.map(_.exprId) &&
        (fin.child match {
          case e: ShuffleExchangeLike =>
            (e.child eq partial) && e.output.map(_.exprId) == partial.output.map(_.exprId)
          case _ => false
        })
      case _ => false
    }
    if (connected) {
      None
    } else {
      stages(offloaded)
        .flatMap(FallbackTags.getOption(_))
        .headOption
        .map(_.reason())
        .orElse(Some("the aggregates of a typed Aggregator would not run together in Velox"))
    }
  }
}

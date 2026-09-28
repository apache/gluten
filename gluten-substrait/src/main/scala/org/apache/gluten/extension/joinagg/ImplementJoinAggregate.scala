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
package org.apache.gluten.extension.joinagg

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, GetStructField, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.planning.PhysicalAggregation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec

import scala.collection.mutable.ArrayBuffer

case class ImplementJoinAggregate(spark: SparkSession) extends SparkStrategy {
  /*
   * This strategy lowers the logical join-aggregate wrapper shape produced by
   * `PushAggregateThroughJoin` into ordinary Spark physical hash aggregates.
   *
   * The optimizer builds two wrapper phases:
   *   - pushed phase: wrapper partial below / through joins
   *   - final phase: wrapper final above the pushed phase
   *
   * Spark's physical aggregate operators still expect normal aggregate functions and normal input
   * / buffer attributes. This strategy therefore:
   *   1. rewrites wrapper aggregates back to the wrapped Spark aggregate with the correct physical
   *      aggregate mode;
   *   2. inserts a post-project for the pushed phase to pack Spark aggregate buffers into a
   *      struct, matching the wrapper's logical output type;
   *   3. inserts a pre-project for the final phase to unpack that struct back into the buffer
   *      attributes expected by the wrapped Spark aggregate.
   *
   * In short: the wrapper exists only at the logical boundary; this strategy makes the plan look
   * like an ordinary Spark aggregate plan again while preserving the wrapper data contract across
   * the join.
   */

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    if (!GlutenConfig.get.pushAggregateThroughJoinEnabled) {
      return Nil
    }

    plan match {
      case agg: Aggregate if containsWrapperAggregate(agg) =>
        planJoinAggregate(agg).toSeq
      case _ =>
        Nil
    }
  }

  private def planJoinAggregate(agg: Aggregate): Option[SparkPlan] = agg match {
    case PhysicalAggregation(groupingExpressions, aes, resultExpressions, child) =>
      // KEEP: For compatibility with Spark version before 3.5, which widens
      // the aggregate expressions
      // to `Seq[NamedExpression]` instead of `Seq[AggregateExpression]`.
      val aggExpressions = aes.map(_.asInstanceOf[AggregateExpression])
      // A single logical aggregate must lower either entirely as pushed-phase wrappers or entirely
      // as final-phase wrappers. Mixed-phase aggregates are rejected here.
      val grouping = groupingExpressions.collect { case ne: NamedExpression => ne }
      if (grouping.size != groupingExpressions.size) {
        return None
      }

      val wrappers = aggExpressions.flatMap {
        case ae @ AggregateExpression(wrapper: JoinAggregateFunctionWrapper, _, _, _, _) =>
          Some((ae, wrapper))
        case _ =>
          None
      }
      if (wrappers.isEmpty) {
        return None
      }

      val phases = wrappers.map(_._2.targetPhase).distinct
      if (phases.size != 1) {
        return None
      }
      val phase = phases.head

      val childPlan = planLater(child)
      phase match {
        case JoinAggregateFunctionWrapper.PartialPhase =>
          planPartialPhase(grouping, aggExpressions, resultExpressions, childPlan)
        case JoinAggregateFunctionWrapper.PartialMergePhase =>
          planPartialMergePhase(grouping, aggExpressions, resultExpressions, childPlan)
        case JoinAggregateFunctionWrapper.FinalPhase =>
          planFinalPhase(grouping, aggExpressions, resultExpressions, childPlan)
      }
    case _ =>
      None
  }

  private def planPartialPhase(
      grouping: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      resultExpressions: Seq[NamedExpression],
      childPlan: SparkPlan): Option[SparkPlan] = {
    planPhase(
      grouping,
      aggregateExpressions,
      resultExpressions,
      childPlan,
      unpackInputBuffers = false,
      packOutputBuffers = true)
  }

  private def planPartialMergePhase(
      grouping: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      resultExpressions: Seq[NamedExpression],
      childPlan: SparkPlan): Option[SparkPlan] = {
    planPhase(
      grouping,
      aggregateExpressions,
      resultExpressions,
      childPlan,
      unpackInputBuffers = true,
      packOutputBuffers = true)
  }

  private def planPhase(
      grouping: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      resultExpressions: Seq[NamedExpression],
      childPlan: SparkPlan,
      unpackInputBuffers: Boolean,
      packOutputBuffers: Boolean): Option[SparkPlan] = {
    val rewrittenAggExprs = rewriteWrapperAggregates(aggregateExpressions)
    if (rewrittenAggExprs.isEmpty) {
      return None
    }
    val preparedChild = if (unpackInputBuffers) {
      unpackInputBufferFields(childPlan, aggregateExpressions, rewrittenAggExprs)
    } else {
      childPlan
    }
    if (packOutputBuffers) {
      planBufferPhase(
        grouping,
        aggregateExpressions,
        resultExpressions,
        preparedChild,
        rewrittenAggExprs)
    } else {
      planFinalOutput(grouping, resultExpressions, preparedChild, rewrittenAggExprs)
    }
  }

  private def unpackInputBufferFields(
      childPlan: SparkPlan,
      aggregateExpressions: Seq[AggregateExpression],
      rewrittenAggExprs: Seq[AggregateExpression]): SparkPlan = {
    // Recreate the wrapped aggregate's physical input-buffer attributes from the struct payload.
    val unpackAliases = ArrayBuffer.empty[Alias]
    val seenExprIds = scala.collection.mutable.HashSet.empty[Long]
    rewrittenAggExprs.zip(aggregateExpressions).foreach {
      case (rewrittenAe, AggregateExpression(wrapper: JoinAggregateFunctionWrapper, _, _, _, _)) =>
        val bufferExpr = wrapper.children.head
        rewrittenAe.aggregateFunction.inputAggBufferAttributes.zipWithIndex.foreach {
          case (bufferAttr, index) if seenExprIds.add(bufferAttr.exprId.id) =>
            unpackAliases += Alias(
              GetStructField(bufferExpr, index, Some(bufferAttr.name)),
              s"_joinagg_buf_${bufferAttr.exprId.id}_$index"
            )(exprId = bufferAttr.exprId, qualifier = bufferAttr.qualifier)
          case _ =>
        }
      case _ =>
    }
    if (unpackAliases.nonEmpty) {
      ProjectExec(childPlan.output ++ unpackAliases, childPlan)
    } else {
      childPlan
    }
  }

  private def rewriteWrapperAggregates(
      aggregateExpressions: Seq[AggregateExpression]): Seq[AggregateExpression] = {
    aggregateExpressions.map {
      case ae @ AggregateExpression(_: JoinAggregateFunctionWrapper, _, _, _, _) =>
        rewriteSingleAggregateExpression(ae)
      case ae =>
        ae
    }
  }

  private def planBufferPhase(
      grouping: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      resultExpressions: Seq[NamedExpression],
      childPlan: SparkPlan,
      rewrittenAggExprs: Seq[AggregateExpression]): Option[SparkPlan] = {
    val hashAgg = HashAggregateExec(
      requiredChildDistributionExpressions = None,
      isStreaming = false,
      numShufflePartitions = None,
      groupingExpressions = grouping,
      aggregateExpressions = rewrittenAggExprs,
      aggregateAttributes = rewrittenAggExprs.flatMap(_.aggregateFunction.aggBufferAttributes),
      initialInputBufferOffset = 0,
      resultExpressions = grouping.map(_.toAttribute) ++ rewrittenAggExprs.flatMap(
        _.aggregateFunction.aggBufferAttributes),
      child = childPlan
    )

    val rewrittenByOriginalResultId = rewrittenAggExprs.map(ae => ae.resultId -> ae).toMap

    val packedByOriginalResultId = aggregateExpressions.map {
      originalAe =>
        // Each pushed wrapper output corresponds to one rewritten Spark aggregate. Repack the
        // physical buffer attrs into a struct so the parent plan still sees the wrapper contract
        // created by PushAggregateThroughJoin.
        val rewrittenAe = rewrittenByOriginalResultId.getOrElse(
          originalAe.resultId,
          throw new IllegalStateException(
            s"Cannot resolve pushed aggregate output for ${originalAe.sql}"))
        originalAe.resultId -> org.apache.spark.sql.catalyst.expressions
          .CreateStruct(rewrittenAe.aggregateFunction.aggBufferAttributes)
    }.toMap

    def packedResultForAttr(attr: org.apache.spark.sql.catalyst.expressions.AttributeReference)
        : Option[NamedExpression] = {
      aggregateExpressions
        .find(_.resultAttribute.exprId == attr.exprId)
        .flatMap(ae => packedByOriginalResultId.get(ae.resultId))
        .map {
          packed => Alias(packed, attr.name)(exprId = attr.exprId, qualifier = attr.qualifier)
        }
    }

    val rewrittenResultExpressions = resultExpressions.map {
      case attr: org.apache.spark.sql.catalyst.expressions.AttributeReference =>
        packedResultForAttr(attr).getOrElse(attr)
      case alias: Alias =>
        alias
          .transformUp {
            case ae: AggregateExpression =>
              packedByOriginalResultId.getOrElse(
                ae.resultId,
                throw new IllegalStateException(
                  s"Cannot resolve pushed aggregate output for ${ae.sql}"))
            case attr: org.apache.spark.sql.catalyst.expressions.AttributeReference =>
              aggregateExpressions
                .find(_.resultAttribute.exprId == attr.exprId)
                .flatMap(ae => packedByOriginalResultId.get(ae.resultId))
                .getOrElse(attr)
          }
          .asInstanceOf[NamedExpression]
      case other =>
        other
          .transformUp {
            case ae: AggregateExpression =>
              packedByOriginalResultId.getOrElse(
                ae.resultId,
                throw new IllegalStateException(
                  s"Cannot resolve pushed aggregate output for ${ae.sql}"))
          }
          .asInstanceOf[NamedExpression]
    }

    Some(ProjectExec(rewrittenResultExpressions, hashAgg))
  }

  private def planFinalPhase(
      grouping: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      resultExpressions: Seq[NamedExpression],
      childPlan: SparkPlan): Option[SparkPlan] = {
    planPhase(
      grouping,
      aggregateExpressions,
      resultExpressions,
      childPlan,
      unpackInputBuffers = true,
      packOutputBuffers = false)
  }

  private def planFinalOutput(
      grouping: Seq[NamedExpression],
      resultExpressions: Seq[NamedExpression],
      childPlan: SparkPlan,
      rewrittenAggExprs: Seq[AggregateExpression]): Option[SparkPlan] = {
    val aggregateAttrs = rewrittenAggExprs.map(_.resultAttribute)
    val rewrittenResultExpressions =
      rewriteResultAsAggregateAttributes(resultExpressions, rewrittenAggExprs)

    Some(
      HashAggregateExec(
        requiredChildDistributionExpressions = Some(grouping.map(_.toAttribute)),
        isStreaming = false,
        numShufflePartitions = None,
        groupingExpressions = grouping,
        aggregateExpressions = rewrittenAggExprs,
        aggregateAttributes = aggregateAttrs,
        initialInputBufferOffset = 0,
        resultExpressions = rewrittenResultExpressions,
        child = childPlan
      ))
  }

  private def containsWrapperAggregate(agg: Aggregate): Boolean = {
    agg.aggregateExpressions.exists {
      _.exists {
        case AggregateExpression(wrapper: JoinAggregateFunctionWrapper, _, _, _, _) => true
        case _ => false
      }
    }
  }

  private def rewriteSingleAggregateExpression(
      original: AggregateExpression): AggregateExpression = {
    original
      .transformUp {
        case ae @ AggregateExpression(wrapper: JoinAggregateFunctionWrapper, _, _, _, _) =>
          // The wrapper carries the logical phase; `semanticMode` turns
          // that phase together with the current Spark aggregate mode into
          // the actual wrapped aggregate mode required by this physical aggregate node.
          val mode = JoinAggregateFunctionWrapper.semanticMode(ae.mode, wrapper.targetPhase)
          ae.copy(aggregateFunction = wrapper.innerAgg, mode = mode)
      }
      .asInstanceOf[AggregateExpression]
  }

  private def rewriteResultAsAggregateAttributes(
      rewrittenOutput: Seq[NamedExpression],
      rewrittenAggExprs: Seq[AggregateExpression]): Seq[NamedExpression] = {
    // After the HashAggregateExec is built, rewrite the original output tree so every aggregate
    // expression points at the corresponding physical aggregate result attribute.
    // Some results already reference the wrapper aggregate's resultAttribute directly instead of
    // carrying the AggregateExpression node, so rewrite those attributes as well to avoid leaking
    // wrapper names into the final physical plan.
    val rewrittenByResultId = rewrittenAggExprs.map(ae => ae.resultId -> ae.resultAttribute).toMap
    rewrittenOutput.map {
      _.transformUp {
        case ae: AggregateExpression =>
          rewrittenByResultId
            .get(ae.resultId)
            .getOrElse(
              throw new IllegalStateException(s"Cannot resolve aggregate attribute for ${ae.sql}"))
        case attr: org.apache.spark.sql.catalyst.expressions.AttributeReference =>
          rewrittenByResultId.getOrElse(attr.exprId, attr)
      }.asInstanceOf[NamedExpression]
    }
  }
}

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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types._

/**
 * For aggregation over grouping sets (rollup/cube), Spark expands every input row once per grouping
 * set before the partial aggregation, so the partial aggregate consumes and hashes
 * `input rows * number of grouping sets` rows:
 *
 * partial aggregate <- expand <- child
 *
 * When the number of distinct full-grouping-key combinations is much smaller than the input row
 * count, it is cheaper to aggregate at the finest grain once, expand only the intermediate
 * aggregation states, and merge the expanded states before shuffle:
 *
 * partial-merge aggregate <- expand (over aggregation buffers) <- partial aggregate <- child
 *
 * The pre-shuffle partial-merge stage collapses duplicated coarse-grained groups locally so the
 * rewrite does not increase shuffle volume (see the ClickHouse backend's lazy expand and its
 * high-cardinality regression, GLUTEN-7986, for why this stage is required).
 *
 * Both new aggregates rely on Velox's flushable-aggregation machinery: if the input has too many
 * distinct full-grouping-key combinations, the finest-grain aggregate abandons itself and streams
 * rows through in intermediate format, and the merge stage over non-raw input abandons to an
 * identity pass-through. The rewrite is therefore disabled when flushable partial aggregation is
 * disabled.
 *
 * Rewrite invariants:
 *   - The rewritten sub-plan's output attributes equal the original partial aggregate's output, so
 *     no operator above the matched aggregate needs adjustment.
 *   - The new expand's output is exactly `grouping attributes ++ aggregation buffer attributes` in
 *     the original order. The partial-merge aggregate binds its buffer inputs by name and position
 *     against `child.output.drop(groupingExpressions.size)`, so this ordering is load-bearing.
 *   - Aggregate filters are only evaluated in the finest-grain (raw input) aggregate. The
 *     partial-merge copies drop them, mirroring Spark's AggUtils.mayRemoveAggFilters.
 */
case class LazyAggregateExpandRule(session: SparkSession) extends Rule[SparkPlan] with Logging {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (
      !VeloxConfig.get.enableVeloxLazyAggregateExpand ||
      !VeloxConfig.get.enableVeloxFlushablePartialAggregation
    ) {
      return plan
    }
    plan.transformUp {
      case agg: RegularHashAggregateExecTransformer if isEligibleAggregate(agg) =>
        agg.child match {
          case expand: ExpandExecTransformer =>
            rewrite(agg, expand, preProject = None, preFilter = None).getOrElse(agg)
          case project @ ProjectExecTransformer(_, expand: ExpandExecTransformer) =>
            rewrite(agg, expand, preProject = Some(project), preFilter = None).getOrElse(agg)
          case filter @ FilterExecTransformer(_, expand: ExpandExecTransformer) =>
            rewrite(agg, expand, preProject = None, preFilter = Some(filter)).getOrElse(agg)
          case _ => agg
        }
    }
  }

  // Matches only Partial-mode regular aggregates with a zero buffer offset. The offset check
  // keeps the rule idempotent for measure-less aggregates (whose mode list is empty): every
  // partial-merge aggregate this rule emits carries offset >= 1, while Spark plans partial
  // aggregates with offset 0.
  private def isEligibleAggregate(agg: RegularHashAggregateExecTransformer): Boolean = {
    agg.initialInputBufferOffset == 0 &&
    agg.groupingExpressions.forall(_.isInstanceOf[Attribute]) &&
    agg.aggregateExpressions.forall(_.mode == Partial) &&
    agg.aggregateExpressions.forall(isSupportedAggregateExpression) &&
    !hasUnsafeFloatingPointAggregate(agg.aggregateExpressions)
  }

  private def isSupportedAggregateExpression(aggExpr: AggregateExpression): Boolean = {
    if (aggExpr.filter.isDefined || aggExpr.isDistinct) {
      return false
    }
    aggExpr.aggregateFunction match {
      case s: Sum => !s.prettyName.equals("try_sum")
      case a: Average => !a.prettyName.equals("try_avg")
      case _: Count => true
      case _: Min => true
      case _: Max => true
      case _ => false
    }
  }

  // The rewrite reorders how partial states are merged, which can change the result of
  // floating-point sum/avg bitwise. Apply the same policy as FlushableHashAggregateRule.
  private def hasUnsafeFloatingPointAggregate(aggExprs: Seq[AggregateExpression]): Boolean = {
    if (VeloxConfig.get.floatingPointMode == "loose") {
      return false
    }

    def isFloatingPointType(dataType: DataType): Boolean = {
      dataType == DoubleType || dataType == FloatType
    }

    aggExprs.exists {
      aggExpr =>
        aggExpr.aggregateFunction match {
          case s: Sum => isFloatingPointType(s.child.dataType)
          case a: Average => isFloatingPointType(a.child.dataType)
          case _ => false
        }
    }
  }

  private def rewrite(
      agg: RegularHashAggregateExecTransformer,
      expand: ExpandExecTransformer,
      preProject: Option[ProjectExecTransformer],
      preFilter: Option[FilterExecTransformer]): Option[SparkPlan] = {
    val numKeys = agg.groupingExpressions.length
    val expandChildOutput = expand.child.output

    // A Partial aggregate's result expressions are its grouping attributes followed by the
    // flattened aggregation buffer attributes. Anything else means an unexpected plan shape.
    val numBufferAttributes =
      agg.aggregateExpressions.map(_.aggregateFunction.aggBufferAttributes.length).sum
    if (
      agg.resultExpressions.length != numKeys + numBufferAttributes ||
      !agg.resultExpressions.forall(_.isInstanceOf[Attribute]) ||
      !agg.resultExpressions
        .take(numKeys)
        .zip(agg.groupingExpressions)
        .forall { case (result, key) => result.toAttribute.semanticEquals(key.toAttribute) }
    ) {
      logDebug(s"Lazy expand: unexpected partial aggregate output shape: ${agg.resultExpressions}")
      return None
    }
    val bufferAttributes = agg.resultExpressions.drop(numKeys).map(_.toAttribute)

    // A pull-out pre-projection between the aggregate and the expand computes aggregate inputs
    // (e.g. `_pre_1 = coalesce(a * b, 0)`) from columns that pass through the expand unchanged.
    // It can be re-grounded onto the expand's child iff its computed expressions reference only
    // pre-expand columns.
    val preProjectAliases = preProject.map(_.projectList.collect { case a: Alias => a })
    if (
      !preProject.forall(
        _.projectList.forall {
          case _: Attribute => true
          // Non-deterministic expressions must keep their original per-expanded-row evaluation;
          // moving them below the expand would share one draw across all grouping sets.
          case a: Alias => a.child.deterministic && resolvableFrom(a.references, expandChildOutput)
          case _ => false
        })
    ) {
      logDebug("Lazy expand: pre-project is not re-groundable onto the expand's child")
      return None
    }

    // Aggregate inputs must come from columns that pass through the expand unchanged (or from
    // the re-grounded pre-projection). This also rejects the look-alike Expand produced by
    // RewriteDistinctAggregates, whose aggregate functions reference expand-created attributes.
    val aggregateInputCandidates =
      expandChildOutput ++ preProjectAliases.getOrElse(Seq.empty).map(_.toAttribute)
    if (
      !agg.aggregateExpressions.forall(
        ae => resolvableFrom(ae.aggregateFunction.references, aggregateInputCandidates))
    ) {
      logDebug("Lazy expand: aggregate inputs are not pass-through columns of the expand")
      return None
    }

    // A filter between the aggregate and the expand may only reference grouping columns; it then
    // filters whole (group, grouping set) rows and can equivalently run above the new expand.
    if (
      !preFilter.forall(
        f =>
          f.condition.deterministic &&
            resolvableFrom(f.condition.references, agg.groupingExpressions.map(_.toAttribute)))
    ) {
      logDebug("Lazy expand: filter references non-grouping columns of the expand")
      return None
    }

    // Maps each expand output attribute to the pre-expand attribute that passes through in that
    // slot. Slots that are literal-only in every projection (grouping id, grouping position,
    // constant grouping keys) have no mapping and are re-attached in the new expand as-is.
    val replaceMap = buildReplaceAttributeMap(expand)
    val bottomGroupingKeys = agg.groupingExpressions
      .map(_.toAttribute)
      .flatMap(attr => findReplacement(attr, replaceMap))
      .distinct

    // A keyless finest-grain aggregate would emit one row on empty input, producing spurious
    // grand-total rows where Spark returns none. Non-atomic key types are excluded because the
    // new expand would need typed null literals for them, which is unaudited.
    if (
      bottomGroupingKeys.isEmpty ||
      !bottomGroupingKeys.forall(key => isSupportedGroupingKeyType(key.dataType))
    ) {
      logDebug(s"Lazy expand: unsupported finest-grain grouping keys: $bottomGroupingKeys")
      return None
    }

    val reGroundedPreProject = preProject.map {
      project =>
        val reGrounded =
          ProjectExecTransformer(expandChildOutput ++ preProjectAliases.get, expand.child)
        reGrounded.copyTagsFrom(project)
        reGrounded
    }
    val bottomChild = reGroundedPreProject.getOrElse(expand.child)

    // Flushable, so Velox can abandon the aggregation when the finest grain barely reduces the
    // row count; a regular aggregate here would hash and spill the whole input on
    // high-cardinality keys.
    val bottomAggregate = FlushableHashAggregateExecTransformer(
      requiredChildDistributionExpressions = None,
      groupingExpressions = bottomGroupingKeys,
      aggregateExpressions = agg.aggregateExpressions,
      aggregateAttributes = agg.aggregateAttributes,
      initialInputBufferOffset = 0,
      resultExpressions = bottomGroupingKeys ++ bufferAttributes,
      child = bottomChild
    )
    bottomAggregate.copyTagsFrom(agg)

    val newExpandOutput = agg.resultExpressions.map(_.toAttribute)
    val newExpandProjections =
      buildPostExpandProjections(expand.projections, expand.output, newExpandOutput)
    val newExpand = ExpandExecTransformer(newExpandProjections, newExpandOutput, bottomAggregate)
    newExpand.copyTagsFrom(expand)

    val newPreFilter = preFilter.map {
      filter =>
        val newFilter = filter.copy(child = newExpand)
        newFilter.copyTagsFrom(filter)
        newFilter
    }
    val mergeChild = newPreFilter.getOrElse(newExpand)

    // Deliberately Regular: FlushableHashAggregateRule runs next and converts this merge stage
    // to flushable (it walks down from the shuffle and stops here, never reaching the bottom
    // aggregate, which is why the bottom one is emitted flushable directly above).
    val mergeAggregate = RegularHashAggregateExecTransformer(
      requiredChildDistributionExpressions = agg.requiredChildDistributionExpressions,
      groupingExpressions = agg.groupingExpressions,
      aggregateExpressions =
        agg.aggregateExpressions.map(_.copy(mode = PartialMerge, filter = None)),
      aggregateAttributes = agg.aggregateAttributes,
      initialInputBufferOffset = numKeys,
      resultExpressions = agg.resultExpressions,
      child = mergeChild
    )
    mergeAggregate.copyTagsFrom(agg)

    val newNodes: Seq[SparkPlan] =
      reGroundedPreProject.toSeq ++ Seq(bottomAggregate, newExpand) ++
        newPreFilter.toSeq :+ mergeAggregate
    if (!newNodes.forall(passesNativeValidation)) {
      logDebug("Lazy expand: native validation failed for the rewritten plan; keeping original")
      return None
    }
    logDebug(s"Lazy expand rewrote aggregate over expand: $mergeAggregate")
    Some(mergeAggregate)
  }

  // The new expand must emit typed null literals for excluded grouping keys; restrict to types
  // whose null literals are known to round-trip through the native ExpandRel.
  private def isSupportedGroupingKeyType(dataType: DataType): Boolean = {
    dataType match {
      // Referenced by type name: TimestampNTZType is private[sql] in Spark 3.3.
      case dt if dt.typeName == "timestamp_ntz" => true
      case BooleanType | StringType | DateType | TimestampType | BinaryType =>
        true
      case _: NumericType => true
      case _ => false
    }
  }

  private def resolvableFrom(references: AttributeSet, candidates: Seq[Attribute]): Boolean = {
    references.forall(ref => candidates.exists(_.semanticEquals(ref)))
  }

  private def findReplacement(
      attribute: Attribute,
      replaceMap: Map[Attribute, Attribute]): Option[Attribute] = {
    replaceMap.collectFirst { case (k, v) if k.semanticEquals(attribute) => v }
  }

  private def buildReplaceAttributeMap(expand: ExpandExecTransformer): Map[Attribute, Attribute] = {
    val passThroughBySlot = expand.output.indices.map {
      i =>
        expand.projections.collectFirst {
          case projection if projection(i).isInstanceOf[Attribute] =>
            projection(i).asInstanceOf[Attribute]
        }
    }
    expand.output
      .zip(passThroughBySlot)
      .collect { case (out, Some(passThrough)) => out -> passThrough }
      .toMap
  }

  // Rebuilds the expand projections against the finest-grain aggregate's output: slots that
  // existed in the original expand keep their per-projection expression (pass-through attribute
  // or literal), and aggregation buffer attributes pass through every projection unchanged.
  private def buildPostExpandProjections(
      originalProjections: Seq[Seq[Expression]],
      originalOutput: Seq[Attribute],
      newOutput: Seq[Attribute]): Seq[Seq[Expression]] = {
    originalProjections.map {
      projection =>
        newOutput.map {
          attr =>
            val index = originalOutput.indexWhere(_.semanticEquals(attr))
            if (index != -1) {
              projection(index)
            } else {
              attr
            }
        }
    }
  }

  private def passesNativeValidation(plan: SparkPlan): Boolean = {
    plan match {
      case validatable: ValidatablePlan =>
        try {
          validatable.doValidate().ok()
        } catch {
          case e: Exception =>
            logDebug(s"Lazy expand: validation threw for ${plan.nodeName}: ${e.getMessage}")
            false
        }
      case _ => true
    }
  }
}

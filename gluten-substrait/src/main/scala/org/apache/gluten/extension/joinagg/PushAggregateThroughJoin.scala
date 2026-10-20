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
import org.apache.gluten.extension.joinagg.JoinAggregateFunctionWrapper.{FinalPhase, PartialMergePhase, PartialPhase}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, EqualTo, Expression, NamedExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, DeclarativeAggregate}
import org.apache.spark.sql.catalyst.optimizer.DecimalAggregates
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}

case class PushAggregateThroughJoinRule(spark: SparkSession)
  extends Rule[LogicalPlan]
  with PredicateHelper {
  /*
   * This rule rewrites:
   *
   *   Aggregate(..., Join(...))
   *
   * into a pair of wrapper aggregates:
   *
   *   FinalWrapperAggregate(..., PushedWrapperAggregate(..., Join(...)))
   *
   * and then repeatedly pushes the wrapper aggregate through inner equi-joins. The pushed
   * aggregate always represents the "pre-join" aggregation work, while the final wrapper above
   * the join preserves the original query semantics.
   * Grouping-only aggregates use the same split-and-push path without wrappers, keeping the
   * original top DISTINCT and pushing local grouping to both join sides.
   *
   * The key invariants are:
   *   1. Only declarative, non-distinct aggregate functions or grouping-only aggregates are
   *      rewritten.
   *   2. Pushability is decided per aggregate subexpression, not per whole result expression.
   *      This is what lets expressions such as `sum(a) - sum(b)` push both aggregate terms while
   *      keeping the arithmetic shape above the join unchanged.
   *   3. Pushed grouping keys must preserve the semantics of all expressions that still remain
   *      above the pushed aggregate, including derived grouping expressions coming from extracted
   *      Project / Filter nodes.
   *   4. Aggregate measure inputs needed to evaluate the pushed aggregate must be preserved in the
   *      rebuilt subtree, but they must not be promoted into grouping keys. Promoting those
   *      inputs would change the pre-aggregation grain and make the rewrite semantically wrong.
   *   5. The rule intentionally pushes one join edge at a time; repeated application performs
   *      deep pushdown for multi-join shapes.
   */

  private case class SidePartialSpec(
      originalExpr: Expression,
      aggregate: DeclarativeAggregate,
      wrapperKey: String)

  private case class SidePartialRef(spec: SidePartialSpec, attr: AttributeReference)

  private case class PushResult(
      plan: LogicalPlan,
      // New lower aggregates eligible for further pushdown.
      pushedAggregates: Seq[JoinAggregate])

  private var successfulSplitCount: Int = 0
  private var successfulPushCount: Int = 0

  def resetSuccessfulSplitCount(): Unit = {
    successfulSplitCount = 0
  }

  def resetSuccessfulPushCount(): Unit = {
    successfulPushCount = 0
  }

  def getSuccessfulSplitCount: Int = successfulSplitCount

  def getSuccessfulPushCount: Int = successfulPushCount

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!isEnabled) {
      return plan
    }
    var splitCount = 0
    val newPlan = plan.transformUp {
      case agg: Aggregate =>
        // Keep the top aggregate and push the lower stage through join edges.
        splitAggregate(agg).flatMap {
          case (finalAgg, lowerPartialAgg) =>
            pushPartialAggregate(lowerPartialAgg).map {
              pushed =>
                splitCount += 1
                finalAgg.copy(child = pushed)
            }
        }.getOrElse(agg)
    }
    if (splitCount > 0) {
      successfulSplitCount += splitCount
      newPlan
    } else {
      assert(newPlan eq plan)
      plan
    }
  }

  private def isEnabled: Boolean = GlutenConfig.get.pushAggregateThroughJoinEnabled

  private def maxDepth: Int = GlutenConfig.get.pushAggregateThroughJoinMaxDepth

  private def splitAggregate(agg: Aggregate): Option[(JoinAggregate, JoinAggregate)] = {
    // Split is intentionally child-agnostic. It only rewrites:
    //
    //   Aggregate(resultExprs, child)
    //
    // into:
    //
    //   Aggregate(rewrittenResultExprs, Aggregate(pushedKeys + wrapperOutputs, child))
    //
    // Any join-specific key widening or subtree rebuild is handled later by pushOnce /
    // pushPartialAggToJoinSide. This keeps the split step reusable even when the current child is
    // not yet a join-shaped subtree.
    if (hasDistinctAggExpr(agg.aggregateExpressions)) {
      return None
    }
    val pushableSpecs = collectPushableSpecs(agg.aggregateExpressions)
    if (
      pushableSpecs.isEmpty &&
      !isGroupingOnly(agg.groupingExpressions, agg.aggregateExpressions)
    ) {
      return None
    }
    val groupingRefAttrs = dedupeAttrs(agg.groupingExpressions.flatMap(referencedAttrsInOrder))
    // Aggregate subexpressions that are not rewritten remain above the lower aggregate after the
    // split. Their referenced attrs must therefore still be produced by the lower aggregate, but
    // at this stage we only derive that requirement from the aggregate itself, not from any
    // particular child shape.
    val nonPushableAggAttrs = dedupeAttrs(agg.aggregateExpressions.flatMap {
      case Alias(expr, _) if !isPushableExpr(expr) => referencedAttrsInOrder(expr)
      case expr if !isPushableExpr(expr) => referencedAttrsInOrder(expr)
      case _ => Nil
    })
    val partialGroupingKeys = dedupeAttrs(groupingRefAttrs ++ nonPushableAggAttrs)
    val partialAliases = pushableSpecs.zipWithIndex.map {
      case (spec, idx) =>
        Alias(
          JoinAggregateFunctionWrapper
            .wrapperPartial(spec.aggregate, spec.wrapperKey)
            .toAggregateExpression(),
          s"_pushed_${spec.aggregate.prettyName}_buffer_$idx"
        )()
    }

    val partialAgg = JoinAggregate(
      groupingExpressions = partialGroupingKeys,
      aggregateExpressions = partialGroupingKeys ++ partialAliases,
      child = agg.child,
      phase = PartialPhase)

    val partialOutputAttrs =
      partialAgg.output
        .slice(partialGroupingKeys.size, partialGroupingKeys.size + partialAliases.size)
        .map(cleanAttr)
    val partialRefs = pushableSpecs.zip(partialOutputAttrs).map {
      case (spec, attr) => SidePartialRef(spec, attr)
    }

    rewriteAggregateExpressions(agg.aggregateExpressions, partialRefs).map {
      rewrittenAggExprs =>
        val finalAgg = JoinAggregate(
          groupingExpressions = agg.groupingExpressions,
          aggregateExpressions = rewrittenAggExprs,
          child = partialAgg,
          phase = FinalPhase)
        (finalAgg, partialAgg)
    }
  }

  private def pushPartialAggregate(lowerPartialAgg: JoinAggregate): Option[LogicalPlan] = {
    // Push one join level per iteration; grouping-only aggregation can push to both sides.
    val partialMergeEnabled = GlutenConfig.get.pushAggregateThroughJoinPartialMergeEnabled
    var current: LogicalPlan = lowerPartialAgg
    var candidates = Seq(lowerPartialAgg)
    var pushCount = 0
    var depth = 0
    while (candidates.nonEmpty && depth < maxDepth) {
      val nextCandidates = Seq.newBuilder[JoinAggregate]
      current = current.transformUp {
        // Only revisit aggregates created by the previous push, not unrelated aggregates.
        case partialAgg: JoinAggregate if candidates.exists(_ eq partialAgg) =>
          // The physical strategy owns the top merge; retain logical merges only between joins.
          val retainPartialMerge = partialMergeEnabled && pushCount > 0
          val pushed = if (retainPartialMerge) {
            val (partialMergeAgg, freshLowerPartialAgg) = splitPartialAggregateForMerge(partialAgg)
            pushOnce(freshLowerPartialAgg).map {
              result =>
                val mergeAgg = partialMergeAgg.copy(child = result.plan)
                result.copy(plan = mergeAgg)
            }
          } else {
            pushOnce(partialAgg)
          }
          pushed match {
            case Some(result) =>
              pushCount += result.pushedAggregates.size
              nextCandidates ++= result.pushedAggregates
              result.plan
            case None =>
              partialAgg
          }
      }
      candidates = nextCandidates.result()
      depth += 1
    }
    successfulPushCount += pushCount
    if (pushCount == 0) {
      // Keep the original aggregate when no join push succeeds.
      None
    } else {
      Some(current)
    }
  }

  private def pushOnce(partialAgg: JoinAggregate): Option[PushResult] = {
    val hasBuffers = collectPartialWrapperAliases(partialAgg.aggregateExpressions).nonEmpty
    if (!isDeterministicJoinPath(partialAgg.child)) {
      // Skip pushdown: grouping rows before evaluating rand(), for example, changes the results.
      return None
    }
    extractJoin(partialAgg.child).flatMap {
      case (join, wrapperRequiredAttrs, rebuild) =>
        val candidates: Seq[(JoinSide, JoinAggregate)] = Seq(JoinLeft, JoinRight).flatMap {
          side =>
            pushPartialAggToJoinSide(
              partialAgg.groupingExpressions,
              partialAgg.aggregateExpressions,
              join,
              wrapperRequiredAttrs,
              side).map(agg => (side, agg))
        }
        val selected = if (!hasBuffers) {
          // Without aggregate functions, DISTINCT can be pushed to both join sides simultaneously.
          candidates
        } else {
          // When both sides qualify, choose one so the same aggregate buffer is not produced on
          // both sides.
          //
          // SUM(fact.amount) qualifies only on the fact side because it requires fact.amount.
          // COUNT(*) requires no columns, so both sides qualify; take(1) prefers the left side.
          // TODO: Use statistics to choose a side when both qualify, rather than preferring the
          //  left.
          candidates.take(1)
        }
        if (selected.isEmpty) {
          None
        } else {
          val pushedJoin = selected.foldLeft(join) {
            case (currentJoin, (side, pushedAgg)) => side.replace(currentJoin, pushedAgg)
          }
          Some(PushResult(
            rebuild(pushedJoin, partialAgg.output.collect { case a: Attribute => a }),
            selected.map(_._2)))
        }
    }
  }

  // Public for testing.
  def pushPartialAggToJoinSide(
      groupingExpressions: Seq[Expression],
      aggregateExpressions: Seq[NamedExpression],
      join: Join,
      wrapperRequiredAttrs: Seq[Attribute],
      side: JoinSide): Option[JoinAggregate] = {
    val wrapperAliases = collectPartialWrapperAliases(aggregateExpressions)
    // Inputs come from the lower aggregate: grouping keys followed by partial wrappers.
    if (
      (wrapperAliases.isEmpty && !join.condition.forall(_.deterministic)) ||
      !wrapperAliases.forall {
        case (_, wrapper) => canPushToSide(wrapper.innerAgg, side.outputSet(join))
      }
    ) {
      return None
    }
    // Keep grouping and join dependencies, but not pure aggregate measure inputs.
    val sideOutputSet = side.outputSet(join)
    val sideGroupingAttrs = groupingExpressions
      .flatMap(referencedAttrsInOrder)
      .collect { case a: Attribute if sideOutputSet.contains(a) => a }
    val sideJoinKeys = join.condition.toSeq.flatMap(splitConjunctivePredicates).collect {
      case EqualTo(l: Attribute, r: Attribute)
          if sideOutputSet.contains(l) && !sideOutputSet.contains(r) =>
        l
      case EqualTo(l: Attribute, r: Attribute)
          if sideOutputSet.contains(r) && !sideOutputSet.contains(l) =>
        r
    }
    val sideJoinCondAttrs = join.condition.toSeq
      .flatMap(referencedAttrsInOrder)
      .collect { case a: Attribute if sideOutputSet.contains(a) => a }

    val sidePushableAggInputAttrs = dedupeAttrs(wrapperAliases.flatMap {
      case (_, wrapper) =>
        wrapper.innerAgg.children.flatMap(referencedAttrsInOrder).collect {
          case a: Attribute if sideOutputSet.contains(a) => a
        }
    })
    // Project / Filter dependencies survive the pushed aggregate, except measure inputs.
    val sideWrapperRequiredAttrs = dedupeAttrs(
      wrapperRequiredAttrs
        .collect {
          case a: Attribute if sideOutputSet.contains(a) => a
        }
        .filterNot(attr => sidePushableAggInputAttrs.exists(_.semanticEquals(attr))))
    val pushedGrouping = dedupeAttrs(
      sideGroupingAttrs ++
        sideJoinKeys ++
        sideJoinCondAttrs ++
        sideWrapperRequiredAttrs)

    if (pushedGrouping.isEmpty) {
      None
    } else {
      // Keep the existing Partial expressions and their ExprIds.
      val pushedAgg = JoinAggregate(
        groupingExpressions = pushedGrouping,
        aggregateExpressions = pushedGrouping ++ wrapperAliases.map(_._1),
        child = side.plan(join),
        phase = PartialPhase)
      Some(pushedAgg)
    }
  }

  // Retain the current buffer IDs above a fresh lower Partial. The lower Partial is then the
  // only aggregate pushed through the next join edge.
  private def splitPartialAggregateForMerge(
      partialAgg: JoinAggregate): (JoinAggregate, JoinAggregate) = {
    // For DISTINCT, both stages group the same keys; there are no buffer aliases to rewrite.
    val wrapperAliases = collectPartialWrapperAliases(partialAgg.aggregateExpressions)
    val lowerAliases = wrapperAliases.map {
      case (alias, wrapper) =>
        Alias(
          JoinAggregateFunctionWrapper
            .wrapperPartial(wrapper.innerAgg, wrapper.wrapperKey)
            .toAggregateExpression(),
          alias.name)()
    }
    val lowerGroupingOutputs =
      partialAgg.aggregateExpressions.take(partialAgg.groupingExpressions.size)
    val lowerAgg = JoinAggregate(
      groupingExpressions = partialAgg.groupingExpressions,
      aggregateExpressions = lowerGroupingOutputs ++ lowerAliases,
      child = partialAgg.child,
      phase = PartialPhase
    )
    val lowerBuffersByExprId = wrapperAliases.zip(
      lowerAgg.output.drop(lowerGroupingOutputs.size))
      .map { case ((alias, _), attr) => alias.exprId.id -> cleanAttr(attr) }
      .toMap
    val partialMergeExpressions = partialAgg.aggregateExpressions.map {
      case alias @ Alias(
            AggregateExpression(wrapper: JoinAggregateFunctionWrapper, _, _, _, _),
            _)
          if wrapper.targetPhase == JoinAggregateFunctionWrapper.PartialPhase =>
        val lowerBuffer = lowerBuffersByExprId.getOrElse(
          alias.exprId.id,
          throw new IllegalStateException(s"Cannot resolve pushed buffer for ${alias.sql}"))
        val partialMerge = JoinAggregateFunctionWrapper
          .wrapperPartialMerge(wrapper.innerAgg, lowerBuffer, wrapper.wrapperKey)
          .toAggregateExpression()
        Alias(partialMerge, alias.name)(
          exprId = alias.exprId,
          qualifier = alias.qualifier,
          explicitMetadata = alias.explicitMetadata,
          nonInheritableMetadataKeys = alias.nonInheritableMetadataKeys
        )
      case other => other
    }
    val mergeAgg = partialAgg.copy(
      phase = PartialMergePhase,
      aggregateExpressions = partialMergeExpressions,
      child = lowerAgg)
    (mergeAgg, lowerAgg)
  }

  private def collectPartialWrapperAliases(
      output: Seq[NamedExpression]): Seq[(Alias, JoinAggregateFunctionWrapper)] = {
    output.collect {
      case alias @ Alias(AggregateExpression(wrapper: JoinAggregateFunctionWrapper, _, _, _, _), _)
          if wrapper.targetPhase == JoinAggregateFunctionWrapper.PartialPhase =>
        (alias, wrapper)
    }
  }

  private def extractJoin(
      child: LogicalPlan): Option[(Join, Seq[Attribute], (Join, Seq[Attribute]) => LogicalPlan)] =
    child match {
      case project @ Project(_, projectChild) =>
        // Preserve dependencies introduced by extracted Project nodes. When pushdown rebuilds the
        // join subtree, these attrs tell us which project outputs (and therefore which child
        // inputs) must remain available above the pushed aggregate.
        extractJoin(projectChild).map {
          case (join, wrapperRequiredAttrs, rebuild) =>
            val projectRequiredAttrs =
              dedupeAttrs(
                project.projectList.flatMap(referencedAttrsInOrder) ++ wrapperRequiredAttrs)
            (
              join,
              projectRequiredAttrs,
              (j: Join, requiredAttrs: Seq[Attribute]) => {
                val requiredAttrSet = AttributeSet(requiredAttrs)
                val retainedProjectList = project.projectList.filter {
                  ne => requiredAttrSet.contains(ne.toAttribute)
                }
                // If a retained project expression depends on child columns (e.g. CASE refs),
                // propagate those dependencies when rebuilding the join subtree.
                val requiredForChild =
                  dedupeAttrs(requiredAttrs ++ retainedProjectList.flatMap(referencedAttrsInOrder))
                val rebuiltChild = rebuild(j, requiredForChild)
                val projectOutputSet = AttributeSet(project.projectList.map(_.toAttribute))
                val passThroughAttrs = requiredAttrs.filter {
                  attr => rebuiltChild.outputSet.contains(attr) && !projectOutputSet.contains(attr)
                }
                // Only keep project outputs required by the consumer above this extracted join.
                // This allows pushdown to replace measure columns with partial-wrapper outputs.
                project.copy(
                  projectList = retainedProjectList ++ dedupeAttrs(passThroughAttrs),
                  child = rebuiltChild
                )
              }
            )
        }
      case join: Join if isInnerEquiJoin(join) =>
        Some((join, Nil, (j: Join, _: Seq[Attribute]) => j))
      case filter @ Filter(_, join: Join) if isInnerEquiJoin(join) =>
        // Filter predicates above the join become additional required attrs for rebuild, because
        // those predicates still execute above the pushed aggregate after rewrite.
        Some(
          (
            join,
            referencedAttrsInOrder(filter.condition),
            (j: Join, _: Seq[Attribute]) => filter.copy(child = j)))
      case _ => None
    }

  private def isInnerEquiJoin(join: Join): Boolean = {
    join.joinType == Inner && join.hint == JoinHint.NONE && join.condition.exists {
      cond =>
        splitConjunctivePredicates(cond).exists {
          case EqualTo(_: Attribute, _: Attribute) => true
          case _ => false
        }
    }
  }

  private def cleanAttr(attr: Attribute): AttributeReference = {
    AttributeReference(attr.name, attr.dataType, attr.nullable)(
      exprId = attr.exprId,
      qualifier = attr.qualifier)
  }

  private def isGroupingOnly(
      groupingExpressions: Seq[Expression],
      aggregateExpressions: Seq[NamedExpression]): Boolean = {
    val hasAggregateFunctions = aggregateExpressions.exists(_.exists {
      case _: AggregateExpression => true
      case _ => false
    })
    !hasAggregateFunctions && groupingExpressions.nonEmpty &&
    groupingExpressions.forall(_.deterministic) && aggregateExpressions.forall(_.deterministic)
  }

  private def isDeterministicJoinPath(child: LogicalPlan): Boolean = child match {
    case Project(projectList, next) =>
      projectList.forall(_.deterministic) && isDeterministicJoinPath(next)
    case Filter(condition, join: Join) =>
      condition.deterministic && isDeterministicJoinPath(join)
    case join: Join => join.condition.forall(_.deterministic)
    case _ => false
  }

  private def isPushableExpr(expr: Expression): Boolean = {
    collectPushableSpecs(expr).nonEmpty
  }

  private def hasDistinctAggExpr(aggExprs: Seq[NamedExpression]): Boolean = {
    aggExprs.exists {
      _.exists {
        case ae: AggregateExpression if ae.isDistinct => true
        case _ => false
      }
    }
  }

  private def pushableSpec(ae: AggregateExpression): Option[SidePartialSpec] = {
    if (!ae.isDistinct && ae.filter.isEmpty) {
      ae.aggregateFunction match {
        case da: DeclarativeAggregate if !da.isInstanceOf[JoinAggregateFunctionWrapper] =>
          val stableExprSql = ae.canonicalized.sql
          Some(SidePartialSpec(ae, da, Integer.toUnsignedString(stableExprSql.hashCode)))
        case _ => None
      }
    } else {
      None
    }
  }

  private def collectPushableSpecs(expr: Expression): Seq[SidePartialSpec] = {
    expr
      .collect { case ae: AggregateExpression => ae }
      .flatMap(pushableSpec)
      .foldLeft(Seq.empty[SidePartialSpec]) {
        case (acc, spec) if acc.exists(_.originalExpr.semanticEquals(spec.originalExpr)) => acc
        case (acc, spec) => acc :+ spec
      }
  }

  private def collectPushableSpecs(aggExprs: Seq[NamedExpression]): Seq[SidePartialSpec] = {
    aggExprs
      .flatMap {
        case Alias(expr, _) => collectPushableSpecs(expr)
        case expr => collectPushableSpecs(expr)
      }
      .foldLeft(Seq.empty[SidePartialSpec]) {
        case (acc, spec) if acc.exists(_.originalExpr.semanticEquals(spec.originalExpr)) => acc
        case (acc, spec) => acc :+ spec
      }
  }

  private def rewriteAggregateExpressions(
      aggExprs: Seq[NamedExpression],
      sidePartials: Seq[SidePartialRef]): Option[Seq[NamedExpression]] = {
    if (sidePartials.isEmpty) {
      // Grouping-only aggregates have no buffers to rewrite.
      return Some(aggExprs)
    }

    // Replace every pushed aggregate subexpression with a final wrapper that consumes the pushed
    // wrapper output. This allows expressions like `sum(a) - sum(b)` to be split and pushed even
    // though the top-level result expression contains multiple aggregate functions.
    var rewrittenAny = false

    val rewrittenAggExprs = aggExprs.map {
      case alias @ Alias(expr, _) =>
        val rewrittenExpr = rewriteAggregateExpr(expr, sidePartials)
        if (!rewrittenExpr.fastEquals(expr)) {
          rewrittenAny = true
          Alias(rewrittenExpr, alias.name)(
            exprId = alias.exprId,
            qualifier = alias.qualifier,
            explicitMetadata = alias.explicitMetadata,
            nonInheritableMetadataKeys = alias.nonInheritableMetadataKeys
          )
        } else {
          alias
        }
      case other =>
        val rewrittenExpr = rewriteAggregateExpr(other, sidePartials)
        if (!rewrittenExpr.fastEquals(other)) {
          rewrittenAny = true
          rewrittenExpr.asInstanceOf[NamedExpression]
        } else {
          other
        }
    }

    if (rewrittenAny) Some(rewrittenAggExprs) else None
  }

  private def rewriteAggregateExpr(
      expr: Expression,
      sidePartials: Seq[SidePartialRef]): Expression = {
    // Rewrite only the aggregate nodes chosen for pushdown. Everything around them stays in its
    // original logical shape, so expressions such as `sum(a) - sum(b)` still look the same above
    // the join after each aggregate term is swapped to a final-phase wrapper.
    expr.transformUp {
      case ae: AggregateExpression =>
        pushableSpec(ae)
          .flatMap {
            spec =>
              sidePartials.collectFirst {
                case SidePartialRef(sideSpec, partialAttr)
                    if sideSpec.originalExpr.semanticEquals(spec.originalExpr) =>
                  JoinAggregateFunctionWrapper
                    .wrapperFinal(spec.aggregate, partialAttr, spec.wrapperKey)
                    .toAggregateExpression()
              }
          }
          .getOrElse(ae)
    }
  }

  private def canPushToSide(agg: DeclarativeAggregate, sideOutputSet: AttributeSet): Boolean = {
    agg.children.forall(_.references.forall(sideOutputSet.contains))
  }

  private def dedupeAttrs(attrs: Seq[Attribute]): Seq[Attribute] = {
    attrs.foldLeft(Seq.empty[Attribute]) {
      case (acc, attr) if acc.exists(_.semanticEquals(attr)) => acc
      case (acc, attr) => acc :+ attr
    }
  }

  private def referencedAttrsInOrder(expr: Expression): Seq[Attribute] = {
    expr.collect { case attr: Attribute => attr }
  }

  sealed trait JoinSide {
    def plan(join: Join): LogicalPlan
    def outputSet(join: Join): AttributeSet
    def replace(join: Join, newPlan: LogicalPlan): Join
  }

  case object JoinLeft extends JoinSide {
    override def plan(join: Join): LogicalPlan = join.left
    override def outputSet(join: Join): AttributeSet = join.left.outputSet
    override def replace(join: Join, newPlan: LogicalPlan): Join = join.copy(left = newPlan)
  }

  case object JoinRight extends JoinSide {
    override def plan(join: Join): LogicalPlan = join.right
    override def outputSet(join: Join): AttributeSet = join.right.outputSet
    override def replace(join: Join, newPlan: LogicalPlan): Join = join.copy(right = newPlan)
  }
}

case class PushAggregateThroughJoinBatch(spark: SparkSession) extends Rule[LogicalPlan] {
  private val decimalAvgRule = DecimalAggregates
  private val pushRule = PushAggregateThroughJoinRule(spark)

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!isEnabled) {
      return plan
    }
    val decimalAvgRewrittenPlan = decimalAvgRule(plan)
    val pushed = pushRule(decimalAvgRewrittenPlan)
    if (pushed.fastEquals(decimalAvgRewrittenPlan)) {
      plan
    } else {
      pushed
    }
  }

  private def isEnabled: Boolean = {
    GlutenConfig.get.pushAggregateThroughJoinEnabled
  }
}

/** Runs eager aggregation after logical optimization, before physical aggregate planning. */
case class PushAggregateThroughJoin(spark: SparkSession) extends SparkStrategy {
  private val rule = PushAggregateThroughJoinBatch(spark)

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case agg: Aggregate =>
      val rewritten = rule(agg)
      if (rewritten.fastEquals(agg)) {
        Nil
      } else {
        Seq(planLater(rewritten))
      }
    case _ => Nil
  }
}

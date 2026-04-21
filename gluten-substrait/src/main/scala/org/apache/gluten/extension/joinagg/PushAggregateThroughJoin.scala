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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, EqualTo, Expression, NamedExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, DeclarativeAggregate}
import org.apache.spark.sql.catalyst.optimizer.DecimalAggregates
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

case class PushAggregateThroughJoin(spark: SparkSession)
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
   *
   * The key invariants are:
   *   1. Only declarative, non-distinct aggregates are rewritten.
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
      case agg: Aggregate
          if !containsWrapperAggregateInOutput(agg.aggregateExpressions) &&
            hasPushableAggExpr(agg.aggregateExpressions) &&
            !hasDistinctAggExpr(agg.aggregateExpressions) =>
        // 1) Aggregate+Join => FinalWrapperAgg(PartialWrapperAgg(...Join...))
        splitAggregate(agg) match {
          case Some(newAgg) =>
            splitCount += 1
            // 2) Exhaustively push PartialWrapperAgg through join edges.
            val pushed = pushPartialWrapperAggregate(newAgg)
            // 3) Return rewritten plan with pushed partial wrapper aggregates.
            pushed
          case None => agg
        }
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

  private def splitAggregate(agg: Aggregate): Option[Aggregate] = {
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
    val pushableSpecs = collectPushableSpecs(agg.aggregateExpressions)
    if (pushableSpecs.isEmpty) {
      None
    } else {
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

      val partialAgg = Aggregate(
        groupingExpressions = partialGroupingKeys,
        aggregateExpressions = partialGroupingKeys ++ partialAliases,
        child = agg.child
      )

      val partialOutputAttrs =
        partialAgg.output
          .slice(partialGroupingKeys.size, partialGroupingKeys.size + partialAliases.size)
          .map(cleanAttr)
      val partialRefs = pushableSpecs.zip(partialOutputAttrs).map {
        case (spec, attr) => SidePartialRef(spec, attr)
      }

      rewriteAggregateExpressions(agg.aggregateExpressions, partialRefs).map {
        rewrittenAggExprs =>
          agg.copy(
            aggregateExpressions = rewrittenAggExprs,
            child = partialAgg
          )
      }
    }
  }

  private def pushPartialWrapperAggregate(agg: Aggregate): LogicalPlan = {
    // Push one join edge per iteration. This keeps the rewrite local and lets `maxDepth` bound
    // how far a pushed aggregate is allowed to travel through a multi-join subtree.
    var current: LogicalPlan = agg
    var changed = true
    var pushCount = 0
    while (changed && pushCount < maxDepth) {
      changed = false
      current = current.transformUp {
        case partialAgg: Aggregate if isPurePartialWrapperAggregate(partialAgg) =>
          pushOnce(
            partialAgg,
            partialAgg.groupingExpressions,
            partialAgg.aggregateExpressions,
            partialAgg.child) match {
            case Some(newPlan) =>
              pushCount += 1
              changed = true
              newPlan
            case None =>
              partialAgg
          }
      }
    }
    successfulPushCount += pushCount
    current
  }

  private def pushOnce(
      partialAgg: Aggregate,
      groupingExprs: Seq[Expression],
      aggExprs: Seq[NamedExpression],
      child: LogicalPlan): Option[LogicalPlan] = {
    extractJoin(child).flatMap {
      case (join, wrapperRequiredAttrs, rebuild) =>
        Seq(JoinLeft, JoinRight).iterator
          .flatMap {
            side =>
              val maybePushedJoin =
                pushPartialAggToJoinSide(join, groupingExprs, aggExprs, wrapperRequiredAttrs, side)
              maybePushedJoin match {
                case Some(pushedJoin) =>
                  val requiredAttrs = partialAgg.output.collect { case a: Attribute => a }
                  Some(rebuild(pushedJoin, requiredAttrs))
                case None => None
              }
          }
          .toSeq
          .headOption
    }
  }

  private def pushPartialAggToJoinSide(
      join: Join,
      groupingExprs: Seq[Expression],
      aggExprs: Seq[NamedExpression],
      wrapperRequiredAttrs: Seq[Attribute],
      side: JoinSide): Option[Join] = {
    // A pushed wrapper aggregate may move to a join side only when all of its aggregate inputs
    // come from that side. The pushed grouping then has to include:
    //   - grouping attrs already present on this side
    //   - join keys / join-condition attrs required to preserve join semantics
    //   - attrs referenced by non-pushable expressions still above the pushed aggregate
    //   - attrs propagated from extracted Project / Filter nodes when those attrs are required to
    //     preserve derived grouping expressions
    //
    // The pushed grouping must *not* include pure measure inputs of the pushed aggregate such as
    // `ss_net_profit`, otherwise the pre-aggregation becomes over-constrained and ineffective.
    val wrapperAliases = collectPartialWrapperAliases(aggExprs)
    if (wrapperAliases.isEmpty) {
      return None
    }

    val sideOutputSet = side.outputSet(join)
    val allPushable = wrapperAliases.forall {
      case (_, wrapper) => canPushToSide(wrapper.innerAgg, sideOutputSet)
    }
    if (!allPushable) {
      return None
    }

    val sideGroupingAttrs = groupingExprs
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

    // These attrs belong to aggregate subexpressions that stay above the pushed aggregate. They
    // must survive subtree rebuild, but they are not themselves proof that the pushed aggregate
    // needs to group by those measures.
    val sideNonPushableAggAttrs = dedupeAttrs(aggExprs.flatMap {
      case Alias(expr, _) if !isPushableExpr(expr) && !containsWrapperAggregateExpr(expr) =>
        referencedAttrsInOrder(expr).collect {
          case a: Attribute if sideOutputSet.contains(a) => a
        }
      case expr if !isPushableExpr(expr) && !containsWrapperAggregateExpr(expr) =>
        referencedAttrsInOrder(expr).collect {
          case a: Attribute if sideOutputSet.contains(a) => a
        }
      case _ => Nil
    })
    val sidePushableAggInputAttrs = dedupeAttrs(wrapperAliases.flatMap {
      case (_, wrapper) =>
        wrapper.innerAgg.children.flatMap(referencedAttrsInOrder).collect {
          case a: Attribute if sideOutputSet.contains(a) => a
        }
    })
    val sideWrapperRequiredAttrs = dedupeAttrs(
      wrapperRequiredAttrs
        .collect {
          case a: Attribute if sideOutputSet.contains(a) => a
        }
        .filterNot(attr => sidePushableAggInputAttrs.exists(_.semanticEquals(attr))))
    // `wrapperRequiredAttrs` carries dependencies from extracted Project/Filter nodes above the
    // join. Those dependencies are needed to preserve grouping semantics for derived grouping
    // expressions such as `substr(w_warehouse_name, 1, 20)`. However, pushed aggregate inputs
    // like `ss_net_profit` must stay as child inputs only; promoting them into grouping keys
    // would over-constrain the pushed pre-aggregation.
    val pushedGrouping =
      dedupeAttrs(
        sideGroupingAttrs ++
          sideJoinKeys ++
          sideJoinCondAttrs ++
          sideNonPushableAggAttrs ++
          sideWrapperRequiredAttrs)
    if (pushedGrouping.isEmpty) {
      return None
    }

    val pushedWrapperAliases = wrapperAliases.map {
      case (alias, wrapper) =>
        val wrapped = JoinAggregateFunctionWrapper
          .wrapperPartial(wrapper.innerAgg, wrapper.wrapperKey)
          .toAggregateExpression()
        Alias(wrapped, alias.name)(
          exprId = alias.exprId,
          qualifier = alias.qualifier,
          explicitMetadata = alias.explicitMetadata,
          nonInheritableMetadataKeys = alias.nonInheritableMetadataKeys
        )
    }

    val pushedAgg = Aggregate(
      groupingExpressions = pushedGrouping,
      aggregateExpressions = pushedGrouping ++ pushedWrapperAliases,
      child = side.plan(join)
    )

    val pushedJoin = side.replace(join, pushedAgg)
    Some(pushedJoin)
  }

  private def isPurePartialWrapperAggregate(agg: Aggregate): Boolean = {
    val wrapperAliases = collectPartialWrapperAliases(agg.aggregateExpressions)
    wrapperAliases.nonEmpty && agg.aggregateExpressions.forall {
      case Alias(_: AggregateExpression, _) => true
      case _: AggregateExpression => false
      case _ => true
    }
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

  private def containsWrapperAggregateInOutput(aggExprs: Seq[NamedExpression]): Boolean = {
    aggExprs.exists {
      _.exists {
        case AggregateExpression(_: JoinAggregateFunctionWrapper, _, _, _, _) => true
        case _ => false
      }
    }
  }

  private def containsWrapperAggregateExpr(expr: Expression): Boolean = {
    expr.exists {
      case AggregateExpression(_: JoinAggregateFunctionWrapper, _, _, _, _) => true
      case _ => false
    }
  }

  private def hasPushableAggExpr(aggExprs: Seq[Expression]): Boolean = {
    aggExprs.exists(expr => collectPushableSpecs(expr).nonEmpty)
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

  sealed private trait JoinSide {
    def plan(join: Join): LogicalPlan
    def outputSet(join: Join): AttributeSet
    def replace(join: Join, newPlan: LogicalPlan): Join
  }

  private case object JoinLeft extends JoinSide {
    override def plan(join: Join): LogicalPlan = join.left
    override def outputSet(join: Join): AttributeSet = join.left.outputSet
    override def replace(join: Join, newPlan: LogicalPlan): Join = join.copy(left = newPlan)
  }

  private case object JoinRight extends JoinSide {
    override def plan(join: Join): LogicalPlan = join.right
    override def outputSet(join: Join): AttributeSet = join.right.outputSet
    override def replace(join: Join, newPlan: LogicalPlan): Join = join.copy(right = newPlan)
  }
}

case class PushAggregateThroughJoinBatch(spark: SparkSession) extends Rule[LogicalPlan] {
  private val decimalAvgRule = DecimalAggregates
  private val pushRule = PushAggregateThroughJoin(spark)

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!isEnabled) {
      return plan
    }
    val decimalAvgRewrittenPlan = decimalAvgRule(plan)
    pushRule(decimalAvgRewrittenPlan)
  }

  private def isEnabled: Boolean = {
    GlutenConfig.get.pushAggregateThroughJoinEnabled
  }
}

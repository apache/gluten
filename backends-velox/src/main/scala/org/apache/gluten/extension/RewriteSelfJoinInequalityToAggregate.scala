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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.RowOrdering
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.LongType

/**
 * Rewrites self-join with inequality into GROUP BY + HAVING COUNT(DISTINCT) > 1.
 *
 * Targets three patterns; all require an existence-only context (LeftSemi/LeftAnti join, or
 * InSubquery/Exists expression) so that row-count multiplicity from the self-join cross-product
 * does not affect semantics.
 *
 *   - Pattern A' (InSubquery/Exists primary): InSubquery/Exists whose subquery top-level join is a
 *     direct self-join. The primary path for TPC-DS Q95.
 *   - Pattern A2 (nested): InSubquery/Exists whose subquery contains an outer InnerJoin that has a
 *     self-join child. Only the self-join child is replaced with Aggregate; the outer join is
 *     preserved.
 *   - Pattern A (LeftSemi/LeftAnti): LeftSemi/LeftAnti whose right child is an Inner self-join
 *     (possibly wrapped in Project). Matches semi/anti joins that already exist in the input --
 *     e.g. from an explicit `LEFT SEMI JOIN` clause. Note: this rule is injected via
 *     `injectOptimizerRule`, which places it in the operator-optimization batch that runs BEFORE
 *     `RewritePredicateSubquery`; A is NOT a post-subquery-rewrite fallback for A'.
 *
 * Correlated subqueries (outer references / joinCond in ListQuery/Exists) are fail-closed at the
 * entry expression, since our ExprId canonicalization does not remap those predicates.
 *
 * All three share:
 *   - [[buildAggregateHavingDistinctGt1]] to construct `Filter(cnt > 1, Aggregate)`
 *   - [[canonicalizeWrapper]] to rebuild a wrapping Project so every equi-key reference points to
 *     the sjLeft-side attribute, with **fresh exprIds** (Spark's SPARK-21835 style -- no reuse of
 *     original exprIds), returning an old->new attribute remap for downstream rewrite.
 *
 * Controlled by `spark.gluten.sql.rewrite.selfJoinInequality` (default false, opt-in).
 */
case class RewriteSelfJoinInequalityToAggregate(spark: SparkSession)
  extends Rule[LogicalPlan]
  with PredicateHelper
  with Logging {

  private val CountDistinctAliasName = "_gluten_rw_selfjoin_cnt_distinct"

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!VeloxConfig.get.enableRewriteSelfJoinInequality) {
      logDebug("RewriteSelfJoinInequalityToAggregate: disabled via config, skipping")
      return plan
    }

    // Pattern A: rewrite LeftSemi/LeftAnti whose right child is an Inner self-join.
    val afterOps = plan.transformUp {
      case j: Join
          if (j.joinType == LeftSemi || j.joinType == LeftAnti) &&
            j.condition.isDefined &&
            isInnerJoinShape(j.right) =>
        tryRewriteSemiWithSelfJoinChild(j).getOrElse(j)
      case other => other
    }

    // Pattern A' / A2: rewrite subquery plans embedded in InSubquery/Exists.
    // Type-based matching (`x: T`) + named-argument copy keeps this portable across
    // Spark 3.3/3.4/3.5/4.x where ListQuery/Exists case-class arity has drifted.
    //
    // Correlated subquery fail-closed: `SubqueryExpression.children.nonEmpty` iff the
    // subquery has outer references / correlated join conditions. These predicates
    // reference attributes INSIDE the subquery plan by ExprId; our canonicalizeWrapper
    // rewrites those ExprIds without remapping the correlated predicates, which would
    // leave dangling references after `RewritePredicateSubquery` folds them back into
    // the semi-join condition. Target workload (TPC-DS Q95) is uncorrelated, so bail
    // on any correlated candidate rather than growing the remap surface.
    val rewritten = afterOps.transformAllExpressions {
      case in @ InSubquery(_, lq: ListQuery) if lq.children.isEmpty =>
        rewriteSubqueryPlan(lq.plan) match {
          case Some(newSub) => in.copy(query = lq.copy(plan = newSub))
          case None => in
        }
      case ex: Exists if ex.children.isEmpty =>
        rewriteSubqueryPlan(ex.plan) match {
          case Some(newSub) => ex.copy(plan = newSub)
          case None => ex
        }
    }
    if (!(rewritten eq plan)) {
      logDebug(
        "RewriteSelfJoinInequalityToAggregate: rewrote self-join to " +
          "GROUP BY + HAVING COUNT(DISTINCT) > 1")
    }
    rewritten
  }

  // ============================================================================
  //  Shared helpers
  // ============================================================================

  private def isInnerJoinShape(plan: LogicalPlan): Boolean = plan match {
    case Project(_, j: Join) if j.joinType == Inner && j.condition.isDefined => true
    case j: Join if j.joinType == Inner && j.condition.isDefined => true
    case _ => false
  }

  /**
   * Build `Filter(cnt > 1, Aggregate(equiKeys, [equiKeys, cnt_alias], Filter(IsNotNull(equiKeys),
   * child)))`. Returns the Filter node whose output is `equiKeys ++ [count_alias_attr]`.
   *
   * The extra `IsNotNull(equiKeys)` filter is essential to preserve the original equi-join's NULL
   * semantics. Under SQL 3VL, `left.k = right.k` never matches when either side is NULL, so the
   * original self-join drops rows with NULL equi-keys. Aggregate, in contrast, groups NULL keys
   * together into a single "NULL group" -- if that group has >= 2 distinct non-null neq values,
   * COUNT(DISTINCT) > 1 fires and injects NULL into the subquery output. That leaked NULL then
   * turns `NOT IN` into a spurious empty result (Spark's null-aware anti-join uses
   * `Or(equi, IsNull(equi))` which any NULL sub-row satisfies) and can flip EXISTS/IN outcomes. The
   * neq column needs no such filter: `COUNT(DISTINCT col)` already ignores NULL.
   */
  private def buildAggregateHavingDistinctGt1(
      equiKeys: Seq[Attribute],
      neqCol: Attribute,
      child: LogicalPlan): LogicalPlan = {
    val countExpr = AggregateExpression(
      Count(Seq(neqCol)),
      mode = Complete,
      isDistinct = true,
      filter = None,
      NamedExpression.newExprId)
    val countAlias = Alias(countExpr, CountDistinctAliasName)()
    // Seq[Attribute] is a Seq[NamedExpression] via covariance; no cast needed.
    val aggExprs: Seq[NamedExpression] = equiKeys :+ countAlias
    val nonNullChild = equiKeys
      .map(a => IsNotNull(a): Expression)
      .reduceOption(And)
      .map(Filter(_, child))
      .getOrElse(child)
    val agg = Aggregate(equiKeys, aggExprs, nonNullChild)
    Filter(GreaterThan(countAlias.toAttribute, Literal(1L, LongType)), agg)
  }

  /**
   * Canonicalize a Project so every equi-key reference points at the sjLeft-side attribute (both
   * sides of a valid self-join share names, so this substitution is semantically safe). Uses
   * **fresh exprIds** (no reuse of original wrapper output exprIds) -- the same technique Spark's
   * own `dedupSubqueryOnSelfJoin` uses when it needs to change subquery output.
   *
   * Returns the rebuilt Project and a map `oldWrapperOutputExprId -> newWrapperOutputAttr`, so
   * downstream references (outer join condition, top-level Project) can be updated consistently.
   *
   * `equiPairs` provides the definitive ExprId-based lookup: `equiPair (l, r)` binds
   * `l.exprId -> l` (identity) and `r.exprId -> l` (sjRight -> sjLeft). Attribute identity in
   * Catalyst is ExprId, not name; two columns can share a name with distinct ExprIds. Name-based
   * lookup would silently drop such entries via `.toMap`.
   *
   * Fails (returns None) when a projectList entry is neither an equi-key Attribute (by ExprId) nor
   * `Alias(equi-key Attribute, _)`. Fail-closed.
   */
  private def canonicalizeWrapper(
      projectList: Seq[NamedExpression],
      equiPairs: Seq[(Attribute, Attribute)],
      newChild: LogicalPlan): Option[(Project, Map[ExprId, Attribute])] = {
    // ExprId-based canonical map: any equi-key attribute (either side) -> sjLeft attribute.
    val exprIdToLeft: Map[ExprId, Attribute] =
      equiPairs.flatMap { case (l, r) => Seq(l.exprId -> l, r.exprId -> l) }.toMap
    val oldOutput: Seq[Attribute] = projectList.map(_.toAttribute)
    val mapped: Seq[Option[NamedExpression]] = projectList.map {
      case a: Attribute if exprIdToLeft.contains(a.exprId) =>
        // Wrap every rewritten output slot in a fresh Alias.
        //
        // When a wrapper reprojects BOTH sides of the same equi pair (e.g.
        // `SELECT s1.k, s2.k FROM T s1 JOIN T s2 ON s1.k = s2.k AND s1.v <> s2.v`),
        // both entries collapse to the same sjLeft Attribute after the self-join is
        // rewritten. Duplicate output ExprIds are not illegal in Spark (`SELECT a, a`
        // is a valid Project), but fresh Aliases give each output slot an independent
        // identity, which keeps the `oldOutput -> newOutput` remap 1-to-1 and lets
        // downstream references (outer join condition, top-level Project) be updated
        // unambiguously via ExprId.
        //
        // The fresh ExprId is on the Alias ITSELF; the referenced child keeps its
        // original ExprId. Spark's logical-plan integrity checks reject reusing a
        // referenced ExprId as the Alias's own ExprId, not duplication across slots.
        Some(Alias(exprIdToLeft(a.exprId), a.name)(): NamedExpression)
      case al @ Alias(a: Attribute, _) if exprIdToLeft.contains(a.exprId) =>
        // Fresh exprId; do NOT reuse `al.exprId`. Reusing another expression's exprId
        // is the pattern that Spark 3.3 flags via structural-integrity checks.
        Some(Alias(exprIdToLeft(a.exprId), al.name)(): NamedExpression)
      case _ => None
    }
    if (mapped.exists(_.isEmpty)) {
      None
    } else {
      val newProjectList = mapped.flatten
      val newWrapper = Project(newProjectList, newChild)
      val newOutput = newWrapper.output
      val remap: Map[ExprId, Attribute] =
        oldOutput.zip(newOutput).map { case (o, n) => o.exprId -> n }.toMap
      Some((newWrapper, remap))
    }
  }

  /**
   * Replace equi-key attribute references inside a NamedExpression according to `remap`, while
   * preserving the NamedExpression shape.
   *
   * `Expression.transformUp` returns `Expression`, not `NamedExpression`. We avoid a blanket
   * `asInstanceOf[NamedExpression]` by handling the two shapes that can appear in a Project's
   * `projectList` explicitly: a bare Attribute (whose top-level may itself be replaced) and an
   * Alias (which stays an Alias while its child is transformed). Anything else in a projectList --
   * e.g. computed expressions we don't own -- is passed through unchanged.
   */
  private def remapNamedExpressionAttributes(
      ne: NamedExpression,
      remap: Map[ExprId, Attribute]): NamedExpression = ne match {
    case a: Attribute if remap.contains(a.exprId) => remap(a.exprId)
    case a: Attribute => a
    case al: Alias =>
      val newChild = al.child.transformUp {
        case a: Attribute if remap.contains(a.exprId) => remap(a.exprId)
      }
      if (newChild eq al.child) al
      else Alias(newChild, al.name)(al.exprId, al.qualifier, al.explicitMetadata)
    case other => other
  }

  // ============================================================================
  //  Pattern A' / A2 dispatch (subquery plans of InSubquery / Exists)
  // ============================================================================

  private def rewriteSubqueryPlan(plan: LogicalPlan): Option[LogicalPlan] = {
    // Candidate-level nondeterminism guard: reject if ANY node in the whole subquery plan
    // is non-repeatable (Rand, LIMIT-without-ORDER-BY, Sample, Offset, streaming). This
    // catches nondeterminism that has been hoisted above the self-join by an earlier
    // optimizer rule -- the per-side `isSameBaseRelation` check alone would miss it because
    // both innerLeft/innerRight can look deterministic after such a hoist.
    if (!isRepeatablePlan(plan)) return None

    val (projectListOpt, innerJoin): (Option[Seq[NamedExpression]], Join) = plan match {
      case Project(pl, j: Join) if j.joinType == Inner && j.condition.isDefined =>
        (Some(pl), j)
      case j: Join if j.joinType == Inner && j.condition.isDefined =>
        (None, j)
      case _ => return None
    }

    if (isSameBaseRelation(innerJoin.left, innerJoin.right)) {
      rewriteDirectSelfJoin(projectListOpt, innerJoin)
    } else {
      rewriteNestedSelfJoin(projectListOpt, innerJoin)
    }
  }

  // ============================================================================
  //  Pattern A' : direct self-join at subquery top level
  // ============================================================================

  private def rewriteDirectSelfJoin(
      projectListOpt: Option[Seq[NamedExpression]],
      innerJoin: Join): Option[LogicalPlan] = {
    val innerLeft = innerJoin.left
    val innerRight = innerJoin.right
    val innerCond = innerJoin.condition.get

    val parsed = parseSelfJoinCondition(innerCond, innerLeft, innerRight)
    if (parsed.isEmpty) return None
    // parseSelfJoinCondition guarantees Seq[(Attribute, Attribute)] and distinct equi-key names.
    val (equiPairs, neqPairs) = parsed.get

    val innerLeftEquiAttrs: Seq[Attribute] = equiPairs.map(_._1)
    val innerLeftNeqAttr: Attribute = neqPairs.head._1
    val filtered = buildAggregateHavingDistinctGt1(innerLeftEquiAttrs, innerLeftNeqAttr, innerLeft)

    // Fail-closed on bare-Join subqueries: without a wrapping Project the subquery output
    // is the full self-join output (both sides' columns). Replacing that with
    // `Project(equiKeys, filtered)` shrinks the output; if the enclosing InSubquery
    // referenced a non-equi column by position, `values.zip(sub.output).map(EqualTo.tupled)`
    // inside RewritePredicateSubquery would build an incorrect semi condition. Q95's
    // subqueries all have an explicit Project wrapper, so this branch does not affect it.
    projectListOpt match {
      case None =>
        None
      case Some(pl) =>
        canonicalizeWrapper(pl, equiPairs, filtered).map {
          case (newWrapper, _) =>
            logDebug(
              s"Pattern A' - equiKeys=[${innerLeftEquiAttrs.map(_.name).mkString(",")}]" +
                s", neqCol=${innerLeftNeqAttr.name}" +
                s", outCols=[${newWrapper.projectList.map(_.name).mkString(",")}]")
            newWrapper
        }
    }
  }

  // ============================================================================
  //  Pattern A2 : self-join nested inside another InnerJoin in the subquery
  // ============================================================================

  private def rewriteNestedSelfJoin(
      projectListOpt: Option[Seq[NamedExpression]],
      outerJoin: Join): Option[LogicalPlan] = {
    val outerCond = outerJoin.condition.get

    val (selfJoinSide, selfJoinOnRight) =
      tryExtractSelfJoin(outerJoin.right) match {
        case Some(_) => (outerJoin.right, true)
        case None =>
          tryExtractSelfJoin(outerJoin.left) match {
            case Some(_) => (outerJoin.left, false)
            case None => return None
          }
      }

    val (selfJoinProjectOpt, selfJoin) = selfJoinSide match {
      case p @ Project(_, j: Join) if j.joinType == Inner && j.condition.isDefined =>
        (Some(p), j)
      case j: Join if j.joinType == Inner && j.condition.isDefined =>
        (None, j)
      case _ => return None
    }

    val sjLeft = selfJoin.left
    val sjRight = selfJoin.right
    val sjCond = selfJoin.condition.get
    if (!isSameBaseRelation(sjLeft, sjRight)) return None

    val parsed = parseSelfJoinCondition(sjCond, sjLeft, sjRight)
    if (parsed.isEmpty) return None
    // parseSelfJoinCondition guarantees Seq[(Attribute, Attribute)] and distinct equi-key names.
    val (equiPairs, neqPairs) = parsed.get

    val sjLeftEquiAttrs: Seq[Attribute] = equiPairs.map(_._1)
    val sjLeftNeqAttr: Attribute = neqPairs.head._1

    val selfJoinOutputSet = selfJoinSide.outputSet
    val sjEquiExprIds: Set[ExprId] =
      equiPairs.flatMap { case (l, r) => Seq(l.exprId, r.exprId) }.toSet
    // wrapper Project may reproject equi-keys under fresh alias exprIds; include those.
    val wrapperEquiExprIds: Set[ExprId] = selfJoinProjectOpt.toSeq.flatMap {
      p =>
        p.projectList.flatMap {
          case a: Attribute if sjEquiExprIds.contains(a.exprId) => Some(a.exprId)
          case al @ Alias(a: Attribute, _) if sjEquiExprIds.contains(a.exprId) => Some(al.exprId)
          case _ => None
        }
    }.toSet
    val allEquiExprIds = sjEquiExprIds ++ wrapperEquiExprIds

    // Outer join condition may reference only equi-key attrs from the self-join side.
    val outerCondRefs = outerCond.references.filter(selfJoinOutputSet.contains)
    if (!outerCondRefs.forall(a => allEquiExprIds.contains(a.exprId))) return None

    // Top-level subquery Project may reference only equi-key attrs from the self-join side.
    val projectOk = projectListOpt.forall {
      pl =>
        val refs = pl.flatMap(_.references).filter(selfJoinOutputSet.contains)
        refs.forall(a => allEquiExprIds.contains(a.exprId))
    }
    if (!projectOk) return None

    val filtered = buildAggregateHavingDistinctGt1(sjLeftEquiAttrs, sjLeftNeqAttr, sjLeft)

    val (newSelfJoinSide, outputRemap): (LogicalPlan, Map[ExprId, Attribute]) =
      selfJoinProjectOpt match {
        case Some(wp) =>
          canonicalizeWrapper(wp.projectList, equiPairs, filtered) match {
            case Some((newWrapper, remap)) => (newWrapper, remap)
            case None => return None
          }
        case None if projectListOpt.isEmpty =>
          // Fail-closed: with neither a wrapper Project around the self-join nor a top-level
          // subquery Project, the outer join currently exposes every self-join column, and
          // replacing the self-join with `Project(equiKeys, filtered)` would shrink the outer
          // join's right-hand output arity. RewritePredicateSubquery's positional zip
          // (`values.zip(sub.output).map(EqualTo.tupled)`) would then bind semi predicates to
          // the wrong attributes -- silently dropping components of a tuple IN/EXISTS. A
          // top-level Project (`projectListOpt`) is what would let the arity be preserved
          // by the top-level rewrite loop; without one, refuse to rewrite.
          return None
        case None =>
          // No wrapper Project but there IS a top-level subquery Project: shrinking the outer
          // join's self-join-side output is safe because the top-level Project is rewritten
          // consistently via `outputRemap` below and the top-level rewrite loop ensures
          // subquery output arity matches what the enclosing InSubquery/Exists expects.
          // Outer references may point at sjRight equi-attributes; remap them to sjLeft
          // (same names in a valid self-join).
          val newP = Project(sjLeftEquiAttrs, filtered)
          val remap: Map[ExprId, Attribute] =
            equiPairs.map { case (l, r) => r.exprId -> l }.toMap
          (newP, remap)
      }

    // Rewrite outer join condition to use new wrapper output attributes.
    val newOuterCond = outerCond.transformUp {
      case a: Attribute if outputRemap.contains(a.exprId) => outputRemap(a.exprId)
    }

    val newOuterJoin = if (selfJoinOnRight) {
      outerJoin.copy(right = newSelfJoinSide, condition = Some(newOuterCond))
    } else {
      outerJoin.copy(left = newSelfJoinSide, condition = Some(newOuterCond))
    }

    // Rewrite top-level Project references.
    val result = projectListOpt match {
      case Some(pl) =>
        val newPl = pl.map(ne => remapNamedExpressionAttributes(ne, outputRemap))
        Project(newPl, newOuterJoin)
      case None => newOuterJoin
    }

    logDebug(
      s"Pattern A2 - equiKeys=[${sjLeftEquiAttrs.map(_.name).mkString(",")}]" +
        s", neqCol=${sjLeftNeqAttr.name}")
    Some(result)
  }

  private def tryExtractSelfJoin(plan: LogicalPlan): Option[Join] = {
    val join = plan match {
      case Project(_, j: Join) if j.joinType == Inner && j.condition.isDefined => j
      case j: Join if j.joinType == Inner && j.condition.isDefined => j
      case _ => return None
    }
    if (!isSameBaseRelation(join.left, join.right)) return None
    val parsed = parseSelfJoinCondition(join.condition.get, join.left, join.right)
    if (parsed.isEmpty) return None
    Some(join)
  }

  // ============================================================================
  //  Pattern A : LeftSemi/LeftAnti whose right child is an Inner self-join
  // ============================================================================

  private def tryRewriteSemiWithSelfJoinChild(original: Join): Option[LogicalPlan] = {
    // Candidate-level nondeterminism guard on the entire right subtree. Same rationale as
    // in [[rewriteSubqueryPlan]]: catches Rand/Limit/Sample/Offset hoisted between the semi
    // join and the inner self-join by an earlier optimizer rule.
    if (!isRepeatablePlan(original.right)) return None

    val left = original.left
    val right = original.right
    val semiCondition = original.condition.get

    val (innerJoin, wrapper): (Join, Option[Project]) = right match {
      case p @ Project(_, j: Join) if j.joinType == Inner && j.condition.isDefined => (j, Some(p))
      case j: Join if j.joinType == Inner && j.condition.isDefined => (j, None)
      case _ => return None
    }

    val innerLeft = innerJoin.left
    val innerRight = innerJoin.right
    val innerCond = innerJoin.condition.get
    if (!isSameBaseRelation(innerLeft, innerRight)) return None

    val parsed = parseSelfJoinCondition(innerCond, innerLeft, innerRight)
    if (parsed.isEmpty) return None
    // parseSelfJoinCondition guarantees Seq[(Attribute, Attribute)] and distinct equi-key names.
    val (innerEquiPairs, innerNeqPairs) = parsed.get

    // All semi predicates must be pure equi-join.
    val semiPreds = splitConjunctivePredicates(semiCondition)
    val leftOutputSet = left.outputSet
    val rightOutputSet = right.outputSet
    val semiEquiPairs = semiPreds.collect {
      case EqualTo(l: Attribute, r: Attribute)
          if leftOutputSet.contains(l) && rightOutputSet.contains(r) =>
        (l, r)
      case EqualTo(r: Attribute, l: Attribute)
          if leftOutputSet.contains(l) && rightOutputSet.contains(r) =>
        (l, r)
    }
    if (semiEquiPairs.size != semiPreds.size) return None
    if (semiEquiPairs.isEmpty) return None

    val innerLeftEquiAttrs: Seq[Attribute] = innerEquiPairs.map(_._1)
    val innerRightEquiAttrs: Seq[Attribute] = innerEquiPairs.map(_._2)
    val innerEquiLeftAttrIds = innerLeftEquiAttrs.map(_.exprId).toSet
    val innerEquiRightAttrIds = innerRightEquiAttrs.map(_.exprId).toSet
    val innerEquiAllIds = innerEquiLeftAttrIds ++ innerEquiRightAttrIds
    val innerNeqAttr: Attribute = innerNeqPairs.head._1

    // Semi right-side keys must derive from inner equi-key attributes, possibly via wrapper alias.
    val rightKeyIds = semiEquiPairs.map(_._2.exprId).toSet
    val validSemiKeys = rightKeyIds.forall {
      id =>
        innerEquiAllIds.contains(id) || wrapper.exists {
          p =>
            p.projectList.exists {
              case al @ Alias(a: Attribute, _) =>
                al.exprId == id && innerEquiAllIds.contains(a.exprId)
              case a: Attribute =>
                a.exprId == id && innerEquiAllIds.contains(a.exprId)
              case _ => false
            }
        }
    }
    if (!validSemiKeys) return None

    val filtered = buildAggregateHavingDistinctGt1(innerLeftEquiAttrs, innerNeqAttr, innerLeft)

    // Replace the entire right subtree with a Project of just the equi keys.
    val projectedKeys = Project(innerLeftEquiAttrs, filtered)

    // Build an ExprId-keyed remap from every attribute reachable via the old right side to
    // the corresponding sjLeft equi-attribute:
    //   (a) direct innerLeft equi attr -> identity (by ExprId)
    //   (b) direct innerRight equi attr -> paired sjLeft attr (looked up via equiPairs, NOT name)
    //   (c) wrapper `Alias(equiAttr, name)` output -> paired sjLeft attr (via inner Attribute's
    //       ExprId, NOT the alias name)
    // Never use column name as identity: Catalyst allows same-name attributes with distinct
    // ExprIds (e.g. `SELECT a AS k, b AS k`) and `.toMap` by name would silently drop one.
    val exprIdToInnerLeft: Map[ExprId, Attribute] =
      innerEquiPairs.flatMap {
        case (l, r) => Seq(l.exprId -> l, r.exprId -> l)
      }.toMap
    val wrapperRemap: Map[ExprId, Attribute] = wrapper.map {
      p =>
        p.projectList.flatMap {
          case al @ Alias(a: Attribute, _) if exprIdToInnerLeft.contains(a.exprId) =>
            Some(al.exprId -> exprIdToInnerLeft(a.exprId))
          case _ => None
        }.toMap
    }.getOrElse(Map.empty)
    val oldToNewMap: Map[ExprId, Attribute] = exprIdToInnerLeft ++ wrapperRemap

    // Fail-closed: refuse to rewrite if any attribute in the old right side is unresolvable.
    val unresolved = semiCondition.collect {
      case a: Attribute if rightOutputSet.contains(a) && !oldToNewMap.contains(a.exprId) => a
    }
    if (unresolved.nonEmpty) return None

    val newSemiCondition = semiCondition.transformUp {
      case a: Attribute if oldToNewMap.contains(a.exprId) && rightOutputSet.contains(a) =>
        oldToNewMap(a.exprId)
    }

    logDebug(
      s"Pattern A (${original.joinType}) - equiKeys=[" +
        innerLeftEquiAttrs.map(_.name).mkString(",") +
        s"], neqCol=${innerNeqAttr.name}")
    Some(original.copy(right = projectedKeys, condition = Some(newSemiCondition)))
  }

  // ============================================================================
  //  parseSelfJoinCondition + isSameBaseRelation
  // ============================================================================

  /**
   * Parse a join condition into equi-pairs and inequality-pairs. Accepts only:
   *   - `EqualTo(attr, attr)` where the two attrs come from opposite sides,
   *   - `Not(EqualTo(attr, attr))` -- same side rule,
   *   - `IsNotNull(attr)` where the attr is one of the join columns.
   * Anything else in the condition disqualifies the whole rewrite (fail-closed).
   */
  private def parseSelfJoinCondition(
      condition: Expression,
      leftPlan: LogicalPlan,
      rightPlan: LogicalPlan)
      : Option[(Seq[(Attribute, Attribute)], Seq[(Attribute, Attribute)])] = {

    val leftOutput = leftPlan.outputSet
    val rightOutput = rightPlan.outputSet
    val predicates = splitConjunctivePredicates(condition)

    val equiPairs = predicates.collect {
      case EqualTo(l: Attribute, r: Attribute)
          if leftOutput.contains(l) && rightOutput.contains(r) =>
        (l, r)
      case EqualTo(r: Attribute, l: Attribute)
          if leftOutput.contains(l) && rightOutput.contains(r) =>
        (l, r)
    }

    val neqPairs = predicates.collect {
      case Not(EqualTo(l: Attribute, r: Attribute))
          if leftOutput.contains(l) && rightOutput.contains(r) =>
        (l, r)
      case Not(EqualTo(r: Attribute, l: Attribute))
          if leftOutput.contains(l) && rightOutput.contains(r) =>
        (l, r)
    }

    // Only IsNotNull predicates on join columns are safe to drop -- they're redundant with
    // the join semantics or auto-added by InferFiltersFromConstraints. IsNotNull on other
    // columns changes semantics if we drop it; bail out.
    val joinAttrIds: Set[ExprId] =
      (equiPairs ++ neqPairs).flatMap { case (l, r) => Seq(l.exprId, r.exprId) }.toSet
    val isNotNullOnJoinCols = predicates.count {
      case IsNotNull(a: Attribute) if joinAttrIds.contains(a.exprId) => true
      case _ => false
    }

    val totalMatched = equiPairs.size + neqPairs.size + isNotNullOnJoinCols
    if (totalMatched != predicates.size) return None

    if (equiPairs.isEmpty || neqPairs.isEmpty) return None

    // Only rewrite single-inequality case. Multi-column inequality
    //   (col_a<>col_a OR col_b<>col_b)
    // is NOT equivalent to COUNT(DISTINCT single_col) > 1.
    if (neqPairs.size != 1) return None

    // COUNT(DISTINCT neqCol) requires a hashable/orderable data type. Use Spark's public
    // `RowOrdering.isOrderable` (which delegates to `OrderUtils.isOrderable`) to reject
    // UDTs / Maps / and any complex type whose order-or-hash isn't defined. `AtomicType`
    // would be a simpler predicate, but `AtomicType` is `protected[sql]` and thus not
    // usable from Gluten's package. Fail-closed here so a query that would otherwise
    // run does not crash in Catalyst's CheckAnalysis after our rule fires.
    if (!neqPairs.forall { case (l, _) => RowOrdering.isOrderable(l.dataType) }) return None

    // Self-join invariant: equi and neq columns share names between the two sides.
    val equiValid = equiPairs.forall { case (l, r) => l.name == r.name }
    val neqValid = neqPairs.forall { case (l, r) => l.name == r.name }
    if (!equiValid || !neqValid) return None

    // Equi-key left-side names must be distinct across pairs. Downstream helpers rely on
    // the self-join name invariant `l.name == r.name` for each pair; two pairs sharing a
    // left name (e.g. `s1.k = s2.k AND s1.k = s2.other_col_aliased_as_k`) would produce
    // ambiguous name-to-attribute wiring downstream. Attribute identity (ExprId) is
    // handled correctly by [[canonicalizeWrapper]], but the name-invariant check
    // ([[equiValid]] / [[neqValid]] above) is still name-based. Fail-closed on this edge.
    val leftEquiNames = equiPairs.map(_._1.name)
    if (leftEquiNames.distinct.size != leftEquiNames.size) return None

    // Defensive: reject when the neq column overlaps an equi-key column
    // (e.g. `t1.k = t2.k AND t1.k <> t2.k`). The original predicate is unsatisfiable, and
    // the rewrite `GROUP BY k HAVING COUNT(DISTINCT k) > 1` is also empty -- but that
    // equivalence relies on a subtle chain of reasoning. Bailing out keeps the correctness
    // argument local to the "distinct neq column vs equi columns" case.
    val equiNames: Set[String] = equiPairs.map(_._1.name).toSet
    if (neqPairs.exists { case (l, _) => equiNames.contains(l.name) }) return None

    Some((equiPairs, neqPairs))
  }

  /**
   * True iff `plan` produces the same row bag on every evaluation.
   *
   * This is the primary safety guard for the rewrite, which folds two occurrences of the same
   * subtree into one aggregate -- sound only when both occurrences produce identical row bags. We
   * check it at TWO levels:
   *   - candidate level: the enclosing subquery / semi-join right subtree, before descending into
   *     the self-join. Catches nondeterminism that has been hoisted OUT of the join by an earlier
   *     optimizer rule -- e.g. a `Filter(rand(...))` moved to sit above the join rather than on
   *     each side. Without this, `isSameBaseRelation(innerLeft, innerRight)` could pass (both sides
   *     look deterministic) while the enclosing plan still contains `Rand`.
   *   - relation level: [[isSameBaseRelation]] additionally requires the two sides to be
   *     structurally identical.
   *
   * Attribute-level `plan.deterministic` alone is NOT sufficient. Catalyst's
   * `Expression.deterministic` only checks explicit `Nondeterministic` annotation; several
   * operators produce a runtime-nondeterministic row bag even though every expression they contain
   * is `deterministic == true`:
   *   - `Aggregate` with `First` / `Last` / `collect_list` / `min_by` / `max_by` (tie order),
   *   - `Window` with `row_number()` / `rank()` over a non-total order,
   *   - `Limit` / `LocalLimit` / `Sample` / `Offset` (row-bag operator-level nondeterminism),
   *   - streaming sources.
   *
   * This rule collapses two evaluations of the same subtree into one aggregate; repeatability must
   * be provable, not assumed. That is why the operator check below is a WHITELIST rather than a
   * blacklist -- unknown operators default to reject.
   */
  private def isRepeatablePlan(plan: LogicalPlan): Boolean = {
    plan.deterministic && !plan.isStreaming && isRowBagRepeatable(plan)
  }

  /**
   * Operator whitelist for `isRepeatablePlan`. A plan is row-bag repeatable iff EVERY node in it is
   * one of a small set of operators known to preserve their child row bag verbatim.
   *
   * Kept intentionally narrow -- the target workload (Q95-shape self-join in a subquery) only needs
   * a plain relation scan optionally wrapped in Project / Filter / SubqueryAlias plus the self-join
   * itself. Adding an operator here requires proving:
   *   - it does not reorder its input non-deterministically,
   *   - it does not depend on shuffle-merge or tie-broken orderings,
   *   - it produces the same output row bag on every evaluation.
   *
   * Streaming sources reach here as `LeafNode`s but are already filtered upstream by
   * `plan.isStreaming` in [[isRepeatablePlan]].
   */
  private def isRowBagRepeatable(plan: LogicalPlan): Boolean = !plan.exists {
    // TreeNode exposes `exists` but not `forall`, so invert: match every node NOT in the
    // whitelist; return true iff any such node exists; negate to get "all whitelisted".
    case _: LeafNode => false
    case _: Project => false
    case _: Filter => false
    case _: SubqueryAlias => false
    // Join is included because our target pattern IS a Join; both children get recursed into.
    case _: Join => false
    // Everything else -- Aggregate (First/Last), Window (row_number ties), Limit, Sample,
    // Offset, Distinct, Union, Except, Intersect, Sort (may break ties nondeterministically),
    // Expand, Generate, etc. -- fail-closed.
    case _ => true
  }

  /**
   * True iff `left` and `right` are the same plan modulo canonicalization AND each side is a
   * repeatable plan. See [[isRepeatablePlan]] for the repeatability contract.
   */
  private def isSameBaseRelation(left: LogicalPlan, right: LogicalPlan): Boolean = {
    left.canonicalized == right.canonicalized &&
    isRepeatablePlan(left) && isRepeatablePlan(right)
  }

  // splitConjunctivePredicates is provided by the mixed-in PredicateHelper trait.
}

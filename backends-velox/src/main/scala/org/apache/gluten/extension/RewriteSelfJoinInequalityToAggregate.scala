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
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, IntegerType, LongType, ShortType, TimestampType}

/**
 * Rewrites supported uncorrelated IN-subquery inequality self-joins into
 * `GROUP BY + HAVING COUNT(DISTINCT) > 1`, avoiding the self-join cross-product.
 *
 * Supports a direct self-join (Pattern A') and a self-join nested under an outer inner join
 * (Pattern A2, where only the self-join child becomes an Aggregate). Unsupported and correlated
 * shapes fail closed.
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
      return plan
    }

    // Fail closed on correlated subqueries: `lq.children` holds the outer references this rule
    // does not remap.
    val rewritten = plan.transformAllExpressions {
      case in @ InSubquery(_, lq: ListQuery) if lq.children.isEmpty =>
        rewriteSubqueryPlan(lq.plan) match {
          case Some(newSub) => in.copy(query = lq.copy(plan = newSub))
          case None => in
        }
    }
    rewritten
  }

  // ============================================================================
  //  Shared helpers
  // ============================================================================

  /**
   * Build `GROUP BY equiKeys HAVING COUNT(DISTINCT neqCol) > 1`, filtering NULL equi keys first to
   * preserve the equi-join's NULL semantics: `=` never matches a NULL key, but GROUP BY would fold
   * all NULL keys into one group that can leak NULL into a `NOT IN`. COUNT(DISTINCT) ignores NULL,
   * so the neq column needs no filter.
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
   * Rebuild the wrapper Project so every equi-key reference points at the sjLeft attribute with a
   * fresh output ExprId, returning `oldOutputExprId -> newOutputAttr` for downstream references
   * (outer join condition, top-level Project). Lookup is by ExprId (Catalyst attribute identity),
   * not name. Fails closed when an entry is neither an equi-key Attribute nor `Alias(equi-key, _)`.
   */
  private def canonicalizeWrapper(
      projectList: Seq[NamedExpression],
      equiPairs: Seq[(Attribute, Attribute)],
      newChild: LogicalPlan): Option[(Project, Map[ExprId, Attribute])] = {
    val exprIdToLeft: Map[ExprId, Attribute] =
      equiPairs.flatMap { case (l, r) => Seq(l.exprId -> l, r.exprId -> l) }.toMap
    val oldOutput: Seq[Attribute] = projectList.map(_.toAttribute)
    val mapped: Seq[Option[NamedExpression]] = projectList.map {
      case a: Attribute if exprIdToLeft.contains(a.exprId) =>
        Some(
          Alias(exprIdToLeft(a.exprId), a.name)(
            qualifier = a.qualifier,
            explicitMetadata = Some(a.metadata)): NamedExpression)
      case al @ Alias(a: Attribute, _) if exprIdToLeft.contains(a.exprId) =>
        Some(
          Alias(exprIdToLeft(a.exprId), al.name)(
            qualifier = al.qualifier,
            explicitMetadata = al.explicitMetadata,
            nonInheritableMetadataKeys = al.nonInheritableMetadataKeys): NamedExpression)
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
   * Replace equi-key references inside a NamedExpression per `remap`, preserving Attribute/Alias
   * shape. Any other expression still referencing a replaced output returns None (fail-closed) to
   * avoid a dangling ExprId.
   */
  private def remapNamedExpressionAttributes(
      ne: NamedExpression,
      remap: Map[ExprId, Attribute]): Option[NamedExpression] = ne match {
    case a: Attribute if remap.contains(a.exprId) => Some(remap(a.exprId))
    case a: Attribute => Some(a)
    case al: Alias =>
      val newChild = al.child.transformUp {
        case a: Attribute if remap.contains(a.exprId) => remap(a.exprId)
      }
      Some(
        if (newChild eq al.child) {
          al
        } else {
          Alias(newChild, al.name)(
            al.exprId,
            al.qualifier,
            al.explicitMetadata,
            al.nonInheritableMetadataKeys)
        })
    case other if other.references.exists(a => remap.contains(a.exprId)) =>
      None
    case other => Some(other)
  }

  // ============================================================================
  //  Pattern A' / A2 dispatch (subquery plans of InSubquery)
  // ============================================================================

  private def rewriteSubqueryPlan(plan: LogicalPlan): Option[LogicalPlan] = {
    // Candidate-level guard: reject if any node in the whole subquery is non-repeatable, catching
    // nondeterminism hoisted above the self-join that the per-side `isSameBaseRelation` misses.
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
    // Fail closed on an explicit join hint: it is a directive about the join this rule deletes.
    if (innerJoin.hint != JoinHint.NONE) return None

    val innerLeft = innerJoin.left
    val innerRight = innerJoin.right
    val innerCond = innerJoin.condition.get

    val parsed = parseSelfJoinCondition(innerCond, innerLeft, innerRight)
    if (parsed.isEmpty) return None
    val (equiPairs, neqPairs) = parsed.get

    val innerLeftEquiAttrs: Seq[Attribute] = equiPairs.map(_._1)
    val innerLeftNeqAttr: Attribute = neqPairs.head._1
    val filtered = buildAggregateHavingDistinctGt1(innerLeftEquiAttrs, innerLeftNeqAttr, innerLeft)

    // Fail closed on a bare-Join subquery: with no wrapper Project, replacing the self-join output
    // with `Project(equiKeys, filtered)` shrinks arity and RewritePredicateSubquery's positional
    // `values.zip(sub.output)` would misbind semi predicates. Q95 subqueries always have a Project.
    projectListOpt match {
      case None =>
        None
      case Some(pl) =>
        canonicalizeWrapper(pl, equiPairs, filtered).map { case (newWrapper, _) => newWrapper }
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
    val (equiPairs, neqPairs) = parsed.get

    val sjLeftEquiAttrs: Seq[Attribute] = equiPairs.map(_._1)
    val sjLeftNeqAttr: Attribute = neqPairs.head._1

    val selfJoinOutputSet = selfJoinSide.outputSet
    val sjEquiExprIds: Set[ExprId] =
      equiPairs.flatMap { case (l, r) => Seq(l.exprId, r.exprId) }.toSet
    // A wrapper Project may reproject equi-keys under fresh alias exprIds; include those.
    val wrapperEquiExprIds: Set[ExprId] = selfJoinProjectOpt.toSeq.flatMap {
      p =>
        p.projectList.flatMap {
          case a: Attribute if sjEquiExprIds.contains(a.exprId) => Some(a.exprId)
          case al @ Alias(a: Attribute, _) if sjEquiExprIds.contains(a.exprId) => Some(al.exprId)
          case _ => None
        }
    }.toSet
    val allEquiExprIds = sjEquiExprIds ++ wrapperEquiExprIds

    // The outer join condition and any top-level Project may reference only equi-key attrs from the
    // self-join side (the neq column does not survive the rewrite).
    val outerCondRefs = outerCond.references.filter(selfJoinOutputSet.contains)
    if (!outerCondRefs.forall(a => allEquiExprIds.contains(a.exprId))) return None
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
          // Fail closed: with no wrapper and no top-level Project, `Project(equiKeys, filtered)`
          // shrinks the outer join's arity and RewritePredicateSubquery's positional zip misbinds.
          return None
        case None =>
          // Defensive path for a rule set without ColumnPruning (unreachable by default): the
          // top-level Project preserves arity via `outputRemap`, remapping sjRight equi-refs to
          // sjLeft (same output position in a valid self-join).
          val newP = Project(sjLeftEquiAttrs, filtered)
          val remap: Map[ExprId, Attribute] =
            equiPairs.map { case (l, r) => r.exprId -> l }.toMap
          (newP, remap)
      }

    val newOuterCond = outerCond.transformUp {
      case a: Attribute if outputRemap.contains(a.exprId) => outputRemap(a.exprId)
    }

    val newOuterJoin = if (selfJoinOnRight) {
      outerJoin.copy(right = newSelfJoinSide, condition = Some(newOuterCond))
    } else {
      outerJoin.copy(left = newSelfJoinSide, condition = Some(newOuterCond))
    }

    val result = projectListOpt match {
      case Some(pl) =>
        val remapped = pl.map(ne => remapNamedExpressionAttributes(ne, outputRemap))
        if (remapped.exists(_.isEmpty)) return None
        Project(remapped.flatten, newOuterJoin)
      case None => newOuterJoin
    }
    Some(result)
  }

  private def tryExtractSelfJoin(plan: LogicalPlan): Option[Join] = {
    val join = plan match {
      case Project(_, j: Join) if j.joinType == Inner && j.condition.isDefined => j
      case j: Join if j.joinType == Inner && j.condition.isDefined => j
      case _ => return None
    }
    // A hinted self-join is not an extraction candidate; see `rewriteDirectSelfJoin`.
    if (join.hint != JoinHint.NONE) return None
    if (!isSameBaseRelation(join.left, join.right)) return None
    if (parseSelfJoinCondition(join.condition.get, join.left, join.right).isEmpty) return None
    Some(join)
  }

  // ============================================================================
  //  parseSelfJoinCondition + isSameBaseRelation
  // ============================================================================

  private def outputOrdinal(plan: LogicalPlan, attr: Attribute): Int =
    plan.output.indexWhere(_.exprId == attr.exprId)

  private def sameOutputPosition(
      leftPlan: LogicalPlan,
      rightPlan: LogicalPlan,
      leftAttr: Attribute,
      rightAttr: Attribute): Boolean = {
    val leftPos = outputOrdinal(leftPlan, leftAttr)
    val rightPos = outputOrdinal(rightPlan, rightAttr)
    leftPos >= 0 && rightPos >= 0 && leftPos == rightPos
  }

  /**
   * Parse a join condition into equi-pairs and inequality-pairs. Accepts only `EqualTo(attr, attr)`
   * and `Not(EqualTo(attr, attr))` across opposite sides, and `IsNotNull(attr)` on a join column;
   * anything else fails the whole rewrite closed.
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

    // Only IsNotNull on a join column is safe to drop -- redundant with the join or auto-added by
    // InferFiltersFromConstraints. IsNotNull on any other column changes semantics; bail out.
    val joinAttrIds: Set[ExprId] =
      (equiPairs ++ neqPairs).flatMap { case (l, r) => Seq(l.exprId, r.exprId) }.toSet
    val isNotNullOnJoinCols = predicates.count {
      case IsNotNull(a: Attribute) if joinAttrIds.contains(a.exprId) => true
      case _ => false
    }

    val totalMatched = equiPairs.size + neqPairs.size + isNotNullOnJoinCols
    if (totalMatched != predicates.size) return None
    if (equiPairs.isEmpty || neqPairs.isEmpty) return None

    // A single inequality only: COUNT(DISTINCT) over one column cannot represent multiple neqs.
    if (neqPairs.size != 1) return None

    // Resolve each predicate end by ExprId and require matching output ordinals, not name equality
    // (canonicalization erases cosmetic Alias names).
    val equiValid =
      equiPairs.forall { case (l, r) => sameOutputPosition(leftPlan, rightPlan, l, r) }
    val neqValid = neqPairs.forall { case (l, r) => sameOutputPosition(leftPlan, rightPlan, l, r) }
    if (!equiValid || !neqValid) return None

    // Equi-key output positions must be distinct, so swapped/duplicate aliases cannot collide.
    val leftEquiOrdinals = equiPairs.map { case (l, _) => outputOrdinal(leftPlan, l) }
    if (leftEquiOrdinals.exists(_ < 0)) return None
    if (leftEquiOrdinals.distinct.size != leftEquiOrdinals.size) return None

    // Reject when the neq column overlaps an equi-key column (e.g. `t1.k = t2.k AND t1.k <> t2.k`).
    val neqLeftOrdinal = outputOrdinal(leftPlan, neqPairs.head._1)
    if (neqLeftOrdinal < 0 || leftEquiOrdinals.contains(neqLeftOrdinal)) return None

    // Datatype safety, checked last: the rewrite swaps comparison equality (`=`/`<>`) for
    // grouping/distinct equality, so every column moved into the aggregate must be a type where the
    // two coincide.
    val comparisonAttrs = (equiPairs ++ neqPairs).flatMap { case (l, r) => Seq(l, r) }
    if (!comparisonAttrs.forall(a => isSafeComparisonGroupingType(a.dataType))) return None

    Some((equiPairs, neqPairs))
  }

  /**
   * Positive allowlist of types where comparison equality (`=`/`<>`) and grouping/distinct equality
   * provably coincide, so a key can move from a join predicate into GROUP BY / COUNT(DISTINCT).
   * Float/Double (NaN, signed zero) and String/CHAR/VARCHAR (collation-dependent) are excluded;
   * complex types, UDTs and unknown types fail closed.
   */
  private def isSafeComparisonGroupingType(dataType: DataType): Boolean = dataType match {
    case ByteType | ShortType | IntegerType | LongType => true
    case _: DecimalType => true
    case BooleanType => true
    case DateType => true
    case TimestampType => true
    case BinaryType => true
    case _ => false
  }

  /**
   * Primary safety guard: the rewrite folds two occurrences of one subtree into a single aggregate,
   * so a plan qualifies only when its operators, leaves and expressions are all allowlisted as
   * repeatable. `plan.deterministic` alone is insufficient -- Aggregate(First), Window row_number
   * over a non-total order and Limit/Sample are row-bag nondeterministic yet report deterministic.
   * Embedded expression subqueries also fail closed.
   */
  private def isRepeatablePlan(plan: LogicalPlan): Boolean = {
    plan.deterministic &&
    !plan.isStreaming &&
    plan.subqueriesAll.isEmpty &&
    isRowBagRepeatable(plan) &&
    hasRepeatableExpressions(plan)
  }

  /** Operator/leaf allowlist for repeatable row bags; everything unknown fails closed. */
  private def isRowBagRepeatable(plan: LogicalPlan): Boolean = !plan.exists {
    case _: Project => false
    case _: Filter => false
    case _: SubqueryAlias => false
    case _: Join => false
    case _: Range => false
    case _: LocalRelation => false
    case relation: LogicalRelation =>
      // Trust a Parquet scan only: exact `ParquetFileFormat` (getClass, not isInstanceOf, since it
      // is non-final); any other FileFormat is not provably repeatable.
      relation.relation match {
        case h: HadoopFsRelation if h.fileFormat.getClass == classOf[ParquetFileFormat] => false
        case _ => true
      }
    case _ => true
  }

  private def hasRepeatableExpressions(plan: LogicalPlan): Boolean = {
    !plan.exists(node => node.expressions.exists(expr => !isRepeatableExpression(expr)))
  }

  /**
   * Expression allowlist: repeatable only when the root type is allowlisted and all children are,
   * so `Add(v, Abs(w))` is rejected. Unknown types fail closed (a missed optimization, not a bug).
   *
   * Spark 3.3 decimal arithmetic carries `PromotePrecision` / `CheckOverflow`, absent here, so it
   * fails closed; Spark 3.4+ removed those wrappers (SPARK-39316) and a decimal `Add` is accepted.
   */
  private def isRepeatableExpression(expr: Expression): Boolean = expr match {
    case _: Attribute | _: Literal =>
      true
    case _: Alias | _: Cast | _: Add | _: Subtract | _: Multiply | _: Divide | _: Remainder |
        _: And | _: Or | _: Not | _: EqualTo | _: EqualNullSafe | _: LessThan |
        _: LessThanOrEqual | _: GreaterThan | _: GreaterThanOrEqual | _: IsNull | _: IsNotNull =>
      expr.children.forall(isRepeatableExpression)
    case _ =>
      false
  }

  /** True iff `left`/`right` are the same plan modulo canonicalization AND each is repeatable. */
  private def isSameBaseRelation(left: LogicalPlan, right: LogicalPlan): Boolean = {
    left.canonicalized == right.canonicalized &&
    isRepeatablePlan(left) && isRepeatablePlan(right)
  }

  // splitConjunctivePredicates is provided by the mixed-in PredicateHelper trait.
}

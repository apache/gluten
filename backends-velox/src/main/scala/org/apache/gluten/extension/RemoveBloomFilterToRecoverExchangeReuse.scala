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
import org.apache.gluten.execution.{BatchScanExecTransformerBase, FilterExecTransformer}
import org.apache.gluten.expression.VeloxBloomFilterMightContain

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, BloomFilterMightContain, Expression, PredicateHelper, XxHash64}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{BinaryExecNode, DataSourceScanExec, FilterExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.types.DataType

import java.util.IdentityHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Fixes a performance regression in TPC-DS Q24a/Q24b (and similar) queries on the Gluten Velox
 * backend where asymmetric runtime BloomFilters injected by Spark cause the same large table (e.g.
 * store_sales) to have different BF counts on the two join-input sides. This asymmetry makes
 * canonicalized sameResult=false => ReusedExchange is disabled => the large table is scanned twice.
 *
 * The fix: on the join-input side that has MORE BloomFilters, precisely strip the extra BF
 * conjuncts so that the canonicalized plans of the main query and the HAVING correlated
 * scalar-subquery side become identical. Spark's native ReuseExchange rule then kicks in naturally,
 * eliminating the duplicate scan.
 *
 * The apply() method runs in 4 phases:
 *
 * STEP1 Collect: traverse ALL physical joins in the current plan -- including those planted inside
 * subquery expressions, see nestedPlans() -- and build one JoinInputEntry per join child
 * (leaf-tables-set, output-column signature, unique BF-keys set). Join inputs whose leaf table
 * names or BF probe columns cannot be identified are dropped individually -- they neither serve as
 * a baseline nor get stripped -- so one opaque relation does not disable the rule for the whole
 * plan. STEP2 Group: cluster join inputs by (leafTables-set, output-signature) so that we only
 * compare BF count asymmetry between join inputs that are actually eligible for exchange reuse.
 * STEP3 Find asymmetry: per group elect ONE baseline bfKeys set -- the one the most members can be
 * reduced to, preferring the largest such set so that as few BFs as possible are given up -- and
 * mark every member whose bfKeys is a strict superset of it. A baseline is by construction a proper
 * subset, i.e. the entry does carry extra BFs, so it gets marked for stripping. STEP4 Strip
 * precisely: for each marked entry, walk top-down through all physical Filters under its subtree
 * and drop ONLY those BF conjuncts whose exprKey is NOT in the baseline's bfExprKeys. Leave
 * isnotnull / other predicates intact. Finally graft the rewritten subtrees back into the original
 * BinaryJoin's left/right children.
 *
 * The rule is stateless: everything it needs to compare is reachable from the plan handed to a
 * single apply() call.
 *
 * Controlled by spark.gluten.sql.columnar.backend.velox.removeBloomFilterToRecoverExchangeReuse;
 * set it to false to keep every BloomFilter Spark injected and leave the duplicate scan in place.
 */
case class RemoveBloomFilterToRecoverExchangeReuse(spark: SparkSession)
  extends Rule[SparkPlan]
  with PredicateHelper
  with Logging {

  import RemoveBloomFilterToRecoverExchangeReuse._

  /**
   * Picks the single BF key set the members of one exchange-reuse group should be aligned on: the
   * one that the largest number of members can be reduced to, because that is what recovers reuse.
   * Ties go to the largest set, so that as few BFs as possible are given up, and then to the
   * lexicographically smallest one to stay deterministic across apply() invocations.
   *
   * Candidates are the key sets the members already have -- reducing everybody to, say, the
   * intersection of two disjoint sets would recover reuse too, but no member would keep its own
   * plan, so that is left out on purpose.
   *
   * @return
   *   None when the group has a single member, i.e. there is nothing to align it with locally.
   */
  private def localBaselineBfKeys(sameGroupEntries: Seq[JoinInputEntry]): Option[Set[String]] = {
    if (sameGroupEntries.length < 2) {
      return None
    }
    val candidates = sameGroupEntries.map(_.bfExprKeys).distinct
    if (candidates.length < 2) {
      // All members already carry the same BFs, no asymmetry within this group.
      return None
    }
    Some(candidates.minBy {
      candidate =>
        val reach = sameGroupEntries.count(e => candidate.subsetOf(e.bfExprKeys))
        (-reach, -candidate.size, candidate.toSeq.sorted.mkString(","))
    })
  }

  /**
   * A physical filter -- vanilla or columnar -- decomposed into the parts this rule needs.
   *
   * @param condition
   *   the filter predicate
   * @param child
   *   the filter's child, used when every conjunct gets stripped and the filter itself goes away
   * @param withCondition
   *   rebuilds this very filter node with another predicate
   */
  private case class FilterParts(
      condition: Expression,
      child: SparkPlan,
      withCondition: Expression => SparkPlan)

  private object PhysicalFilter {
    def unapply(p: SparkPlan): Option[FilterParts] = p match {
      case f: FilterExec =>
        Some(FilterParts(f.condition, f.child, cond => f.copy(condition = cond)))
      case f: FilterExecTransformer =>
        Some(FilterParts(f.condition, f.child, cond => f.copy(condition = cond)))
      case _ => None
    }
  }

  private def isBloomFilter(expr: Expression): Boolean = expr match {
    case _: BloomFilterMightContain => true
    case _: VeloxBloomFilterMightContain => true
    case _ => false
  }

  /**
   * Identity key of a BloomFilter conjunct: the probe column plus, when the probe is hashed, the
   * hash seed. The BF bytes themselves are intentionally not part of the key -- they come from
   * different subquery instances on the two join-input sides even when the two BFs are equivalent.
   *
   * @return
   *   None when the probe column cannot be resolved, in which case the whole join input is skipped
   *   rather than lumping unrelated BFs under one sentinel key.
   */
  private def probeKey(bf: Expression): Option[String] = {
    bf.children.lift(1).flatMap {
      rawProbe =>
        val (attrOpt, seedOpt) = rawProbe match {
          case XxHash64(children, seed) =>
            // collectFirst depth-first left-to-right picks up the first Attribute (incl.
            // children.head)
            (children.headOption.flatMap(_.collectFirst { case a: Attribute => a }), Some(seed))
          case other =>
            (other.collectFirst { case a: Attribute => a }, None)
        }
        attrOpt.map {
          a => s"probe=${a.name}:${a.dataType.simpleString}|seed=${seedOpt.getOrElse("NONE")}"
        }
    }
  }

  /**
   * Replaces query stages by the plans they wrap so that leaves and BloomFilters inside stages that
   * were already created become visible. QueryStageExec is a leaf node, hence the explicit
   * recursion into its plan; ReusedExchangeExec's child on the other hand is a regular child that
   * transformUp has already visited.
   */
  private def unfoldQueryStages(plan: SparkPlan): SparkPlan =
    plan.transformUp {
      case s: QueryStageExec => unfoldQueryStages(s.plan)
      case r: ReusedExchangeExec => r.child
    }

  /**
   * @return
   *   the identity keys of all BloomFilter conjuncts under `plan`, or None if any of them has an
   *   unresolvable probe column.
   */
  private def bloomFilterKeys(plan: SparkPlan): Option[Set[String]] = {
    val keys = Set.newBuilder[String]
    var unresolvable = false
    plan.foreach {
      case PhysicalFilter(f) =>
        splitConjunctivePredicates(f.condition).filter(isBloomFilter).foreach {
          bf =>
            probeKey(bf) match {
              case Some(key) => keys += key
              case None => unresolvable = true
            }
        }
      case _ =>
    }
    if (unresolvable) None else Some(keys.result())
  }

  private def stripExtraBloomFilters(
      root: SparkPlan,
      baselineBfKeys: Set[String]): (SparkPlan, Int) = {
    var strippedCount = 0
    val rewritten = root.transformDown {
      case plan @ PhysicalFilter(f) =>
        val conjuncts = splitConjunctivePredicates(f.condition)
        val remaining = conjuncts.filter {
          expr =>
            // A BF whose probe cannot be keyed is kept: it cannot be matched against the baseline.
            val shouldStrip =
              isBloomFilter(expr) && probeKey(expr).exists(key => !baselineBfKeys.contains(key))
            if (shouldStrip) {
              strippedCount += 1
            }
            !shouldStrip
        }
        if (remaining.length == conjuncts.length) {
          // Nothing stripped here, keep the node as is rather than rebuilding its condition.
          plan
        } else {
          remaining.reduceOption[Expression](And) match {
            case Some(cond) => f.withCondition(cond)
            case None => f.child
          }
        }
    }
    (rewritten, strippedCount)
  }

  /**
   * Extract the table name a leaf (Scan-like) SparkPlan reads from. This is used as one dimension
   * of the exchange-reuse grouping key. Values are taken directly from strongly-typed fields
   * (tableIdentifier.table / last segment of Table.name) -- no reliance on plan.simpleString and
   * its truncation. Leaves whose table cannot be identified yield None, which drops the enclosing
   * join input from consideration.
   *
   * The cases are keyed on the widest common supertype that still exposes the table, so that
   * vanilla and columnar scans -- and the V2 scans of every connector -- are handled by one branch
   * each.
   */
  private def extractTableName(leaf: SparkPlan): Option[String] = {
    leaf match {
      // Covers vanilla FileSourceScanExec and FileSourceScanExecTransformer alike: both are
      // DataSourceScanExec, which declares tableIdentifier on every supported Spark version.
      // Matching FileSourceScanLike instead would not compile against Spark 3.3, which does not
      // have that trait yet.
      case scan: DataSourceScanExec =>
        scan.tableIdentifier.map(_.table)
      // Covers BatchScanExecTransformer and IcebergScanTransformer alike: both extend
      // BatchScanExecTransformerBase, which lives in a module this one already depends on, so no
      // reflection is needed for the Iceberg case either.
      case scan: BatchScanExecTransformerBase =>
        Option(scan.table).map(t => stripCatalog(t.name()))
      // Vanilla BatchScanExec is not a DataSourceScanExec and exposes `table` only since
      // Spark 3.4, so it is the one case that has to be read reflectively.
      case scan: BatchScanExec =>
        reflectiveTableName(scan)
      case _ =>
        None
    }
  }

  private def stripCatalog(name: String): String = {
    val i = name.lastIndexOf('.')
    if (i >= 0) name.substring(i + 1) else name
  }

  /** Reads `table.name()` off a plan that only exposes it on some Spark versions. */
  private def reflectiveTableName(leaf: SparkPlan): Option[String] = {
    try {
      val tableObj = leaf.getClass.getMethod("table").invoke(leaf)
      val name = tableObj.getClass.getMethod("name").invoke(tableObj)
      Option(name).map(n => stripCatalog(n.toString))
    } catch {
      case _: Exception => None
    }
  }

  /**
   * Intermediate result shared between STEP1 and STEP3: everything that characterises one side of a
   * join (one join input).
   *
   * @param joinInput
   *   the actual join-child subtree (used as IdentityHashMap key)
   * @param strippable
   *   whether STEP4 is able to graft a rewrite of this subtree back in. Only true for joins in the
   *   main plan tree: joins nested in subquery expressions are collected for their BF keys (they
   *   are valuable baselines) but are not reachable by the final transformDown pass.
   * @param outputSignature
   *   output (name, type) pairs -- required equality dimension for exchange reuse
   * @param leafTableNames
   *   set of all leaf table names reachable from this subtree -- required equality dimension for
   *   exchange reuse
   * @param bfExprKeys
   *   unique BloomFilter identity keys encoded via probeKey() -- what STEP3 compares across sides
   */
  private case class JoinInputEntry(
      joinInput: SparkPlan,
      strippable: Boolean,
      outputSignature: Seq[(String, DataType)],
      leafTableNames: Set[String],
      bfExprKeys: Set[String]) {
    def hasBf: Boolean = bfExprKeys.nonEmpty
    def group: ReuseGroup = (leafTableNames, outputSignature)
  }

  /**
   * STEP3 output: one join-input side that needs BloomFilter stripping plus the baseline it will be
   * stripped against.
   *
   * @param entry
   *   the side that has MORE BFs (the one carrying extras)
   * @param baselineBfKeys
   *   the reference BF key set, a strict proper subset of entry.bfExprKeys -- on stripping, only
   *   BFs whose key is in this set are kept, all the rest are removed.
   */
  private case class StripTarget(entry: JoinInputEntry, baselineBfKeys: Set[String])

  private def mkEntry(input: SparkPlan, strippable: Boolean): Option[JoinInputEntry] = {
    val unfolded = unfoldQueryStages(input)
    val leafNameOpts = unfolded.collectLeaves().map(extractTableName)

    if (leafNameOpts.contains(None)) {
      logDebug(
        "Skip a join input in RemoveBloomFilterToRecoverExchangeReuse" +
          " because the table name of some leaf nodes could not be extracted.")
      return None
    }

    bloomFilterKeys(unfolded) match {
      case None =>
        logDebug(
          "Skip a join input in RemoveBloomFilterToRecoverExchangeReuse" +
            " because the probe column of some BloomFilters could not be resolved.")
        None
      case Some(bfExprKeys) =>
        Some(
          JoinInputEntry(
            input,
            strippable,
            input.output.map(a => (a.name, a.dataType)),
            leafNameOpts.flatten.toSet,
            bfExprKeys))
    }
  }

  /**
   * Plans nested inside `plan` that plain tree traversal does not reach: the plans of subquery
   * expressions, and the plans wrapped by an AdaptiveSparkPlanExec -- an AQE node is a leaf, it
   * keeps its plan in `inputPlan` instead of among its children.
   *
   * Descending into AQE nodes is what makes the other side of a query like TPC-DS Q24a visible from
   * a single apply(). InsertAdaptiveSparkPlan plans the subqueries and plants them through
   * PlanAdaptiveSubqueries BEFORE it builds the enclosing AdaptiveSparkPlanExec, so by the time
   * this rule runs on the main query, the subquery's own AdaptiveSparkPlanExec -- and every
   * BloomFilter below it -- is already sitting in the plan, only hidden behind that leaf.
   *
   * Note this cannot help the reverse direction: while the subquery is being planned, the main
   * query it belongs to has no AdaptiveSparkPlanExec yet, so an asymmetry that would have to be
   * repaired on the subquery side is not visible from there.
   */
  private def nestedPlans(root: SparkPlan): Seq[SparkPlan] = {
    val visited = new IdentityHashMap[SparkPlan, java.lang.Boolean]()
    val collected = ArrayBuffer.empty[SparkPlan]

    def collectFrom(plan: SparkPlan): Unit = {
      val nested = plan.collect {
        case aqe: AdaptiveSparkPlanExec => Seq(aqe.inputPlan)
        case p => p.subqueries
      }.flatten
      nested.foreach {
        p =>
          if (visited.put(p, true) == null) {
            collected += p
            collectFrom(p)
          }
      }
    }

    collectFrom(root)
    collected.toSeq
  }

  /**
   * STEP1 helper: traverses ALL physical BinaryJoins inside the current physical plan (including
   * any nested subqueries) and invokes mkEntry() separately on each join's left and right child.
   *
   * @return
   *   all producible JoinInputEntry instances; join inputs mkEntry() cannot characterise are left
   *   out.
   */
  private def collectJoinInputs(plan: SparkPlan): Seq[JoinInputEntry] = {
    def joinInputsOf(p: SparkPlan): Seq[SparkPlan] =
      p.collect { case b: BinaryExecNode => Seq(b.left, b.right) }.flatten

    val fromMainTree = joinInputsOf(plan).flatMap(mkEntry(_, strippable = true))
    val fromNestedPlans =
      nestedPlans(plan).flatMap(joinInputsOf).flatMap(mkEntry(_, strippable = false))
    fromMainTree ++ fromNestedPlans
  }

  override def apply(plan: SparkPlan): SparkPlan = {

    // Giving up BloomFilters to win back an exchange is a trade-off, so it is switchable. Read per
    // apply() rather than once per session so that it can be flipped between queries.
    if (!VeloxConfig.get.removeBloomFilterToRecoverExchangeReuse) {
      return plan
    }

    // Without Spark's runtime BloomFilter injection there is no asymmetry to repair.
    if (!spark.sessionState.conf.runtimeFilterBloomFilterEnabled) {
      return plan
    }

    // ============================================================
    // STEP1 Collect JoinInputEntry: for every physical join
    //       (including nested subqueries), build one entry per
    //       join child: leaf-tables set, output-signature, and
    //       unique BloomFilter identity-key set.
    // ============================================================
    val allJoinInputs = collectJoinInputs(plan)

    if (!allJoinInputs.exists(_.hasBf)) {
      return plan
    }

    // ============================================================
    // STEP2 Group: cluster join inputs directly by
    //       (leafTableNames, outputSignature).
    //       Only pairs that match on BOTH dimensions are
    //       considered by Spark as exchange-canonicalisation
    //       equivalent (and therefore eligible for ReusedExchange)
    //       so asymmetry comparison is strictly scoped to the
    //       inside of each group.
    // ============================================================
    val groupedInputs: Map[ReuseGroup, Seq[JoinInputEntry]] =
      allJoinInputs.groupBy(_.group)

    // ============================================================
    // STEP3 Locate asymmetry: per group pick ONE baseline (see
    //       localBaselineBfKeys): the key set the most members
    //       can be reduced to, since that is what recovers
    //       reuse, preferring the largest such set on ties so
    //       that as few BFs as possible are given up. Every
    //       strippable member whose keys are a strict superset
    //       of the baseline becomes a strip target.
    // ============================================================
    val joinInputsToStrip = new IdentityHashMap[SparkPlan, StripTarget]()
    groupedInputs.foreach {
      case (_, sameGroupEntries) =>
        // One baseline per group, not per entry: reducing every member of a subset chain
        // (e.g. {a,b,c}, {a,b}, {}) to its own nearest baseline would leave all of them different
        // and recover no reuse at all.
        val groupBaseline = localBaselineBfKeys(sameGroupEntries)
        sameGroupEntries.filter(e => e.hasBf && e.strippable).foreach {
          entry =>
            groupBaseline
              .filter(keys => keys.subsetOf(entry.bfExprKeys) && keys != entry.bfExprKeys)
              .foreach(keys => joinInputsToStrip.put(entry.joinInput, StripTarget(entry, keys)))
        }
    }

    if (joinInputsToStrip.isEmpty) {
      return plan
    }

    // ============================================================
    // STEP4.1 Strip BFs precisely: for each strip target walk
    //         its join-input subtree top-down with transformDown;
    //         at every physical Filter drop only those BF
    //         conjuncts whose exprKey is absent from
    //         baseline.bfExprKeys. Non-BF predicates
    //         (isnotnull, equality, ranges, ...) are left alone.
    // ============================================================
    val childReplacements = new IdentityHashMap[SparkPlan, SparkPlan]()
    joinInputsToStrip.asScala.foreach {
      case (joinInputChild, target) =>
        val (result, strippedCount) =
          stripExtraBloomFilters(joinInputChild, target.baselineBfKeys)
        if (strippedCount > 0) {
          logDebug(
            s"Stripped $strippedCount extra BloomFilter(s) from a join input over " +
              s"${target.entry.leafTableNames.mkString(",")} to recover exchange reuse.")
          childReplacements.put(joinInputChild, result)
        }
    }

    // ============================================================
    // STEP4.2 Graft rewritten join-input children back into the
    //         original BinaryJoin nodes. One single O(N)
    //         transformDown pass over the whole plan -- only
    //         BinaryJoin nodes are candidates for replacement.
    //         Do NOT wrap with transformWithSubqueries at an
    //         outer level or you get O(N^2) duplicate traversals;
    //         join inputs living inside subqueries are excluded
    //         from stripping for that reason (see
    //         JoinInputEntry.strippable).
    // ============================================================
    if (childReplacements.isEmpty) {
      plan
    } else {
      val reps = childReplacements.asScala
      plan.transformDown {
        case b: BinaryExecNode =>
          val left = b.left
          val right = b.right
          val newLeft = reps.getOrElse(left, left)
          val newRight = reps.getOrElse(right, right)
          if ((newLeft eq left) && (newRight eq right)) b
          else b.withNewChildren(Seq(newLeft, newRight))
      }
    }
  }
}

object RemoveBloomFilterToRecoverExchangeReuse {

  /** (leafTableNames, outputSignature) -- see minBfKeysPerGroup. */
  private type ReuseGroup = (Set[String], Seq[(String, DataType)])
}

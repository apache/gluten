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

import org.apache.gluten.execution.WholeStageTransformerSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf

/**
 * Correctness tests for [[RewriteSelfJoinInequalityToAggregate]].
 *
 * Assertions center on **result equivalence** between `rewrite=true` and `rewrite=false`. That is
 * the direct check of the rule's semantic contract: the rewrite must not change what the query
 * returns. Plan-shape assertions (e.g. "an Aggregate node exists") are avoided because downstream
 * optimizer rules differ across Spark versions -- e.g. Spark 4.0's `RewritePredicateSubquery` and
 * constant folding collapse LocalRelation-based EXISTS bodies so aggressively that our rule may
 * never see the original self-join shape yet the final result is still correct.
 *
 * Where a plan-level signal is useful, we look for the alias name our rule injects
 * (`_gluten_rw_selfjoin_cnt_distinct`) as a soft indicator that the rule fired. Its absence is not
 * treated as a failure -- an equivalent result via a different path is still a pass.
 */
class RewriteSelfJoinInequalityToAggregateSuite extends WholeStageTransformerSuite {

  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.gluten.sql.rewrite.selfJoinInequality", "true")
    .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")

  /** Signature alias produced by the rewrite; presence => rule definitely fired. */
  private val CountDistinctAlias = "_gluten_rw_selfjoin_cnt_distinct"

  private def ruleFired(plan: LogicalPlan): Boolean =
    plan.exists {
      p =>
        p.expressions.exists(_.exists {
          case a: Alias if a.name == CountDistinctAlias => true
          case _ => false
        })
    }

  /** Run `sql` twice, first with rewrite ON then OFF, and return the two result row sets. */
  private def runBoth(sql: String): (Set[Row], Set[Row]) = {
    var on: Set[Row] = null
    var off: Set[Row] = null
    withSQLConf("spark.gluten.sql.rewrite.selfJoinInequality" -> "true") {
      on = spark.sql(sql).collect().toSet
    }
    withSQLConf("spark.gluten.sql.rewrite.selfJoinInequality" -> "false") {
      off = spark.sql(sql).collect().toSet
    }
    (on, off)
  }

  private def setupTable(): Unit = {
    // k=1: distinct v={10,20}      -> matches (has 2 non-null distinct)
    // k=2: distinct v={30}         -> no match (only 1)
    // k=3: distinct v={40,50,60}   -> matches
    // k=4: v={70, NULL}            -> no match (only 1 non-null)
    // k=5: v={NULL, NULL}          -> no match (0 non-null)
    // k=6: v={80, 90, NULL}        -> matches
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW T AS SELECT * FROM VALUES
        |  (1, 10), (1, 10), (1, 20),
        |  (2, 30),
        |  (3, 40), (3, 50), (3, 60),
        |  (4, 70), (4, CAST(NULL AS INT)),
        |  (5, CAST(NULL AS INT)), (5, CAST(NULL AS INT)),
        |  (6, 80), (6, 90), (6, CAST(NULL AS INT))
        |AS T(k, v)""".stripMargin)
  }

  // ==================== Positive: rewrite is semantically equivalent ====================

  test("Pattern A': EXISTS subquery with bare self-join produces equivalent results") {
    setupTable()
    val sql =
      """SELECT k FROM T ws1 WHERE EXISTS (
        |  SELECT 1 FROM T s WHERE s.k = ws1.k AND s.v <> ws1.v)""".stripMargin
    val (on, off) = runBoth(sql)
    assert(on == off, s"rewrite ON $on != OFF $off")
    // Ground truth: only k in {1,3,6} have >=2 non-null distinct v.
    assert(on == Set(Row(1), Row(3), Row(6)), s"expected {1,3,6}, got $on")
  }

  test("Pattern A': InSubquery with bare self-join produces equivalent results") {
    setupTable()
    val sql =
      """SELECT k FROM T ws1 WHERE k IN (
        |  SELECT s1.k FROM T s1, T s2
        |  WHERE s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    val (on, off) = runBoth(sql)
    assert(on == off, s"rewrite ON $on != OFF $off")
    assert(on == Set(Row(1), Row(3), Row(6)))
  }

  test("Pattern A2: self-join nested inside outer InnerJoin produces equivalent results") {
    setupTable()
    // Only k in {1,3,6} qualify from the self-join side; the outer InnerJoin with D
    // (values {1,3,6}) intersects, so the final answer is again {1,3,6}.
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW D AS SELECT * FROM VALUES
        |  (1), (3), (6) AS D(k)""".stripMargin)
    val sql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT d.k
        |  FROM D d, (SELECT s1.k FROM T s1, T s2
        |             WHERE s1.k = s2.k AND s1.v <> s2.v) sj
        |  WHERE d.k = sj.k)""".stripMargin
    val (on, off) = runBoth(sql)
    assert(on == off, s"Pattern A2 rewrite ON $on != OFF $off")
    assert(on == Set(Row(1), Row(3), Row(6)))
  }

  // ==================== Semantic parity on NULL / 3VL ====================

  test("NULL / 3VL: rows with only-NULL or single-non-null inequality column are excluded") {
    setupTable()
    val sql =
      """SELECT k FROM T ws1 WHERE EXISTS (
        |  SELECT 1 FROM T s WHERE s.k = ws1.k AND s.v <> ws1.v)""".stripMargin
    val (on, off) = runBoth(sql)
    // k=4 (v={70,NULL}) fails: <> with NULL is UNKNOWN -> filtered by WHERE.
    // k=5 (v={NULL,NULL}) fails: every <> is UNKNOWN.
    assert(on == Set(Row(1), Row(3), Row(6)), s"expected {1,3,6}, got $on")
    assert(off == on, s"NULL/3VL semantics diverge between rewrite ON and OFF: $on vs $off")
  }

  // ==================== Negative: rewrite must produce equivalent results (or bail) ==========

  test("Plain InnerJoin at top level: results unchanged (rewrite must not touch it)") {
    setupTable()
    val sql =
      """SELECT ws1.k FROM T ws1 JOIN T ws2
        |ON ws1.k = ws2.k AND ws1.v <> ws2.v""".stripMargin
    // Row-multiplicity matters here; using count() to catch any drop or dup.
    var onCount: Long = -1L
    var offCount: Long = -1L
    withSQLConf("spark.gluten.sql.rewrite.selfJoinInequality" -> "true") {
      onCount = spark.sql(sql).count()
    }
    withSQLConf("spark.gluten.sql.rewrite.selfJoinInequality" -> "false") {
      offCount = spark.sql(sql).count()
    }
    assert(
      onCount == offCount,
      s"plain InnerJoin row-count differs: rewrite=$onCount vs baseline=$offCount")
  }

  test("IS DISTINCT FROM: NULL-safe inequality preserves original semantics") {
    setupTable()
    // IS DISTINCT FROM treats NULL as distinguishable (NULL IS DISTINCT FROM x = TRUE,
    // NULL IS DISTINCT FROM NULL = FALSE). Our rewrite must NOT fold this into
    // COUNT(DISTINCT), because COUNT(DISTINCT) ignores NULL.
    val sql =
      """SELECT k FROM T ws1 WHERE EXISTS (
        |  SELECT 1 FROM T s WHERE s.k = ws1.k
        |    AND (s.v IS DISTINCT FROM ws1.v))""".stripMargin
    val (on, off) = runBoth(sql)
    assert(on == off, s"IS DISTINCT FROM semantics diverge: ON=$on OFF=$off")
    // Sanity check: k=4 has (70, NULL) -- pair (v=70, v=NULL) IS DISTINCT FROM => TRUE
    // so k=4 must be included (unlike the plain-neq case above where it's excluded).
    assert(on.contains(Row(4)), s"k=4 should be in IS DISTINCT FROM result: $on")
    // And rule fire signal must be absent: this is a rejection path.
    val plan = spark.sql(sql).queryExecution.optimizedPlan
    assert(!ruleFired(plan), s"rule must not fire on IS DISTINCT FROM:\n$plan")
  }

  test("IsNotNull on a non-join column: results unchanged and rule bails out") {
    // k=2 has TWO rows with w=NULL and distinct v values. If the rule buggily drops
    // `IsNotNull(w)`, the rewrite `GROUP BY k HAVING COUNT(DISTINCT v) > 1` would
    // include k=2 (distinct v={30,40} => 2). Correct semantics keep k=2 out because
    // no s.w passes `IS NOT NULL`. Result divergence -> assert(on == off) catches
    // the bug without relying on the alias-absence check alone.
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW T3 AS SELECT * FROM VALUES
        |  (1, 10, 100), (1, 20, 200),
        |  (2, 30, CAST(NULL AS INT)), (2, 40, CAST(NULL AS INT))
        |AS T3(k, v, w)""".stripMargin)
    val sql =
      """SELECT k FROM T3 outer_t WHERE EXISTS (
        |  SELECT 1 FROM T3 s
        |  WHERE s.k = outer_t.k AND s.v <> outer_t.v AND s.w IS NOT NULL)""".stripMargin
    val (on, off) = runBoth(sql)
    assert(on == off, s"IsNotNull(non-join-col) semantics diverge: ON=$on OFF=$off")
    // Correct result contains only k=1; a buggy rewrite would also include k=2.
    assert(on == Set(Row(1)), s"expected {1}, got $on")
    val plan = spark.sql(sql).queryExecution.optimizedPlan
    assert(!ruleFired(plan), s"rule must not fire when IsNotNull is on non-join col:\n$plan")
  }

  test("Multi-column inequality (v<>v AND w<>w): results unchanged and rule bails out") {
    // k=3 has TWO rows with distinct v but identical w. If the rule buggily folds
    // only `v <> v` and drops `w <> w`, the rewrite would match k=3 (distinct v={40,50}).
    // Correct semantics reject k=3 because no pair satisfies both `v<>v` AND `w<>w`
    // (the two w's are equal). Result divergence catches the bug.
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW T2 AS SELECT * FROM VALUES
        |  (1, 10, 100), (1, 20, 200),
        |  (2, 30, 300),
        |  (3, 40, 100), (3, 50, 100)
        |AS T2(k, v, w)""".stripMargin)
    val sql =
      """SELECT k FROM T2 ws1 WHERE EXISTS (
        |  SELECT 1 FROM T2 s WHERE s.k = ws1.k
        |    AND s.v <> ws1.v AND s.w <> ws1.w)""".stripMargin
    val (on, off) = runBoth(sql)
    assert(on == off, s"multi-column neq semantics diverge: ON=$on OFF=$off")
    // Correct result is {1}; a buggy rewrite that only honored v<>v would also match k=3.
    assert(on == Set(Row(1)), s"expected {1}, got $on")
    val plan = spark.sql(sql).queryExecution.optimizedPlan
    assert(!ruleFired(plan), s"rule must not fire on multi-column neq:\n$plan")
  }

  test("LeftOuter join is outside existence context: results unchanged") {
    setupTable()
    val sql =
      """SELECT ws1.k FROM T ws1 LEFT OUTER JOIN T ws2
        |ON ws1.k = ws2.k AND ws1.v <> ws2.v""".stripMargin
    // Row multiplicity matters here.
    var onCount: Long = -1L
    var offCount: Long = -1L
    withSQLConf("spark.gluten.sql.rewrite.selfJoinInequality" -> "true") {
      onCount = spark.sql(sql).count()
    }
    withSQLConf("spark.gluten.sql.rewrite.selfJoinInequality" -> "false") {
      offCount = spark.sql(sql).count()
    }
    assert(onCount == offCount, s"LeftOuter row-count differs: $onCount vs $offCount")
    val plan = spark.sql(sql).queryExecution.optimizedPlan
    assert(!ruleFired(plan), s"rule must not fire on LeftOuter:\n$plan")
  }

  test("Nondeterministic subtree (rand() in Filter): results correct and rule bails out") {
    setupTable()
    // Two `rand() > 0.5` filters have identical canonical plans but draw independent
    // samples per side. Rewrite would fold two samples into one, systematically
    // over-reporting; the `deterministic` guard on isSameBaseRelation must reject.
    val sql =
      """SELECT k FROM (SELECT * FROM T WHERE rand() > 0.5) ws1 WHERE EXISTS (
        |  SELECT 1 FROM (SELECT * FROM T WHERE rand() > 0.5) s
        |  WHERE s.k = ws1.k AND s.v <> ws1.v)""".stripMargin
    val plan = spark.sql(sql).queryExecution.optimizedPlan
    assert(!ruleFired(plan), s"rule must not fire on nondeterministic subtree:\n$plan")
    // Cannot compare row sets: rand() makes each run different. The invariant is that
    // the rule refuses to fold, which is checked by the alias-absence assertion above.
  }

  test("Inequality column overlaps equi-key (k = k AND k <> k): empty result, rule bails out") {
    setupTable()
    // Logically unsatisfiable predicate. Original query returns empty; rewrite would
    // also produce empty (COUNT DISTINCT of grouping key = 1 always), but the guard
    // rejects this shape defensively so the correctness argument stays local.
    val sql =
      """SELECT k FROM T ws1 WHERE EXISTS (
        |  SELECT 1 FROM T s WHERE s.k = ws1.k AND s.k <> ws1.k)""".stripMargin
    val (on, off) = runBoth(sql)
    assert(on == off, s"unsatisfiable predicate diverges: ON=$on OFF=$off")
    assert(on.isEmpty, s"unsatisfiable predicate should produce empty set, got $on")
    val plan = spark.sql(sql).queryExecution.optimizedPlan
    assert(!ruleFired(plan), s"rule must not fire when neq overlaps equi:\n$plan")
  }

  test("Config gate: selfJoinInequality=false disables the rule entirely") {
    setupTable()
    val sql =
      """SELECT k FROM T ws1 WHERE EXISTS (
        |  SELECT 1 FROM T s WHERE s.k = ws1.k AND s.v <> ws1.v)""".stripMargin
    withSQLConf("spark.gluten.sql.rewrite.selfJoinInequality" -> "false") {
      val plan = spark.sql(sql).queryExecution.optimizedPlan
      assert(!ruleFired(plan), s"config off must not fire rewrite:\n$plan")
      // And the query itself must still return correct results.
      val res = spark.sql(sql).collect().toSet
      assert(res == Set(Row(1), Row(3), Row(6)), s"config off correctness broken: $res")
    }
  }

  // ==================== Correlated subquery: rule must fail-closed ====================

  private def setupOuterT(): Unit = {
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW OuterT AS SELECT * FROM VALUES
        |  (1), (3), (6) AS OuterT(k)""".stripMargin)
  }

  // Correlated subqueries: whether the rule fires depends on which optimizer batch this
  // rule is placed in. `injectOptimizerRule` (production Gluten) inserts the rule BEFORE
  // `RewriteSubquery`; at that phase `ex.children.nonEmpty` (outerAttrs ++ joinCond, both
  // populated by `PullupCorrelatedPredicates`) blocks the rewrite. `extraOptimizations`
  // (diagnostic harness) places the rule AFTER `RewriteSubquery`; the plan is already a
  // `LeftSemi` and Pattern A may safely rewrite via `oldToNewMap` when the correlated
  // predicate references an equi key. Both phases must preserve query semantics, so we
  // assert result parity only; we do not assert whether the rule fired.
  //
  // C03/C04 below cover the *reference-non-equi-column* case, which must fail-closed in
  // any phase: pre-RewriteSubquery via `children.nonEmpty`, post-RewriteSubquery via
  // Pattern A's `validSemiKeys` guard.
  test("Correlated EXISTS references equi key: rewrite is semantics-preserving") {
    setupTable()
    setupOuterT()
    val sql =
      """SELECT o.k FROM OuterT o WHERE EXISTS (
        |  SELECT 1 FROM T s1 JOIN T s2
        |    ON s1.k = s2.k AND s1.v <> s2.v
        |  WHERE s2.k = o.k)""".stripMargin
    val (on, off) = runBoth(sql)
    assert(on == off, s"correlated EXISTS parity: ON=$on OFF=$off")
    assert(on == Set(Row(1), Row(3), Row(6)), s"expected {1,3,6}, got $on")
  }

  test("Correlated IN references equi key: rewrite is semantics-preserving") {
    setupTable()
    setupOuterT()
    val sql =
      """SELECT o.k FROM OuterT o WHERE o.k IN (
        |  SELECT s1.k FROM T s1 JOIN T s2
        |    ON s1.k = s2.k AND s1.v <> s2.v
        |  WHERE s2.k = o.k)""".stripMargin
    val (on, off) = runBoth(sql)
    assert(on == off, s"correlated IN parity: ON=$on OFF=$off")
    assert(on == Set(Row(1), Row(3), Row(6)), s"expected {1,3,6}, got $on")
  }

  test("Correlated EXISTS references NEQ column (s2.v): rule bails out") {
    setupTable()
    setupOuterT()
    // Correlation on the non-equi column: `validSemiKeys` guard must reject regardless
    // of optimizer phase.
    val sql =
      """SELECT o.k FROM OuterT o WHERE EXISTS (
        |  SELECT 1 FROM T s1 JOIN T s2
        |    ON s1.k = s2.k AND s1.v <> s2.v
        |  WHERE s2.v = o.k)""".stripMargin
    val (on, off) = runBoth(sql)
    assert(on == off, s"C03 parity: ON=$on OFF=$off")
    val plan = spark.sql(sql).queryExecution.optimizedPlan
    assert(!ruleFired(plan), s"rule must not fire when correlation refs neq column:\n$plan")
  }

  test("Correlated EXISTS references non-key column (s2.w): rule bails out") {
    setupOuterT()
    // Correlation on a column that is neither the equi key nor the neq column. This is a
    // distinct coverage angle from C03: `validSemiKeys` must reject not only "referring
    // to the neq column" but also "referring to any column outside `innerEquiAllIds`".
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW TW AS SELECT * FROM VALUES
        |  (1, 10, 100), (1, 20, 100),
        |  (3, 40, 300), (3, 50, 300),
        |  (6, 80, 600), (6, 90, 600)
        |AS TW(k, v, w)""".stripMargin)
    val sql =
      """SELECT o.k FROM OuterT o WHERE EXISTS (
        |  SELECT 1 FROM TW s1 JOIN TW s2
        |    ON s1.k = s2.k AND s1.v <> s2.v
        |  WHERE s2.w = o.k)""".stripMargin
    val (on, off) = runBoth(sql)
    assert(on == off, s"C04 parity: ON=$on OFF=$off")
    val plan = spark.sql(sql).queryExecution.optimizedPlan
    assert(
      !ruleFired(plan),
      s"rule must not fire when correlation refs non-key column:\n$plan")
  }

  // ==================== Repeatability whitelist: unknown operators fail-closed ==============

  test("Aggregate (first) inside subquery breaks row-bag repeatability: rule bails out") {
    // FIRST() is order-dependent and its aggregate result is not row-bag repeatable across
    // two evaluations, yet Catalyst's Expression.deterministic returns true. The whitelist
    // in `isRowBagRepeatable` must reject any Aggregate node inside the subquery plan.
    // range(...) avoids ConvertToLocalRelation folding the Aggregate away.
    val sql =
      """SELECT k FROM (SELECT CAST(id AS INT) AS k, CAST(id AS INT) AS v FROM range(100)) t
        |WHERE k IN (
        |  SELECT s1.k FROM (
        |      SELECT CAST(id % 10 AS INT) AS k, first(CAST(id AS INT)) AS v
        |      FROM range(200) GROUP BY id % 10
        |    ) s1
        |  JOIN (
        |      SELECT CAST(id % 10 AS INT) AS k, first(CAST(id AS INT)) AS v
        |      FROM range(200) GROUP BY id % 10
        |    ) s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    val plan = spark.sql(sql).queryExecution.optimizedPlan
    assert(!ruleFired(plan), s"whitelist must reject Aggregate inside subquery:\n$plan")
  }

  test("Window (row_number) inside subquery breaks row-bag repeatability: rule bails out") {
    // ROW_NUMBER over non-total order breaks ties nondeterministically. Whitelist rejects
    // any Window node inside the subquery plan.
    val sql =
      """SELECT k FROM (SELECT CAST(id AS INT) AS k, CAST(id AS INT) AS v FROM range(100)) t
        |WHERE k IN (
        |  SELECT s1.k FROM (
        |      SELECT k, ROW_NUMBER() OVER (PARTITION BY k ORDER BY grp) AS v
        |      FROM (SELECT CAST(id % 10 AS INT) AS k, CAST(id % 3 AS INT) AS grp FROM range(200))
        |    ) s1
        |  JOIN (
        |      SELECT k, ROW_NUMBER() OVER (PARTITION BY k ORDER BY grp) AS v
        |      FROM (SELECT CAST(id % 10 AS INT) AS k, CAST(id % 3 AS INT) AS grp FROM range(200))
        |    ) s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    val plan = spark.sql(sql).queryExecution.optimizedPlan
    assert(!ruleFired(plan), s"whitelist must reject Window inside subquery:\n$plan")
  }

  test("Uncorrelated rand() filter inside subquery: candidate-level bail out") {
    // Rand at the candidate level (uncorrelated IN whose plan contains Filter(rand)).
    // Distinct from the earlier per-side rand test, which is a correlated EXISTS whose
    // per-side isSameBaseRelation check catches it. Here the check must be at the whole
    // candidate: plan.deterministic == false because a Rand lives anywhere in the plan.
    val sql =
      """SELECT k FROM (SELECT CAST(id AS INT) AS k, CAST(id AS INT) AS v FROM range(100)) t
        |WHERE k IN (
        |  SELECT s1.k FROM (
        |      SELECT CAST(id % 10 AS INT) AS k, CAST(id AS INT) AS v
        |      FROM range(1000) WHERE rand(41) < 0.5
        |    ) s1
        |  JOIN (
        |      SELECT CAST(id % 10 AS INT) AS k, CAST(id AS INT) AS v
        |      FROM range(1000) WHERE rand(42) < 0.5
        |    ) s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    val plan = spark.sql(sql).queryExecution.optimizedPlan
    assert(!ruleFired(plan), s"candidate-level Rand guard must reject:\n$plan")
  }
}

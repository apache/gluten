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
import org.apache.spark.sql.catalyst.expressions.{Alias, GreaterThan, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

/**
 * Correctness tests for [[RewriteSelfJoinInequalityToAggregate]].
 *
 * Positive A' / A2 cases assert both result equivalence and that the rewrite actually fired.
 *
 * `assert(!ruleFired(plan))` on its own only proves the rewrite did not happen -- not that it was
 * the guard under test that stopped it. A fixture whose two self-join sides are not structurally
 * identical is rejected by `isSameBaseRelation` before any predicate is even parsed, and such a
 * test passes while covering nothing. So six important rejection paths -- the predicate parser, the
 * single-inequality requirement, output-position identity, the nondeterminism guard, the
 * leaf-source allowlist (LogicalRDD vs Parquet), and the expression-type allowlist (`abs(v)` vs
 * `v + 1`) -- are tested as single-variable pairs: the same fixture and the same query shape, one
 * control query that must fire and one variant that changes only the feature under test and must
 * not. A firing control does not pin the rejection to a particular line, but it does rule out an
 * unrelated fixture mismatch as the reason its partner was rejected. The row-bag whitelist
 * (Aggregate, Window) stays a plain negative: dropping the operator would change the query shape
 * rather than one feature.
 *
 * Self-joined fixtures are real tables, not temp views over VALUES. Spark deduplicates a self-join
 * over a [[org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation]] via `newInstance()`,
 * which refreshes one side's ExprIds without inserting a rename-only Project, so both sides stay
 * structurally identical. A temp view over VALUES cannot, and Spark renames one side with a Project
 * instead, which would make `isSameBaseRelation` false for every self-join below. `range()` needs
 * no such treatment -- Range is a MultiInstanceRelation already.
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

  private def assertRuleFired(sql: String): Unit = {
    withSQLConf("spark.gluten.sql.rewrite.selfJoinInequality" -> "true") {
      val plan = spark.sql(sql).queryExecution.optimizedPlan
      assert(ruleFired(plan), s"self-join inequality rewrite should fire:\n$plan")
    }
  }

  private def assertRuleNotFired(sql: String): Unit = {
    withSQLConf("spark.gluten.sql.rewrite.selfJoinInequality" -> "true") {
      val plan = spark.sql(sql).queryExecution.optimizedPlan
      assert(!ruleFired(plan), s"self-join inequality rewrite must not fire:\n$plan")
    }
  }

  /** Optimized plan with the rewrite enabled. */
  private def onOptimizedPlan(sql: String): LogicalPlan = {
    var plan: LogicalPlan = null
    withSQLConf("spark.gluten.sql.rewrite.selfJoinInequality" -> "true") {
      plan = spark.sql(sql).queryExecution.optimizedPlan
    }
    plan
  }

  /**
   * Assert the rewrite's structural contract without pinning the full optimized plan. Exact
   * optimized-plan equality is brittle across the Spark versions Gluten supports, because
   * optimizer-derived filters, alias names, and Project collapsing differ between versions. We
   * verify only the invariant the rewrite owns: a `HAVING count > 1` [[Filter]] sitting directly
   * over a COUNT(DISTINCT) [[Aggregate]]. We deliberately do not assert the plan has no join: the
   * rewrite may legitimately leave other joins in place, including Pattern A2's preserved outer
   * join, and `assertRuleFired` already proves the self-join inequality was the thing rewritten.
   */
  private def assertAggregateRewriteShape(plan: LogicalPlan): Unit =
    assert(
      hasAggregateRewriteShape(plan),
      s"expected a HAVING count > 1 Filter directly over a COUNT(DISTINCT) Aggregate:\n$plan")

  /** True iff a `HAVING count > 1` Filter sits directly over a COUNT(DISTINCT) Aggregate. */
  private def hasAggregateRewriteShape(plan: LogicalPlan): Boolean = plan.exists {
    case Filter(cond, agg: Aggregate) =>
      val hasDistinctCount = agg.aggregateExpressions.exists(_.exists {
        case ae: AggregateExpression => ae.isDistinct && ae.aggregateFunction.isInstanceOf[Count]
        case _ => false
      })
      val hasGt1 = cond.exists {
        case GreaterThan(_, Literal(v: Long, LongType)) => v == 1L
        case _ => false
      }
      hasDistinctCount && hasGt1
    case _ => false
  }

  /**
   * A real table, so that a self-join of it dedups into two structurally identical sides. See the
   * class comment for why a temp view over VALUES cannot be used for a self-joined fixture.
   */
  private def createTable(name: String, schema: String, values: String): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $name")
    spark.sql(s"CREATE TABLE $name($schema) USING parquet")
    spark.sql(s"INSERT INTO $name SELECT * FROM VALUES $values")
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
    createTable(
      "T",
      "k INT, v INT",
      """  (1, 10), (1, 10), (1, 20),
        |  (2, 30),
        |  (3, 40), (3, 50), (3, 60),
        |  (4, 70), (4, CAST(NULL AS INT)),
        |  (5, CAST(NULL AS INT)), (5, CAST(NULL AS INT)),
        |  (6, 80), (6, 90), (6, CAST(NULL AS INT))""".stripMargin
    )
  }

  // ==================== Positive: rewrite fires and is semantically equivalent ===============

  test("Pattern A': direct InSubquery self-join is rewritten") {
    setupTable()
    val sql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT s1.k FROM T s1 JOIN T s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin

    assertRuleFired(sql)
    assertAggregateRewriteShape(onOptimizedPlan(sql))
    val (on, off) = runBoth(sql)
    assert(on == off, s"rewrite ON $on != OFF $off")
    assert(on == Set(Row(1), Row(3), Row(6)), s"expected {1,3,6}, got $on")
  }

  test("Pattern A2: nested self-join is rewritten") {
    setupTable()
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW D AS SELECT * FROM VALUES
        |  (1), (3), (6) AS D(k)""".stripMargin)
    val sql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT d.k
        |  FROM D d, (SELECT s1.k FROM T s1 JOIN T s2
        |             ON s1.k = s2.k AND s1.v <> s2.v) sj
        |  WHERE d.k = sj.k)""".stripMargin

    assertRuleFired(sql)
    assertAggregateRewriteShape(onOptimizedPlan(sql))
    val (on, off) = runBoth(sql)
    assert(on == off, s"Pattern A2 rewrite ON $on != OFF $off")
    assert(on == Set(Row(1), Row(3), Row(6)))
  }

  test("Pattern A2: self-join on the LEFT of the outer join is rewritten") {
    // Mirror of the Pattern A2 test above. There the self-join is the RIGHT child of the outer join
    // (`selfJoinOnRight = true`); here it is the LEFT child (`selfJoinOnRight = false`). The rule
    // has an explicit branch for each side, so both are covered.
    setupTable()
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW D AS SELECT * FROM VALUES
        |  (1), (3), (6) AS D(k)""".stripMargin)
    val sql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT d.k
        |  FROM (SELECT s1.k FROM T s1 JOIN T s2
        |        ON s1.k = s2.k AND s1.v <> s2.v) sj, D d
        |  WHERE sj.k = d.k)""".stripMargin

    assertRuleFired(sql)
    assertAggregateRewriteShape(onOptimizedPlan(sql))
    val (on, off) = runBoth(sql)
    assert(on == off, s"Pattern A2 (self-join on left) rewrite ON $on != OFF $off")
    assert(on == Set(Row(1), Row(3), Row(6)))
  }

  test("Pattern A': multi-equi tuple IN with sjRight key remap is rewritten") {
    // Exercises the multi-equi-key path: two equi keys (k1, k2) drive the GROUP BY, and the tuple
    // IN projects `s1.k1, s2.k2` -- so the second output column comes from the RIGHT self-join side
    // and must be remapped to its sjLeft counterpart by `canonicalizeWrapper`. This one case covers
    // multiple equi keys, tuple IN output arity, the two injected IsNotNull(equiKey) filters, and
    // the sjRight-attribute remap at once.
    createTable(
      "TM",
      "k1 INT, k2 INT, v INT",
      """  (1, 1, 10), (1, 1, 20),
        |  (1, 2, 30), (1, 2, 30),
        |  (2, 1, 40), (2, 1, 50),
        |  (CAST(NULL AS INT), 1, 60), (CAST(NULL AS INT), 1, 70),
        |  (3, CAST(NULL AS INT), 80), (3, CAST(NULL AS INT), 90)""".stripMargin
    )
    val sql =
      """SELECT k1, k2 FROM TM outer_t WHERE (k1, k2) IN (
        |  SELECT s1.k1, s2.k2 FROM TM s1 JOIN TM s2
        |    ON s1.k1 = s2.k1 AND s1.k2 = s2.k2 AND s1.v <> s2.v)""".stripMargin

    assertRuleFired(sql)
    assertAggregateRewriteShape(onOptimizedPlan(sql))
    val (on, off) = runBoth(sql)
    assert(on == off, s"multi-equi tuple IN rewrite ON $on != OFF $off")
    // (1,1): distinct v={10,20} -> matches; (1,2): v={30} -> no; (2,1): v={40,50} -> matches;
    // (NULL,1) and (3,NULL): NULL equi key filtered out by the injected IsNotNull. -> {(1,1),(2,1)}
    assert(on == Set(Row(1, 1), Row(2, 1)), s"expected {(1,1),(2,1)}, got $on")
  }

  test("NULL / 3VL on inequality column is preserved") {
    setupTable()
    val sql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT s1.k FROM T s1 JOIN T s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin

    assertRuleFired(sql)
    val (on, off) = runBoth(sql)
    // k=4 (v={70,NULL}) and k=5 (v={NULL,NULL}) do not satisfy plain SQL <>.
    assert(on == Set(Row(1), Row(3), Row(6)), s"expected {1,3,6}, got $on")
    assert(off == on, s"NULL/3VL semantics diverge between rewrite ON and OFF: $on vs $off")
  }

  test("NULL equi-key is filtered before aggregation for NOT IN") {
    createTable(
      "TN",
      "k INT, v INT",
      """  (CAST(NULL AS INT), 10),
        |  (CAST(NULL AS INT), 20),
        |  (1, 10), (1, 20),
        |  (2, 30)""".stripMargin
    )
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW OuterKeys AS SELECT * FROM VALUES
        |  (1), (2), (3) AS OuterKeys(k)""".stripMargin)
    val sql =
      """SELECT k FROM OuterKeys o WHERE k NOT IN (
        |  SELECT s1.k FROM TN s1 JOIN TN s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin

    assertRuleFired(sql)
    val (on, off) = runBoth(sql)
    assert(on == off, s"NULL equi-key NOT IN semantics diverge: ON=$on OFF=$off")
    assert(on == Set(Row(2), Row(3)), s"expected {2,3}, got $on")
  }

  test("Swapped aliases must not be treated as the same self-join columns") {
    // The guard under test is `sameOutputPosition`. Both queries alias the same two base columns
    // to the names `k` and `v` on both sides, so a rule that compares attribute names would fire
    // on both; only the output ordinal tells them apart. The control fires, which is what makes
    // the negative case evidence that the ordinal check -- not a structural mismatch -- rejected
    // the swapped one.
    createTable("AliasBase", "a INT, b INT", "  (1, 10), (1, 20), (2, 30)")

    val alignedSql =
      """SELECT a FROM AliasBase outer_t WHERE a IN (
        |  SELECT s1.k
        |  FROM (SELECT a AS k, b AS v FROM AliasBase) s1
        |  JOIN (SELECT a AS k, b AS v FROM AliasBase) s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleFired(alignedSql)
    val (alignedOn, alignedOff) = runBoth(alignedSql)
    assert(
      alignedOn == alignedOff,
      s"aligned-alias control diverges: ON=$alignedOn OFF=$alignedOff")
    assert(alignedOn == Set(Row(1)), s"aligned-alias control expected {1}, got $alignedOn")

    // s1.k is `a` (output position 0) but s2.k is `b` (output position 1): same name, different
    // column. Rewriting this would count distinct `b` per `a`, which is a different query.
    val swappedSql =
      """SELECT a FROM AliasBase outer_t WHERE a IN (
        |  SELECT s1.k
        |  FROM (SELECT a AS k, b AS v FROM AliasBase) s1
        |  JOIN (SELECT a AS v, b AS k FROM AliasBase) s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleNotFired(swappedSql)
    val (on, off) = runBoth(swappedSql)
    assert(on == off, s"swapped-alias semantics diverge: ON=$on OFF=$off")
    assert(on.isEmpty, s"swapped-alias baseline should be empty, got $on")
  }

  test("Two different relations with the same schema must not be treated as a self-join") {
    // The guard under test is `isSameBaseRelation`: it must reject a join between two DIFFERENT
    // base tables even when they share a schema and column names. Distinct Parquet tables
    // canonicalize to distinct `rootPaths`, so `left.canonicalized == right.canonicalized` is
    // false and the rewrite must not fire. This pins a real correctness boundary, not just a
    // missed optimization: rewriting `TLeft JOIN TRight` as COUNT(DISTINCT) over TLeft alone would
    // drop TRight's rows and change the answer, so removing the guard would make ON diverge from
    // OFF here.
    createTable("TLeft", "k INT, v INT", "  (1, 10), (1, 10), (2, 30)")
    createTable("TRight", "k INT, v INT", "  (1, 20), (1, 20), (2, 30)")
    val sql =
      """SELECT k FROM TLeft outer_t WHERE k IN (
        |  SELECT s1.k FROM TLeft s1 JOIN TRight s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleNotFired(sql)
    val (on, off) = runBoth(sql)
    assert(on == off, s"different-relation join semantics diverge: ON=$on OFF=$off")
    // k=1: TLeft v={10} vs TRight v={20} -> 10<>20 true -> qualifies; k=2: 30<>30 false -> no.
    assert(on == Set(Row(1)), s"expected {1}, got $on")
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

  test("IS DISTINCT FROM is rejected by the self-join condition parser") {
    setupTable()
    val sql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT s1.k FROM T s1 JOIN T s2
        |    ON s1.k = s2.k AND s1.v IS DISTINCT FROM s2.v)""".stripMargin
    val (on, off) = runBoth(sql)
    assert(on == off, s"IS DISTINCT FROM semantics diverge: ON=$on OFF=$off")
    assert(on.contains(Row(4)), s"k=4 should be in IS DISTINCT FROM result: $on")
    assertRuleNotFired(sql)
  }

  test("IsNotNull on a non-join column is rejected") {
    // The guard under test is the predicate parser: it accepts IsNotNull only on a column the
    // join condition already references, because such a predicate is implied by the equi-key or
    // the inequality and can be dropped, while IsNotNull(w) filters rows the aggregate would
    // otherwise count. The control is the same query without that one conjunct.
    createTable(
      "T3",
      "k INT, v INT, w INT",
      """  (1, 10, 100), (1, 20, 200),
        |  (2, 30, CAST(NULL AS INT)), (2, 40, CAST(NULL AS INT))""".stripMargin)

    val controlSql =
      """SELECT k FROM T3 outer_t WHERE k IN (
        |  SELECT s1.k FROM T3 s1 JOIN T3 s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleFired(controlSql)
    val (controlOn, controlOff) = runBoth(controlSql)
    assert(controlOn == controlOff, s"T3 control diverges: ON=$controlOn OFF=$controlOff")
    assert(controlOn == Set(Row(1), Row(2)), s"T3 control expected {1,2}, got $controlOn")

    val sql =
      """SELECT k FROM T3 outer_t WHERE k IN (
        |  SELECT s1.k FROM T3 s1 JOIN T3 s2
        |    ON s1.k = s2.k AND s1.v <> s2.v AND s1.w IS NOT NULL)""".stripMargin
    assertRuleNotFired(sql)
    val (on, off) = runBoth(sql)
    assert(on == off, s"IsNotNull(non-join-col) semantics diverge: ON=$on OFF=$off")
    assert(on == Set(Row(1)), s"expected {1}, got $on")
  }

  test("Multiple inequality columns are rejected") {
    // The guard under test is `neqPairs.size != 1`. Two inequalities need "at least two rows
    // differing in v AND in w", which no count-distinct over a single column can express. The
    // control is the same query with only the first inequality.
    createTable(
      "T2",
      "k INT, v INT, w INT",
      """  (1, 10, 100), (1, 20, 200),
        |  (2, 30, 300),
        |  (3, 40, 100), (3, 50, 100)""".stripMargin)

    val controlSql =
      """SELECT k FROM T2 outer_t WHERE k IN (
        |  SELECT s1.k FROM T2 s1 JOIN T2 s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleFired(controlSql)
    val (controlOn, controlOff) = runBoth(controlSql)
    assert(controlOn == controlOff, s"T2 control diverges: ON=$controlOn OFF=$controlOff")
    assert(controlOn == Set(Row(1), Row(3)), s"T2 control expected {1,3}, got $controlOn")

    val sql =
      """SELECT k FROM T2 outer_t WHERE k IN (
        |  SELECT s1.k FROM T2 s1 JOIN T2 s2
        |    ON s1.k = s2.k AND s1.v <> s2.v AND s1.w <> s2.w)""".stripMargin
    assertRuleNotFired(sql)
    val (on, off) = runBoth(sql)
    assert(on == off, s"multi-column neq semantics diverge: ON=$on OFF=$off")
    assert(on == Set(Row(1)), s"expected {1}, got $on")
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
    assertRuleNotFired(sql)
  }

  test("Inequality column overlapping an equi-key is rejected") {
    setupTable()
    val sql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT s1.k FROM T s1 JOIN T s2
        |    ON s1.k = s2.k AND s1.k <> s2.k)""".stripMargin
    val (on, off) = runBoth(sql)
    assert(on == off, s"unsatisfiable predicate diverges: ON=$on OFF=$off")
    assert(on.isEmpty, s"unsatisfiable predicate should produce empty set, got $on")
    assertRuleNotFired(sql)
  }

  test("Subquery output referencing the inequality column is not rewritten") {
    // Survival-reference invariant: the rewrite replaces the self-join with a GROUP BY that keeps
    // only the equi-keys and the COUNT(DISTINCT) alias, so the neq column (`v`) no longer exists in
    // the rewritten output. A candidate whose surviving output still references that soon-to-be-
    // deleted column must be refused rather than rewritten into a plan with a dangling reference.
    // Here the tuple IN exposes `s1.v` as an output column, so `canonicalizeWrapper` sees a
    // non-equi projectList entry and fails the rewrite closed.
    //
    // This is the SQL-reachable realization of the fail-closed remap guard. The
    // `remapNamedExpressionAttributes` None branch itself only triggers on a projectList entry that
    // is neither an Attribute nor an Alias yet still references a replaced output; an analyzed plan
    // does not produce such an entry, so that branch is defensive rather than SQL-testable and is
    // deliberately not exercised by a hand-built fake expression here.
    setupTable()
    val sql =
      """SELECT k, v FROM T outer_t WHERE (k, v) IN (
        |  SELECT s1.k, s1.v FROM T s1 JOIN T s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleNotFired(sql)
    val (on, off) = runBoth(sql)
    assert(on == off, s"neq-column-in-output semantics diverge: ON=$on OFF=$off")
  }

  test("Config gate: selfJoinInequality=false disables a valid A' candidate") {
    setupTable()
    val sql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT s1.k FROM T s1 JOIN T s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    withSQLConf("spark.gluten.sql.rewrite.selfJoinInequality" -> "false") {
      val plan = spark.sql(sql).queryExecution.optimizedPlan
      assert(!ruleFired(plan), s"config off must not fire rewrite:\n$plan")
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

  test("Correlated InSubquery is fail-closed") {
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
    assertRuleNotFired(sql)
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
    assertRuleNotFired(sql)
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
    assertRuleNotFired(sql)
  }

  test("Nondeterministic self-join input is rejected") {
    // The guard under test is `plan.deterministic` inside `isRepeatablePlan`. Both sides use the
    // same explicit seed, so the two subplans do have the same canonical shape and the rejection
    // cannot come from `isSameBaseRelation`. The control replaces `rand(41) < 0.5` with a
    // deterministic filter and nothing else, proving this Range/Filter/Project shape does reach
    // the rewrite.
    val controlSql =
      """SELECT k FROM (SELECT CAST(id AS INT) AS k, CAST(id AS INT) AS v FROM range(100)) t
        |WHERE k IN (
        |  SELECT s1.k FROM (
        |      SELECT CAST(id % 10 AS INT) AS k, CAST(id AS INT) AS v
        |      FROM range(1000) WHERE id % 2 = 0
        |    ) s1
        |  JOIN (
        |      SELECT CAST(id % 10 AS INT) AS k, CAST(id AS INT) AS v
        |      FROM range(1000) WHERE id % 2 = 0
        |    ) s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleFired(controlSql)
    val (controlOn, controlOff) = runBoth(controlSql)
    assert(controlOn == controlOff, s"range control diverges: ON=$controlOn OFF=$controlOff")
    assert(
      controlOn == Set(Row(0), Row(2), Row(4), Row(6), Row(8)),
      s"range control expected the even keys, got $controlOn")

    val sql =
      """SELECT k FROM (SELECT CAST(id AS INT) AS k, CAST(id AS INT) AS v FROM range(100)) t
        |WHERE k IN (
        |  SELECT s1.k FROM (
        |      SELECT CAST(id % 10 AS INT) AS k, CAST(id AS INT) AS v
        |      FROM range(1000) WHERE rand(41) < 0.5
        |    ) s1
        |  JOIN (
        |      SELECT CAST(id % 10 AS INT) AS k, CAST(id AS INT) AS v
        |      FROM range(1000) WHERE rand(41) < 0.5
        |    ) s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleNotFired(sql)
  }

  test("Pattern A2: nondeterminism in the outer join condition must not be rewritten") {
    // Both self-join sides are still repeatable here, so the per-side `isSameBaseRelation` check
    // would pass; the `rand()` conjunct lives on the outer join ABOVE the self-join. Only the
    // candidate-level `isRepeatablePlan` walk over the whole subquery catches it, so the rule must
    // fail closed. This is the case the candidate-level guard exists for.
    setupTable()
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW D AS SELECT * FROM VALUES
        |  (1), (3), (6) AS D(k)""".stripMargin)
    val sql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT d.k
        |  FROM D d, (SELECT s1.k FROM T s1 JOIN T s2
        |             ON s1.k = s2.k AND s1.v <> s2.v) sj
        |  WHERE d.k = sj.k AND rand() < 0.5)""".stripMargin

    assertRuleNotFired(sql)
  }

  test("LogicalRDD leaf is not a trusted repeatable source: rule bails out") {
    // The guard under test is the leaf allowlist in `isRowBagRepeatable`: a Parquet
    // `LogicalRelation` is trusted, but a `LogicalRDD` (createDataFrame over an RDD) wraps an
    // arbitrary RDD lineage whose runtime row bag Catalyst cannot prove repeatable, so it must
    // fail closed even though `plan.deterministic` is true. Both fixtures are MultiInstanceRelation
    // leaves, so each self-join dedups into two structurally identical sides without a rename-only
    // Project -- the rejection therefore comes from the leaf allowlist, not `isSameBaseRelation`.
    // The Parquet control uses the same schema, data and query shape and fires, which is what makes
    // the LogicalRDD negative evidence that the leaf allowlist -- not a structural mismatch --
    // rejected it.
    createTable("RddCtl", "k INT, v INT", "  (1, 10), (1, 20), (2, 30)")
    val controlSql =
      """SELECT k FROM RddCtl outer_t WHERE k IN (
        |  SELECT s1.k FROM RddCtl s1 JOIN RddCtl s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleFired(controlSql)
    val (controlOn, controlOff) = runBoth(controlSql)
    assert(controlOn == controlOff, s"Parquet control diverges: ON=$controlOn OFF=$controlOff")
    assert(controlOn == Set(Row(1)), s"Parquet control expected {1}, got $controlOn")

    val schema = StructType(Seq(StructField("k", IntegerType), StructField("v", IntegerType)))
    val rows = spark.sparkContext.parallelize(Seq(Row(1, 10), Row(1, 20), Row(2, 30)))
    spark.createDataFrame(rows, schema).createOrReplaceTempView("RddT")
    val sql =
      """SELECT k FROM RddT outer_t WHERE k IN (
        |  SELECT s1.k FROM RddT s1 JOIN RddT s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleNotFired(sql)
    val (on, off) = runBoth(sql)
    assert(on == off, s"LogicalRDD semantics diverge: ON=$on OFF=$off")
    assert(on == Set(Row(1)), s"expected {1}, got $on")
  }

  test("Non-allowlisted deterministic expression (Abs) fails closed") {
    // The guard under test is the expression allowlist in `isRepeatableExpression`: it trusts
    // expression TYPES, not merely `deterministic`. Abs is deterministic, but is intentionally not
    // yet part of the expression allowlist; until its repeatability contract is explicitly admitted
    // there, a self-join whose side projects abs(v) fails closed -- a missed optimization, not a
    // correctness bug. This test verifies that an unknown-but-deterministic expression is not
    // silently let through by `plan.deterministic`.
    //
    // Control and negative are a single-variable pair: both project one derived column and differ
    // ONLY in its expression. The control uses `v + 1` (Add over Attribute + Literal, all
    // allowlisted) and fires; wrapping the same column in abs() -- the sole change -- makes it not
    // fire, so the rejection is attributable to the expression allowlist rather than a structural
    // mismatch. `+ 1` (not `+ 0`) is used so the Add survives arithmetic simplification and the
    // control genuinely exercises a compound allowlisted expression. v is INT here, so the Add is a
    // plain `Add(v, 1)` with no decimal PromotePrecision / CheckOverflow wrappers.
    setupTable()

    val controlSql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT s1.k
        |  FROM (SELECT k, v + 1 AS x FROM T) s1
        |  JOIN (SELECT k, v + 1 AS x FROM T) s2
        |    ON s1.k = s2.k AND s1.x <> s2.x)""".stripMargin
    assertRuleFired(controlSql)
    val (controlOn, controlOff) = runBoth(controlSql)
    assert(controlOn == controlOff, s"Add control diverges: ON=$controlOn OFF=$controlOff")
    // v+1 is injective over the (non-null) v values, so distinctness per k is unchanged: {1,3,6}.
    assert(
      controlOn == Set(Row(1), Row(3), Row(6)),
      s"Add control expected {1,3,6}, got $controlOn")

    val sql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT s1.k
        |  FROM (SELECT k, abs(v) AS x FROM T) s1
        |  JOIN (SELECT k, abs(v) AS x FROM T) s2
        |    ON s1.k = s2.k AND s1.x <> s2.x)""".stripMargin
    assertRuleNotFired(sql)
    val (on, off) = runBoth(sql)
    assert(on == off, s"Abs-projected self-join semantics diverge: ON=$on OFF=$off")
  }

  test("Float/Double inequality column fails closed") {
    // The guard under test is `isSafeComparisonGroupingType` on the neq column. The rewrite
    // replaces comparison equality (`s1.v <> s2.v`) with grouping / DISTINCT equality
    // (`COUNT(DISTINCT v)`); we keep floating-point neq columns fail-closed rather than depend on
    // NaN / signed-zero normalization semantics staying aligned across Spark versions and native
    // backends. The fixture carries the exact values the reviewer called out (-0.0 / +0.0, and two
    // NaNs) in BOTH a FLOAT and a DOUBLE column, alongside an INT `vi` control column. Single-
    // variable family: the neq column is `vi` (fires) vs `vf` / `vd` (must not fire).
    createTable(
      "TFloat",
      "k INT, vi INT, vf FLOAT, vd DOUBLE",
      """  (1, 10, CAST(-0.0 AS FLOAT), CAST(-0.0 AS DOUBLE)),
        |  (1, 20, CAST(0.0 AS FLOAT), CAST(0.0 AS DOUBLE)),
        |  (2, 30, CAST('NaN' AS FLOAT), CAST('NaN' AS DOUBLE)),
        |  (2, 40, CAST('NaN' AS FLOAT), CAST('NaN' AS DOUBLE))""".stripMargin
    )

    val controlSql =
      """SELECT k FROM TFloat outer_t WHERE k IN (
        |  SELECT s1.k FROM TFloat s1 JOIN TFloat s2
        |    ON s1.k = s2.k AND s1.vi <> s2.vi)""".stripMargin
    assertRuleFired(controlSql)
    val (controlOn, controlOff) = runBoth(controlSql)
    assert(controlOn == controlOff, s"INT neq control diverges: ON=$controlOn OFF=$controlOff")
    assert(controlOn == Set(Row(1), Row(2)), s"INT neq control expected {1,2}, got $controlOn")

    // Do not pin an expected set for the floating arms -- the whole point is that we refuse to
    // reason about -0.0/NaN equality here. We only require ON and OFF to agree with each other.
    Seq("vf", "vd").foreach {
      col =>
        val sql =
          s"""SELECT k FROM TFloat outer_t WHERE k IN (
             |  SELECT s1.k FROM TFloat s1 JOIN TFloat s2
             |    ON s1.k = s2.k AND s1.$col <> s2.$col)""".stripMargin
        assertRuleNotFired(sql)
        val (on, off) = runBoth(sql)
        assert(on == off, s"$col neq semantics diverge: ON=$on OFF=$off")
    }
  }

  test("Float/Double equi-key fails closed") {
    // Same guard on the equi-key: the rewrite turns `s1.gk = s2.gk` into GROUP BY gk, swapping
    // comparison equality for grouping equality. As with the neq column, we keep floating-point
    // equi-keys fail-closed rather than depending on -0.0/NaN normalization staying aligned across
    // Spark versions and native backends. Single-variable family: the equi-key cast to INT fires,
    // and the same equi-key cast to FLOAT / DOUBLE does not. The subquery projects the equi-key
    // `gk` itself (not the outer `k`), so the wrapper-output contract -- Project output is equi-key
    // or its alias -- is satisfied and the rejection is attributable to the type guard.
    setupTable()

    val controlSql =
      """SELECT k FROM T outer_t WHERE CAST(k AS INT) IN (
        |  SELECT s1.gk
        |  FROM (SELECT CAST(k AS INT) AS gk, v FROM T) s1
        |  JOIN (SELECT CAST(k AS INT) AS gk, v FROM T) s2
        |    ON s1.gk = s2.gk AND s1.v <> s2.v)""".stripMargin
    assertRuleFired(controlSql)
    val (controlOn, controlOff) = runBoth(controlSql)
    assert(controlOn == controlOff, s"INT equi control diverges: ON=$controlOn OFF=$controlOff")
    assert(
      controlOn == Set(Row(1), Row(3), Row(6)),
      s"INT equi control expected {1,3,6}, got $controlOn")

    // k is a clean positive integer, so CAST-to-FLOAT/DOUBLE introduces no -0.0/NaN and the OFF
    // baseline stays {1,3,6}; the point is which plan runs, verified by (not) firing.
    Seq("FLOAT", "DOUBLE").foreach {
      tpe =>
        val sql =
          s"""SELECT k FROM T outer_t WHERE CAST(k AS $tpe) IN (
             |  SELECT s1.gk
             |  FROM (SELECT CAST(k AS $tpe) AS gk, v FROM T) s1
             |  JOIN (SELECT CAST(k AS $tpe) AS gk, v FROM T) s2
             |    ON s1.gk = s2.gk AND s1.v <> s2.v)""".stripMargin
        assertRuleNotFired(sql)
        val (on, off) = runBoth(sql)
        assert(on == off, s"$tpe equi-key semantics diverge: ON=$on OFF=$off")
        assert(on == Set(Row(1), Row(3), Row(6)), s"$tpe equi-key expected {1,3,6}, got $on")
    }
  }

  test("Complex comparison columns (Array/Struct) fail closed") {
    // The guard under test is `isSafeComparisonGroupingType` rejecting complex types wholesale. A
    // self-join whose neq column is ARRAY<DOUBLE> or STRUCT<..., DOUBLE> is exactly the hole that
    // motivated the type gate (a complex type can nest the very floats we reject); the positive
    // allowlist rejects it directly rather than relying on RowOrdering admitting/denying it.
    // Single-variable family: the neq column is the INT `vi` (fires) vs `a` / `s` (must not fire).
    createTable(
      "TComplex",
      "k INT, vi INT, a ARRAY<DOUBLE>, s STRUCT<x: INT, y: DOUBLE>",
      """  (1, 10, ARRAY(1.0D), NAMED_STRUCT('x', 1, 'y', 1.0D)),
        |  (1, 20, ARRAY(2.0D), NAMED_STRUCT('x', 2, 'y', 2.0D)),
        |  (2, 30, ARRAY(3.0D), NAMED_STRUCT('x', 3, 'y', 3.0D)),
        |  (2, 40, ARRAY(4.0D), NAMED_STRUCT('x', 4, 'y', 4.0D))""".stripMargin
    )

    val controlSql =
      """SELECT k FROM TComplex outer_t WHERE k IN (
        |  SELECT s1.k FROM TComplex s1 JOIN TComplex s2
        |    ON s1.k = s2.k AND s1.vi <> s2.vi)""".stripMargin
    assertRuleFired(controlSql)
    val (controlOn, controlOff) = runBoth(controlSql)
    assert(controlOn == controlOff, s"INT control diverges: ON=$controlOn OFF=$controlOff")
    assert(controlOn == Set(Row(1), Row(2)), s"INT control expected {1,2}, got $controlOn")

    Seq("a", "s").foreach {
      col =>
        val sql =
          s"""SELECT k FROM TComplex outer_t WHERE k IN (
             |  SELECT s1.k FROM TComplex s1 JOIN TComplex s2
             |    ON s1.k = s2.k AND s1.$col <> s2.$col)""".stripMargin
        assertRuleNotFired(sql)
        val (on, off) = runBoth(sql)
        assert(on == off, s"$col complex neq semantics diverge: ON=$on OFF=$off")
    }
  }

  test("String comparison column fails closed") {
    // A behavior narrowing that the positive allowlist introduces: String is intentionally outside
    // the allowlist (its equality under non-binary collation is version-dependent), so a String neq
    // column fails closed even though String is orderable and would have passed a bare
    // `RowOrdering.isOrderable` gate. Single-variable pair: the INT `vi` control fires, the STRING
    // `vs` column does not.
    createTable(
      "TStr",
      "k INT, vi INT, vs STRING",
      """  (1, 10, 'a'), (1, 20, 'b'),
        |  (2, 30, 'c'), (2, 40, 'd')""".stripMargin
    )

    val controlSql =
      """SELECT k FROM TStr outer_t WHERE k IN (
        |  SELECT s1.k FROM TStr s1 JOIN TStr s2
        |    ON s1.k = s2.k AND s1.vi <> s2.vi)""".stripMargin
    assertRuleFired(controlSql)
    val (controlOn, controlOff) = runBoth(controlSql)
    assert(controlOn == controlOff, s"INT control diverges: ON=$controlOn OFF=$controlOff")
    assert(controlOn == Set(Row(1), Row(2)), s"INT control expected {1,2}, got $controlOn")

    val sql =
      """SELECT k FROM TStr outer_t WHERE k IN (
        |  SELECT s1.k FROM TStr s1 JOIN TStr s2
        |    ON s1.k = s2.k AND s1.vs <> s2.vs)""".stripMargin
    assertRuleNotFired(sql)
    val (on, off) = runBoth(sql)
    assert(on == off, s"String neq semantics diverge: ON=$on OFF=$off")
    assert(on == Set(Row(1), Row(2)), s"String neq expected {1,2}, got $on")
  }

  test("Explicit join hint on the self-join fails closed (Pattern A')") {
    // The guard under test is `innerJoin.hint != JoinHint.NONE` in the direct path. A user
    // BROADCAST hint on the self-join is an explicit optimizer directive about the very join this
    // rule would delete, so we fail closed rather than silently discard it. Single-variable pair:
    // the same query shape without a hint fires; adding the hint -- the sole change -- must not
    // fire.
    setupTable()

    val controlSql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT s1.k FROM T s1 JOIN T s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleFired(controlSql)

    val sql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT /*+ BROADCAST(s2) */ s1.k FROM T s1 JOIN T s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleNotFired(sql)
    val (on, off) = runBoth(sql)
    assert(on == off, s"hinted self-join parity: ON=$on OFF=$off")
    assert(on == Set(Row(1), Row(3), Row(6)), s"expected {1,3,6}, got $on")
  }

  test("Explicit join hint on the nested self-join fails closed (Pattern A2)") {
    // The nested extraction path (`tryExtractSelfJoin`) applies the same hint guard, so a hinted
    // self-join buried under an outer join is not recognized as a rewrite candidate. Single-
    // variable pair against the Pattern A2 positive shape: the un-hinted self-join fires; the
    // hinted one does not, while the outer join is untouched either way.
    setupTable()
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW D AS SELECT * FROM VALUES
        |  (1), (3), (6) AS D(k)""".stripMargin)

    val controlSql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT d.k
        |  FROM D d, (SELECT s1.k FROM T s1 JOIN T s2
        |             ON s1.k = s2.k AND s1.v <> s2.v) sj
        |  WHERE d.k = sj.k)""".stripMargin
    assertRuleFired(controlSql)

    val sql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT d.k
        |  FROM D d, (SELECT /*+ BROADCAST(s2) */ s1.k FROM T s1 JOIN T s2
        |             ON s1.k = s2.k AND s1.v <> s2.v) sj
        |  WHERE d.k = sj.k)""".stripMargin
    assertRuleNotFired(sql)
    val (on, off) = runBoth(sql)
    assert(on == off, s"hinted nested self-join parity: ON=$on OFF=$off")
    assert(on == Set(Row(1), Row(3), Row(6)), s"expected {1,3,6}, got $on")
  }

  test("Explicit hint on the preserved outer join still rewrites the self-join (Pattern A2)") {
    // The hint guard targets only the join the rewrite DELETES (the self-join). A hint on the outer
    // join -- which the rewrite preserves -- must NOT block the rewrite: the self-join here carries
    // no hint, so it is still rewritten while the hinted outer join is left intact. This pins the
    // guard's contract as "do not silently discard a hint on a join we remove", not "reject any
    // hint anywhere in the candidate subtree". Counterpart to the two hinted-self-join negatives.
    setupTable()
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW D AS SELECT * FROM VALUES
        |  (1), (3), (6) AS D(k)""".stripMargin)
    val sql =
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT /*+ BROADCAST(d) */ d.k
        |  FROM D d JOIN (SELECT s1.k FROM T s1 JOIN T s2
        |                 ON s1.k = s2.k AND s1.v <> s2.v) sj
        |  ON d.k = sj.k)""".stripMargin

    assertRuleFired(sql)
    assertAggregateRewriteShape(onOptimizedPlan(sql))
    val (on, off) = runBoth(sql)
    assert(on == off, s"outer-hinted self-join parity: ON=$on OFF=$off")
    assert(on == Set(Row(1), Row(3), Row(6)), s"expected {1,3,6}, got $on")
  }
}

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

import org.apache.gluten.execution.{HashAggregateExecBaseTransformer, WholeStageTransformerSuite}

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Alias, GreaterThan, InSubquery, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
 * Correctness coverage for [[RewriteSelfJoinInequalityToAggregate]].
 *
 * Positive cases require result parity and a fired rewrite; rejection tests use a firing control
 * where needed to make the targeted guard provable.
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

  /** Optimized plan with the rewrite disabled. */
  private def offOptimizedPlan(sql: String): LogicalPlan = {
    var plan: LogicalPlan = null
    withSQLConf("spark.gluten.sql.rewrite.selfJoinInequality" -> "false") {
      plan = spark.sql(sql).queryExecution.optimizedPlan
    }
    plan
  }

  /** Optimize the uncorrelated IN subquery alone so a test can assert its real shape. */
  private def optimizedInSubqueryPlan(sql: String): LogicalPlan = {
    val analyzed = spark.sql(sql).queryExecution.analyzed
    var subqueryPlan: LogicalPlan = null
    analyzed.foreach {
      node =>
        node.expressions.foreach(_.foreach {
          case in: InSubquery if subqueryPlan == null => subqueryPlan = in.query.plan
          case _ =>
        })
    }
    assert(subqueryPlan != null, s"expected an InSubquery in analyzed plan:\n$analyzed")
    spark.sessionState.optimizer.execute(subqueryPlan)
  }

  /**
   * Assert the rewrite yields the same optimized plan as an equivalent hand-written `GROUP BY k
   * HAVING COUNT(DISTINCT v) > 1`. The equivalent SQL spells out `k IS NOT NULL` / `v IS NOT NULL`
   * so both plans canonicalize identically.
   */
  private def assertRewriteMatchesEquivalent(selfJoinSql: String, equivalentSql: String): Unit = {
    val actual = onOptimizedPlan(selfJoinSql)
    val expected = offOptimizedPlan(equivalentSql)
    assert(ruleFired(actual), s"precondition: rewrite should fire:\n$actual")
    assert(
      !ruleFired(expected),
      s"precondition: the equivalent query must already be the hand-written aggregate:\n$expected")
    comparePlans(actual.canonicalized, expected.canonicalized, checkAnalysis = false)
  }

  /**
   * Assert a `HAVING count > 1` Filter sits directly over a COUNT(DISTINCT) Aggregate, without
   * pinning the full plan (brittle across Spark versions). A2 keeps its outer join, so no no-join
   * assertion.
   */
  private def assertAggregateRewriteShape(plan: LogicalPlan): Unit =
    assert(
      hasAggregateRewriteShape(plan),
      s"expected a HAVING count > 1 Filter directly over a COUNT(DISTINCT) Aggregate:\n$plan")

  private def hasAggregateRewriteShape(plan: LogicalPlan): Boolean = plan.exists {
    case Filter(cond, agg: Aggregate) =>
      val hasDistinctCount = agg.aggregateExpressions.exists(_.exists {
        case ae: AggregateExpression => ae.isDistinct && ae.aggregateFunction.isInstanceOf[Count]
        case _ => false
      })
      val hasGt1 = cond.exists {
        case GreaterThan(_, Literal(1L, _)) => true
        case _ => false
      }
      hasDistinctCount && hasGt1
    case _ => false
  }

  /** A real table, so a self-join of it dedups into two structurally identical sides. */
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

    assertRewriteMatchesEquivalent(
      sql,
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT k FROM T WHERE k IS NOT NULL AND v IS NOT NULL
        |  GROUP BY k HAVING COUNT(DISTINCT v) > 1)""".stripMargin
    )
    val (on, off) = runBoth(sql)
    assert(on == off, s"rewrite ON $on != OFF $off")
    assert(on == Set(Row(1), Row(3), Row(6)), s"expected {1,3,6}, got $on")

    withSQLConf("spark.gluten.sql.rewrite.selfJoinInequality" -> "true") {
      val df = spark.sql(sql)
      df.collect()
      checkGlutenPlan[HashAggregateExecBaseTransformer](df)
    }
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

    assertRewriteMatchesEquivalent(
      sql,
      """SELECT k FROM T outer_t WHERE k IN (
        |  SELECT d.k
        |  FROM D d, (SELECT k FROM T WHERE k IS NOT NULL AND v IS NOT NULL
        |             GROUP BY k HAVING COUNT(DISTINCT v) > 1) sj
        |  WHERE d.k = sj.k)""".stripMargin
    )
    val (on, off) = runBoth(sql)
    assert(on == off, s"Pattern A2 rewrite ON $on != OFF $off")
    assert(on == Set(Row(1), Row(3), Row(6)))
  }

  test("Pattern A2: self-join on the LEFT of the outer join is rewritten") {
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
    // The tuple IN projects `s1.k1, s2.k2`, so the second column comes from the RIGHT side and must
    // be remapped to sjLeft by the wrapper.
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
    // (1,1) and (2,1) match; (1,2) has one v; (NULL,1)/(3,NULL) have a NULL equi key filtered out.
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
    // Both queries alias to the same names on both sides; only the output ordinal tells the aligned
    // control (fires) from the swapped variant (must not).
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
    // Distinct Parquet tables canonicalize differently, so `isSameBaseRelation` fails; rewriting
    // over TLeft alone would drop TRight's rows.
    createTable("TLeft", "k INT, v INT", "  (1, 10), (1, 10), (2, 30)")
    createTable("TRight", "k INT, v INT", "  (1, 20), (1, 20), (2, 30)")
    val sql =
      """SELECT k FROM TLeft outer_t WHERE k IN (
        |  SELECT s1.k FROM TLeft s1 JOIN TRight s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    assertRuleNotFired(sql)
    val (on, off) = runBoth(sql)
    assert(on == off, s"different-relation join semantics diverge: ON=$on OFF=$off")
    assert(on == Set(Row(1)), s"expected {1}, got $on")
  }

  // ==================== Negative: rewrite must produce equivalent results (or bail) ==========

  test("Plain InnerJoin at top level: results unchanged (rewrite must not touch it)") {
    setupTable()
    val sql =
      """SELECT ws1.k FROM T ws1 JOIN T ws2
        |ON ws1.k = ws2.k AND ws1.v <> ws2.v""".stripMargin
    // Row multiplicity matters here; use count() to catch any drop or dup.
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

  test("Bare-Join subquery (no wrapper Project) fails closed: Pattern A' arity guard") {
    setupTable()
    val sql =
      """SELECT k FROM T outer_t WHERE (k, v, k, v) IN (
        |  SELECT * FROM T s1 JOIN T s2
        |    ON s1.k = s2.k AND s1.v <> s2.v)""".stripMargin
    // Prove RemoveNoopOperators exposes the bare Join guard.
    optimizedInSubqueryPlan(sql) match {
      case _: Join =>
      case other => fail(s"expected a bare Join subquery, got:\n$other")
    }
    assertRuleNotFired(sql)
    val (on, off) = runBoth(sql)
    assert(on == off, s"bare-Join A' arity guard semantics diverge: ON=$on OFF=$off")
  }

  test("Bare-Join nested self-join (no Project anywhere) fails closed: Pattern A2 arity guard") {
    setupTable()
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW D AS SELECT * FROM VALUES
        |  (1), (3), (6) AS D(k)""".stripMargin)
    val sql =
      """SELECT k FROM T outer_t WHERE (k, v, k, k, v) IN (
        |  SELECT * FROM D d JOIN (SELECT * FROM T s1 JOIN T s2
        |                          ON s1.k = s2.k AND s1.v <> s2.v) sj
        |  ON d.k = sj.k)""".stripMargin
    // Prove RemoveNoopOperators exposes the nested bare-Join guard.
    optimizedInSubqueryPlan(sql) match {
      case j: Join if j.left.isInstanceOf[Join] || j.right.isInstanceOf[Join] =>
      case other => fail(s"expected a bare nested self-join, got:\n$other")
    }
    assertRuleNotFired(sql)
    val (on, off) = runBoth(sql)
    assert(on == off, s"bare-Join A2 arity guard semantics diverge: ON=$on OFF=$off")
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
    // Guard: IsNotNull is accepted only on a join column (droppable); on another column it filters
    // rows the aggregate would count. Control drops it.
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
    // Control keeps only the first inequality; two cannot map to COUNT(DISTINCT) over one column.
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
    // The neq column `v` does not survive the rewrite, so an output referencing it is refused:
    // canonicalizeWrapper sees a non-equi projectList entry and fails closed.
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
    // FIRST() is order-dependent (not row-bag repeatable) yet reports deterministic; the operator
    // whitelist must reject any Aggregate. range() avoids ConvertToLocalRelation.
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
    // ROW_NUMBER over a non-total order breaks ties nondeterministically; whitelist rejects Window.
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
    // Same seed keeps both sides structurally identical, so rejection is from `plan.deterministic`,
    // not isSameBaseRelation. Control swaps rand() for a deterministic filter.
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
    // The rand() conjunct is on the outer join above the self-join, so only the candidate-level
    // `isRepeatablePlan` walk over the whole subquery catches it.
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
    // LogicalRDD wraps an arbitrary RDD lineage outside the trusted leaf allowlist, so it fails
    // closed though deterministic; the Parquet control fires, isolating the leaf allowlist.
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
    // Abs is deterministic but not allowlisted. Control `v + 1` (allowlisted, survives arithmetic
    // simplification; INT so no decimal wrappers) fires; abs(v) must not.
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
    // Guard: `isSafeComparisonGroupingType` on the neq column. The fixture carries -0.0/+0.0 and
    // two NaNs in FLOAT and DOUBLE plus an INT `vi` control: `vi` fires, vf/vd stay fail-closed.
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

    // Do not pin an expected set for the floating arms -- we refuse to reason about -0.0/NaN here.
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
    // Same guard on the equi-key: `s1.gk = s2.gk` becomes GROUP BY gk. INT cast fires, FLOAT/DOUBLE
    // does not.
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

    // k is a clean positive integer, so CAST-to-FLOAT/DOUBLE introduces no -0.0/NaN.
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
    // `isSafeComparisonGroupingType` rejects complex types. INT `vi` fires; ARRAY/STRUCT do not.
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
    // The hint guard targets only the join the rewrite deletes; a hint on the preserved outer join
    // must not block rewriting the (un-hinted) self-join.
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

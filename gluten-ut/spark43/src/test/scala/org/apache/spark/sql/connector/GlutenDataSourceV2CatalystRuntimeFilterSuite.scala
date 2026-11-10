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
package org.apache.spark.sql.connector

import org.apache.gluten.execution.FilterExecTransformerBase

import org.apache.spark.sql.{DataFrame, GlutenSQLTestsTrait, Row}
import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, DynamicPruning, EqualTo, Expression, Literal}
import org.apache.spark.sql.connector.catalog.InMemoryCatalystRuntimeFilterTable
import org.apache.spark.sql.execution.{FilterExec, ScalarSubquery => ExecScalarSubquery}
import org.apache.spark.sql.execution.ExplainUtils.stripAQEPlan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType

class GlutenDataSourceV2CatalystRuntimeFilterSuite
  extends DataSourceV2CatalystRuntimeFilterSuite
  with GlutenSQLTestsTrait {

  // Gluten offloads the post-scan filter to a FilterExecTransformer, so look for the scalar
  // subquery in both filter operators.
  private def checkEvaluatedAfterScan(df: DataFrame, expected: Boolean): Unit = {
    val postScanConditions = stripAQEPlan(df.queryExecution.executedPlan).collect {
      case f: FilterExec => f.condition
      case f: FilterExecTransformerBase => f.cond
    }
    val evaluated = postScanConditions.exists(_.exists(_.isInstanceOf[ExecScalarSubquery]))
    assert(
      evaluated === expected,
      s"Expected scalar subquery evaluated after scan to be $expected, " +
        s"post-scan filter conditions: $postScanConditions")
  }

  private def batchScan(df: DataFrame): BatchScanExec = {
    stripAQEPlan(df.queryExecution.executedPlan)
      .collectFirst { case b: BatchScanExec => b }
      .getOrElse(fail("Expected BatchScanExec in plan"))
  }

  private def checkScalarSubqueryRuntimeFilters(df: DataFrame, expectedCount: Int = 1): Unit = {
    val runtimeFilters = batchScan(df).runtimeFilters
    assert(runtimeFilters.count(!_.isInstanceOf[DynamicPruning]) === expectedCount)
    assert(!runtimeFilters.exists(_.isInstanceOf[DynamicPruning]))
  }

  private def pushedPredicates(df: DataFrame): Seq[Expression] = {
    batchScan(df).scan match {
      case s: InMemoryCatalystRuntimeFilterTable#InMemoryCatalystRuntimeFilterBatchScan =>
        s.pushedCatalystPredicates
      case other => fail(s"Expected InMemoryCatalystRuntimeFilterBatchScan, got $other")
    }
  }

  private def checkPushedPredicate(df: DataFrame, expected: Expression): Unit = {
    val output = batchScan(df).output
    val resolver = SQLConf.get.resolver
    val boundExpected = expected.transformUp {
      case a: AttributeReference =>
        output
          .find(o => resolver(o.name, a.name))
          .map(_.withNullability(a.nullable).withQualifier(a.qualifier))
          .getOrElse(a)
    }
    val actual = pushedPredicates(df)
    assert(actual.size === 1, s"Expected 1 pushed Catalyst predicate, got $actual")
    assert(actual.head.semanticEquals(boundExpected), s"Expected $boundExpected, got $actual")
  }

  testGluten("scalar subquery on partition column -> pushed as Catalyst expression") {
    val tbl = s"$catalogName.tbl1"
    val dim = s"$catalogName.dim1"
    withTable(tbl, dim) {
      sql(s"CREATE TABLE $tbl (id INT, part INT) USING $v2Source PARTITIONED BY (part)")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, $i)")
      }
      sql(s"CREATE TABLE $dim (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (3)")

      val df = sql(s"SELECT * FROM $tbl WHERE part = (SELECT max(val) FROM $dim)")
      checkAnswer(df, Row(3, 3))

      checkScalarSubqueryRuntimeFilters(df)
      val part = AttributeReference("part", IntegerType, nullable = false)()
      checkPushedPredicate(df, EqualTo(part, Literal(3)))
      checkEvaluatedAfterScan(df, expected = true)
    }
  }

  testGluten("Catalyst runtime filtering fixture fully pushes only identity partition sources") {
    val tbl = s"$catalogName.tbl_transformed_fully_pushed"
    val dim = s"$catalogName.dim_transformed_fully_pushed"
    withTable(tbl, dim) {
      sql(
        s"CREATE TABLE $tbl (id INT, part DATE) USING $v2Source " +
          "PARTITIONED BY (days(part)) " +
          "TBLPROPERTIES('fully-pushed-filter-attributes' = 'part')")
      sql(s"INSERT INTO $tbl VALUES (1, DATE '2026-08-01'), (2, DATE '2026-08-02')")
      sql(s"CREATE TABLE $dim (value DATE) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (DATE '2026-08-02')")

      val df = sql(s"SELECT * FROM $tbl WHERE part = (SELECT max(value) FROM $dim)")
      checkAnswer(df, Row(2, java.sql.Date.valueOf("2026-08-02")))

      checkScalarSubqueryRuntimeFilters(df)
      assert(pushedPredicates(df).size === 1)
      checkEvaluatedAfterScan(df, expected = true)
      val scan = batchScan(df)
      assert(scan.inputPartitions.size === 2)
      assert(scan.filteredPartitions.flatten.size === 2)
    }
  }

  testGluten("non-deterministic predicate on fully pushed attributes -> evaluated after the scan") {
    val tbl = s"$catalogName.tbl_nondeterministic"
    val dim = s"$catalogName.dim_nondeterministic"
    withTable(tbl, dim) {
      sql(
        s"CREATE TABLE $tbl (id INT, part INT) USING $v2Source PARTITIONED BY (part) " +
          "TBLPROPERTIES('fully-pushed-filter-attributes' = 'part')")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, $i)")
      }
      sql(s"CREATE TABLE $dim (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (3)")

      val df =
        sql(s"SELECT * FROM $tbl WHERE part = (SELECT max(val) FROM $dim) OR rand() < 0.5")
      assert(df.collect().contains(Row(3, 3)))

      checkEvaluatedAfterScan(df, expected = true)
      checkScalarSubqueryRuntimeFilters(df, expectedCount = 0)
      assert(pushedPredicates(df).isEmpty)
    }
  }

  testGluten("predicate on partly fully pushed filter attributes -> evaluated after the scan") {
    val tbl = s"$catalogName.tbl_partly_pushed"
    val dim = s"$catalogName.dim_partly_pushed"
    withTable(tbl, dim) {
      sql(
        s"CREATE TABLE $tbl (id INT, p1 INT, p2 INT) USING $v2Source " +
          "PARTITIONED BY (p1, p2) " +
          "TBLPROPERTIES('fully-pushed-filter-attributes' = 'p1')")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, 1, 2)")
      }
      sql(s"CREATE TABLE $dim (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (3)")

      val df = sql(s"SELECT * FROM $tbl WHERE p1 + p2 = (SELECT max(val) FROM $dim)")
      checkAnswer(df, (0 until 5).map(i => Row(i, 1, 2)))

      checkScalarSubqueryRuntimeFilters(df)
      val p1 = AttributeReference("p1", IntegerType, nullable = false)()
      val p2 = AttributeReference("p2", IntegerType, nullable = false)()
      checkPushedPredicate(df, EqualTo(Add(p1, p2), Literal(3)))
      checkEvaluatedAfterScan(df, expected = true)
    }
  }

  testGluten("sibling of a nested filter attribute remains evaluated after the scan") {
    val fact = s"$catalogName.fact_nested_sibling"
    val dim = s"$catalogName.dim_nested_sibling"
    withTable(fact, dim) {
      sql(
        s"CREATE TABLE $fact (id INT, s STRUCT<part: INT, other: INT>) USING $v2Source " +
          "PARTITIONED BY (s.part)")
      sql(
        s"INSERT INTO $fact VALUES " +
          "(1, named_struct('part', 1, 'other', 10)), " +
          "(2, named_struct('part', 1, 'other', 20)), " +
          "(3, named_struct('part', 2, 'other', 30))")
      sql(s"CREATE TABLE $dim (value INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (10)")

      val df = sql(s"SELECT id FROM $fact WHERE s.other = (SELECT max(value) FROM $dim)")
      checkAnswer(df, Row(1))
      checkEvaluatedAfterScan(df, expected = true)
    }
  }

  testGluten("filter on column outside filterAttributes -> not pushed") {
    val tbl = s"$catalogName.tbl_restricted_filter_attrs"
    val dim = s"$catalogName.dim_restricted_filter_attrs"
    withTable(tbl, dim) {
      sql(
        s"CREATE TABLE $tbl (id INT, p1 INT, p2 INT) USING $v2Source " +
          "PARTITIONED BY (p1, p2) " +
          "TBLPROPERTIES('filter-attributes' = 'p1')")
      for (i <- 0 until 5) {
        sql(s"INSERT INTO $tbl VALUES ($i, $i, $i)")
      }
      sql(s"CREATE TABLE $dim (val INT) USING $v2Source")
      sql(s"INSERT INTO $dim VALUES (3)")

      val df = sql(s"SELECT * FROM $tbl WHERE p2 = (SELECT max(val) FROM $dim)")
      checkAnswer(df, Row(3, 3, 3))

      val scan = batchScan(df)
      assert(scan.runtimeFilters.isEmpty)
      assert(pushedPredicates(df).isEmpty)
      checkEvaluatedAfterScan(df, expected = true)
      assert(scan.inputPartitions.size === 5)
      assert(scan.filteredPartitions.flatten.size === 5)
    }
  }
}

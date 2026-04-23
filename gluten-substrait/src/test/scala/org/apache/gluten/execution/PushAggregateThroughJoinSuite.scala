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
package org.apache.gluten.execution

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.extension.joinagg.ImplementJoinAggregate
import org.apache.gluten.extension.joinagg.PushAggregateThroughJoin

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

import java.sql.Date

class PushAggregateThroughJoinSuite extends PlanTest with SharedSparkSession {
  private val joinAggregateRule = PushAggregateThroughJoin(spark)
  private val debugMode: Boolean = true

  private case class PushdownCase(inputSql: String, expectedAggCount: Int)

  override protected def sparkConf: SparkConf = {
    // Avoid Janino projection codegen here because Spark 4's QueryExecutionErrors
    // has Arrow-typed methods, which breaks test runs as arrow-vector is excluded.
    super.sparkConf
      .set(SQLConf.CODEGEN_FACTORY_MODE.key, "NO_CODEGEN")
      .set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "false")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    registerSampleTables()
  }

  override def afterAll(): Unit = {
    try {
      spark.catalog.dropTempView("store_sales")
      spark.catalog.dropTempView("date_dim")
      spark.catalog.dropTempView("item")
    } finally {
      super.afterAll()
    }
  }

  private def registerSampleTables(): Unit = {
    import testImplicits._

    Seq(
      (1, 100, 10.0, 1.0, 2.0),
      (1, 100, 12.5, 2.0, 3.0),
      (1, 100, 7.5, 3.0, 1.5),
      (1, 101, 9.0, 2.0, 2.5),
      (2, 100, 3.5, 1.0, 0.5),
      (2, 100, 4.5, 2.0, 1.0),
      (2, 103, 8.0, 4.0, 4.0)
    ).toDF("ss_item_sk", "ss_sold_date_sk", "ss_sales_price", "ss_quantity", "ss_net_profit")
      .createOrReplaceTempView("store_sales")

    Seq(
      (100, 1999, Date.valueOf("2020-01-01")),
      (100, 1999, Date.valueOf("2020-01-01")),
      (101, 2000, Date.valueOf("2020-01-02")),
      (103, 2003, Date.valueOf("2020-01-03"))
    ).toDF("d_date_sk", "d_year", "d_date")
      .createOrReplaceTempView("date_dim")

    Seq((1, "item-one", 1), (1, "item-one", 1), (2, "item-two", 6))
      .toDF("i_item_sk", "i_item_desc", "i_category_id")
      .createOrReplaceTempView("item")
  }

  private def runCaseWithMaxDepth(
      testCase: PushdownCase,
      maxDepth: Int,
      expectedPushCount: Int): Unit = {
    withSQLConf(
      GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_ENABLED.key -> "true",
      GlutenConfig.PUSH_AGGREGATE_THROUGH_JOIN_MAX_DEPTH.key -> maxDepth.toString) {
      val (withoutRuleRows, withoutRuleLogicalPlan, withoutRulePhysicalPlan) =
        withExtraPlanning(Nil, Nil) {
          val df = spark.sql(testCase.inputSql)
          (
            df.collect().toSeq.sortBy(_.toString()),
            df.queryExecution.optimizedPlan,
            finalExecutedPlan(df.queryExecution.executedPlan)
          )
        }

      val (withRuleRows, withRuleLogicalPlan, withRulePhysicalPlan) =
        withExtraPlanning(Seq(joinAggregateRule), Nil) {
          joinAggregateRule.resetSuccessfulPushCount()
          val df = spark.sql(testCase.inputSql)
          val withRuleRows = df.collect().toSeq.sortBy(_.toString())
          val withRulePlan = df.queryExecution.optimizedPlan
          val withRulePhysicalPlan = finalExecutedPlan(df.queryExecution.executedPlan)
          val aggregateNodeCount = withRulePlan.collect { case _: Aggregate => 1 }.size
          val nodesWithMissingInput = withRulePlan.collect {
            case p if p.missingInput.nonEmpty => p
          }

          assert(
            withRulePlan.resolved,
            s"Optimized plan unresolved:\n${withRulePlan.treeString}\n" +
              s"MissingInput=${withRulePlan.missingInput}")
          assert(
            nodesWithMissingInput.isEmpty,
            s"Plan has missing input:\n${nodesWithMissingInput
                .map(_.treeString)
                .mkString("\n---\n")}")
          assert(joinAggregateRule.getSuccessfulPushCount == expectedPushCount)
          assert(aggregateNodeCount == testCase.expectedAggCount)
          (withRuleRows, withRulePlan, withRulePhysicalPlan)
        }

      val (
        withRuleAndStrategyRows,
        withRuleAndStrategyLogicalPlan,
        withRuleAndStrategyPhysicalPlan) =
        withExtraPlanning(Seq(joinAggregateRule), Seq(ImplementJoinAggregate(spark))) {
          joinAggregateRule.resetSuccessfulPushCount()
          val df = spark.sql(testCase.inputSql)
          (
            df.collect().toSeq.sortBy(_.toString()),
            df.queryExecution.optimizedPlan,
            finalExecutedPlan(df.queryExecution.executedPlan)
          )
        }

      if (debugMode) {
        // scalastyle:off println
        println("=== Optimized Plan Before (without PushJoinAggregatePreAggregation) ===")
        println(withoutRuleLogicalPlan.treeString)
        println("=== Optimized Plan After (with PushJoinAggregatePreAggregation) ===")
        println(withRuleLogicalPlan.treeString)
        println("=== Optimized Plan After (with PushJoinAggregatePreAggregation and strategy) ===")
        println(withRuleAndStrategyLogicalPlan.treeString)
        println("=== Physical Plan Before (without PushJoinAggregatePreAggregation) ===")
        println(withoutRulePhysicalPlan.treeString)
        println("=== Physical Plan After (with PushJoinAggregatePreAggregation only) ===")
        println(withRulePhysicalPlan.treeString)
        println("=== Physical Plan After (with PushJoinAggregatePreAggregation and strategy) ===")
        println(withRuleAndStrategyPhysicalPlan.treeString)
        println("=== Result Before (without PushJoinAggregatePreAggregation) ===")
        println(withoutRuleRows.mkString("\n"))
        println("=== Result After (with PushJoinAggregatePreAggregation only) ===")
        println(withRuleRows.mkString("\n"))
        println("=== Result After (with PushJoinAggregatePreAggregation and strategy) ===")
        println(withRuleAndStrategyRows.mkString("\n"))
        // scalastyle:on println
      }

      assertRowsEqual(withRuleRows, withoutRuleRows)
      assertRowsEqual(withRuleAndStrategyRows, withoutRuleRows)
    }
  }

  private def assertRowsEqual(left: Seq[Row], right: Seq[Row]): Unit = {
    assert(left == right, s"Result mismatch:\nleft=$left\nright=$right")
  }

  private def finalExecutedPlan(plan: SparkPlan): SparkPlan = plan match {
    case adaptive: AdaptiveSparkPlanExec =>
      adaptive.executedPlan
    case other =>
      other
  }

  private def withExtraPlanning[T](rules: Seq[Rule[LogicalPlan]], strategies: Seq[SparkStrategy])(
      f: => T): T = {
    val previousOptimizations = spark.experimental.extraOptimizations
    val previousStrategies = spark.experimental.extraStrategies
    try {
      spark.experimental.extraOptimizations = rules
      spark.experimental.extraStrategies = strategies
      f
    } finally {
      spark.experimental.extraOptimizations = previousOptimizations
      spark.experimental.extraStrategies = previousStrategies
    }
  }

  test("pre-aggregate store_sales for both joins with having filter") {
    val pushdownCase = PushdownCase(
      inputSql = """
                   |SELECT
                   |  substring(i_item_desc, 1, 30) AS itemdesc,
                   |  i_item_sk AS item_sk,
                   |  d_date AS solddate,
                   |  count(1) AS cnt
                   |FROM store_sales
                   |JOIN date_dim ON ss_sold_date_sk = d_date_sk
                   |JOIN item ON ss_item_sk = i_item_sk
                   |WHERE d_year IN (1999, 2000, 2001, 2002)
                   |GROUP BY substring(i_item_desc, 1, 30), i_item_sk, d_date
                   |HAVING count(1) > 4
                   |""".stripMargin,
      expectedAggCount = 2
    )
    runCaseWithMaxDepth(pushdownCase, maxDepth = Int.MaxValue, expectedPushCount = 2)
  }

  test("pre-aggregate store_sales for sum") {
    val pushdownCase = PushdownCase(
      inputSql = """
                   |SELECT
                   |  i_item_sk AS item_sk,
                   |  sum(ss_sales_price) AS total_sales_price
                   |FROM store_sales
                   |JOIN item ON ss_item_sk = i_item_sk
                   |GROUP BY i_item_sk
                   |""".stripMargin,
      expectedAggCount = 2
    )
    runCaseWithMaxDepth(pushdownCase, maxDepth = Int.MaxValue, expectedPushCount = 1)
  }

  test("pre-aggregate store_sales for avg") {
    val pushdownCase = PushdownCase(
      inputSql = """
                   |SELECT
                   |  i_item_sk AS item_sk,
                   |  avg(ss_sales_price) AS avg_sales_price
                   |FROM store_sales
                   |JOIN item ON ss_item_sk = i_item_sk
                   |GROUP BY i_item_sk
                   |""".stripMargin,
      expectedAggCount = 2
    )
    runCaseWithMaxDepth(pushdownCase, maxDepth = Int.MaxValue, expectedPushCount = 1)
  }

  test("pre-aggregate store_sales for sum on fact table") {
    val pushdownCase = PushdownCase(
      inputSql = """
                   |SELECT
                   |  ss_sold_date_sk,
                   |  sum(ss_sales_price) AS total_sales_price
                   |FROM store_sales
                   |JOIN item ON ss_item_sk = i_item_sk
                   |GROUP BY ss_sold_date_sk
                   |""".stripMargin,
      expectedAggCount = 2
    )
    runCaseWithMaxDepth(pushdownCase, maxDepth = Int.MaxValue, expectedPushCount = 1)
  }

  test("pre-aggregate store_sales for avg on fact table") {
    val pushdownCase = PushdownCase(
      inputSql = """
                   |SELECT
                   |  ss_sold_date_sk,
                   |  avg(ss_sales_price) AS avg_sales_price
                   |FROM store_sales
                   |JOIN item ON ss_item_sk = i_item_sk
                   |GROUP BY ss_sold_date_sk
                   |""".stripMargin,
      expectedAggCount = 2
    )
    runCaseWithMaxDepth(pushdownCase, maxDepth = Int.MaxValue, expectedPushCount = 1)
  }

  test("pre-aggregate store_sales for sum on three-way join") {
    val pushdownCase = PushdownCase(
      inputSql = """
                   |SELECT
                   |  i_item_desc AS item_desc,
                   |  d_date AS sold_date,
                   |  sum(ss_sales_price) AS total_sales_price
                   |FROM store_sales
                   |JOIN date_dim ON ss_sold_date_sk = d_date_sk
                   |JOIN item ON ss_item_sk = i_item_sk
                   |GROUP BY item_desc, d_date
                   |""".stripMargin,
      expectedAggCount = 2
    )
    runCaseWithMaxDepth(pushdownCase, maxDepth = Int.MaxValue, expectedPushCount = 2)
  }

  test("pre-aggregate store_sales for sum and avg on different fact columns on three-way join") {
    val pushdownCase = PushdownCase(
      inputSql = """
                   |SELECT
                   |  i_item_desc AS item_desc,
                   |  d_date AS sold_date,
                   |  sum(ss_sales_price) AS total_sales_price,
                   |  avg(ss_quantity) AS avg_quantity
                   |FROM store_sales
                   |JOIN date_dim ON ss_sold_date_sk = d_date_sk
                   |JOIN item ON ss_item_sk = i_item_sk
                   |GROUP BY item_desc, d_date
                   |""".stripMargin,
      expectedAggCount = 2
    )
    runCaseWithMaxDepth(pushdownCase, maxDepth = Int.MaxValue, expectedPushCount = 2)
  }

  test("pre-aggregate store_sales for sum and avg on same fact column on three-way join") {
    val pushdownCase = PushdownCase(
      inputSql = """
                   |SELECT
                   |  i_item_desc AS item_desc,
                   |  d_date AS sold_date,
                   |  sum(ss_sales_price) AS total_sales_price,
                   |  avg(ss_sales_price) AS avg_sales_price
                   |FROM store_sales
                   |JOIN date_dim ON ss_sold_date_sk = d_date_sk
                   |JOIN item ON ss_item_sk = i_item_sk
                   |GROUP BY item_desc, d_date
                   |""".stripMargin,
      expectedAggCount = 2
    )
    runCaseWithMaxDepth(pushdownCase, maxDepth = Int.MaxValue, expectedPushCount = 2)
  }

  test("pre-aggregate store_sales by i_item_desc") {
    val pushdownCase = PushdownCase(
      inputSql = """
                   |SELECT
                   |  i_item_desc AS item_desc,
                   |  avg(ss_sales_price) AS avg_sales_price
                   |FROM store_sales
                   |JOIN item ON ss_item_sk = i_item_sk
                   |GROUP BY item_desc
                   |""".stripMargin,
      expectedAggCount = 2
    )
    runCaseWithMaxDepth(pushdownCase, maxDepth = Int.MaxValue, expectedPushCount = 1)
  }

  test("pre-aggregate store_sales by substr(i_item_desc, 3), 3 ways") {
    val pushdownCase = PushdownCase(
      inputSql = """
                   |SELECT
                   |  d_date AS sold_date,
                   |  substr(i_item_desc, 3) AS item_desc,
                   |  avg(ss_sales_price) AS avg_sales_price
                   |FROM store_sales
                   |JOIN date_dim ON ss_sold_date_sk = d_date_sk
                   |JOIN item ON ss_item_sk = i_item_sk
                   |GROUP BY d_date, item_desc
                   |""".stripMargin,
      expectedAggCount = 2
    )
    runCaseWithMaxDepth(pushdownCase, maxDepth = Int.MaxValue, expectedPushCount = 2)
  }

  test("pre-aggregate store_sales for sum with item filter") {
    val pushdownCase = PushdownCase(
      inputSql = """
                   |SELECT
                   |  sum(ss_net_profit) AS profit
                   |FROM store_sales
                   |JOIN item ON ss_item_sk = i_item_sk
                   |WHERE i_category_id IN (1, 2, 3, 4, 5)
                   |""".stripMargin,
      expectedAggCount = 2
    )
    runCaseWithMaxDepth(pushdownCase, maxDepth = Int.MaxValue, expectedPushCount = 1)
  }

  test("pre-aggregate three-way joins independently under union all") {
    val pushdownCase = PushdownCase(
      inputSql =
        """
          |SELECT key, total_sales_price
          |FROM (
          |  SELECT
          |    concat('item-', cast(i_item_sk AS string), '-', cast(d_date_sk AS string)) AS key,
          |    sum(ss_sales_price) AS total_sales_price
          |  FROM store_sales
          |  JOIN date_dim ON ss_sold_date_sk = d_date_sk
          |  JOIN item ON ss_item_sk = i_item_sk
          |  GROUP BY concat('item-', cast(i_item_sk AS string), '-', cast(d_date_sk AS string))
          |
          |  UNION ALL
          |
          |  SELECT
          |    concat('desc-', i_item_desc, '-', cast(d_date_sk AS string)) AS key,
          |    sum(ss_sales_price) AS total_sales_price
          |  FROM store_sales
          |  JOIN date_dim ON ss_sold_date_sk = d_date_sk
          |  JOIN item ON ss_item_sk = i_item_sk
          |  GROUP BY concat('desc-', i_item_desc, '-', cast(d_date_sk AS string))
          |
          |  UNION ALL
          |
          |  SELECT
          |    concat('year-', cast(d_year AS string), '-', cast(i_item_sk AS string)) AS key,
          |    sum(ss_sales_price) AS total_sales_price
          |  FROM store_sales
          |  JOIN date_dim ON ss_sold_date_sk = d_date_sk
          |  JOIN item ON ss_item_sk = i_item_sk
          |  GROUP BY concat('year-', cast(d_year AS string), '-', cast(i_item_sk AS string))
          |)
          |""".stripMargin,
      expectedAggCount = 6
    )
    runCaseWithMaxDepth(pushdownCase, maxDepth = 1, expectedPushCount = 3)
    runCaseWithMaxDepth(pushdownCase, maxDepth = 2, expectedPushCount = 6)
    runCaseWithMaxDepth(pushdownCase, maxDepth = Int.MaxValue, expectedPushCount = 6)
  }

  test("pre-aggregate store_sales for sum on three-way join with maxDepth=1 / maxDepth=2") {
    val pushdownCase = PushdownCase(
      inputSql = """
                   |SELECT
                   |  i_item_desc AS item_desc,
                   |  d_date AS sold_date,
                   |  sum(ss_sales_price) AS total_sales_price
                   |FROM store_sales
                   |JOIN date_dim ON ss_sold_date_sk = d_date_sk
                   |JOIN item ON ss_item_sk = i_item_sk
                   |GROUP BY item_desc, d_date
                   |""".stripMargin,
      expectedAggCount = 2
    )
    runCaseWithMaxDepth(pushdownCase, maxDepth = 1, expectedPushCount = 1)
    runCaseWithMaxDepth(pushdownCase, maxDepth = 2, expectedPushCount = 2)
    runCaseWithMaxDepth(pushdownCase, maxDepth = Int.MaxValue, expectedPushCount = 2)
  }

  test("pre-aggregate with filter inside inner equi-join") {
    val pushdownCase = PushdownCase(
      inputSql = """
                   |SELECT
                   |  i_item_sk AS item_sk,
                   |  sum(ss_sales_price) AS total_sales_price
                   |FROM store_sales
                   |JOIN item ON ss_item_sk = i_item_sk AND ss_quantity > 1
                   |GROUP BY i_item_sk
                   |""".stripMargin,
      expectedAggCount = 2
    )
    runCaseWithMaxDepth(pushdownCase, maxDepth = Int.MaxValue, expectedPushCount = 1)
  }
}

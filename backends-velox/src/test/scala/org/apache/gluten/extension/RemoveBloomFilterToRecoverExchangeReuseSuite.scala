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
import org.apache.gluten.execution.VeloxWholeStageTransformerSuite
import org.apache.gluten.expression.VeloxBloomFilterMightContain

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{FilterExec, ScalarSubquery, SparkPlan, SubqueryExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveExecutionContext, AdaptiveSparkPlanExec}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.types._

class RemoveBloomFilterToRecoverExchangeReuseSuite
  extends VeloxWholeStageTransformerSuite {

  override protected val resourcePath: String = "N/A"
  override protected val fileFormat: String = "N/A"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.optimizer.runtimeFilterSemiJoinReductionEnabled", "true")
      .set("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")
      .set("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold", "1")
      .set("spark.sql.shuffle.partitions", "4")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
  }

  test("baseline is found behind a subquery's own AdaptiveSparkPlanExec") {
    // This is the TPC-DS Q24a shape: the side carrying fewer BFs sits in a scalar subquery that has
    // already been turned into its own AdaptiveSparkPlanExec. That node is a leaf, so the rule only
    // sees the join inputs below it because STEP1 descends into its inputPlan.
    withTable("unit_tab") {
      spark
        .range(0, 40, 1, 2)
        .selectExpr("id", "CAST(id % 5 AS INT) AS grp")
        .write
        .format("parquet")
        .mode("overwrite")
        .saveAsTable("unit_tab")

      val scan = scanOf("SELECT id, grp FROM unit_tab WHERE grp >= 0")
      val idAttr = scan.output.find(_.name == "id").get
      val grpAttr = scan.output.find(_.name == "grp").get
      val probe1 = fakeBloomFilterMightContain(idAttr, seed = 42L)
      val probe2 = fakeBloomFilterMightContain(idAttr, seed = 99L)

      // The subquery side: a join whose inputs carry a single BF, wrapped the way
      // PlanAdaptiveSubqueries wraps a planned subquery.
      val subqueryJoin = joinOf(FilterExec(probe1, scan), FilterExec(probe1, scan), grpAttr)
      val subqueryPlan = AdaptiveSparkPlanExec(
        subqueryJoin,
        AdaptiveExecutionContext(spark, spark.range(1).queryExecution),
        preprocessingRules = Nil,
        isSubquery = true)
      val scalarSubquery =
        ScalarSubquery(SubqueryExec("test-subquery", subqueryPlan), NamedExpression.newExprId)

      // The main query side: both join inputs carry two BFs, so there is no asymmetry to be found
      // within the main tree alone.
      val mainJoin =
        joinOf(
          FilterExec(And(probe1, probe2), scan),
          FilterExec(And(probe1, probe2), scan),
          grpAttr)
      val root: SparkPlan = FilterExec(GreaterThan(idAttr, scalarSubquery), mainJoin)

      val resultPlan = RemoveBloomFilterToRecoverExchangeReuse(spark).apply(root)

      val rewrittenJoin = resultPlan.asInstanceOf[FilterExec].child
      Seq(rewrittenJoin.children.head, rewrittenJoin.children(1)).foreach {
        joinInput =>
          assert(
            countBloomFilterMightContain(joinInput) == 1,
            "each main-query join input should be left with the single BF the subquery side has")
          assert(
            collectBloomFilterExprs(joinInput).forall(exprKey(_) != exprKey(probe2)),
            "the extra BF (seed 99) should have been stripped")
      }
    }
  }

  test("rule strips asymmetric BFs when two join inputs share sig+leaves") {
    withTable("unit_tab") {
      spark
        .range(0, 40, 1, 2)
        .selectExpr("id", "CAST(id % 5 AS INT) AS grp")
        .write
        .format("parquet")
        .mode("overwrite")
        .saveAsTable("unit_tab")

      val executedPlan =
        sql("SELECT id, grp FROM unit_tab WHERE grp >= 0").queryExecution.executedPlan
      val unitTabScan: SparkPlan = executedPlan match {
        case a: AdaptiveSparkPlanExec => a.executedPlan
        case other => other
      }

      val idAttr = unitTabScan.output.find(_.name == "id").get
      val grpAttr = unitTabScan.output.find(_.name == "grp").get

      val probe1 = fakeBloomFilterMightContain(idAttr, seed = 42L)
      val probe2 = fakeBloomFilterMightContain(idAttr, seed = 99L)

      val leftWith2Bfs: SparkPlan = FilterExec(And(probe1, probe2), unitTabScan)
      val rightWith1Bf: SparkPlan = FilterExec(probe1, unitTabScan)

      val joinKeys = Seq(grpAttr)
      val syntheticJoin: SparkPlan = SortMergeJoinExec(
        leftKeys = joinKeys,
        rightKeys = joinKeys,
        joinType = org.apache.spark.sql.catalyst.plans.Inner,
        condition = None,
        left = leftWith2Bfs,
        right = rightWith1Bf
      )

      assert(countBloomFilterMightContain(leftWith2Bfs) == 2)
      assert(countBloomFilterMightContain(rightWith1Bf) == 1)

      val rule = RemoveBloomFilterToRecoverExchangeReuse(spark)
      val resultPlan = rule.apply(syntheticJoin)

      val newLeftBfCount = countBloomFilterMightContain(resultPlan.children.head)
      val newRightBfCount = countBloomFilterMightContain(resultPlan.children(1))

      assert(
        newLeftBfCount == newRightBfCount,
        s"BF counts must match after rule apply. left=$newLeftBfCount right=$newRightBfCount")
      assert(
        newRightBfCount == 1,
        s"Rule should not touch the right side (smallest BF side). Got $newRightBfCount")
      assert(
        collectBloomFilterExprs(resultPlan.children.head)
          .forall(exprKey(_) != exprKey(probe2)),
        "The extra BF (seed 99) should have been stripped from left join input."
      )
    }
  }

  test("rule is a no-op when the switch is off") {
    withTable("unit_tab") {
      spark
        .range(0, 40, 1, 2)
        .selectExpr("id", "CAST(id % 5 AS INT) AS grp")
        .write
        .format("parquet")
        .mode("overwrite")
        .saveAsTable("unit_tab")

      val scan = scanOf("SELECT id, grp FROM unit_tab WHERE grp >= 0")
      val idAttr = scan.output.find(_.name == "id").get
      val grpAttr = scan.output.find(_.name == "grp").get
      val probe1 = fakeBloomFilterMightContain(idAttr, seed = 42L)
      val probe2 = fakeBloomFilterMightContain(idAttr, seed = 99L)

      // The exact asymmetry the rule repairs when it is on: 2 BFs against 1.
      val syntheticJoin =
        joinOf(FilterExec(And(probe1, probe2), scan), FilterExec(probe1, scan), grpAttr)

      withSQLConf(
        VeloxConfig.REMOVE_BLOOM_FILTER_TO_RECOVER_EXCHANGE_REUSE.key -> "false"
      ) {
        val resultPlan = RemoveBloomFilterToRecoverExchangeReuse(spark).apply(syntheticJoin)
        assert(
          resultPlan.fastEquals(syntheticJoin),
          "the plan must be handed back untouched when the rule is disabled")
        assert(countBloomFilterMightContain(resultPlan.children.head) == 2)
        assert(countBloomFilterMightContain(resultPlan.children(1)) == 1)
      }

      withSQLConf(
        VeloxConfig.REMOVE_BLOOM_FILTER_TO_RECOVER_EXCHANGE_REUSE.key -> "true"
      ) {
        val resultPlan = RemoveBloomFilterToRecoverExchangeReuse(spark).apply(syntheticJoin)
        assert(
          countBloomFilterMightContain(resultPlan.children.head) == 1,
          "sanity check: the same plan is rewritten when the rule is enabled")
      }
    }
  }

  test("skip rule without error when join leaves are RDDScanExec (in-memory DataFrames)") {
    val left = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, "a"), Row(2, "b"), Row(3, "c"))),
      new StructType().add("id", IntegerType).add("val", StringType)
    )
    val right = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, "x"), Row(2, "y"), Row(3, "z"))),
      new StructType().add("id", IntegerType).add("val2", StringType)
    )
    left.createOrReplaceTempView("rdd_left")
    right.createOrReplaceTempView("rdd_right")
    try {
      val sqlText =
        """
          |SELECT l.id, l.val, r.val2
          |FROM rdd_left l
          |JOIN rdd_right r ON l.id = r.id
          |WHERE l.id IN (SELECT id FROM rdd_right WHERE id IS NOT NULL)
          |""".stripMargin
      runQueryAndCompare(sqlText)(df => df.collect())
    } finally {
      spark.catalog.dropTempView("rdd_left")
      spark.catalog.dropTempView("rdd_right")
    }
  }

  test("skip rule without error for temp views built from range (no tableIdentifier)") {
    withTempView("anon_view") {
      spark.range(50).selectExpr("id", "id % 7 AS grp").createOrReplaceTempView("anon_view")
      val sqlText =
        """
          |SELECT a.grp, SUM(a.id) AS s
          |FROM anon_view a
          |JOIN anon_view b ON a.grp = b.grp
          |GROUP BY a.grp
          |""".stripMargin
      runQueryAndCompare(sqlText) { _ => }
    }
  }

  /** The physical scan behind `sqlText`, with the AQE wrapper peeled off. */
  private def scanOf(sqlText: String): SparkPlan = {
    sql(sqlText).queryExecution.executedPlan match {
      case a: AdaptiveSparkPlanExec => a.executedPlan
      case other => other
    }
  }

  private def joinOf(left: SparkPlan, right: SparkPlan, joinKey: Attribute): SparkPlan = {
    SortMergeJoinExec(
      leftKeys = Seq(joinKey),
      rightKeys = Seq(joinKey),
      joinType = org.apache.spark.sql.catalyst.plans.Inner,
      condition = None,
      left = left,
      right = right
    )
  }

  private def fakeBloomFilterMightContain(probe: Attribute, seed: Long): Expression = {
    val emptyBf = Literal.create(Array.fill[Byte](32)(0), BinaryType)
    BloomFilterMightContain(emptyBf, XxHash64(Seq(probe, Literal(seed)), seed))
  }

  private def exprKey(bf: Expression): String = bf match {
    case _: BloomFilterMightContain | _: VeloxBloomFilterMightContain =>
      val probe = bf.children(1)
      val seedLit: Option[Long] = probe.collectFirst {
        case XxHash64(_, seed) => seed
        case Literal(v, _) if v != null =>
          v match {
            case l: java.lang.Long => l.longValue()
            case _ => -1L
          }
      }
      s"${probe.canonicalized.toString()}#seed=${seedLit.getOrElse(-1L)}"
    case _ =>
      throw new IllegalArgumentException(s"Not a BF expr: ${bf.getClass.getName}")
  }

  private def collectBloomFilterExprs(plan: SparkPlan): Seq[Expression] =
    plan.expressions.flatMap(_.collect {
      case b @ (_: BloomFilterMightContain | _: VeloxBloomFilterMightContain) => b
    })

  private def countBloomFilterMightContain(plan: SparkPlan): Int = {
    plan.collectWithSubqueries(PartialFunction[
      SparkPlan,
      Seq[Expression]](collectBloomFilterExprs)).map(_.size).sum
  }
}

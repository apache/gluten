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
package org.apache.spark.sql.execution.benchmark.expression

import org.apache.gluten.execution.VeloxWholeStageTransformerSuite

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Complete, Final, Partial}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange.Exchange

import org.scalatest.{DoNotDiscover, SequentialNestedSuiteExecution, Suite, Suites}
import org.scalatest.exceptions.TestCanceledException

import scala.collection.immutable

/** Group catalog correctness tests without sharing Spark sessions between groups. */
class ExpressionEvaluatorSuite
  extends Suites(
    new ExpressionEvaluatorCasesSuite("scalar", Catalog.load()),
    new ExpressionEvaluatorCasesSuite(
      "aggregate",
      Catalog.load(root = "expression-benchmark/aggregate", aggregate = true)
        .filter(_.keyCardinality.forall(_ <= 512)))
  )
  with SequentialNestedSuiteExecution

@DoNotDiscover
private[expression] class ExpressionEvaluatorCasesSuite(
    group: String,
    cases: Seq[Catalog.CaseDef])
  extends VeloxWholeStageTransformerSuite {
  override val invokeBeforeAllAndAfterAllEvenIfNoTestsAreExpected = true
  override def suiteName: String = group
  override def suiteId: String = s"${getClass.getName}:$group"
  override protected val resourcePath: String = "N/A"
  override protected val fileFormat: String = "N/A"
  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.extensions", classOf[ExpressionBenchmarkExtensions].getName)
    .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
    .set("spark.shuffle.compress", "false")

  private val ignoreCompareCases = Set(
    // collect_set output ordering differs between engines; still execute both paths.
    "collect_set/global",
    "collect_set/grouped",
    // Known issue: Spark and Velox HLL estimates differ; exact result comparison fails.
    "approx_count_distinct/projected-count",
    "approx_count_distinct/long-g1024",
    "approx_count_distinct/string-g1024",
    // Known issue: native HLL counts distinct NaN payloads separately instead of one value.
    "approx_count_distinct/nan-payloads"
  )

  private var unsupportedCases = Set.empty[String]

  override def beforeAll(): Unit = {
    super.beforeAll()
    withSQLConf((RunOptions.sqlConf ++ ExpressionEvaluator.aggregateSqlConf): _*) {
      unsupportedCases = cases
        .filterNot {
          scenario =>
            new ExpressionPlan(
              spark,
              scenario,
              Data.Context(
                10000,
                scenario.keyCardinality,
                inputPartitions = scenario.maps.getOrElse(3)),
              Data.compile(scenario)).validate().ok()
        }
        .map(_.id)
        .toSet
    }
  }

  override def tags: Map[String, Set[String]] = super.tags ++
    testNames.filter(unsupportedCases).map(_ -> Set("org.scalatest.Ignore")).toMap

  override val nestedSuites: immutable.IndexedSeq[Suite] = {
    val files = cases.groupBy(_.sourceLocation.file)
    val (single, multiple) = cases.map(_.sourceLocation.file).distinct.map(files)
      .partition(_.size == 1)
    val suites = multiple.map(cases => new FunctionSuite(cases)).toVector
    single.flatten.foreach {
      scenario =>
        test(scenario.id)(verifyCase(scenario, ignoreCompare = ignoreCompareCases(scenario.id)))
    }
    suites
  }

  private def verifyCase(scenario: Catalog.CaseDef, ignoreCompare: Boolean): Unit =
    try {
      verifySupportedCase(scenario, ignoreCompare)
    } catch {
      case canceled: TestCanceledException =>
        fail("A supported case was canceled during execution", canceled)
    }

  private def verifySupportedCase(scenario: Catalog.CaseDef, ignoreCompare: Boolean): Unit =
    withSQLConf((RunOptions.sqlConf ++ ExpressionEvaluator.aggregateSqlConf): _*) {
      val context =
        Data.Context(10000, scenario.keyCardinality, inputPartitions = scenario.maps.getOrElse(3))
      val data = Data.compile(scenario)
      val checkedData = Some(data.copy(row = (context, rowId, local, count) => {
        val row = data.row(context, rowId, local, count)
        assert(
          ExpressionResultChecks.checkResult(row, row, data.inputSchema, false),
          "Invalid logical input")
        row
      }))
      var checked = 0L
      def checkRow(rowId: Long, value: Any): Unit = {
        assert(rowId == checked)
        checked += 1
      }
      withClue(scenario.sourceLocation.toString + " case=" + scenario.id + ": ") {
        ExpressionResultChecks.withCollected[ExpressionEvaluator](
          (vanilla, native) =>
            ExpressionEvaluator(
              spark,
              scenario,
              context,
              1003,
              checkedData,
              Some(vanilla),
              Some(native))) {
          (evaluator, verify) =>
            val expectedModes = if (!evaluator.plan.aggregate) Seq.empty
            else if (scenario.mode.contains("complete")) Seq(Complete)
            else Seq(Final, Partial)
            val expectedExchanges =
              if (evaluator.plan.window || expectedModes == Seq(Final, Partial)) 1 else 0
            Seq(evaluator.plan.vanillaPlan, evaluator.plan.nativePlan).foreach {
              physical =>
                withClue(physical.treeString) {
                  val modes = physical.collect {
                    case aggregate: BaseAggregateExec => aggregate.aggregateExpressions.map(_.mode)
                  }
                  assert(modes == expectedModes.map(Seq(_)))
                  assert(physical.collect { case exchange: Exchange => exchange }.size ==
                    expectedExchanges)
                }
            }
            assert(evaluator.inputRows.size == context.rows)
            if (ignoreCompare) {
              evaluator.runVanilla()
              evaluator.runNative()
            } else {
              verify(checkRow)
              if (!evaluator.plan.aggregate) assert(checked == context.rows)
              else if (evaluator.plan.groupingExpressions.isEmpty) assert(checked == 1)
            }
        }
      }
    }

  @DoNotDiscover
  private class FunctionSuite(cases: Seq[Catalog.CaseDef]) extends SparkFunSuite {
    override def suiteName: String = cases.head.id.takeWhile(_ != '/')
    override def suiteId: String = s"${ExpressionEvaluatorCasesSuite.this.suiteId}:$suiteName"
    // The enclosing group owns the Spark session, thread audit and accumulator cleanup.
    private var ownsFixture = false
    override protected def beforeAll(): Unit = {
      if (spark == null) {
        ownsFixture = true
        ExpressionEvaluatorCasesSuite.this.beforeAll()
      }
    }
    override protected def afterAll(): Unit = {
      if (ownsFixture) {
        try ExpressionEvaluatorCasesSuite.this.afterAll()
        finally ownsFixture = false
      }
    }
    override val invokeBeforeAllAndAfterAllEvenIfNoTestsAreExpected = true
    override def tags: Map[String, Set[String]] = super.tags ++
      cases.filter(c => unsupportedCases(c.id)).map(
        c =>
          c.id.split("/", 2)(1) -> Set("org.scalatest.Ignore")).toMap
    override protected def beforeEach(): Unit = ExpressionEvaluatorCasesSuite.this.beforeEach()
    override protected def afterEach(): Unit = ExpressionEvaluatorCasesSuite.this.afterEach()

    cases.foreach {
      scenario =>
        val name = scenario.id.split("/", 2)(1)
        test(name)(verifyCase(scenario, ignoreCompare = ignoreCompareCases(scenario.id)))
    }
  }
}

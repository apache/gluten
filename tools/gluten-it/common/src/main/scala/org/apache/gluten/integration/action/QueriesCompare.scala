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
package org.apache.gluten.integration.action

import org.apache.gluten.integration.{Query, QueryRunner, Suite}
import org.apache.gluten.integration.QueryRunner.QueryResult
import org.apache.gluten.integration.action.Actions.QuerySelector
import org.apache.gluten.integration.action.QueriesCompare.TestResultLine
import org.apache.gluten.integration.action.TableRender.RowParser.FieldAppender.RowAppender
import org.apache.gluten.integration.stat.RamStat

import org.apache.spark.sql.{SparkSession, TestUtils}

import java.io.PrintStream

case class QueriesCompare(
    queries: QuerySelector,
    explain: Boolean,
    iterations: Int,
    noSessionReuse: Boolean,
    suppressFailureMessages: Boolean)
  extends Action {

  override def execute(suite: Suite): Boolean = {
    val runner: QueryRunner =
      new QueryRunner(suite.dataSource(), suite.dataWritePath())
    val querySet = queries.select(suite)
    val sessionSwitcher = suite.sessionSwitcher

    sessionSwitcher.useSession("baseline", "Run Baseline Queries")
    runner.createTables(suite.tableCreator(), suite.tableAnalyzer(), sessionSwitcher.spark())
    val baselineResults = (0 until iterations).flatMap {
      iteration =>
        querySet.queries.map {
          query =>
            println(s"Running baseline query ${query.id} (iteration $iteration)...")
            try {
              QueriesCompare.runBaselineQuery(
                runner,
                sessionSwitcher.spark(),
                suite.desc(),
                query,
                explain)
            } finally {
              if (noSessionReuse) {
                sessionSwitcher.renewSession()
                runner.createTables(
                  suite.tableCreator(),
                  suite.tableAnalyzer(),
                  sessionSwitcher.spark())
              }
            }
        }
    }.toList

    sessionSwitcher.useSession("test", "Run Test Queries")
    runner.createTables(suite.tableCreator(), suite.tableAnalyzer(), sessionSwitcher.spark())
    val testResults = (0 until iterations).flatMap {
      iteration =>
        querySet.queries.map {
          query =>
            println(s"Running test query ${query.id} (iteration $iteration)...")
            try {
              QueriesCompare.runTestQuery(
                runner,
                sessionSwitcher.spark(),
                suite.desc(),
                query,
                explain)
            } finally {
              if (noSessionReuse) {
                sessionSwitcher.renewSession()
                runner.createTables(
                  suite.tableCreator(),
                  suite.tableAnalyzer(),
                  sessionSwitcher.spark())
              }
            }
        }
    }.toList

    assert(baselineResults.size == testResults.size)

    val results: Seq[TestResultLine] = baselineResults.zip(testResults).map {
      case (b, t) =>
        assert(b.caseId() == t.caseId())
        TestResultLine(b.caseId(), b, t)
    }

    val passedCount = results.count(l => l.testPassed())
    val count = results.count(_ => true)
    val succeededQueries = results.filter(_.testPassed())
    val failedQueries = results.filter(!_.testPassed)

    // RAM stats
    println("Performing GC to collect RAM statistics... ")
    System.gc()
    System.gc()
    printf(
      "RAM statistics: JVM Heap size: %d KiB (total %d KiB), Process RSS: %d KiB\n",
      RamStat.getJvmHeapUsed(),
      RamStat.getJvmHeapTotal(),
      RamStat.getProcessRamUsed()
    )
    println()

    // Write out test report.
    val reportAppender = suite.getReporter().actionAppender(getClass.getSimpleName)
    if (failedQueries.nonEmpty) {
      reportAppender.err.println(s"There are failed queries.")
      if (!suppressFailureMessages) {
        reportAppender.err.println()
        failedQueries.foreach {
          failedQuery =>
            reportAppender.err.println(
              s"Query ${failedQuery.queryId} failed by error: ${failedQuery.error()}")
        }
      }
    }

    reportAppender.out.println()
    reportAppender.out.println("Test report: ")
    reportAppender.out.println()
    reportAppender.out.println("Summary: %d out of %d queries passed.".format(passedCount, count))
    reportAppender.out.println()
    val all = succeededQueries match {
      case Nil => None
      case several =>
        val allExpected = several.map(_.expected).asSuccesses().agg("all expected").get
        val allActual = several.map(_.actual).asSuccesses().agg("all actual").get
        Some(TestResultLine("all", allExpected, allActual))
    }
    QueriesCompare.printResults(reportAppender.out, succeededQueries ++ all)
    reportAppender.out.println()

    if (failedQueries.isEmpty) {
      reportAppender.out.println("No failed queries. ")
      reportAppender.out.println()
    } else {
      reportAppender.err.println(
        "Failed queries (a failed query with correct row count indicates value mismatches): ")
      reportAppender.err.println()
      QueriesCompare.printResults(reportAppender.err, failedQueries)
      reportAppender.err.println()
    }

    if (passedCount != count) {
      return false
    }
    true
  }
}

object QueriesCompare {
  case class TestResultLine(queryId: String, expected: QueryResult, actual: QueryResult) {
    private def tryGetError(): Option[String] = {
      if (!expected.succeeded()) {
        return Some(expected.asFailure().error.toString)
      }
      if (!actual.succeeded()) {
        return Some(actual.asFailure().error.toString)
      }
      TestUtils
        .compareAnswers(
          actual.asSuccess().runResult.rows,
          expected.asSuccess().runResult.rows,
          sort = true)
    }

    private val maybeError = tryGetError()

    def testPassed(): Boolean = maybeError.isEmpty

    def error(): String = maybeError.get
  }

  object TestResultLine {
    implicit object Parser extends TableRender.RowParser[TestResultLine] {
      override def parse(rowAppender: RowAppender, line: TestResultLine): Unit = {
        val inc = rowAppender.incremental()
        inc.next().write(line.queryId)
        inc.next().write(line.testPassed())
        inc.next().write(line.expected.asSuccessOption().map(_.runResult.rows.size))
        inc.next().write(line.actual.asSuccessOption().map(_.runResult.rows.size))
        inc.next().write(line.expected.asSuccessOption().map(_.runResult.planningTimeMillis))
        inc.next().write(line.actual.asSuccessOption().map(_.runResult.planningTimeMillis))
        inc.next().write(line.expected.asSuccessOption().map(_.runResult.executionTimeMillis))
        inc.next().write(line.actual.asSuccessOption().map(_.runResult.executionTimeMillis))

        val speedUp =
          if (line.expected.succeeded() && line.actual.succeeded()) {
            Some(
              ((line.expected.asSuccess().runResult.executionTimeMillis - line.actual
                .asSuccess()
                .runResult
                .executionTimeMillis).toDouble
                / line.actual.asSuccess().runResult.executionTimeMillis) * 100)
          } else None
        inc.next().write(speedUp.map("%.2f%%".format(_)))
      }
    }
  }

  private def printResults(out: PrintStream, results: Seq[TestResultLine]): Unit = {
    import org.apache.gluten.integration.action.TableRender.Field._

    val render = TableRender.create[TestResultLine](
      Leaf("Query ID"),
      Leaf("Passed"),
      Branch("Row Count", List(Leaf("Vanilla"), Leaf("Gluten"))),
      Branch("Planning Time (Millis)", List(Leaf("Vanilla"), Leaf("Gluten"))),
      Branch("Query Time (Millis)", List(Leaf("Vanilla"), Leaf("Gluten"))),
      Leaf("Speedup")
    )

    results.foreach(line => render.appendRow(line))

    render.print(out)
  }

  private def runBaselineQuery(
      runner: QueryRunner,
      session: SparkSession,
      desc: String,
      query: Query,
      explain: Boolean): QueryResult = {
    val testDesc = "Baseline %s [%s]".format(desc, query.id)
    val result = runner.runQuery(session, testDesc, query, explain = explain)
    result
  }

  private def runTestQuery(
      runner: QueryRunner,
      session: SparkSession,
      desc: String,
      query: Query,
      explain: Boolean): QueryResult = {
    val testDesc = "Query %s [%s]".format(desc, query.id)
    val result = runner.runQuery(session, testDesc, query, explain = explain)
    result
  }
}

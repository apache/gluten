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

import org.apache.gluten.integration.{Query, QueryRunner, Suite, TableCreator}
import org.apache.gluten.integration.QueryRunner.QueryResult
import org.apache.gluten.integration.action.Actions.QuerySelector
import org.apache.gluten.integration.action.TableRender.RowParser.FieldAppender.RowAppender
import org.apache.gluten.integration.metrics.{MetricMapper, PlanMetric}
import org.apache.gluten.integration.stat.RamStat

import org.apache.spark.sql.SparkSession

import java.io.PrintStream

case class Queries(
    queries: QuerySelector,
    explain: Boolean,
    iterations: Int,
    randomKillTasks: Boolean,
    noSessionReuse: Boolean,
    suppressFailureMessages: Boolean,
    metricsReporters: Seq[PlanMetric.Reporter])
  extends Action {
  import Queries._

  override def execute(suite: Suite): Boolean = {
    val querySet = queries.select(suite)
    val runner: QueryRunner =
      new QueryRunner(suite.dataSource(), suite.dataWritePath())
    val sessionSwitcher = suite.sessionSwitcher
    sessionSwitcher.useSession("test", "Run Queries")
    runner.createTables(suite.tableCreator(), suite.tableAnalyzer(), sessionSwitcher.spark())
    val results = (0 until iterations).flatMap {
      iteration =>
        println(s"Running tests (iteration $iteration)...")
        querySet.queries.map {
          query =>
            try {
              Queries.runQuery(
                runner,
                suite.tableCreator(),
                sessionSwitcher.spark(),
                query,
                suite.desc(),
                explain,
                suite.getTestMetricMapper(),
                randomKillTasks)
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

    val passedCount = results.count(l => l.queryResult.succeeded())
    val count = results.count(_ => true)
    val succeededQueries = results.filter(_.queryResult.succeeded())
    val failedQueries = results.filter(!_.queryResult.succeeded())

    println()
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
            println(
              s"Query ${failedQuery.queryResult.caseId()} failed by error: ${failedQuery.queryResult.asFailure().error}")
        }
      }
    }

    val sqlMetrics = succeededQueries.flatMap(_.queryResult.asSuccess().runResult.sqlMetrics)
    metricsReporters.foreach {
      r =>
        val report = r.toString(sqlMetrics)
        reportAppender.out.println(report)
        reportAppender.out.println()
    }

    reportAppender.out.println("Test report: ")
    reportAppender.out.println()
    reportAppender.out.println("Summary: %d out of %d queries passed.".format(passedCount, count))
    reportAppender.out.println()
    val all =
      succeededQueries.map(_.queryResult).asSuccesses().agg("all").map(s => TestResultLine(s))
    Queries.printResults(reportAppender.out, succeededQueries ++ all)
    reportAppender.out.println()

    if (failedQueries.isEmpty) {
      reportAppender.out.println("No failed queries. ")
      reportAppender.out.println()
    } else {
      reportAppender.err.println("Failed queries: ")
      reportAppender.err.println()
      Queries.printResults(reportAppender.err, failedQueries)
      reportAppender.err.println()
    }

    if (passedCount != count) {
      return false
    }
    true
  }
}

object Queries {
  case class TestResultLine(queryResult: QueryResult)

  object TestResultLine {
    implicit object Parser extends TableRender.RowParser[TestResultLine] {
      override def parse(rowAppender: RowAppender, line: TestResultLine): Unit = {
        val inc = rowAppender.incremental()
        inc.next().write(line.queryResult.caseId())
        inc.next().write(line.queryResult.succeeded())
        line.queryResult match {
          case QueryRunner.Success(_, runResult) =>
            inc.next().write(runResult.rows.size)
            inc.next().write(runResult.planningTimeMillis)
            inc.next().write(runResult.executionTimeMillis)
          case QueryRunner.Failure(_, error) =>
            inc.next().write(None)
            inc.next().write(None)
            inc.next().write(None)
        }
      }
    }
  }

  private def printResults(out: PrintStream, results: Seq[TestResultLine]): Unit = {
    val render = TableRender.plain[TestResultLine](
      "Query ID",
      "Was Passed",
      "Row Count",
      "Plan Time (Millis)",
      "Query Time (Millis)")

    results.foreach(line => render.appendRow(line))

    render.print(out)
  }

  private def runQuery(
      runner: QueryRunner,
      creator: TableCreator,
      session: SparkSession,
      query: Query,
      desc: String,
      explain: Boolean,
      metricMapper: MetricMapper,
      randomKillTasks: Boolean): TestResultLine = {
    println(s"Running query: ${query.id}...")
    val testDesc = "Query %s [%s]".format(desc, query.id)
    val result =
      runner.runQuery(
        session,
        testDesc,
        query,
        explain = explain,
        sqlMetricMapper = metricMapper,
        randomKillTasks = randomKillTasks)
    TestResultLine(result)
  }
}

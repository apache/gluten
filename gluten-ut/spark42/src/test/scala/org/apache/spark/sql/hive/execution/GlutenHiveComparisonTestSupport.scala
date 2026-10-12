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
package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.execution.HiveResult.hiveResultString
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.hive.test.{TestHive, TestHiveQueryExecution}

import java.io.{File, FileOutputStream}
import java.nio.file.Files
import java.util
import java.util.Locale

import scala.util.control.NonFatal

/**
 * Mostly copied from Spark's [[HiveComparisonTest]]. This support reuses [[GlutenTestHiveTables]]
 * for TestHive table registrations and customizes [[createQueryTest]] so `../../data` resolves to
 * workspace-backed Hive test resources.
 */
trait GlutenHiveComparisonTestSupport extends GlutenHiveResourcePathSupport {
  this: HiveComparisonTest =>

  private val testDataPath: String = {
    hiveResourcePath("data").toAbsolutePath.normalize.toUri.getPath
      .stripSuffix("/")
  }

  override def createQueryTest(
      testCaseName: String,
      sql: String,
      reset: Boolean = true,
      tryWithoutResettingFirst: Boolean = false,
      skip: Boolean = false): Unit = {
    // testCaseName must not contain ':', which is not allowed to appear in a filename of Windows
    assert(!testCaseName.contains(":"))

    // If test sharding is enable, skip tests that are not in the correct shard.
    shardInfo.foreach {
      case (shardId, numShards) if testCaseName.hashCode % numShards != shardId => return
      case (shardId, _) => logDebug(s"Shard $shardId includes test '$testCaseName'")
    }

    // Skip tests found in directories specified by user.
    skipDirectories
      .map(new File(_, testCaseName))
      .filter(_.exists)
      .foreach(_ => return)

    // If runonlytests is set, skip this test unless we find a file in one of the specified
    // directories.
    val runIndicators =
      runOnlyDirectories
        .map(new File(_, testCaseName))
        .filter(_.exists)
    if (runOnlyDirectories.nonEmpty && runIndicators.isEmpty) {
      logDebug(
        s"Skipping test '$testCaseName' not found in ${runOnlyDirectories.map(_.getCanonicalPath)}")
      return
    }

    test(testCaseName) {
      assume(!skip)
      logDebug(s"=== HIVE TEST: $testCaseName ===")

      val sqlWithoutComment =
        sql.split("\n").filterNot(l => l.matches("--.*(?<=[^\\\\]);")).mkString("\n")
      val allQueries =
        sqlWithoutComment.split("(?<=[^\\\\]);").map(_.trim).filterNot(q => q == "").toSeq

      // TODO: DOCUMENT UNSUPPORTED
      val queryList =
        allQueries
          // In hive, setting the hive.outerjoin.supports.filters flag to "false" essentially tells
          // the system to return the wrong answer. Since we have no intention of mirroring their
          // previously broken behavior we simply filter out changes to this setting.
          .filterNot(_.contains("hive.outerjoin.supports.filters"))
          .filterNot(_.contains("hive.exec.post.hooks"))

      if (allQueries != queryList) {
        logWarning(s"Simplifications made on unsupported operations for test $testCaseName")
      }

      lazy val consoleTestCase = {
        val quotes = "\"\"\""
        queryList.zipWithIndex
          .map {
            case (query, i) =>
              s"""val q$i = sql($quotes$query$quotes); q$i.collect()"""
          }
          .mkString("\n== Console version of this test ==\n", "\n", "\n")
      }

      def doTest(reset: Boolean, isSpeculative: Boolean = false): Unit = {
        // Clear old output for this testcase.
        outputDirectories.map(new File(_, testCaseName)).filter(_.exists()).foreach(_.delete())

        if (reset) {
          TestHive.reset()
        }

        // Register workspace-backed table definitions before lazy TestHive auto-loading kicks in.
        GlutenTestHiveTables.registerHiveQTestUtilsTables(hiveTestResourceDir)

        // Many tests drop indexes on src and srcpart at the beginning, so we need to load those
        // tables here. Since DROP INDEX DDL is just passed to Hive, it bypasses the analyzer and
        // thus the tables referenced in those DDL commands cannot be extracted for use by our
        // test table auto-loading mechanism. In addition, the tests which use the SHOW TABLES
        // command expect these tables to exist.
        val hasShowTableCommand =
          queryList.exists(_.toLowerCase(Locale.ROOT).contains("show tables"))
        for (table <- Seq("src", "srcpart")) {
          val hasMatchingQuery = queryList.exists {
            query =>
              val normalizedQuery = query.toLowerCase(Locale.ROOT).stripSuffix(";")
              normalizedQuery.endsWith(table) ||
              normalizedQuery.contains(s"from $table") ||
              normalizedQuery.contains(s"from default.$table")
          }
          if (hasShowTableCommand || hasMatchingQuery) {
            TestHive.loadTestTable(table)
          }
        }

        val hiveCacheFiles = queryList.zipWithIndex.map {
          case (queryString, i) =>
            val cachedAnswerName = s"$testCaseName-$i-${getMd5(queryString)}"
            new File(answerCache, cachedAnswerName)
        }

        val hiveCachedResults = hiveCacheFiles
          .flatMap {
            cachedAnswerFile =>
              logDebug(s"Looking for cached answer file $cachedAnswerFile.")
              if (cachedAnswerFile.exists) {
                Some(Files.readString(cachedAnswerFile.toPath))
              } else {
                logDebug(s"File $cachedAnswerFile not found")
                None
              }
          }
          .map {
            case "" => Nil
            case "\n" => Seq("")
            case other => other.split("\n").toSeq
          }

        val hiveResults: Seq[Seq[String]] =
          if (hiveCachedResults.size == queryList.size) {
            logInfo(s"Using answer cache for test: $testCaseName")
            hiveCachedResults
          } else {
            throw new UnsupportedOperationException(
              "Cannot find result file for test case: " + testCaseName)
          }

        // Run w/ catalyst
        val catalystResults = queryList.zip(hiveResults).map {
          case (queryString, hive) =>
            val query = new TestHiveQueryExecution(queryString.replace("../../data", testDataPath))
            def getResult(): Seq[String] = {
              SQLExecution.withNewExecutionId(query)(hiveResultString(query.executedPlan))
            }
            try { (query, prepareAnswer(query, getResult())) }
            catch {
              case e: Throwable =>
                val errorMessage =
                  s"""
                     |Failed to execute query using catalyst:
                     |Error: ${e.getMessage}
                     |${stackTraceToString(e)}
                     |$queryString
                     |$query
                     |== HIVE - ${hive.size} row(s) ==
                     |${hive.mkString("\n")}
                 """.stripMargin
                stringToFile(
                  new File(failedDirectory, testCaseName),
                  errorMessage + consoleTestCase)
                fail(errorMessage)
            }
        }

        queryList.lazyZip(hiveResults).lazyZip(catalystResults).foreach {
          case (query, hive, (hiveQuery, catalyst)) =>
            // Check that the results match unless its an EXPLAIN query.
            val preparedHive = prepareAnswer(hiveQuery, hive)

            // We will ignore the ExplainCommand, ShowFunctions, DescribeFunction
            if (
              (!hiveQuery.logical.isInstanceOf[ExplainCommand]) &&
              (!hiveQuery.logical.isInstanceOf[ShowFunctions]) &&
              (!hiveQuery.logical.isInstanceOf[DescribeFunction]) &&
              (!hiveQuery.logical.isInstanceOf[DescribeCommandBase]) &&
              (!hiveQuery.logical.isInstanceOf[DescribeRelation]) &&
              (!hiveQuery.logical.isInstanceOf[DescribeColumn]) &&
              preparedHive != catalyst
            ) {

              val hivePrintOut = s"== HIVE - ${preparedHive.size} row(s) ==" +: preparedHive
              val catalystPrintOut = s"== CATALYST - ${catalyst.size} row(s) ==" +: catalyst

              val resultComparison = sideBySide(hivePrintOut, catalystPrintOut).mkString("\n")

              if (recomputeCache) {
                logWarning(s"Clearing cache files for failed test $testCaseName")
                hiveCacheFiles.foreach(_.delete())
              }

              // If this query is reading other tables that were created during this test run
              // also print out the query plans and results for those.
              val computedTablesMessages: String =
                try {
                  val tablesRead =
                    new TestHiveQueryExecution(query).executedPlan.collect {
                      case ts: HiveTableScanExec => ts.relation.tableMeta.identifier
                    }.toSet

                  TestHive.reset()
                  val executions = queryList.map(new TestHiveQueryExecution(_))
                  executions.foreach(_.toRdd)
                  val tablesGenerated = queryList.zip(executions).flatMap {
                    case (q, e) =>
                      e.analyzed.collect {
                        case i: InsertIntoHiveTable if tablesRead.contains(i.table.identifier) =>
                          (q, e, i)
                      }
                  }

                  tablesGenerated
                    .map {
                      case (hiveql, execution, insert) =>
                        val rdd =
                          Dataset.ofRows(TestHive.sparkSession, insert.query).queryExecution.toRdd
                        s"""
                           |=== Generated Table ===
                           |$hiveql
                           |$execution
                           |== Results ==
                           |${rdd.collect().mkString("\n")}
                   """.stripMargin
                    }
                    .mkString("\n")

                } catch {
                  case NonFatal(e) =>
                    logError("Failed to compute generated tables", e)
                    s"Couldn't compute dependent tables: $e"
                }

              val errorMessage =
                s"""
                   |Results do not match for $testCaseName:
                   |$hiveQuery\n${hiveQuery.analyzed.output.map(_.name).mkString("\t")}
                   |$resultComparison
                   |$computedTablesMessages
                 """.stripMargin

              stringToFile(new File(wrongDirectory, testCaseName), errorMessage + consoleTestCase)
              if (isSpeculative && !reset) {
                fail("Failed on first run; retrying")
              } else {
                fail(errorMessage)
              }
            }
        }

        // Touch passed file.
        new FileOutputStream(new File(passedDirectory, testCaseName)).close()
      }

      val canSpeculativelyTryWithoutReset: Boolean = {
        val excludedSubstrings = Seq("into table", "create table", "drop index")
        !queryList.map(_.toLowerCase(Locale.ROOT)).exists {
          query => excludedSubstrings.exists(s => query.contains(s))
        }
      }

      val savedSettings = new util.HashMap[String, String]
      savedSettings.putAll(TestHive.conf.settings)
      try {
        try {
          if (tryWithoutResettingFirst && canSpeculativelyTryWithoutReset) {
            doTest(reset = false, isSpeculative = true)
          } else {
            doTest(reset)
          }
        } catch {
          case tf: org.scalatest.exceptions.TestFailedException =>
            if (tryWithoutResettingFirst && canSpeculativelyTryWithoutReset) {
              logWarning("Test failed without reset(); retrying with reset()")
              doTest(reset = true)
            } else {
              throw tf
            }
        }
      } catch {
        case tf: org.scalatest.exceptions.TestFailedException => throw tf
      } finally {
        TestHive.conf.settings.clear()
        TestHive.conf.settings.putAll(savedSettings)
      }
    }
  }
}

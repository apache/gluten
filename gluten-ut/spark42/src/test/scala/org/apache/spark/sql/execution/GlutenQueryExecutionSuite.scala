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
package org.apache.spark.sql.execution

import org.apache.spark.sql.GlutenSQLTestsBaseTrait
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.LoggerContext

import scala.io.Source

class GlutenQueryExecutionSuite extends QueryExecutionSuite with GlutenSQLTestsBaseTrait {

  override def checkDumpedPlans(path: String, expected: Int): Unit =
    Utils.tryWithResource(Source.fromFile(path)) {
      source =>
        assert(
          source.getLines.toList
            .takeWhile(_ != "== Whole Stage Codegen ==")
            .map(_.replaceAll("#\\d+", "#x")) == List(
            "== Parsed Logical Plan ==",
            s"Range (0, $expected, step=1, splits=Some(2))",
            "",
            "== Analyzed Logical Plan ==",
            "id: bigint",
            s"Range (0, $expected, step=1, splits=Some(2))",
            "",
            "== Optimized Logical Plan ==",
            s"Range (0, $expected, step=1, splits=Some(2))",
            "",
            "== Physical Plan ==",
            "*(1) ColumnarToRow",
            s"+- ColumnarRange 0, $expected, 1, 2, $expected, [id#xL]",
            ""
          ))
    }

  testGluten("dumping query execution info to a file - explainMode=formatted") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath + "/plans.txt"
        val df = spark.range(0, 10)
        df.queryExecution.debug.toFile(path, explainMode = Option("formatted"))
        val lines = Utils.tryWithResource(Source.fromFile(path))(_.getLines().toList)
        assert(
          lines
            .takeWhile(_ != "== Whole Stage Codegen ==")
            .map(_.replaceAll("#\\d+", "#x")) == List(
            "== Physical Plan ==",
            "* ColumnarToRow (2)",
            "+- ColumnarRange (1)",
            "",
            "",
            "(1) ColumnarRange",
            "Output [1]: [id#xL]",
            "Arguments: Range (0, 10, step=1, splits=Some(2))",
            "",
            "(2) ColumnarToRow [codegen id : 1]",
            "Input [1]: [id#xL]",
            "",
            ""
          ))
    }
  }

  testGluten("Logging plan changes for execution") {
    val ctx = LogManager.getContext(false).asInstanceOf[LoggerContext]
    val config = ctx.getConfiguration
    val loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME)
    loggerConfig.setLevel(Level.INFO)
    ctx.updateLoggers()

    val testAppender = new LogAppender("plan changes")
    withLogAppender(testAppender) {
      withSQLConf(
        SQLConf.PLAN_CHANGE_LOG_LEVEL.key -> "INFO"
      ) {
        spark.range(1).groupBy("id").count().queryExecution.executedPlan
      }
    }
    Seq("=== Applying Rule org.apache.spark.sql.execution", "=== Result of Batch Preparations ===")
      .foreach {
        expectedMsg =>
          assert(
            testAppender.loggingEvents.exists(
              _.getMessage.getFormattedMessage.contains(expectedMsg)
            )
          )
      }
  }
}

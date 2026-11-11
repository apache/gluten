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
package org.apache.gluten.utils.bolt

import org.apache.spark.sql.GlutenTestConstants.GLUTEN_TEST

import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Paths}

class BoltTestSettingsSuite extends AnyFunSuite {
  private val settings = new BoltTestSettings

  test("full nightly selects common Spark regressions and native plan rewrites") {
    val cases = Seq(
      "org.apache.spark.sql.GlutenDataFrameSuite" -> "repartitionByRange",
      "org.apache.spark.sql.GlutenDataFrameAggregateSuite" -> "count",
      "org.apache.spark.sql.GlutenJoinSuite" -> "test case sensitive for BHJ",
      "org.apache.spark.sql.execution.GlutenWholeStageCodegenSuite" ->
        "range/filter should be combined"
    )
    cases.foreach {
      case (suite, name) => assert(settings.shouldRun(suite, GLUTEN_TEST + name), suite)
    }
    // The native WholeStage rewrite replaces this Spark-only plan assertion.
    assert(!settings.shouldRun(
      "org.apache.spark.sql.execution.GlutenWholeStageCodegenSuite",
      "range/filter should be combined"))
    assert(settings.shouldRun(
      "org.apache.spark.sql.execution.GlutenLogicalPlanTagInSparkPlanSuite",
      "Filter and Project"))
  }

  test("selection rejects unavailable and backend-specific suites") {
    assert(!settings.shouldRun("org.apache.spark.sql.UnknownSuite", "example"))
    assert(!settings.shouldRun(
      "org.apache.spark.sql.execution.adaptive.velox.VeloxAdaptiveQueryExecSuite",
      "example"))
    assert(!settings.shouldRun("org.apache.spark.sql.GlutenTPCHPlanStabilitySuite", "example"))
  }

  test("extended SQL selects joins aggregates subqueries and windows with Spark goldens") {
    val sql = settings.getSQLQueryTestSettings
    val required = Set(
      "inner-join.sql",
      "outer-join.sql",
      "group-by.sql",
      "subquery/scalar-subquery/scalar-subquery-select.sql",
      "postgreSQL/window_part1.sql",
      "bitwise.sql",
      "count.sql",
      "conditional-functions.sql",
      "decimalArithmeticOperations.sql",
      "udf/udf-group-by.sql - Scala UDF"
    )
    assert(required.subsetOf(sql.getSupportedSQLQueryTests))
    assert(!sql.getSupportedSQLQueryTests.exists(_.startsWith("ansi/")))
    assert(sql.getOverwriteSQLQueryTests.isEmpty)
    assert(!sql.getResourceFilePath.contains("backends-velox"))
  }

  test("every selected SQL case has a release input and golden file") {
    val sql = settings.getSQLQueryTestSettings
    val root = Paths.get(sql.getResourceFilePath)
    sql.getSupportedSQLQueryTests.foreach {
      name =>
        val input = name.split(" - ")(0)
        assert(Files.isRegularFile(root.resolve("inputs").resolve(input)), name)
        assert(Files.isRegularFile(root.resolve("results").resolve(input + ".out")), name)
    }
  }
}

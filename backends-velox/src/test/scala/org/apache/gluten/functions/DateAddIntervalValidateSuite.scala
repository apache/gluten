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
package org.apache.gluten.functions

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.ProjectExecTransformer

import org.apache.spark.SparkConf
import org.apache.spark.SparkException
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.internal.SQLConf

class DateAddIntervalValidateSuite extends FunctionsValidateSuite {

  disableFallbackCheck

  import testImplicits._

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
      .set(SQLConf.ANSI_ENABLED.key, "true")
      // A CalendarInterval literal, needed to reach DateAddInterval at all, only parses
      // under the legacy config; the ANSI interval types take every other `INTERVAL` syntax.
      .set("spark.sql.legacy.interval.enabled", "true")
  }

  private def withDateTable(dates: Option[java.sql.Date]*)(f: => Unit): Unit = {
    withTempPath {
      path =>
        dates.toDF("d").write.parquet(path.getCanonicalPath)
        withTempView("dates_tab") {
          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("dates_tab")
          f
        }
    }
  }

  // A date near an int32 day-count bound, built via date_add instead of a huge year string.
  private def withBoundaryDateTable(days: Int)(f: => Unit): Unit = {
    // Parquet's date/timestamp rebasing can otherwise rewrite dates this far from the epoch.
    withSQLConf(
      SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> "CORRECTED",
      SQLConf.PARQUET_REBASE_MODE_IN_READ.key -> "CORRECTED") {
      withTempPath {
        path =>
          sql(s"SELECT date_add(DATE'1970-01-01', $days) AS d")
            .write
            .parquet(path.getCanonicalPath)
          withTempView("boundary_tab") {
            spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("boundary_tab")
            f
          }
      }
    }
  }

  // Asserts fallback and a result matching vanilla Spark. Fails if it throws instead.
  private def checkFallsBackSucceeding(sqlStr: String): Array[Row] = {
    val df = sql(sqlStr)
    checkGlutenPlanCount[ProjectExecTransformer](df, 0)
    checkSparkPlan[ProjectExec](df)
    val rows = df.collect()
    // withSQLConf returns Unit before Spark 4.0, so the result is captured in a var.
    var vanilla: Array[Row] = null
    withSQLConf(vanillaSparkConfs(): _*) {
      vanilla = sql(sqlStr).collect()
    }
    assert(rows.sameElements(vanilla))
    rows
  }

  // Asserts fallback and that its error, and vanilla Spark's, both contain `expectedMessage`.
  private def checkFallsBackThrowing(sqlStr: String, expectedMessage: String): Unit = {
    val df = sql(sqlStr)
    checkGlutenPlanCount[ProjectExecTransformer](df, 0)
    checkSparkPlan[ProjectExec](df)
    val fallbackError = intercept[SparkException](df.collect())
    assert(fallbackError.getMessage.contains(expectedMessage), fallbackError.getMessage)
    val vanillaError = intercept[SparkException] {
      withSQLConf(vanillaSparkConfs(): _*) {
        sql(sqlStr).collect()
      }
    }
    assert(vanillaError.getMessage.contains(expectedMessage), vanillaError.getMessage)
  }

  private def checkMonthsOverflowThrowsNatively(query: String): Unit = {
    for (ansiEnabled <- Seq("true", "false")) {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled) {
        val df = sql(query)
        checkGlutenPlan[ProjectExecTransformer](df)
        val nativeError = intercept[SparkException](df.collect())
        assert(
          nativeError.getMessage.contains("Integer overflow in add_months"),
          nativeError.getMessage)
        withSQLConf(vanillaSparkConfs(): _*) {
          val vanillaError = intercept[SparkException](sql(query).collect())
          assert(vanillaError.getMessage.contains("integer overflow"), vanillaError.getMessage)
        }
      }
    }
  }

  test("months-only interval offloads") {
    withDateTable(Some(java.sql.Date.valueOf("2024-01-15"))) {
      for (ansiEnabled <- Seq("true", "false")) {
        withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled) {
          for (months <- Seq(3, -3, 0)) {
            runQueryAndCompare(s"SELECT d + INTERVAL $months MONTH FROM dates_tab") {
              checkGlutenPlan[ProjectExecTransformer]
            }
          }
        }
      }
    }
  }

  test("month-end clamping") {
    withDateTable(Some(java.sql.Date.valueOf("2024-01-31"))) {
      runQueryAndCompare("SELECT d + INTERVAL 1 MONTH FROM dates_tab") {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }

  test("leap year") {
    // 2023-01-31 + 13 months lands on Feb 2024, a leap year.
    withDateTable(Some(java.sql.Date.valueOf("2023-01-31"))) {
      runQueryAndCompare("SELECT d + INTERVAL 13 MONTH FROM dates_tab") {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }

  test("NULL date") {
    withDateTable(None) {
      runQueryAndCompare("SELECT d + INTERVAL 1 MONTH FROM dates_tab") {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }

  test("months-only overflow throws natively") {
    withBoundaryDateTable(2147483646) {
      checkMonthsOverflowThrowsNatively("SELECT d + INTERVAL 100000000 MONTH FROM boundary_tab")
    }
  }

  // Lower-bound counterpart: a large negative months value overflows the same way.
  test("months-only overflow at the lower bound") {
    withBoundaryDateTable(-2147483647) {
      checkMonthsOverflowThrowsNatively("SELECT d + INTERVAL -70555327 MONTH FROM boundary_tab")
    }
  }

  test("nonzero days falls back") {
    withDateTable(Some(java.sql.Date.valueOf("2024-01-15"))) {
      val query = "SELECT d + INTERVAL 1 MONTH 2 DAY FROM dates_tab"
      checkFallsBackSucceeding(query)
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        checkFallsBackSucceeding(query)
      }
    }
  }

  test("nonzero microseconds falls back") {
    withDateTable(Some(java.sql.Date.valueOf("2024-01-15"))) {
      val query = "SELECT d + INTERVAL 1 HOUR FROM dates_tab"
      // Error text differs by Spark version; "microseconds" is common to both.
      checkFallsBackThrowing(query, "microseconds")
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        checkFallsBackSucceeding(query)
      }
    }
  }

  test("non-literal interval falls back") {
    withTempPath {
      path =>
        Seq((java.sql.Date.valueOf("2024-01-15"), 2))
          .toDF("d", "m")
          .write
          .parquet(path.getCanonicalPath)
        withTempView("months_tab") {
          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("months_tab")
          checkFallsBackSucceeding(
            "SELECT d + make_interval(0, m, 0, 0, 0, 0, 0) FROM months_tab")
        }
    }
  }

  // ConstantFolding is excluded in this suite, so `d - INTERVAL ...`'s UnaryMinus is never
  // folded into a Literal; the converter, which only matches a bare Literal, falls back.
  test("subtraction falls back") {
    withDateTable(Some(java.sql.Date.valueOf("2024-03-15"))) {
      checkFallsBackSucceeding("SELECT d - INTERVAL 1 MONTH FROM dates_tab")
    }
  }

  // Counterexample 1: AddMonths alone would reject this intermediate, even though adding the
  // days brings Spark's real result back in range. Must fall back and match Spark exactly.
  test("overflow counterexample 1") {
    withDateTable(Some(java.sql.Date.valueOf("1970-01-01"))) {
      val rows = checkFallsBackSucceeding(
        "SELECT unix_date(d + INTERVAL 70555327 MONTH -26 DAY) FROM dates_tab")
      assert(rows(0).getInt(0) == 2147483642)
    }
  }

  // Counterexample 2: DateAdd's raw, unchecked addition would silently wrap where
  // DateAddInterval throws. Must fall back and throw, not return a wrapped wrong date.
  test("overflow counterexample 2") {
    withDateTable(Some(java.sql.Date.valueOf("1970-01-02"))) {
      checkFallsBackThrowing(
        "SELECT d + INTERVAL 2147483647 DAY FROM dates_tab",
        "integer overflow")
    }
  }
}

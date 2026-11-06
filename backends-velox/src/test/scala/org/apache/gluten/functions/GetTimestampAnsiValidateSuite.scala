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

import org.apache.spark.SPARK_VERSION
import org.apache.spark.SparkConf
import org.apache.spark.SparkException
import org.apache.spark.sql.Row
import org.apache.spark.sql.internal.SQLConf

import scala.util.{Failure, Success, Try}

// Covers GetTimestamp (to_date/to_timestamp with a format, and their try_ variants) under the
// checked_get_timestamp / get_timestamp split -- see
// VeloxSparkPlanExecApi.genGetTimestampTransformer.
class GetTimestampAnsiValidateSuite extends FunctionsValidateSuite {

  disableFallbackCheck

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      // Without this, Gluten falls back to Spark whenever ANSI is on, and every assertion below
      // would silently exercise the fallback path instead of the native one.
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
      .set(SQLConf.ANSI_ENABLED.key, "true")
  }

  // try_to_date only exists from Spark 4.1 (TryToDateExpressionBuilder); try_to_timestamp and
  // to_timestamp_ntz need no such guard, confirmed back to 3.4.
  private def tryToDateSupported: Boolean = {
    val parts = SPARK_VERSION.split("\\.")
    val major = parts(0).toInt
    val minor = if (parts.length > 1) parts(1).toInt else 0
    major > 4 || (major == 4 && minor >= 1)
  }

  // Writes `rows` to a real parquet file and reads it back, so `str`/`fmt` are genuine
  // row-dependent column values rather than literals Spark could constant-fold away.
  private def withTimestampStrTable(rows: (String, String)*)(f: => Unit): Unit = {
    withTempPath {
      path =>
        val values = rows
          .map {
            case (str, fmt) =>
              val strSql = if (str == null) "CAST(NULL AS STRING)" else s"'$str'"
              val fmtSql = if (fmt == null) "CAST(NULL AS STRING)" else s"'$fmt'"
              s"($strSql, $fmtSql)"
          }
          .mkString(", ")
        sql(s"SELECT * FROM VALUES $values AS t(str, fmt)").write.parquet(path.getCanonicalPath)
        withTempView("ts_str_tab") {
          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("ts_str_tab")
          f
        }
    }
  }

  // Checks the plan is native *before* the throw: a plan that fell back to Spark would also
  // throw under ANSI on, for an unrelated reason, so checking only after collect() wouldn't
  // prove anything. Also checks the message and that vanilla Spark rejects the same input.
  private def checkNativeThrows(sqlStr: String, messageContains: String): Unit = {
    val df = sql(sqlStr)
    checkGlutenPlan[ProjectExecTransformer](df)
    val nativeError = intercept[SparkException](df.collect())
    assert(nativeError.getMessage.contains(messageContains), nativeError.getMessage)
    withSQLConf(vanillaSparkConfs(): _*) {
      intercept[Exception](sql(sqlStr).collect())
    }
  }

  // Checks the plan fell back to Spark (no signature is registered for this case) and that the
  // result or error still matches vanilla Spark.
  private def checkFallsBackMatchingVanilla(sqlStr: String): Unit = {
    val df = sql(sqlStr)
    assert(!df.queryExecution.executedPlan.exists(_.isInstanceOf[ProjectExecTransformer]))
    Try(df.collect()) match {
      case Success(rows) =>
        // withSQLConf returns Unit before Spark 4.0, so the result is captured in a var.
        var vanilla: Array[Row] = null
        withSQLConf(vanillaSparkConfs(): _*) {
          vanilla = sql(sqlStr).collect()
        }
        assert(rows.sameElements(vanilla))
      case Failure(_) =>
        withSQLConf(vanillaSparkConfs(): _*) {
          intercept[Exception](sql(sqlStr).collect())
        }
    }
  }

  test("to_timestamp ansi on: valid input, constant and column format") {
    withTimestampStrTable(("2024-01-01 10:30:00", "yyyy-MM-dd HH:mm:ss")) {
      runQueryAndCompare("SELECT to_timestamp(str, 'yyyy-MM-dd HH:mm:ss') FROM ts_str_tab") {
        checkGlutenPlan[ProjectExecTransformer]
      }
      runQueryAndCompare("SELECT to_timestamp(str, fmt) FROM ts_str_tab") {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }

  test("to_timestamp ansi on: invalid data throws, constant and column format") {
    withTimestampStrTable(("not a date", "yyyy-MM-dd HH:mm:ss")) {
      checkNativeThrows(
        "SELECT to_timestamp(str, 'yyyy-MM-dd HH:mm:ss') FROM ts_str_tab",
        "Invalid date format")
      checkNativeThrows("SELECT to_timestamp(str, fmt) FROM ts_str_tab", "Invalid date format")
    }
  }

  test("to_timestamp ansi off: invalid data returns null natively") {
    withTimestampStrTable(("not a date", "yyyy-MM-dd HH:mm:ss")) {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        runQueryAndCompare("SELECT to_timestamp(str, fmt) FROM ts_str_tab") {
          checkGlutenPlan[ProjectExecTransformer]
        }
        assert(sql("SELECT to_timestamp(str, fmt) FROM ts_str_tab").collect()(0).isNullAt(0))
      }
    }
  }

  test("to_timestamp: invalid format pattern throws under both ANSI settings") {
    // 'b' is not a supported Joda specifier: a format-pattern failure, never gated by ANSI.
    withTimestampStrTable(("2024-01-01", "yyyy-MM-dd bb")) {
      checkNativeThrows(
        "SELECT to_timestamp(str, fmt) FROM ts_str_tab",
        "Specifier b is not supported")
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        checkNativeThrows(
          "SELECT to_timestamp(str, fmt) FROM ts_str_tab",
          "Specifier b is not supported")
      }
    }
  }

  test("to_timestamp ansi on: NULL string and NULL format each stay NULL, no error") {
    withTimestampStrTable((null, "yyyy-MM-dd"), ("2024-01-01", null)) {
      runQueryAndCompare("SELECT to_timestamp(str, fmt) FROM ts_str_tab") {
        checkGlutenPlan[ProjectExecTransformer]
      }
      val rows = sql("SELECT to_timestamp(str, fmt) FROM ts_str_tab ORDER BY str").collect()
      assert(rows(0).isNullAt(0) && rows(1).isNullAt(0))
    }
  }

  test("try_to_timestamp ansi on: invalid data returns null natively, not an error") {
    // try_to_timestamp always analyzes with failOnError = false, regardless of ANSI.
    withTimestampStrTable(("not a date", "yyyy-MM-dd")) {
      runQueryAndCompare("SELECT try_to_timestamp(str, fmt) FROM ts_str_tab") {
        checkGlutenPlan[ProjectExecTransformer]
      }
      assert(sql("SELECT try_to_timestamp(str, fmt) FROM ts_str_tab").collect()(0).isNullAt(0))
    }
  }

  test("to_timestamp throws while try_to_timestamp succeeds, same query, same native projection") {
    // One row, two columns: a valid string for to_timestamp, an invalid one for try_to_timestamp.
    withTempPath {
      path =>
        sql("SELECT '2024-01-01' AS valid_str, 'not a date' AS invalid_str, 'yyyy-MM-dd' AS fmt")
          .write
          .parquet(path.getCanonicalPath)
        withTempView("mixed_tab") {
          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("mixed_tab")
          val query =
            "SELECT to_timestamp(valid_str, fmt), try_to_timestamp(invalid_str, fmt) FROM mixed_tab"
          val df = sql(query)
          checkGlutenPlan[ProjectExecTransformer](df)
          val row = df.collect()(0)
          assert(!row.isNullAt(0) && row.getTimestamp(0) != null)
          assert(row.isNullAt(1))

          // Confirms the invalid column alone still throws -- the success above isn't masking it.
          checkNativeThrows(
            "SELECT to_timestamp(invalid_str, fmt) FROM mixed_tab",
            "Invalid date format")
        }
    }
  }

  test("to_date ansi on: valid input, constant and column format") {
    withTimestampStrTable(("2024-01-01", "yyyy-MM-dd")) {
      runQueryAndCompare("SELECT to_date(str, 'yyyy-MM-dd') FROM ts_str_tab") {
        checkGlutenPlan[ProjectExecTransformer]
      }
      runQueryAndCompare("SELECT to_date(str, fmt) FROM ts_str_tab") {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }

  test("to_date ansi on: invalid input throws, constant and column format") {
    withTimestampStrTable(("not a date", "yyyy-MM-dd")) {
      checkNativeThrows("SELECT to_date(str, 'yyyy-MM-dd') FROM ts_str_tab", "Invalid date format")
      checkNativeThrows("SELECT to_date(str, fmt) FROM ts_str_tab", "Invalid date format")
    }
  }

  test("try_to_date ansi on: invalid data returns null natively, where supported") {
    assume(tryToDateSupported, s"try_to_date requires Spark 4.1+, running $SPARK_VERSION")
    withTimestampStrTable(("not a date", "yyyy-MM-dd")) {
      runQueryAndCompare("SELECT try_to_date(str, fmt) FROM ts_str_tab") {
        checkGlutenPlan[ProjectExecTransformer]
      }
      assert(sql("SELECT try_to_date(str, fmt) FROM ts_str_tab").collect()(0).isNullAt(0))
    }
  }

  // dataType = TIMESTAMP_NTZ is out of scope: neither get_timestamp nor checked_get_timestamp
  // has an NTZ registration, so these fall back.
  test("to_timestamp_ntz: falls back, matches vanilla Spark") {
    withTimestampStrTable(("2024-01-01", "yyyy-MM-dd")) {
      checkFallsBackMatchingVanilla("SELECT to_timestamp_ntz(str, fmt) FROM ts_str_tab")
    }
  }

  test(
    "to_timestamp with spark.sql.timestampType=TIMESTAMP_NTZ: falls back, matches vanilla Spark") {
    withSQLConf(SQLConf.TIMESTAMP_TYPE.key -> "TIMESTAMP_NTZ") {
      withTimestampStrTable(("2024-01-01", "yyyy-MM-dd")) {
        checkFallsBackMatchingVanilla("SELECT to_timestamp(str, fmt) FROM ts_str_tab")
      }
    }
  }

  test("to_timestamp_ntz ansi on, invalid input: falls back, matches vanilla Spark's error") {
    withTimestampStrTable(("not a date", "yyyy-MM-dd")) {
      checkFallsBackMatchingVanilla("SELECT to_timestamp_ntz(str, fmt) FROM ts_str_tab")
    }
  }
}

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
import org.apache.spark.sql.internal.SQLConf

import scala.util.{Failure, Success, Try}

class GetTimestampAnsiValidateSuite extends FunctionsValidateSuite {

  disableFallbackCheck

  import testImplicits._

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
      .set(SQLConf.ANSI_ENABLED.key, "true")
  }

  // Writes rows to a parquet file so str/fmt are real columns, not literals Spark could fold.
  private def withTimestampStrTable(rows: (String, String)*)(f: => Unit): Unit = {
    withTempPath {
      path =>
        rows.toDF("str", "fmt").write.parquet(path.getCanonicalPath)
        withTempView("ts_str_tab") {
          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("ts_str_tab")
          f
        }
    }
  }

  // Confirms the plan is native before the throw, and that vanilla Spark also rejects the input.
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

  test("to_timestamp valid") {
    withTimestampStrTable(("2024-01-01 10:30:00", "yyyy-MM-dd HH:mm:ss")) {
      runQueryAndCompare("SELECT to_timestamp(str, 'yyyy-MM-dd HH:mm:ss') FROM ts_str_tab") {
        checkGlutenPlan[ProjectExecTransformer]
      }
      runQueryAndCompare("SELECT to_timestamp(str, fmt) FROM ts_str_tab") {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }

  test("to_timestamp invalid data throws") {
    withTimestampStrTable(("not a date", "yyyy-MM-dd HH:mm:ss")) {
      checkNativeThrows(
        "SELECT to_timestamp(str, 'yyyy-MM-dd HH:mm:ss') FROM ts_str_tab",
        "Invalid date format")
      checkNativeThrows("SELECT to_timestamp(str, fmt) FROM ts_str_tab", "Invalid date format")
    }
  }

  test("to_timestamp ansi off returns null") {
    withTimestampStrTable(("not a date", "yyyy-MM-dd HH:mm:ss")) {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        runQueryAndCompare("SELECT to_timestamp(str, fmt) FROM ts_str_tab") {
          checkGlutenPlan[ProjectExecTransformer]
        }
        assert(sql("SELECT to_timestamp(str, fmt) FROM ts_str_tab").collect()(0).isNullAt(0))
      }
    }
  }

  test("to_timestamp invalid format pattern") {
    // 'b' is not a supported Joda specifier: a format-pattern failure, unchanged by ANSI.
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

  test("to_timestamp null input") {
    withTimestampStrTable((null, "yyyy-MM-dd"), ("2024-01-01", null)) {
      runQueryAndCompare("SELECT to_timestamp(str, fmt) FROM ts_str_tab") {
        checkGlutenPlan[ProjectExecTransformer]
      }
      val rows = sql("SELECT to_timestamp(str, fmt) FROM ts_str_tab ORDER BY str").collect()
      assert(rows(0).isNullAt(0) && rows(1).isNullAt(0))
    }
  }

  test("try_to_timestamp invalid data") {
    // try_to_timestamp always analyzes with failOnError = false, regardless of ANSI.
    withTimestampStrTable(("not a date", "yyyy-MM-dd")) {
      runQueryAndCompare("SELECT try_to_timestamp(str, fmt) FROM ts_str_tab") {
        checkGlutenPlan[ProjectExecTransformer]
      }
      assert(sql("SELECT try_to_timestamp(str, fmt) FROM ts_str_tab").collect()(0).isNullAt(0))
    }
  }

  test("to_timestamp and try_to_timestamp mixed") {
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

          checkNativeThrows(
            "SELECT to_timestamp(invalid_str, fmt) FROM mixed_tab",
            "Invalid date format")
        }
    }
  }

  test("to_date valid") {
    withTimestampStrTable(("2024-01-01", "yyyy-MM-dd")) {
      runQueryAndCompare("SELECT to_date(str, 'yyyy-MM-dd') FROM ts_str_tab") {
        checkGlutenPlan[ProjectExecTransformer]
      }
      runQueryAndCompare("SELECT to_date(str, fmt) FROM ts_str_tab") {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }

  test("to_date invalid data throws") {
    withTimestampStrTable(("not a date", "yyyy-MM-dd")) {
      checkNativeThrows("SELECT to_date(str, 'yyyy-MM-dd') FROM ts_str_tab", "Invalid date format")
      checkNativeThrows("SELECT to_date(str, fmt) FROM ts_str_tab", "Invalid date format")
    }
  }

  testWithMinSparkVersion("try_to_date invalid data", "4.1") {
    withTimestampStrTable(("not a date", "yyyy-MM-dd")) {
      runQueryAndCompare("SELECT try_to_date(str, fmt) FROM ts_str_tab") {
        checkGlutenPlan[ProjectExecTransformer]
      }
      assert(sql("SELECT try_to_date(str, fmt) FROM ts_str_tab").collect()(0).isNullAt(0))
    }
  }

  // dataType = TIMESTAMP_NTZ is out of scope: neither get_timestamp nor checked_get_timestamp
  // has an NTZ registration, so these fall back.
  test("to_timestamp_ntz falls back") {
    withTimestampStrTable(("2024-01-01", "yyyy-MM-dd")) {
      checkFallsBackMatchingVanilla("SELECT to_timestamp_ntz(str, fmt) FROM ts_str_tab")
    }
  }

  test("to_timestamp with timestampType=TIMESTAMP_NTZ falls back") {
    withSQLConf(SQLConf.TIMESTAMP_TYPE.key -> "TIMESTAMP_NTZ") {
      withTimestampStrTable(("2024-01-01", "yyyy-MM-dd")) {
        checkFallsBackMatchingVanilla("SELECT to_timestamp(str, fmt) FROM ts_str_tab")
      }
    }
  }

  test("to_timestamp_ntz invalid data falls back") {
    withTimestampStrTable(("not a date", "yyyy-MM-dd")) {
      checkFallsBackMatchingVanilla("SELECT to_timestamp_ntz(str, fmt) FROM ts_str_tab")
    }
  }
}

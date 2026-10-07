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
import org.apache.spark.sql.internal.SQLConf

class ArithmeticAnsiValidateSuite extends FunctionsValidateSuite {

  disableFallbackCheck

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
      .set(SQLConf.ANSI_ENABLED.key, "true")
  }

  private val maxDecimal = "CAST(%s AS DECIMAL(38,0))".format("9" * 38)
  private val minDecimal = "CAST(-%s AS DECIMAL(38,0))".format("9" * 38)

  // Reads `columns` from a table and evaluates `expr` on them, so the arithmetic runs in Velox.
  // With ANSI mode on, the overflow throws Velox's `error`. With ANSI mode off, the result
  // matches Spark: NULL for decimals and a wrapped value for integers.
  private def checkOverflow(columns: String, expr: String, error: String): Unit = {
    withTempPath {
      path =>
        sql(s"SELECT $columns").write.parquet(path.getCanonicalPath)
        withTempView("overflow_tab") {
          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("overflow_tab")
          val query = s"SELECT $expr FROM overflow_tab"
          withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
            val e = intercept[SparkException](sql(query).collect())
            assert(e.getMessage.contains(error), e.getMessage)
          }
          withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
            runQueryAndCompare(query) {
              checkGlutenPlan[ProjectExecTransformer]
            }
          }
        }
    }
  }

  test("add") {
    runQueryAndCompare("SELECT int_field1 + 100 FROM datatab WHERE int_field1 IS NOT NULL") {
      checkGlutenPlan[ProjectExecTransformer]
    }

    checkOverflow("2147483647 AS a, 1 AS b", "a + b", "Arithmetic overflow")
  }

  test("subtract") {
    runQueryAndCompare("SELECT int_field1 - 50 FROM datatab WHERE int_field1 IS NOT NULL") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("multiply") {
    runQueryAndCompare("SELECT int_field1 * 2 FROM datatab WHERE int_field1 IS NOT NULL") {
      checkGlutenPlan[ProjectExecTransformer]
    }

    checkOverflow("2147483647 AS a, 2 AS b", "a * b", "Arithmetic overflow")
  }

  test("divide") {
    runQueryAndCompare("SELECT int_field1 / 2 FROM datatab WHERE int_field1 IS NOT NULL") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    // Spark 3.4+ throws exception for division by zero in ANSI mode
    intercept[SparkException] {
      sql("SELECT 1 / 0").collect()
    }
  }

  test("div") {
    runQueryAndCompare("SELECT int_field1 div 2 FROM datatab WHERE int_field1 IS NOT NULL") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    intercept[SparkException] {
      sql("SELECT 1 div 0 ").collect()
    }
  }

  test("decimal add overflow") {
    // Normal decimal add should succeed and match Spark results
    runQueryAndCompare(
      "SELECT CAST(1.0 AS DECIMAL(10,2)) + CAST(2.0 AS DECIMAL(10,2))") {
      checkGlutenPlan[ProjectExecTransformer]
    }

    // Overflow: max DECIMAL(38,0) + 1
    checkOverflow(
      s"$maxDecimal AS a, CAST(1 AS DECIMAL(38,0)) AS b",
      "a + b",
      "Decimal overflow in add")
  }

  test("decimal subtract overflow") {
    // Normal decimal subtract should succeed and match Spark results
    runQueryAndCompare(
      "SELECT CAST(5.0 AS DECIMAL(10,2)) - CAST(2.0 AS DECIMAL(10,2))") {
      checkGlutenPlan[ProjectExecTransformer]
    }

    // Overflow: -max DECIMAL(38,0) - 1
    checkOverflow(
      s"$minDecimal AS a, CAST(1 AS DECIMAL(38,0)) AS b",
      "a - b",
      "Decimal overflow in subtract")
  }

  test("decimal try_add") {
    // Normal case should match Spark results
    runQueryAndCompare(
      "SELECT try_add(CAST(1.0 AS DECIMAL(10,2)), CAST(2.0 AS DECIMAL(10,2)))") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    // Overflow should return null
    runQueryAndCompare(
      "SELECT try_add(CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0)), " +
        "CAST(1 AS DECIMAL(38,0)))") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("decimal try_subtract") {
    // Normal case should match Spark results
    runQueryAndCompare(
      "SELECT try_subtract(CAST(5.0 AS DECIMAL(10,2)), CAST(2.0 AS DECIMAL(10,2)))") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    // Overflow should return null
    runQueryAndCompare(
      "SELECT try_subtract(CAST(-99999999999999999999999999999999999999 AS DECIMAL(38,0)), " +
        "CAST(1 AS DECIMAL(38,0)))") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("decimal multiply overflow") {
    // Normal decimal multiply should succeed and match Spark results
    runQueryAndCompare(
      "SELECT CAST(2.0 AS DECIMAL(10,2)) * CAST(3.0 AS DECIMAL(10,2))") {
      checkGlutenPlan[ProjectExecTransformer]
    }

    // Overflow: max DECIMAL(38,0) * 2
    checkOverflow(
      s"$maxDecimal AS a, CAST(2 AS DECIMAL(38,0)) AS b",
      "a * b",
      "Decimal overflow in multiply")
  }

  test("decimal try_multiply") {
    // Normal case should match Spark results
    runQueryAndCompare(
      "SELECT try_multiply(CAST(2.0 AS DECIMAL(10,2)), CAST(3.0 AS DECIMAL(10,2)))") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    // Overflow should return null
    runQueryAndCompare(
      "SELECT try_multiply(CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0)), " +
        "CAST(2 AS DECIMAL(38,0)))") {
      checkGlutenPlan[ProjectExecTransformer]
    }
  }

  test("decimal overflow with allowPrecisionLoss disabled") {
    // Uses checked_*_deny_precision_loss. A missing function would fail validation and fall back
    // to Spark, which also throws on overflow, so check that the query runs on Velox first.
    withSQLConf(SQLConf.DECIMAL_OPERATIONS_ALLOW_PREC_LOSS.key -> "false") {
      runQueryAndCompare(
        "SELECT CAST(1.5 AS DECIMAL(10,2)) + CAST(2.25 AS DECIMAL(10,2)), " +
          "CAST(1.5 AS DECIMAL(10,2)) - CAST(2.25 AS DECIMAL(10,2)), " +
          "CAST(1.5 AS DECIMAL(10,2)) * CAST(2.25 AS DECIMAL(10,2))") {
        checkGlutenPlan[ProjectExecTransformer]
      }

      val columns = s"$maxDecimal AS max, $minDecimal AS min, " +
        "CAST(1 AS DECIMAL(38,0)) AS one, CAST(2 AS DECIMAL(38,0)) AS two"
      checkOverflow(columns, "max + one", "Decimal overflow in add")
      checkOverflow(columns, "min - one", "Decimal overflow in subtract")
      checkOverflow(columns, "max * two", "Decimal overflow in multiply")
    }
  }
}

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
import org.apache.gluten.execution.{ProjectExecTransformer, WholeStageTransformerSuite}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class DecimalPmodValidateSuite extends WholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  disableFallbackCheck

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
      .set(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "true")
      .set(GlutenConfig.ENABLE_COLUMNAR_PARTIAL_PROJECT.key, "false")
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
  }

  private def withDecimalTable(
      leftType: DecimalType,
      rightType: DecimalType,
      values: Seq[(String, String)])(f: => Unit): Unit = {
    val schema = StructType(Seq(StructField("a", leftType), StructField("n", rightType)))
    def decimal(value: String): java.math.BigDecimal =
      if (value == null) null else new java.math.BigDecimal(value)
    val rows = values.map { case (a, n) => Row(decimal(a), decimal(n)) }
    withTempPath {
      path =>
        spark.createDataFrame(sparkContext.parallelize(rows, 1), schema)
          .write.parquet(path.getCanonicalPath)
        withTempView("decimal_pmod") {
          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("decimal_pmod")
          f
        }
    }
  }

  private def checkNative(resultType: DecimalType): Unit = {
    val df = runQueryAndCompare("SELECT pmod(a, n) FROM decimal_pmod") {
      checkGlutenPlan[ProjectExecTransformer]
    }
    assert(df.schema.head.dataType == resultType)
  }

  private def checkNativeError(message: String): Unit = {
    val df = sql("SELECT pmod(a, n) FROM decimal_pmod")
    checkGlutenPlan[ProjectExecTransformer](df)
    val error = intercept[SparkException](df.collect())
    assert(error.getMessage.contains(message), error.getMessage)
  }

  private def checkFallback(df: DataFrame): Unit = {
    checkSparkPlan[ProjectExec](df)
    checkGlutenPlanCount[ProjectExecTransformer](df, 0)
  }

  test("decimal pmod offloads mixed precision and scale in both modes") {
    withDecimalTable(
      DecimalType(2, 1),
      DecimalType(5, 2),
      Seq(
        "-5.5" -> "3.00",
        "5.5" -> "-3.00",
        "-5.5" -> "-3.00",
        "0.0" -> "3.00",
        "6.0" -> "3.00",
        (null, "3.00"),
        "5.5" -> null,
        (null, null))
    ) {
      for (ansi <- Seq("false", "true"); precisionLoss <- Seq("false", "true")) {
        withSQLConf(
          SQLConf.ANSI_ENABLED.key -> ansi,
          SQLConf.DECIMAL_OPERATIONS_ALLOW_PREC_LOSS.key -> precisionLoss) {
          checkNative(DecimalType(3, 2))
        }
      }
    }
  }

  test("decimal pmod handles precision 38 and large scale differences") {
    val maximum = "9" * 38
    withDecimalTable(
      DecimalType(38, 0),
      DecimalType(38, 38),
      Seq(
        maximum -> "0.12345678901234567890123456789012345678",
        s"-$maximum" -> "0.12345678901234567890123456789012345678",
        maximum -> "-0.12345678901234567890123456789012345678"
      )
    ) {
      for (ansi <- Seq("false", "true"); precisionLoss <- Seq("false", "true")) {
        withSQLConf(
          SQLConf.ANSI_ENABLED.key -> ansi,
          SQLConf.DECIMAL_OPERATIONS_ALLOW_PREC_LOSS.key -> precisionLoss) {
          checkNative(DecimalType(38, 38))
        }
      }
    }
  }

  test("decimal pmod zero divisor returns null or throws, with native execution") {
    withDecimalTable(DecimalType(12, 2), DecimalType(12, 2), Seq("1.00" -> "0.00")) {
      for (precisionLoss <- Seq("false", "true")) {
        withSQLConf(SQLConf.DECIMAL_OPERATIONS_ALLOW_PREC_LOSS.key -> precisionLoss) {
          withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
            checkNative(DecimalType(12, 2))
            checkAnswer(sql("SELECT pmod(a, n) FROM decimal_pmod"), Row(null))
          }
          withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
            checkNativeError("Division by zero")
          }
        }
      }
    }
    withDecimalTable(
      DecimalType(12, 2),
      DecimalType(12, 2),
      Seq((null, "0.00"), "1.00" -> null, (null, null))) {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        checkNative(DecimalType(12, 2))
      }
    }
  }

  test("decimal pmod checks overflow after positive remainder adjustment") {
    // The result type has precision 1, although the corrected remainder is 99.
    withDecimalTable(DecimalType(1, 0), DecimalType(3, 0), Seq("-1" -> "100")) {
      for (precisionLoss <- Seq("false", "true")) {
        withSQLConf(SQLConf.DECIMAL_OPERATIONS_ALLOW_PREC_LOSS.key -> precisionLoss) {
          withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
            checkNative(DecimalType(1, 0))
            checkAnswer(sql("SELECT pmod(a, n) FROM decimal_pmod"), Row(null))
          }
          withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
            checkNativeError("Decimal overflow in pmod")
          }
        }
      }
    }
  }

  test("decimal pmod preserves captured ANSI mode after physical planning") {
    for (
      (leftType, rightType, values, message) <- Seq(
        (DecimalType(12, 2), DecimalType(12, 2), Seq("1.00" -> "0.00"), "Division by zero"),
        (DecimalType(1, 0), DecimalType(3, 0), Seq("-1" -> "100"), "Decimal overflow in pmod")
      )
    ) {
      withDecimalTable(leftType, rightType, values) {
        for (capturedAnsi <- Seq("false", "true")) {
          withSQLConf(SQLConf.ANSI_ENABLED.key -> capturedAnsi) {
            val df = sql("SELECT pmod(a, n) FROM decimal_pmod")
            // Force physical planning before changing the setting used to create the native
            // runtime. Neither the checked operation nor its TRY wrapper may read that setting.
            checkGlutenPlan[ProjectExecTransformer](df)
            withSQLConf(SQLConf.ANSI_ENABLED.key -> (capturedAnsi == "false").toString) {
              if (capturedAnsi == "false") {
                checkAnswer(df, Row(null))
              } else {
                val error = intercept[SparkException](df.collect())
                assert(error.getMessage.contains(message), error.getMessage)
              }
            }
          }
        }
      }
    }
  }

  test("decimal pmod keeps computed operands in Spark for short-circuit evaluation") {
    withDecimalTable(DecimalType(12, 2), DecimalType(12, 2), Seq("1.00" -> "0.00")) {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        // A legacy PMOD must skip even an error-producing dividend when its divisor is zero.
        val query = "SELECT pmod(cast(raise_error('must not evaluate') AS DECIMAL(12,2)), n) " +
          "FROM decimal_pmod"
        runQueryAndCompare(query)(checkFallback)
      }
    }
    withDecimalTable(DecimalType(12, 2), DecimalType(12, 2), Seq("1.00" -> null)) {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        val query = "SELECT pmod(cast(raise_error('must not evaluate') AS DECIMAL(12,2)), n) " +
          "FROM decimal_pmod"
        runQueryAndCompare(query)(checkFallback)
      }
    }
  }

  test("decimal pmod falls back when native capability validation is disabled") {
    withDecimalTable(DecimalType(12, 2), DecimalType(12, 2), Seq("-5.00" -> "3.00")) {
      withSQLConf(
        SQLConf.ANSI_ENABLED.key -> "false",
        GlutenConfig.NATIVE_VALIDATION_ENABLED.key -> "false") {
        runQueryAndCompare("SELECT pmod(a, n) FROM decimal_pmod")(checkFallback)
      }
    }
  }

  test("partial projection does not evaluate a skipped decimal pmod dividend") {
    val fail: String => String = _ => throw new IllegalStateException("must not evaluate")
    spark.udf.register("decimal_pmod_fail", fail)
    for ((ansi, divisor) <- Seq("false" -> "0.00", "true" -> null)) {
      withDecimalTable(DecimalType(12, 2), DecimalType(12, 2), Seq("1.00" -> divisor)) {
        withSQLConf(
          SQLConf.ANSI_ENABLED.key -> ansi,
          GlutenConfig.ENABLE_COLUMNAR_PARTIAL_PROJECT.key -> "true") {
          val query = "SELECT pmod(cast(decimal_pmod_fail(cast(a AS STRING)) " +
            "AS DECIMAL(12,2)), n) FROM decimal_pmod"
          // The complete PMOD needs both input columns, so partial projection correctly gives
          // way to a Spark project. Extracting just the UDF would evaluate it and fail instead.
          runQueryAndCompare(query)(checkFallback)
        }
      }
    }
  }
}

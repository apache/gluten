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
package org.apache.spark.sql.catalyst.expressions

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.exception.NativeCastException
import org.apache.gluten.execution.{ProjectExecTransformer, VeloxWholeStageTransformerSuite}

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.UTC_OPT
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.sql.Timestamp
import java.util.TimeZone

class VeloxCastSuite extends VeloxWholeStageTransformerSuite with ExpressionEvalHelper {
  def cast(v: Any, targetType: DataType, timeZoneId: Option[String] = None): Cast = {
    v match {
      case lit: Expression =>
        Cast(lit, targetType, timeZoneId)
      case _ =>
        val lit = Literal(v)
        Cast(lit, targetType, timeZoneId)
    }
  }

  test("cast binary to string type") {

    val testCases = Seq(
      ("Hello, World!".getBytes, "Hello, World!"),
      ("12345".getBytes, "12345"),
      ("".getBytes, ""),
      ("Some special characters: !@#$%^&*()".getBytes, "Some special characters: !@#$%^&*()"),
      ("Line\nbreak".getBytes, "Line\nbreak")
    )

    for ((binaryValue, expectedString) <- testCases) {
      checkEvaluation(cast(cast(binaryValue, BinaryType), StringType), expectedString)
    }
  }

  test("cast from double to timestamp format") {
    val originalDefaultTz = TimeZone.getDefault
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

      checkEvaluation(
        cast(0.0, TimestampType, UTC_OPT),
        Timestamp.valueOf("1970-01-01 00:00:00")
      )

      checkEvaluation(
        cast(1.5, TimestampType, UTC_OPT),
        Timestamp.valueOf("1970-01-01 00:00:01.5")
      )

      checkEvaluation(
        cast(12345.6789, TimestampType, UTC_OPT),
        Timestamp.valueOf("1970-01-01 03:25:45.6789")
      )

      checkEvaluation(
        cast(-1.2, TimestampType, UTC_OPT),
        Timestamp.valueOf("1969-12-31 23:59:58.8")
      )
    } finally {
      TimeZone.setDefault(originalDefaultTz)
    }
  }

  private val nativeCastExpressions = Seq(
    "cast(i as tinyint)",
    "cast(i as smallint)",
    "cast(l as int)",
    "cast(d as bigint)",
    "cast(l as decimal(7, 2))",
    "named_struct('value', cast(nested.value as int))",
    "cast(s as int)",
    "cast(dec as decimal(3, 2))"
  )

  private def withNativeCastInput(f: DataFrame => Unit): Unit = {
    withTempPath {
      path =>
        val schema = new StructType()
          .add("i", IntegerType)
          .add("l", LongType)
          .add("d", DoubleType)
          .add("nested", new StructType().add("value", LongType))
          .add("s", StringType)
          .add("dec", DecimalType(3, 1))
        val row = Row(
          Int.MaxValue,
          Long.MaxValue,
          1.2345678901234567e19,
          Row(Long.MaxValue),
          Long.MaxValue.toString,
          new java.math.BigDecimal("12.3"))
        spark
          .createDataFrame(spark.sparkContext.parallelize(Seq(row), 1), schema)
          .write
          .parquet(path.getCanonicalPath)
        f(spark.read.parquet(path.getCanonicalPath))
    }
  }

  test("native ANSI casts retain Spark exception classes, parameters and native causes") {
    val expected = Seq(
      QueryExecutionErrors.castingCauseOverflowError(Int.MaxValue, IntegerType, ByteType),
      QueryExecutionErrors.castingCauseOverflowError(Int.MaxValue, IntegerType, ShortType),
      QueryExecutionErrors.castingCauseOverflowError(Long.MaxValue, LongType, IntegerType),
      QueryExecutionErrors.castingCauseOverflowError(1.2345678901234567e19, DoubleType, LongType),
      QueryExecutionErrors.cannotChangeDecimalPrecisionError(Decimal(Long.MaxValue), 7, 2, null),
      QueryExecutionErrors.castingCauseOverflowError(Long.MaxValue, LongType, IntegerType),
      QueryExecutionErrors.invalidInputInCastToNumberError(
        IntegerType,
        UTF8String.fromString(Long.MaxValue.toString),
        null),
      QueryExecutionErrors.cannotChangeDecimalPrecisionError(Decimal("12.3"), 3, 2, null)
    )
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key -> "false") {
      withNativeCastInput {
        input =>
          Seq("LEGACY", "ANSI").foreach {
            policy =>
              withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> policy) {
                nativeCastExpressions.zip(expected).foreach {
                  case (expression, expectedError) =>
                    val query = input.selectExpr(expression)
                    assert(query.queryExecution.executedPlan.collect {
                      case p: ProjectExecTransformer => p
                    }.nonEmpty)
                    val error = intercept[Exception](query.collect())
                    val causes = Iterator.iterate[Throwable](error)(_.getCause)
                      .takeWhile(_ != null)
                      .toSeq
                    val original = causes.collectFirst { case e: NativeCastException => e }
                      .getOrElse(fail(s"No native cast error for $expression", error))
                    val translated = causes.find(_.getCause eq original)
                      .getOrElse(fail(s"No translated error for $expression", error))
                    assert(translated.getClass == expectedError.getClass)
                    (translated, expectedError) match {
                      case (actual: SparkThrowable, expected: SparkThrowable) =>
                        assert(actual.getErrorClass == expected.getErrorClass)
                        assert(actual.getMessageParameters == expected.getMessageParameters)
                      case _ => fail("Expected Spark cast exceptions")
                    }
                }
              }
          }
      }
    }
  }

  test("native cast exception translation leaves legacy and TRY cast results unchanged") {
    withSQLConf(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key -> "false") {
      withNativeCastInput {
        input =>
          withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
            checkAnswer(
              input.selectExpr(nativeCastExpressions: _*),
              Row((-1).toByte, (-1).toShort, -1, Long.MaxValue, null, Row(-1), null, null))
          }
          Seq("false", "true").foreach {
            ansi =>
              withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
                checkAnswer(
                  input.selectExpr(nativeCastExpressions.map(_.replace("cast(", "try_cast(")): _*),
                  Row(null, null, null, null, null, Row(null), null, null))
              }
          }
      }
    }
  }

  override protected val resourcePath: String = "N/A"
  override protected val fileFormat: String = "N/A"
}

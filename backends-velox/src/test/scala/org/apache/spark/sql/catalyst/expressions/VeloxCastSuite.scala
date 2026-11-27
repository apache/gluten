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

import org.apache.gluten.config.{GlutenConfig, VeloxConfig}
import org.apache.gluten.exception.NativeCastException
import org.apache.gluten.execution.{ProjectExecTransformer, VeloxResizeBatchesExec, VeloxWholeStageTransformerSuite}

import org.apache.spark.{SparkArithmeticException, SparkConf, SparkThrowable}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.UTC_OPT
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.SparkVersionUtil

import java.sql.Timestamp
import java.util.TimeZone

class VeloxCastSuite extends VeloxWholeStageTransformerSuite with ExpressionEvalHelper {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
  }

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

  // The failing value, its type and the target type of each expression above.
  private val nativeCastValues = Seq[(Any, DataType, DataType)](
    (Int.MaxValue, IntegerType, ByteType),
    (Int.MaxValue, IntegerType, ShortType),
    (Long.MaxValue, LongType, IntegerType),
    (1.2345678901234567e19, DoubleType, LongType),
    (Long.MaxValue, LongType, DecimalType(7, 2)),
    (Long.MaxValue, LongType, IntegerType),
    (Long.MaxValue.toString, StringType, IntegerType),
    (new java.math.BigDecimal("12.3"), DecimalType(3, 1), DecimalType(3, 2))
  )

  // The exception Spark's own cast raises for the value. Call it with ANSI enabled.
  private def catalystCastError(value: Any, from: DataType, to: DataType): RuntimeException =
    intercept[RuntimeException](cast(Literal.create(value, from), to).eval())

  private def withNativeCastInput(
      intValue: Int = Int.MaxValue,
      doubleValue: Double = 1.2345678901234567e19,
      stringValue: String = Long.MaxValue.toString)(f: DataFrame => Unit): Unit = {
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
          intValue,
          Long.MaxValue,
          doubleValue,
          Row(Long.MaxValue),
          stringValue,
          new java.math.BigDecimal("12.3"))
        spark
          .createDataFrame(spark.sparkContext.parallelize(Seq(row), 1), schema)
          .write
          .parquet(path.getCanonicalPath)
        f(spark.read.parquet(path.getCanonicalPath))
    }
  }

  private def checkNativeCastError(
      input: DataFrame,
      expression: String,
      expectedError: RuntimeException): NativeCastException = {
    val query = input.selectExpr(expression)
    assert(query.queryExecution.executedPlan.exists(_.isInstanceOf[ProjectExecTransformer]))
    val error = intercept[Exception](query.collect())
    val causes = Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).toList
    val original = causes.collectFirst { case e: NativeCastException => e }
      .getOrElse(fail(s"No native cast error for $expression", error))
    val translated = causes.find(_.getCause eq original)
      .getOrElse(fail(s"No translated error for $expression", error))
    assert(translated.getClass == expectedError.getClass)
    (translated, expectedError) match {
      case (actual: SparkThrowable, expected: SparkThrowable) =>
        assert(actual.getErrorClass == expected.getErrorClass)
        assert(actual.getMessageParameters == expected.getMessageParameters)
        assert(actual.getSqlState == expected.getSqlState)
      case _ => fail("Expected Spark cast exceptions")
    }
    original
  }

  test("native ANSI casts retain Spark exception classes, parameters and native causes") {
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key -> "false") {
      withNativeCastInput() {
        input =>
          Seq("LEGACY", "ANSI").foreach {
            policy =>
              withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> policy) {
                nativeCastExpressions.zip(nativeCastValues).foreach {
                  case (expression, (value, from, to)) =>
                    checkNativeCastError(input, expression, catalystCastError(value, from, to))
                }
              }
          }
      }
    }
  }

  test("native negative overflow, NaN and embedded NUL match Catalyst ANSI casts") {
    val text = "bad\u0000tail"
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key -> "false",
      VeloxConfig.COLUMNAR_VELOX_ENABLE_USER_EXCEPTION_STACKTRACE.key -> "true"
    ) {
      withNativeCastInput(-129, Double.NaN, text) {
        input =>
          def compare(expression: String, value: Any, target: DataType): NativeCastException = {
            val expected = intercept[RuntimeException](cast(value, target).eval())
            checkNativeCastError(input, expression, expected)
          }
          compare("cast(i as tinyint)", -129, ByteType)
          compare("cast(d as bigint)", Double.NaN, LongType)
          val original = compare("cast(s as int)", text, IntegerType)
          assert(original.getReason.contains(text))
          assert(original.getMessage.contains(s"Reason: ${original.getReason}\n"))
          assert(original.getMessage.contains("\nStack trace:\n#"))
      }
    }
  }

  private def withSingleColumn(value: Any, dataType: DataType)(f: DataFrame => Unit): Unit = {
    withTempPath {
      path =>
        spark
          .createDataFrame(
            spark.sparkContext.parallelize(Seq(Row(value)), 1),
            new StructType().add("c", dataType))
          .write
          .parquet(path.getCanonicalPath)
        f(spark.read.parquet(path.getCanonicalPath))
    }
  }

  test("native cast errors match Catalyst for each recognized reason form") {
    // Forms the tests above don't produce natively: REAL sources, infinities, exponent and
    // negative values, integral to decimal, and each integral target of a string cast.
    val cases = Seq[(Any, DataType, DataType)](
      (1.0e30f, FloatType, IntegerType),
      (Float.NaN, FloatType, IntegerType),
      (1.0e21, DoubleType, LongType),
      (-1.0e10, DoubleType, IntegerType),
      (Double.PositiveInfinity, DoubleType, IntegerType),
      (Double.NegativeInfinity, DoubleType, ShortType),
      (Long.MinValue, LongType, ShortType),
      (1000, IntegerType, DecimalType(3, 2)),
      (new java.math.BigDecimal("-12.3"), DecimalType(3, 1), DecimalType(3, 2)),
      ("bad' to sleep", StringType, ByteType),
      ("99999", StringType, ShortType),
      ("", StringType, LongType)
    )
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key -> "false") {
      cases.foreach {
        case (value, from, to) =>
          withClue(s"cast($value as ${to.sql}) from ${from.sql}: ") {
            withSingleColumn(value, from) {
              input =>
                checkNativeCastError(
                  input,
                  s"cast(c as ${to.sql})",
                  catalystCastError(value, from, to))
            }
          }
      }
    }
  }

  test("native cast errors before a shuffle reach the driver as Spark exceptions") {
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key -> "false",
      VeloxConfig.COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_INPUT.key -> "true"
    ) {
      withNativeCastInput() {
        input =>
          Seq("false", "true").foreach {
            adaptive =>
              withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive) {
                val query = input.selectExpr("cast(i as tinyint) as c").repartition(col("c"))
                if (adaptive == "false") {
                  // The resize operator reads the cast's output through a second native iterator.
                  val plan = query.queryExecution.executedPlan
                  assert(plan.exists(_.isInstanceOf[VeloxResizeBatchesExec]), plan.treeString)
                }
                val error = intercept[Exception](query.collect())
                // Spark 4 rethrows a task's user-facing SparkThrowable as is; Spark 3 wraps it.
                val taskError = if (SparkVersionUtil.gteSpark40) error else error.getCause
                taskError match {
                  case e: SparkArithmeticException => assert(e.getErrorClass == "CAST_OVERFLOW")
                  case _ =>
                    val actual = error.toString.linesIterator.next()
                    fail(s"Expected the Spark cast exception (adaptive=$adaptive): $actual", error)
                }
              }
          }
      }
    }
  }

  test("native cast exception translation leaves legacy and TRY cast results unchanged") {
    withSQLConf(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key -> "false") {
      withNativeCastInput() {
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

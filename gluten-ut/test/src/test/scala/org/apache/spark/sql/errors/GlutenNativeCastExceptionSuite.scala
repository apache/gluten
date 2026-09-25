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
package org.apache.spark.sql.errors

import org.apache.gluten.exception.NativeCastException

import org.apache.spark.{SparkArithmeticException, SparkFunSuite, SparkNumberFormatException, SparkThrowable}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.nio.charset.StandardCharsets.UTF_8

class GlutenNativeCastExceptionSuite extends SparkFunSuite {
  private def nativeException(reason: String): NativeCastException = {
    new NativeCastException(
      reason.getBytes(UTF_8),
      s"Exception: VeloxUserError\nReason: $reason\nNative stack trace".getBytes(UTF_8))
  }

  private def checkError(reason: String, expected: RuntimeException): Unit = {
    val original = nativeException(reason)
    val actual = GlutenCastErrors.fromNativeReason(original.getReason, original)
    assert(actual != null)
    assert(actual.getClass == expected.getClass)
    (actual, expected) match {
      case (a: SparkThrowable, e: SparkThrowable) =>
        assert(a.getErrorClass == e.getErrorClass)
        assert(a.getMessageParameters == e.getMessageParameters)
        assert(a.getSqlState == e.getSqlState)
      case _ => fail("Expected Spark exceptions")
    }
    assert(actual.getCause eq original)
    assert(actual.getCause.getMessage.contains("Native stack trace"))
  }

  private val integralCases: Seq[(String, String, Any, DataType, String, DataType)] = Seq(
    ("INT to TINYINT", "INTEGER", Int.MaxValue, IntegerType, "TINYINT", ByteType),
    ("INT to SMALLINT", "INTEGER", Int.MaxValue, IntegerType, "SMALLINT", ShortType),
    ("BIGINT to INT", "BIGINT", Long.MaxValue, LongType, "INTEGER", IntegerType),
    ("struct member BIGINT to INT", "BIGINT", Long.MaxValue, LongType, "INTEGER", IntegerType),
    ("negative BIGINT to INT", "BIGINT", Long.MinValue, LongType, "INTEGER", IntegerType)
  )

  integralCases.foreach {
    case (name, fromName, value, fromType, toName, toType) =>
      test(s"native $name overflow uses Spark's cast exception") {
        checkError(
          s"Cannot cast $fromName '$value' to $toName. Overflow during arithmetic conversion: ",
          QueryExecutionErrors.castingCauseOverflowError(value, fromType, toType)
        )
      }
  }

  test("native DOUBLE to BIGINT overflow preserves the typed SQL value") {
    checkError(
      "Cannot cast DOUBLE '12345678901234567000' to BIGINT. " +
        "Cannot cast floating-point value to an integral value due to overflow.",
      QueryExecutionErrors.castingCauseOverflowError(
        1.2345678901234567e19,
        DoubleType,
        LongType)
    )
  }

  test("native REAL overflow retains the Spark FLOAT source type") {
    checkError(
      "Cannot cast REAL '1e+30' to INTEGER. " +
        "Cannot cast floating-point value to an integral value due to overflow.",
      QueryExecutionErrors.castingCauseOverflowError(1e30f, FloatType, IntegerType)
    )
  }

  test("native BIGINT to DECIMAL uses the Spark-version-specific decimal error") {
    checkError(
      "Cannot cast BIGINT '9223372036854775807' to DECIMAL(7, 2)",
      QueryExecutionErrors.cannotChangeDecimalPrecisionError(Decimal(Long.MaxValue), 7, 2, null))
  }

  test("native DECIMAL to DECIMAL preserves the original decimal value") {
    checkError(
      "Cannot cast DECIMAL '12.3' to DECIMAL(3, 2)",
      QueryExecutionErrors.cannotChangeDecimalPrecisionError(Decimal("12.3"), 3, 2, null))
  }

  test("native STRING to INT overflow is a number-format error, not an arithmetic error") {
    val reason =
      """Cannot cast VARCHAR '9223372036854775807' to INTEGER. Overflow during conversion: """""
    val original = nativeException(reason)
    val actual = GlutenCastErrors.fromNativeReason(reason, original)
    assert(actual.isInstanceOf[SparkNumberFormatException])
    assert(!actual.isInstanceOf[SparkArithmeticException])
    checkError(
      reason,
      QueryExecutionErrors.invalidInputInCastToNumberError(
        IntegerType,
        UTF8String.fromString("9223372036854775807"),
        null))
  }

  test("native string metadata preserves quotes, backslashes, newlines, NUL and Unicode") {
    // scalastyle:off nonascii
    val value = "bad'\"\\\n\r\t\u0000\u00e9\uD83D\uDE00"
    // scalastyle:on nonascii
    val reason = s"""Cannot cast VARCHAR '$value' to INTEGER. Invalid leading character: """""
    assert(nativeException(reason).getReason == reason)
    checkError(
      reason,
      QueryExecutionErrors.invalidInputInCastToNumberError(
        IntegerType,
        UTF8String.fromString(value),
        null))
  }

  test("unknown, malformed and ambiguous cast reasons retain the native error") {
    val reasons = Seq(
      "integer overflow",
      "division by zero",
      "Cannot cast ARRAY<INTEGER> '[1]' to INTEGER.",
      "Cannot cast TIMESTAMP '2025-01-01' to INTEGER. Overflow during arithmetic conversion: ",
      "Cannot cast INTEGER '2147483648' to TINYINT. Overflow during arithmetic conversion: ",
      "Cannot cast INTEGER 'not a number' to TINYINT. Overflow during arithmetic conversion: ",
      "Cannot cast INTEGER '2147483647' to TINYINT. An unrecognized error",
      "Cannot cast DECIMAL '12.3' to DECIMAL(39, 2)",
      "Cannot cast DECIMAL '12.3' to DECIMAL(0, 0)",
      "Cannot cast DECIMAL '12.3' to DECIMAL(3, 4)",
      "Cannot cast BIGINT '12.3' to DECIMAL(3, 2)",
      "Cannot cast VARCHAR 'prefix' to INTEGER. suffix' to INTEGER. Invalid leading character",
      "Context: Cannot cast INTEGER '2147483647' to TINYINT. " +
        "Overflow during arithmetic conversion: "
    )
    reasons.foreach {
      reason =>
        val original = nativeException(reason)
        assert(GlutenCastErrors.fromNativeReason(reason, original) == null, reason)
    }
    assert(GlutenCastErrors.fromNativeReason(null, nativeException("unknown")) == null)
  }

  test("diagnostic text is not used to classify a native error") {
    val original = new NativeCastException(
      "unknown native error".getBytes(UTF_8),
      ("Cannot cast INTEGER '2147483647' to TINYINT. " +
        "Overflow during arithmetic conversion: ").getBytes(UTF_8))
    assert(GlutenCastErrors.fromNativeReason(original.getReason, original) == null)
  }
}

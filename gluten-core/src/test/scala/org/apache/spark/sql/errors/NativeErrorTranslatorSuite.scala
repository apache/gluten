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

import org.apache.gluten.exception.GlutenException

import org.apache.spark.{SparkArithmeticException, SparkRuntimeException}
import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException
import org.apache.spark.util.SparkVersionUtil

import org.scalatest.funsuite.AnyFunSuite

class NativeErrorTranslatorSuite extends AnyFunSuite {

  private val message = "Exception: VeloxUserError\nReason: (0 vs. 0) division by zero"
  private val lengthMessage = "Reason: Exceeds allowed length limitation: 3"

  test("translates DIVIDE_BY_ZERO to SparkArithmeticException") {
    val native = new GlutenException(message, SparkErrorCodes.DivideByZero)
    val translated = NativeErrorTranslator.translate(native)
    assert(translated.isInstanceOf[SparkArithmeticException])
    val sparkError = translated.asInstanceOf[SparkArithmeticException]
    assert(sparkError.getMessage.startsWith("[DIVIDE_BY_ZERO]"), sparkError.getMessage)
    assert(sparkError.getSqlState == "22012")
    assert(sparkError.getMessageParameters.get("config") == "\"spark.sql.ansi.enabled\"")
    assert(sparkError.getCause eq native)
    // PySpark reads every query context entry, so a null entry fails the Python error conversion.
    assert(sparkError.getQueryContext.isEmpty)
  }

  test("translates error code only on the native error itself") {
    val native = new GlutenException(message, SparkErrorCodes.DivideByZero)
    assert(NativeErrorTranslator.translate(new IllegalStateException("failed", native)) == null)
    assert(NativeErrorTranslator.translate(new GlutenException("wrapped", native)) == null)
  }

  test("does not translate an error without an error code or a known message") {
    val withCodeInMessage = s"Error Code: ${SparkErrorCodes.DivideByZero}\n$message"
    assert(NativeErrorTranslator.translate(new GlutenException(withCodeInMessage)) == null)
    assert(NativeErrorTranslator.translate(new RuntimeException(withCodeInMessage)) == null)
    assert(NativeErrorTranslator.translate(null) == null)
  }

  test("returns null for unsupported error codes without a known message") {
    Seq("NOT_A_SUPPORTED_CONDITION", "").foreach {
      errorCode =>
        val native = new GlutenException(message, errorCode)
        assert(NativeErrorTranslator.translate(native) == null, errorCode)
    }
  }

  test("falls back to the message for errors without a supported error code") {
    Seq(
      new GlutenException(lengthMessage),
      new GlutenException(lengthMessage, "NOT_A_SUPPORTED_CONDITION"),
      new IllegalStateException(null, new GlutenException(lengthMessage))
    ).foreach {
      native =>
        val translated = NativeErrorTranslator.translate(native)
        if (SparkVersionUtil.gteSpark35) {
          assert(translated.isInstanceOf[SparkRuntimeException], native)
          assert(translated.getMessage.startsWith("[EXCEED_LIMIT_LENGTH]"), translated.getMessage)
        } else {
          assert(translated.getMessage == "Exceeds char/varchar type length limitation: 3")
        }
        assert(translated.getCause eq native)
    }
  }

  test("falls back to the message for type conversion errors") {
    val native = new GlutenException("Reason: Not a valid type for INTEGER")
    val translated = NativeErrorTranslator.translate(native)
    assert(translated.isInstanceOf[SchemaColumnConvertNotSupportedException])
    assert(translated.getCause eq native)
  }
}

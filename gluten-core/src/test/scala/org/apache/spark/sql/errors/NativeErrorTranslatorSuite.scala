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

import org.apache.spark.SparkArithmeticException

import org.scalatest.funsuite.AnyFunSuite

class NativeErrorTranslatorSuite extends AnyFunSuite {

  private val message = "Exception: VeloxUserError\nReason: (0 vs. 0) division by zero"

  test("translates DIVIDE_BY_ZERO to SparkArithmeticException") {
    val native = new GlutenException(message, NativeErrorCodes.DivideByZero)
    val translated = NativeErrorTranslator.translate(native)
    assert(translated.isInstanceOf[SparkArithmeticException])
    val sparkError = translated.asInstanceOf[SparkArithmeticException]
    assert(sparkError.getMessage.startsWith("[DIVIDE_BY_ZERO]"), sparkError.getMessage)
    assert(sparkError.getSqlState == "22012")
    assert(sparkError.getMessageParameters.get("config") == "\"spark.sql.ansi.enabled\"")
    assert(sparkError.getCause eq native)
  }

  test("does not translate a native error inside another exception") {
    val native = new GlutenException(message, NativeErrorCodes.DivideByZero)
    assert(NativeErrorTranslator.translate(new IllegalStateException("failed", native)) == null)
    assert(NativeErrorTranslator.translate(new GlutenException("wrapped", native)) == null)
  }

  test("does not translate an error without an error code") {
    val withCodeInMessage = s"Error Code: ${NativeErrorCodes.DivideByZero}\n$message"
    assert(NativeErrorTranslator.translate(new GlutenException(withCodeInMessage)) == null)
    assert(NativeErrorTranslator.translate(new RuntimeException(withCodeInMessage)) == null)
    assert(NativeErrorTranslator.translate(null) == null)
  }

  test("returns null for unsupported error codes") {
    Seq("INVALID_ARGUMENT", "SPARK:NOT_A_SUPPORTED_CONDITION", "SPARK:", "").foreach {
      errorCode =>
        val native = new GlutenException(message, errorCode)
        assert(NativeErrorTranslator.translate(native) == null, errorCode)
    }
  }
}

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

  private def veloxMessage(errorCode: String, reason: String): String =
    s"""Exception: VeloxUserError
       |Error Source: USER
       |Error Code: $errorCode
       |Reason: $reason
       |Retriable: False
       |Expression: b == 0
       |""".stripMargin

  test("translates DIVIDE_BY_ZERO to SparkArithmeticException") {
    val native =
      new GlutenException(veloxMessage("SPARK:DIVIDE_BY_ZERO", "(0 vs. 0) division by zero"))
    val translated = NativeErrorTranslator.translate(native)
    assert(translated.isInstanceOf[SparkArithmeticException])
    val sparkError = translated.asInstanceOf[SparkArithmeticException]
    assert(sparkError.getMessage.startsWith("[DIVIDE_BY_ZERO]"), sparkError.getMessage)
    assert(sparkError.getSqlState == "22012")
    assert(sparkError.getMessageParameters.get("config") == "\"spark.sql.ansi.enabled\"")
    assert(sparkError.getQueryContext.isEmpty)
    assert(sparkError.getCause eq native)
  }

  test("finds the error code in a cause") {
    val native = new GlutenException(veloxMessage("SPARK:DIVIDE_BY_ZERO", "division by zero"))
    val wrapper = new RuntimeException("wrapper", native)
    val translated = NativeErrorTranslator.translate(wrapper)
    assert(translated.isInstanceOf[SparkArithmeticException])
    assert(translated.getCause eq wrapper)
  }

  test("returns null for errors without a supported Spark error condition") {
    Seq(
      veloxMessage("INVALID_ARGUMENT", "Arithmetic overflow: -2147483648 / -1"),
      veloxMessage("SPARK:NOT_A_SUPPORTED_CONDITION", "error"),
      veloxMessage("SPARK:", "error"),
      "no error code",
      null
    ).foreach {
      message =>
        assert(NativeErrorTranslator.translate(new GlutenException(message)) == null, message)
    }
  }

  test("ignores an error code in the reason") {
    val message =
      veloxMessage("INVALID_ARGUMENT", "Cannot cast 'x\nError Code: SPARK:DIVIDE_BY_ZERO'")
    assert(NativeErrorTranslator.sparkCondition(message).isEmpty)
    assert(NativeErrorTranslator.translate(new GlutenException(message)) == null)
  }

  test("stops on a cyclic cause chain") {
    val first = new RuntimeException("first")
    val second = new RuntimeException("second", first)
    first.initCause(second)
    assert(NativeErrorTranslator.translate(first) == null)
  }
}

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
package org.apache.gluten.vectorized

import org.apache.gluten.exception.{GlutenException, NativeCastException}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.nio.charset.StandardCharsets.UTF_8

class ColumnarBatchOutIteratorExceptionSuite extends SparkFunSuite {
  private def iterator(error: RuntimeException): ColumnarBatchOutIterator = {
    new ColumnarBatchOutIterator(null, 0L) {
      override def hasNext0(): Boolean = throw error
      override def next0(): ColumnarBatch = throw error
    }
  }

  private val overflow =
    "Cannot cast INTEGER '2147483647' to TINYINT. Overflow during arithmetic conversion: "

  test("specific native cast translation precedes GlutenException pass-through") {
    Seq(true, false).foreach {
      hasNext =>
        val original =
          new NativeCastException(overflow.getBytes(UTF_8), "native trace".getBytes(UTF_8))
        val it = iterator(original)
        val error = intercept[ArithmeticException] {
          if (hasNext) it.hasNext() else it.next()
        }
        assert(error.getClass.getName == "org.apache.spark.SparkArithmeticException")
        assert(error.getCause eq original)
        assert(error.getCause.getMessage == "native trace")
    }
  }

  test("an existing GlutenException with cast-like text is returned unchanged") {
    val original = new GlutenException(overflow)
    val error = intercept[GlutenException] {
      iterator(original).hasNext()
    }
    assert(error eq original)
  }

  test("a Spark exception from a nested iterator is returned unchanged") {
    val original =
      new NativeCastException(overflow.getBytes(UTF_8), "native trace".getBytes(UTF_8))
    val translated = intercept[ArithmeticException](iterator(original).next())
    Seq(true, false).foreach {
      hasNext =>
        val outer = iterator(translated)
        val error = intercept[ArithmeticException] {
          if (hasNext) outer.hasNext() else outer.next()
        }
        assert(error eq translated)
    }
  }

  test("untranslated native casts retain their original exception and diagnostic") {
    val diagnostic = "Not a valid type for x".getBytes(UTF_8)
    val validForSpark = "Cannot cast VARCHAR '\u000042' to INTEGER. Invalid leading character: \"\""
    Seq("unknown", validForSpark).foreach {
      reason =>
        val original = new NativeCastException(reason.getBytes(UTF_8), diagnostic)
        val error = intercept[NativeCastException](iterator(original).next())
        assert(error eq original)
    }
  }

  test("native string input survives UTF-8 transport and translation") {
    // scalastyle:off nonascii
    val value = "bad'\"\\\n\r\t\u0000\u00e9\uD83D\uDE00"
    // scalastyle:on nonascii
    val reason = s"""Cannot cast VARCHAR '$value' to INTEGER. Invalid leading character: """""
    val original = new NativeCastException(reason.getBytes(UTF_8), "native trace".getBytes(UTF_8))
    val error = intercept[NumberFormatException](iterator(original).next())
    assert(original.getReason == reason)
    assert(error.getCause eq original)
  }

  test("raw exceptions are still wrapped with their original cause") {
    val original = new IllegalArgumentException(overflow)
    val error = intercept[GlutenException] {
      iterator(original).next()
    }
    assert(error.getCause eq original)
  }

  test("schema conversion precedes GlutenException pass-through and preserves the cause") {
    Seq("not allowed for requested type", "Not a valid type for").foreach {
      message =>
        val original = new GlutenException(message)
        val error = intercept[SchemaColumnConvertNotSupportedException] {
          iterator(original).hasNext()
        }
        assert(error.getCause eq original)
    }
  }
}

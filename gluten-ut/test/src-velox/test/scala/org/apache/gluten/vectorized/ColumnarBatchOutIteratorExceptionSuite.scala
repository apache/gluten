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

  test("hasNext and next translate typed native cast metadata and retain the original cause") {
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

  test("an ordinary native error with cast-like diagnostic text is not translated") {
    val original = new GlutenException(overflow)
    val error = intercept[GlutenException] {
      iterator(original).hasNext()
    }
    assert(error.getCause eq original)
  }

  test("unknown native cast reasons keep the existing GlutenException fallback") {
    val original =
      new NativeCastException("unknown".getBytes(UTF_8), "native trace".getBytes(UTF_8))
    val error = intercept[GlutenException] {
      iterator(original).next()
    }
    assert(error.getClass == classOf[GlutenException])
    assert(error.getCause eq original)
  }

  test("schema conversion errors still use the existing translator and preserve their cause") {
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

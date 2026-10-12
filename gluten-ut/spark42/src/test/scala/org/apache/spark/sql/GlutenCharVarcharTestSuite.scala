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
package org.apache.spark.sql

import org.apache.spark.{SparkException, SparkRuntimeException, SparkThrowable}

trait GlutenCharVarcharTestSuite extends CharVarcharTestSuite with GlutenSQLTestsTrait {
  protected val ERROR_MESSAGE =
    "Exceeds char/varchar type length limitation: 5"

  protected val VELOX_ERROR_MESSAGE =
    "Exceeds allowed length limitation: 5"

  override def assertLengthCheckFailure(func: () => Unit): Unit = {
    val e = intercept[SparkThrowable](func())
    e match {
      // Spark throws exception
      case _: SparkRuntimeException =>
        checkError(
          exception = e,
          condition = "EXCEED_LIMIT_LENGTH",
          parameters = Map("limit" -> "5")
        )
      // Gluten throws exception. but sometimes, Spark exception is wrapped in GlutenException.
      case e: SparkException =>
        assert(e.getMessage.contains(VELOX_ERROR_MESSAGE) || e.getMessage.contains(ERROR_MESSAGE))
      case _ => throw new RuntimeException(s"Unexpected exception: $e")
    }
  }
}

class GlutenFileSourceCharVarcharTestSuite
  extends FileSourceCharVarcharTestSuite
  with GlutenCharVarcharTestSuite {
  private def testTableWrite(f: String => Unit): Unit = {
    withTable("t")(f("char"))
    withTable("t")(f("varchar"))
  }

  testGluten("length check for input string values: nested in array") {
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c ARRAY<$typeName(5)>) USING $format")
        sql("INSERT INTO t VALUES (array(null))")
        checkAnswer(spark.table("t"), Row(Seq(null)))
        val e = intercept[SparkException] {
          sql("INSERT INTO t VALUES (array('a', '123456'))")
        }
        assert(e.getMessage.contains(VELOX_ERROR_MESSAGE))
    }
  }
}

class GlutenDSV2CharVarcharTestSuite
  extends DSV2CharVarcharTestSuite
  with GlutenCharVarcharTestSuite {}

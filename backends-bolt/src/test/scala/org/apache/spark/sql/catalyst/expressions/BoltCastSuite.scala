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

import org.apache.gluten.execution.BoltWholeStageTransformerSuite

import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.UTC_OPT
import org.apache.spark.sql.types._

import java.sql.Timestamp
import java.util.TimeZone

class BoltCastSuite extends BoltWholeStageTransformerSuite with ExpressionEvalHelper {
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

  override protected val resourcePath: String = "N/A"
  override protected val fileFormat: String = "N/A"
}

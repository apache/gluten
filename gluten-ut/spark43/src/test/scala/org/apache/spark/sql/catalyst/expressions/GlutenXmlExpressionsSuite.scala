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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{GenericArrayData, SparkDateTimeUtils}
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.UTC_OPT
import org.apache.spark.sql.shim
import org.apache.spark.sql.types.{ArrayType, StructField, StructType, TimeType}
import org.apache.spark.unsafe.types.UTF8String

class GlutenXmlExpressionsSuite extends XmlExpressionsSuite with shim.GlutenTestsTrait {

  // Dataset.collect() cannot deserialize ARRAY<TIME(p)> with p < 6 into external rows because
  // Spark's UpCastRule has no TIME(p) -> TIME(6) rule, so compare the collected internal rows.
  private def checkEvaluationOnInternalRows(expression: Expression, expected: Any): Unit = {
    val expr = resolveExpression(expression)
    val rows = buildResultDF(expr, EmptyRow).queryExecution.executedPlan.executeCollect()
    assert(rows.length == 1)
    val actual = rows.head.get(0, expr.dataType)
    assert(
      checkResult(actual, expected, expr.dataType, expr.nullable),
      s"Incorrect evaluation: $expr, actual: $actual, expected: $expected")
  }

  testGluten("TIME type with arrays") {
    val schema = StructType(StructField("times", ArrayType(TimeType(3))) :: Nil)
    val time1 = SparkDateTimeUtils.stringToTimeAnsi(UTF8String.fromString("09:00:00"))
    val time2 = SparkDateTimeUtils.stringToTimeAnsi(UTF8String.fromString("17:30:45.123"))
    val times = new GenericArrayData(Array(time1, time2))

    val xmlInput = """<record><times>09:00:00</times><times>17:30:45.123</times></record>"""

    // Test parsing array
    checkEvaluationOnInternalRows(
      XmlToStructs(schema, Map("rowTag" -> "record"), Literal(xmlInput), UTC_OPT),
      InternalRow(times)
    )

    // Test roundtrip
    val struct = Literal.create(InternalRow(times), schema)
    val xmlResult = StructsToXml(Map("rowTag" -> "record"), struct, UTC_OPT)
    checkEvaluationOnInternalRows(
      XmlToStructs(schema, Map("rowTag" -> "record"), xmlResult, UTC_OPT),
      InternalRow(times)
    )
  }
}

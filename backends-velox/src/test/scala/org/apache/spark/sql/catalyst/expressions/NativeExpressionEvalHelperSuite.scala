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

import org.apache.gluten.execution.VeloxWholeStageTransformerSuite
import org.apache.gluten.utils.Arm

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, TimestampType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.task.TaskResources

class NativeExpressionEvalHelperSuite
  extends VeloxWholeStageTransformerSuite
  with NativeExpressionEvalHelper {
  override protected val resourcePath: String = "N/A"
  override protected val fileFormat: String = "N/A"

  test("invalid inputs and incorrect native results remain failures") {
    TaskResources.runUnsafe {
      intercept[IllegalArgumentException] {
        prepareNativeExpression(Seq(BoundReference(0, StringType, nullable = true)), Seq.empty)
      }
      Arm.withResource(new ColumnarBatch(Array.empty[ColumnVector], 1)) {
        input =>
          intercept[org.scalatest.exceptions.TestFailedException] {
            checkEvaluationWithNative(Literal("actual"), Seq("wrong"), input, Seq.empty)
          }
      }
    }
  }

  test("closed native preparation rejects evaluation and close is idempotent") {
    TaskResources.runUnsafe {
      Arm.withResource(new ColumnarBatch(Array.empty[ColumnVector], 1)) {
        input =>
          val prepared = prepareNativeExpression(Seq(Literal("value")), Seq.empty)
          prepared.close()
          prepared.close()
          val error = intercept[IllegalArgumentException](prepared.evaluate(input))
          assert(error.getMessage.contains("closed"))
      }
    }
  }

  test("repeated evaluation owns outputs and preserves borrowed null and empty inputs") {
    TaskResources.runUnsafe {
      Arm.withResource(new ColumnarBatch(Array.empty[ColumnVector], 2)) {
        constantInput =>
          val attribute = AttributeReference("value", StringType, nullable = true)()
          Arm.withResource(prepareNativeExpression(Seq(attribute), Seq(attribute))) {
            identity =>
              Seq("value", null, "", "next", null).foreach {
                expected =>
                  val output = Arm.withResource(
                    evaluateWithNative(
                      Literal.create(expected, StringType),
                      constantInput,
                      Seq.empty)) {
                    input =>
                      val first = identity.evaluate(input)
                      first.close()
                      withReadableBatch(input) {
                        readable =>
                          val value = readable.getRow(0)
                          assert(if (expected == null) value.isNullAt(0)
                          else value.getUTF8String(0).toString == expected)
                      }
                      identity.evaluate(input)
                  }
                  Arm.withResource(output) {
                    result =>
                      withReadableBatch(result) {
                        readable =>
                          assert(readable.numRows() == 2)
                          (0 until 2).foreach {
                            i =>
                              val row = readable.getRow(i)
                              assert(if (expected == null) row.isNullAt(0)
                              else row.getUTF8String(0).toString == expected)
                          }
                      }
                  }
              }
          }
      }
    }
  }

  test("native evaluation returns null for division by zero with ANSI disabled") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      TaskResources.runUnsafe {
        Arm.withResource(new ColumnarBatch(Array.empty[ColumnVector], 1)) {
          constantInput =>
            Arm.withResource(evaluateWithNative(Literal(1), constantInput, Seq.empty)) {
              input =>
                val attribute = AttributeReference("value", IntegerType, nullable = false)()
                checkEvaluationWithNative(
                  Remainder(attribute, Literal(0)),
                  Seq(null),
                  input,
                  Seq(attribute))
            }
        }
      }
    }
  }

  Seq("UTC" -> "1970-01-01 00:00:00", "Asia/Tokyo" -> "1970-01-01 09:00:00").foreach {
    case (zone, expected) =>
      test(s"native evaluation honors the session time zone: $zone") {
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> zone) {
          TaskResources.runUnsafe {
            Arm.withResource(new ColumnarBatch(Array.empty[ColumnVector], 1)) {
              constantInput =>
                Arm.withResource(
                  evaluateWithNative(Literal(0L, TimestampType), constantInput, Seq.empty)) {
                  input =>
                    val attribute = AttributeReference("value", TimestampType, nullable = false)()
                    val expression =
                      DateFormatClass(attribute, Literal("yyyy-MM-dd HH:mm:ss"), Some(zone))
                    checkEvaluationWithNative(expression, Seq(expected), input, Seq(attribute))
                }
            }
          }
        }
      }
  }

  test("compares native strings, nulls and nested values without consuming the input") {
    TaskResources.runUnsafe {
      Arm.withResource(new ColumnarBatch(Array.empty[ColumnVector], 3)) {
        input =>
          val values = Seq(
            ("spark", StringType),
            (null, StringType),
            (Seq("spark", null), ArrayType(StringType)))
          values.foreach {
            case (value, dataType) =>
              val expression = Literal.create(value, dataType)
              checkEvaluationWithNative(expression, Seq.fill(3)(value), input, Seq.empty)
              Arm.withResource(prepareNativeExpression(Seq(expression), Seq.empty)) {
                prepared =>
                  Arm.withResource(prepared.evaluate(input)) {
                    output =>
                      val attribute = AttributeReference("value", dataType)()
                      checkEvaluationWithNative(
                        BoundReference(0, dataType, nullable = true),
                        Seq.fill(3)(value),
                        output,
                        Seq(attribute))
                      assert(output.numRows() == 3)
                  }
              }
          }
          assert(input.numRows() == 3)
      }
    }
  }
}

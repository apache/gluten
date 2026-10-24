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
package org.apache.spark.sql.execution.benchmark.expression

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{NativeExpressionEvalHelper, RowOrdering, UnsafeProjection}
import org.apache.spark.sql.types._
import org.apache.spark.task.TaskResources

import java.lang.{Double => JDouble}

/** Verification stays outside benchmark preparation and timed execution. */
private[expression] object ExpressionResultChecks extends SparkFunSuite
  with NativeExpressionEvalHelper {

  def withCollected[T <: ExpressionEvaluator](
      create: (InternalRow => Unit, InternalRow => Unit) => T)(
      test: (T, ((Long, InternalRow) => Any) => Unit) => Unit): Unit = {
    val expected = Vector.newBuilder[InternalRow]
    val actual = Vector.newBuilder[InternalRow]
    val evaluator = create(row => { expected += row }, row => { actual += row })
    def check(result: (Long, InternalRow) => Any): Unit = {
      expected.clear()
      actual.clear()
      evaluator.runVanilla()
      evaluator.runNative()
      assert(evaluator.outputSchema.nonEmpty)
      verify(evaluator, expected.result(), actual.result())(result)

    }
    TaskResources.runUnsafe(test(evaluator, check))
  }

  def verify(
      evaluator: ExpressionEvaluator,
      vanilla: Vector[InternalRow],
      native: Vector[InternalRow])(
      result: (Long, InternalRow) => Any = (_, _) => ()): Unit = {
    val schema = evaluator.outputSchema
    val (expected, actual, comparable, canMatchGroups) = evaluator match {
      case window if window.plan.window =>
        assert(vanilla.size == window.numRows && native.size == window.numRows)
        val ordering =
          RowOrdering.createNaturalAscendingOrdering(schema.fields.map(_.dataType).toSeq)
        (vanilla.sorted(ordering), native.sorted(ordering), window.plan.outputDeterminism, true)
      case scalar if !scalar.plan.aggregate =>
        checkScalarInputs(scalar, vanilla.size, native.size)
        (vanilla, native, scalar.plan.outputDeterminism, true)
      case aggregate =>
        val flags = aggregate.plan.outputDeterminism
        val keyCount = aggregate.plan.groupingExpressions.size
        val canMatchGroups = flags.take(keyCount).forall(identity)
        if (canMatchGroups) {
          val keyTypes = schema.fields.take(keyCount).map(_.dataType)
          val ordering = RowOrdering.createNaturalAscendingOrdering(keyTypes.toSeq)
          (vanilla.sorted(ordering), native.sorted(ordering), flags, true)
        } else (vanilla, native, flags, false)
    }
    require(comparable.size == schema.length, "Output comparison flags differ from schema")
    val allComparable = comparable.forall(identity)
    if (!canMatchGroups || !allComparable) {
      Seq(expected, actual).foreach(_.foreach {
        row => assert(checkResult(row, row, schema, false), "Invalid result structure")
      })
    }
    if (canMatchGroups) {
      assert(expected.size == actual.size, "Result row counts differ")
      val columns = schema.fields.indices.filter(comparable)
      if (allComparable || columns.nonEmpty) {
        expected.indices.foreach {
          i =>
            withClue(s"Result $i: ") {
              if (allComparable) {
                assert(
                  checkResult(actual(i), expected(i), schema, false),
                  s"Native result differs at row $i")
              } else {
                columns.foreach {
                  column =>
                    val field = schema(column)
                    assert(
                      checkResult(
                        actual(i).get(column, field.dataType),
                        expected(i).get(column, field.dataType),
                        field.dataType,
                        field.nullable),
                      s"Native result differs at column $column"
                    )
                }
              }
            }
        }
      }
    }
    expected.indices.foreach(i => result(i.toLong, expected(i)))
  }

  private def checkScalarInputs(
      scalar: ExpressionEvaluator,
      vanillaRows: Int,
      nativeRows: Int): Unit = {
    val inputProjection = UnsafeProjection.create(scalar.dataPlan.inputSchema.asNullable)
    inputProjection.initialize(0)
    val inputs = scalar.inputRows.map(inputProjection(_))
    val batches = scalar.nativeInputBatches
    val inputSchema = scalar.dataPlan.inputSchema
    assert(
      batches.map(_.numRows()) == (0 until vanillaRows by scalar.batchSize).map(
        start => math.min(scalar.batchSize, vanillaRows - start)),
      "Native batch sizes differ"
    )
    val read = UnsafeProjection.create(inputSchema.asNullable)
    read.initialize(0)
    var offset = 0
    batches.foreach {
      input =>
        withReadableBatch(input) {
          readable =>
            assert(readable.numRows() == input.numRows())
            assert(readable.numCols() == inputSchema.length)
            (0 until input.numRows()).foreach {
              i =>
                assert(
                  inputs.hasNext &&
                    checkResult(read(readable.getRow(i)), inputs.next(), inputSchema, false),
                  s"Native input differs at ${offset + i}")
            }
        }
        offset += input.numRows()
    }
    assert(
      !inputs.hasNext && offset == nativeRows,
      "Native inputs do not cover every logical row")
  }

  override def checkResult(
      actual: Any,
      expected: Any,
      dataType: DataType,
      nullable: Boolean): Boolean = (actual, expected, dataType) match {
    case (a: Float, b: Float, FloatType) =>
      super.checkResult(actual, expected, dataType, nullable) ||
      withinTolerance(a.toDouble, b.toDouble, absolute = 1e-6, relative = 1e-5)
    case (a: Double, b: Double, DoubleType) =>
      super.checkResult(actual, expected, dataType, nullable) || withinTolerance(a, b)
    case _ => super.checkResult(actual, expected, dataType, nullable)
  }

  private def withinTolerance(
      a: Double,
      b: Double,
      absolute: Double = 1e-12,
      relative: Double = 1e-12): Boolean =
    JDouble.isFinite(a) && JDouble.isFinite(b) &&
      math.abs(a - b) <= math.max(absolute, relative * math.max(math.abs(a), math.abs(b)))
}

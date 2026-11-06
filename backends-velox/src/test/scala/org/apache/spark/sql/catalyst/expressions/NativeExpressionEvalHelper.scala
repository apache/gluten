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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.{ColumnarBatches, ColumnarBatchJniWrapper}
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.substrait.`type`.TypeBuilder
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.utils.{Arm, SubstraitUtil}
import org.apache.gluten.vectorized.VeloxExpressionEvaluatorJniWrapper

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.task.TaskResources
import org.apache.spark.util.Utils

import io.substrait.proto.{ExpressionReference, ExtendedExpression, Type}
import org.scalatest.Assertions.cancel

import scala.collection.JavaConverters._

/** Compile inside the caller's live TaskResources scope, which also owns cleanup. */
final private[spark] class NativeExpressionEvaluator(
    expressions: Seq[Expression],
    inputAttributes: Seq[Attribute])
  extends AutoCloseable {
  private val numInputColumns = inputAttributes.size
  private val (backendName, jni, handle) = {
    require(expressions.size == 1, "Native preparation supports exactly one output expression")
    require(
      TaskResources.inSparkTask(),
      "Native expression evaluation requires a TaskResources.runUnsafe scope")
    val expression = expressions.head
    val attributes = inputAttributes
    require(expression.resolved, s"Unresolved native expression: $expression")
    expression.foreach {
      case ref: BoundReference =>
        require(
          ref.ordinal >= 0 && ref.ordinal < attributes.size,
          s"Input ordinal ${ref.ordinal} is outside ${attributes.size} columns")
        require(
          ref.dataType == attributes(ref.ordinal).dataType,
          s"Input type mismatch for ordinal ${ref.ordinal}")
      case _ =>
    }

    val context = new SubstraitContext
    val expressionNode =
      try {
        SubstraitUtil.toSubstraitExpression(expression, attributes, context)
      } catch {
        case unsupported: GlutenNotSupportException => cancel(unsupported.getMessage, unsupported)
      }
    val inputType =
      TypeBuilder.makeStruct(
        false,
        ConverterUtils.collectAttributeTypeNodes(attributes.asJava))
    if (
      !BackendsApiManager
        .getValidatorApiInstance
        .doNativeValidateExpression(context, expressionNode, inputType)
    ) {
      cancel(s"Native validation does not support: ${expression.sql}")
    }
    val schema = SubstraitUtil.toNameStruct(attributes.asJava).toBuilder
    schema.getStructBuilder.setNullability(Type.Nullability.NULLABILITY_REQUIRED)
    val extensions = context.registeredFunction.asScala.map {
      case (name, id) => ExtensionBuilder.makeFunctionMapping(name, id).toProtobuf
    }
    val serialized = ExtendedExpression
      .newBuilder()
      .setBaseSchema(schema)
      .addAllExtensions(extensions.toSeq.asJava)
      .addReferredExpr(
        ExpressionReference
          .newBuilder()
          .setExpression(expressionNode.toProtobuf)
          .addOutputNames("result"))
      .build()
      .toByteArray
    val backend = BackendsApiManager.getBackendName
    val runtime = Runtimes.contextInstance(backend, "NativeExpressionEvalHelper")
    val evaluator = VeloxExpressionEvaluatorJniWrapper.create(runtime)
    (backend, evaluator, evaluator.compile(serialized))
  }
  private var closed = false
  TaskResources.addRecycler("Native expression evaluator", 100) {
    close()
  }

  def evaluate(input: ColumnarBatch): ColumnarBatch = {
    require(!closed, "Native expression evaluator is closed")
    require(input.numCols() == numInputColumns, "Input columns and attributes must match")
    ColumnarBatches.checkOffloaded(input)
    val inputHandle = ColumnarBatches.getNativeHandle(backendName, input)
    Utils.tryWithSafeFinally {
      ColumnarBatches.create(jni.evaluate(handle, inputHandle))
    } {
      // Zero-column lookup owns a fresh handle; nonempty lookup borrows the input's handle.
      if (input.numCols() == 0) ColumnarBatchJniWrapper.close(inputHandle)
    }
  }

  override def close(): Unit = {
    if (!closed) {
      jni.close(handle)
      closed = true
    }
  }
}

trait NativeExpressionEvalHelper extends ExpressionEvalHelper {
  self: SparkFunSuite =>

  protected def prepareNativeExpression(
      expressions: Seq[Expression],
      inputAttributes: Seq[Attribute]): NativeExpressionEvaluator =
    new NativeExpressionEvaluator(expressions, inputAttributes)

  protected def evaluateWithNative(
      expression: Expression,
      input: ColumnarBatch,
      inputAttributes: Seq[Attribute]): ColumnarBatch = {
    Arm.withResource(prepareNativeExpression(Seq(expression), inputAttributes))(_.evaluate(input))
  }

  /** Loads a separate batch handle because loading consumes the native handle it receives. */
  protected def withReadableBatch[T](batch: ColumnarBatch)(f: ColumnarBatch => T): T = {
    if (batch.numCols() == 0) {
      f(batch)
    } else {
      Arm.withResource(
        ColumnarBatches
          .select(
            BackendsApiManager.getBackendName,
            batch,
            (0 until batch.numCols()).toArray)) {
        readable =>
          ColumnarBatches.load(ArrowBufferAllocators.contextInstance(), readable)
          f(readable)
      }
    }
  }

  protected def checkEvaluationWithNative(
      expression: Expression,
      expected: Seq[Any],
      input: ColumnarBatch,
      inputAttributes: Seq[Attribute]): Unit = {
    val catalystValues = expected.map(CatalystTypeConverters.convertToCatalyst)
    require(catalystValues.size == input.numRows(), "Expected values must cover every input row")
    Arm.withResource(evaluateWithNative(expression, input, inputAttributes)) {
      output =>
        assert(output.numCols() == 1, "Native expression must produce one result column")
        assert(output.numRows() == catalystValues.size, "Native result row count differs")
        withReadableBatch(output) {
          readable =>
            catalystValues.indices.foreach {
              rowIndex =>
                val row = readable.getRow(rowIndex)
                val actual = if (row.isNullAt(0)) null else row.get(0, expression.dataType)
                withClue(s"Native row $rowIndex: expression=$expression, actual=$actual. ") {
                  assert(checkResult(actual, catalystValues(rowIndex), expression))
                }
            }
        }
    }
  }
}

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
package org.apache.gluten.backendsapi.velox

import org.apache.gluten.backendsapi.{BackendsApiManager, ValidatorApi}
import org.apache.gluten.config.VeloxConfig
import org.apache.gluten.execution.ValidationResult
import org.apache.gluten.expression.{ConverterUtils, LiteralTransformer}
import org.apache.gluten.substrait.`type`.TypeNode
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.ExpressionNode
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.substrait.plan.PlanNode
import org.apache.gluten.validate.NativePlanValidationInfo
import org.apache.gluten.vectorized.NativePlanEvaluator

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, EvalMode, Expression, Literal, Pmod}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types._
import org.apache.spark.task.TaskResources

import io.substrait.proto.SimpleExtensionDeclaration

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

class VeloxValidatorApi extends ValidatorApi with Logging {
  import VeloxValidatorApi._

  /** For velox backend, key validation is on native side. */
  override def doExprValidate(substraitExprName: String, expr: Expression): Boolean = expr match {
    case pmod: Pmod => validatePmod(pmod)
    case _ => true
  }

  private def validatePmod(pmod: Pmod): Boolean = {
    val divisor = pmod.dataType match {
      case ByteType => Literal(1.toByte)
      case ShortType => Literal(1.toShort)
      case IntegerType => Literal(1)
      case LongType => Literal(1L)
      case FloatType => Literal(1.0f)
      case DoubleType => Literal(1.0d)
      case _ =>
        logDebug("Native pmod supports only primitive numeric types; falling back to Spark.")
        return false
    }
    if (
      pmod.left.dataType != pmod.right.dataType ||
      (pmod.evalMode != EvalMode.LEGACY && pmod.evalMode != EvalMode.ANSI)
    ) {
      logDebug(
        "Native pmod requires coerced operands and LEGACY or ANSI mode; falling back to Spark.")
      return false
    }
    val harmlessLeft = pmod.left match {
      case _: Literal | _: Attribute | _: BoundReference => true
      case _ => false
    }
    if (
      pmod.evalMode == EvalMode.ANSI &&
      !pmod.left.nullable && !pmod.right.nullable && !harmlessLeft
    ) {
      // Spark's interpreter and generated code choose different errors for these expressions.
      logDebug(
        "Native pmod cannot select Spark's composed error precedence; falling back to Spark.")
      return false
    }

    val context = new SubstraitContext
    val transformer = new VeloxSparkPlanExecApi().genPmodTransformer(
      "pmod",
      LiteralTransformer(Literal.default(pmod.dataType)),
      LiteralTransformer(divisor),
      pmod)
    try {
      val supported = doNativeValidateExpression(
        context,
        transformer.doTransform(context),
        ConverterUtils.getTypeNode(StructType(Nil), nullable = false))
      if (!supported) {
        logDebug("Native captured-mode pmod capability is unavailable; falling back to Spark.")
      }
      supported
    } catch {
      case NonFatal(error) =>
        logWarning("Could not validate native pmod capability; falling back to Spark.", error)
        false
    }
  }

  override def doNativeValidateWithFailureReason(plan: PlanNode): ValidationResult = {
    TaskResources.runUnsafe {
      val validator = NativePlanEvaluator.create(BackendsApiManager.getBackendName)
      asValidationResult(validator.doNativeValidateWithFailureReason(plan.toProtobuf.toByteArray))
    }
  }

  override def doNativeValidateExpression(
      substraitContext: SubstraitContext,
      expression: ExpressionNode,
      inputTypeNode: TypeNode): Boolean = {
    TaskResources.runUnsafe {
      val validator = NativePlanEvaluator.create(BackendsApiManager.getBackendName)
      val extensionNodes =
        new ArrayBuffer[SimpleExtensionDeclaration](substraitContext.registeredFunction.size)
      substraitContext.registeredFunction.forEach {
        (key, value) =>
          extensionNodes.append(ExtensionBuilder.makeFunctionMapping(key, value).toProtobuf)
      }
      validator.doNativeValidateExpression(
        expression.toProtobuf.toByteArray,
        inputTypeNode.toProtobuf.toByteArray,
        extensionNodes.map(_.toByteArray).toArray)
    }
  }

  private def asValidationResult(info: NativePlanValidationInfo): ValidationResult = {
    if (info.isSupported == 1) {
      return ValidationResult.succeeded
    }
    ValidationResult.failed(
      String.format(
        "Native validation failed: %n   |- %s",
        info.fallbackInfo.asScala.reduce[String] { case (l, r) => l + "\n   |- " + r }))
  }

  override def doSchemaValidate(schema: DataType): Option[String] = {
    validateSchema(schema)
  }

  override def doColumnarShuffleExchangeExecValidate(
      outputAttributes: Seq[Attribute],
      outputPartitioning: Partitioning,
      child: SparkPlan): Option[String] = {
    if (!BackendsApiManager.getSettings.supportEmptySchemaColumnarShuffle()) {
      if (outputAttributes.isEmpty) {
        // See: https://github.com/apache/gluten/issues/7600.
        return Some("Shuffle with empty output schema is not supported")
      }
      if (child.output.isEmpty) {
        // See: https://github.com/apache/gluten/issues/7600.
        return Some("Shuffle with empty input schema is not supported")
      }
    }
    doSchemaValidate(child.schema)
  }
}

object VeloxValidatorApi {
  private def isPrimitiveType(dataType: DataType): Boolean = {
    val enableTimestampNtzValidation = VeloxConfig.get.enableTimestampNtzValidation
    dataType match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
          StringType | BinaryType | _: DecimalType | DateType | TimestampType |
          YearMonthIntervalType.DEFAULT | NullType =>
        true
      case dt if !enableTimestampNtzValidation && dt.catalogString == "timestamp_ntz" =>
        // Allow TimestampNTZ when validation is disabled (for development/testing)
        true
      case _ => false
    }
  }

  def validateSchema(schema: DataType): Option[String] = {
    if (isPrimitiveType(schema)) {
      return None
    }
    schema match {
      case map: MapType =>
        validateSchema(map.keyType).orElse(validateSchema(map.valueType))
      case struct: StructType =>
        // Detect variant shredded struct produced by Spark's PushVariantIntoScan.
        // These structs have all fields annotated with __VARIANT_METADATA_KEY metadata.
        // Velox cannot read the variant shredding encoding in Parquet files.
        if (
          struct.fields.nonEmpty &&
          struct.fields.forall(_.metadata.contains("__VARIANT_METADATA_KEY"))
        ) {
          return Some(s"Variant shredded struct is not supported: $struct")
        }
        struct.foreach {
          field =>
            val reason = validateSchema(field.dataType)
            if (reason.isDefined) {
              return reason
            }
        }
        None
      case array: ArrayType =>
        validateSchema(array.elementType)
      case _ =>
        Some(s"Schema / data type not supported: $schema")
    }
  }
}

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
import org.apache.spark.sql.catalyst.expressions.{Attribute, BRound, Expression, Literal, Round}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types._
import org.apache.spark.task.TaskResources

import io.substrait.proto.SimpleExtensionDeclaration

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Properties
import scala.util.control.NonFatal

class VeloxValidatorApi extends ValidatorApi with Logging {
  import VeloxValidatorApi._

  /** For velox backend, key validation is on native side. */
  override def doExprValidate(substraitExprName: String, expr: Expression): Boolean = {
    expr match {
      case round: Round =>
        round.scale match {
          case scale @ Literal(null, IntegerType) =>
            validateRoundCapability(round, scale)
          case literal @ Literal(scale: Int, IntegerType) =>
            val supportedScale = scale >= MIN_ROUNDING_SCALE && scale <= MAX_ROUNDING_SCALE
            val needsModernJava = scale != 0 &&
              (round.child.dataType == FloatType || round.child.dataType == DoubleType)
            if (
              !supportedScale ||
              (needsModernJava && !Properties.isJavaAtLeast(MIN_ROUNDING_FLOATING_JAVA_VERSION))
            ) {
              logDebug(
                "round scale or JVM decimal conversion is unsupported; falling back to Spark.")
              false
            } else {
              validateRoundCapability(round, literal)
            }
          case _ =>
            logDebug("round scale must be a folded INTEGER literal; falling back to Spark.")
            false
        }
      case round: BRound =>
        round.scale match {
          case Literal(null, IntegerType) => true
          case Literal(scale: Int, IntegerType) =>
            if (scale < MIN_ROUNDING_SCALE || scale > MAX_ROUNDING_SCALE) {
              logDebug(
                s"bround scale $scale is outside the native " +
                  s"[$MIN_ROUNDING_SCALE, $MAX_ROUNDING_SCALE] interval; " +
                  "falling back to Spark.")
              false
            } else if (
              scale != 0 &&
              (round.child.dataType == FloatType || round.child.dataType == DoubleType) &&
              !Properties.isJavaAtLeast(MIN_ROUNDING_FLOATING_JAVA_VERSION)
            ) {
              logDebug(
                "Floating-point bround with nonzero scale requires " +
                  s"Java $MIN_ROUNDING_FLOATING_JAVA_VERSION or later " +
                  "for matching decimal conversion; falling back to Spark.")
              false
            } else {
              true
            }
          case _ =>
            logDebug("bround scale must be a folded INTEGER literal; falling back to Spark.")
            false
        }
      case _ => true
    }
  }

  private def validateRoundCapability(round: Round, scale: Literal): Boolean = {
    // Bare round is present in older dependencies and may be shadowed by Gluten's overlay.
    val value = Literal.default(round.child.dataType)
    val probe = new Round(value, scale, round.ansiEnabled)
    val context = new SubstraitContext
    val transformer = new VeloxSparkPlanExecApi().genRoundTransformer(
      "round",
      Seq(LiteralTransformer(value), LiteralTransformer(scale)),
      probe)
    try {
      val supported = doNativeValidateExpression(
        context,
        transformer.doTransform(context),
        ConverterUtils.getTypeNode(StructType(Nil), nullable = false))
      if (!supported) {
        logDebug("Native Spark-compatible round capability is unavailable; falling back to Spark.")
      }
      supported
    } catch {
      case NonFatal(error) =>
        logWarning("Could not validate native round capability; falling back to Spark.", error)
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
  val MIN_ROUNDING_SCALE: Int = -400
  val MAX_ROUNDING_SCALE: Int = 400
  val MIN_ROUNDING_FLOATING_JAVA_VERSION: String = "21"

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

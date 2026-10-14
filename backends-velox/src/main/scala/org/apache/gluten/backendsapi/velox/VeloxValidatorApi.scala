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
import org.apache.gluten.substrait.`type`.TypeNode
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.ExpressionNode
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.substrait.plan.PlanNode
import org.apache.gluten.validate.NativePlanValidationInfo
import org.apache.gluten.vectorized.NativePlanEvaluator

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, FormatString, Literal}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types._
import org.apache.spark.task.TaskResources
import org.apache.spark.unsafe.types.UTF8String

import io.substrait.proto.SimpleExtensionDeclaration

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class VeloxValidatorApi extends ValidatorApi {
  import VeloxValidatorApi._

  /** For velox backend, key validation is on native side. */
  override def doExprValidate(substraitExprName: String, expr: Expression): Boolean = {
    expr match {
      case formatString: FormatString =>
        supportsFormatString(formatString)
      case _ => true
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
  private val nativeFormatFlags = Set('-', '+', ' ', '0')
  private val maxNativeFormatWidth = 1 << 20

  private def supportsFormatString(formatString: FormatString): Boolean = {
    val format = formatString.children.headOption match {
      case Some(Literal(value: UTF8String, dataType))
          if dataType.isInstanceOf[StringType] =>
        value.toString
      case _ =>
        return false
    }
    val conversions = parseNativeFormatString(format).getOrElse {
      return false
    }
    conversions.length <= formatString.children.length - 1 &&
    conversions
      .zip(formatString.children.tail)
      .forall {
        case (conversion, argument) =>
          supportsNativeFormatType(conversion, argument.dataType)
      }
  }

  private def parseNativeFormatString(format: String): Option[Seq[Char]] = {
    val conversions = ArrayBuffer.empty[Char]
    var index = 0
    while (index < format.length) {
      if (format.charAt(index) != '%') {
        index += 1
      } else if (index + 1 < format.length && format.charAt(index + 1) == '%') {
        index += 2
      } else {
        index += 1
        val flags = scala.collection.mutable.Set.empty[Char]
        while (index < format.length && nativeFormatFlags.contains(format.charAt(index))) {
          if (!flags.add(format.charAt(index))) {
            return None
          }
          index += 1
        }
        val widthStart = index
        while (
          index < format.length && format.charAt(index) >= '0' &&
          format.charAt(index) <= '9'
        ) {
          index += 1
        }
        val width =
          if (widthStart == index) None
          else parseBoundedFormatNumber(format, widthStart, index)
        if (widthStart != index && width.isEmpty) {
          return None
        }
        var precision: Option[Int] = None
        if (index < format.length && format.charAt(index) == '.') {
          index += 1
          val precisionStart = index
          while (
            index < format.length && format.charAt(index) >= '0' &&
            format.charAt(index) <= '9'
          ) {
            index += 1
          }
          if (precisionStart == index) {
            return None
          }
          precision = parseBoundedFormatNumber(format, precisionStart, index)
          if (precision.isEmpty) {
            return None
          }
        }
        if (index >= format.length) {
          return None
        }
        val conversion = format.charAt(index)
        index += 1
        if (!isNativeFormatSpecifierSupported(conversion, flags.toSet, width, precision)) {
          return None
        }
        conversions += conversion
      }
    }
    Some(conversions.toSeq)
  }

  private def parseBoundedFormatNumber(format: String, start: Int, end: Int): Option[Int] = {
    var value = 0
    var index = start
    while (index < end) {
      val digit = format.charAt(index) - '0'
      if (value > (maxNativeFormatWidth - digit) / 10) {
        return None
      }
      value = value * 10 + digit
      index += 1
    }
    Some(value)
  }

  private def isNativeFormatSpecifierSupported(
      conversion: Char,
      flags: Set[Char],
      width: Option[Int],
      precision: Option[Int]): Boolean = {
    if ((flags.contains('-') || flags.contains('0')) && width.isEmpty) {
      return false
    }
    if (
      (flags.contains('-') && flags.contains('0')) ||
      (flags.contains('+') && flags.contains(' '))
    ) {
      return false
    }
    conversion match {
      case 's' => flags.isEmpty && width.isEmpty && precision.isEmpty
      case 'd' => precision.isEmpty
      case 'o' | 'x' | 'X' =>
        precision.isEmpty && !flags.contains('+') && !flags.contains(' ')
      case _ => false
    }
  }

  private def supportsNativeFormatType(conversion: Char, dataType: DataType): Boolean = {
    if (dataType == NullType) {
      true
    } else {
      conversion match {
        case 's' =>
          dataType.isInstanceOf[StringType] ||
          dataType == BooleanType ||
          dataType == ByteType ||
          dataType == ShortType ||
          dataType == IntegerType ||
          dataType == LongType
        case 'd' | 'o' | 'x' | 'X' =>
          dataType == ByteType ||
          dataType == ShortType ||
          dataType == IntegerType ||
          dataType == LongType
        case _ => false
      }
    }
  }

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

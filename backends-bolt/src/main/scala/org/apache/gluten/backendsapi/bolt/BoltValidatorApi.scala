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
package org.apache.gluten.backendsapi.bolt

import org.apache.gluten.backendsapi.{BackendsApiManager, ValidatorApi}
import org.apache.gluten.execution.ValidationResult
import org.apache.gluten.expression.ExpressionNames.GET_TIMESTAMP
import org.apache.gluten.substrait.plan.PlanNode
import org.apache.gluten.validate.NativePlanValidationInfo
import org.apache.gluten.vectorized.NativePlanEvaluator

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.task.TaskResources

import scala.collection.JavaConverters._

class BoltValidatorApi extends ValidatorApi {
  import BoltValidatorApi._

  /** For bolt backend, key validation is on native side. */
  override def doExprValidate(substraitExprName: String, expr: Expression): Boolean = {
    substraitExprName match {
      case GET_TIMESTAMP =>
        if (SQLConf.get.legacyTimeParserPolicy.toString == "EXCEPTION") false
        else true
      case _ => true
    }
  }

  override def doNativeValidateWithFailureReason(plan: PlanNode): ValidationResult = {
    TaskResources.runUnsafe {
      val validator = NativePlanEvaluator.create(BackendsApiManager.getBackendName)
      asValidationResult(validator.doNativeValidateWithFailureReason(plan.toProtobuf.toByteArray))
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
    if (outputAttributes.isEmpty) {
      // See: https://github.com/apache/incubator-gluten/issues/7600.
      return Some("Shuffle with empty output schema is not supported")
    }
    if (child.output.isEmpty) {
      // See: https://github.com/apache/incubator-gluten/issues/7600.
      return Some("Shuffle with empty input schema is not supported")
    }
    doSchemaValidate(child.schema)
  }
}

object BoltValidatorApi {
  private def isPrimitiveType(dataType: DataType): Boolean = {
    dataType match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
          StringType | BinaryType | _: DecimalType | DateType | TimestampType |
          YearMonthIntervalType.DEFAULT | NullType =>
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

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
package org.apache.gluten.expression

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenNotSupportException

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, DecimalType}

/**
 * Transformer for Spark `RoundCeil(decimal, scale)` and `RoundFloor(decimal, scale)`. These power
 * the 2-argument forms of `ceiling(x, scale)` / `floor(x, scale)` and dispatch to the Velox
 * `decimal_ceil` / `decimal_floor` special forms (substrait names `ceil` / `floor`, remapped on the
 * C++ side based on arity + decimal arg type).
 *
 * The output `DataType` is recomputed from the original Spark decimal input type and the constant
 * folded scale, matching Spark's `RoundBase.dataType` formula. Mirrors the structure of
 * `DecimalRoundTransformer`.
 */
case class DecimalCeilFloorTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: Expression,
    scaleExpr: Expression)
  extends BinaryExpressionTransformer {

  // Velox's `decimal_ceil` / `decimal_floor` return NULL when the rounded result exceeds the
  // declared decimal precision, whereas Spark's `RoundBase` raises a precision-overflow error
  // under ANSI mode (e.g. DECIMAL(38, 0) at its maximum value rounded with a negative scale).
  // Offloading under ANSI would silently substitute NULL for that error, so fall back to vanilla
  // Spark and preserve the ANSI semantics. Under non-ANSI mode Spark also returns NULL on
  // overflow, matching Velox, so offloading is safe.
  if (SQLConf.get.ansiEnabled) {
    throw new GlutenNotSupportException(
      s"${original.nodeName} on decimal is not offloaded under ANSI mode because Velox returns " +
        "NULL on precision overflow while Spark raises. Falling back to Spark.")
  }

  // Spark requires the scale to be a foldable integer literal, but guard defensively so any
  // non-foldable scale, evaluation failure, or unexpected value type triggers a clean fallback
  // via GlutenNotSupportException instead of aborting the whole transformation.
  private val toScale: Int = {
    if (!scaleExpr.foldable) {
      throw new GlutenNotSupportException(
        s"Scale expression is not foldable for ${original.nodeName}. Falling back to Spark.")
    }
    val evaluated =
      try {
        scaleExpr.eval(EmptyRow)
      } catch {
        case e: Exception =>
          throw new GlutenNotSupportException(
            s"Failed to evaluate scale expression for ${original.nodeName}: ${e.getMessage}. " +
              "Falling back to Spark.")
      }
    evaluated match {
      case null =>
        throw new GlutenNotSupportException(
          s"Scale expression evaluated to null for ${original.nodeName}. Falling back to Spark.")
      case i: Int => i
      case other =>
        throw new GlutenNotSupportException(
          s"Scale expression for ${original.nodeName} is expected to be an int but evaluated to " +
            s"${other.getClass.getSimpleName}. Falling back to Spark.")
    }
  }

  override val dataType: DataType = original.children.head.dataType match {
    case decimalType: DecimalType =>
      BackendsApiManager.getSparkPlanExecApiInstance.genDecimalRoundExpressionOutput(
        decimalType,
        toScale)
    case other =>
      throw new GlutenNotSupportException(
        s"Decimal type is expected for ${original.nodeName} but received ${other.typeName}.")
  }

  override def left: ExpressionTransformer = child
  override def right: ExpressionTransformer = LiteralTransformer(toScale)
}

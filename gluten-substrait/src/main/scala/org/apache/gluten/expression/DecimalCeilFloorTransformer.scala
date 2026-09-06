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

  private val toScale: Int = {
    val evaluated = scaleExpr.eval(EmptyRow)
    if (evaluated == null) {
      throw new GlutenNotSupportException(
        s"Scale expression evaluated to null for ${original.nodeName}. Falling back to Spark.")
    }
    evaluated.asInstanceOf[Int]
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

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

import org.apache.spark.sql.catalyst.expressions.{Add, Cast, Divide, EvalMode, Expression, IntegralDivide, LeafExpression, Multiply, Subtract}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

object ExpressionUtils {

  private def getExpressionTreeDepth(expr: Expression): Integer = {
    if (expr.isInstanceOf[LeafExpression]) {
      return 0
    }
    val childrenDepth = expr.children.map(child => getExpressionTreeDepth(child))
    if (childrenDepth.isEmpty) {
      1
    } else {
      1 + childrenDepth.max
    }
  }

  def hasComplexExpressions(expressions: Seq[Expression], threshold: Int): Boolean = {
    expressions.exists(e => ExpressionUtils.getExpressionTreeDepth(e) > threshold)
  }

  def hasComplexExpressions(plan: SparkPlan, threshold: Int): Boolean = {
    hasComplexExpressions(plan.expressions, threshold)
  }

  def hasUppercaseStructFieldName(dataType: DataType): Boolean = {
    dataType match {
      case StructType(fields) => fields.exists(_.name.exists(_.isUpper))
      case ArrayType(elementType, _) => hasUppercaseStructFieldName(elementType)
      case MapType(keyType, valueType, _) =>
        hasUppercaseStructFieldName(keyType) || hasUppercaseStructFieldName(valueType)
      case _ => false
    }
  }

  def withTryEvalMode(expr: Expression): Boolean = {
    expr match {
      case a: Add => a.evalMode == EvalMode.TRY
      case s: Subtract => s.evalMode == EvalMode.TRY
      case d: Divide => d.evalMode == EvalMode.TRY
      case m: Multiply => m.evalMode == EvalMode.TRY
      case c: Cast => c.evalMode == EvalMode.TRY
      case _ => false
    }
  }

  def withAnsiEvalMode(expr: Expression): Boolean = {
    expr match {
      case a: Add => a.evalMode == EvalMode.ANSI
      case s: Subtract => s.evalMode == EvalMode.ANSI
      case d: Divide => d.evalMode == EvalMode.ANSI
      case m: Multiply => m.evalMode == EvalMode.ANSI
      case c: Cast => c.evalMode == EvalMode.ANSI
      case i: IntegralDivide => i.evalMode == EvalMode.ANSI
      case _ => false
    }
  }
}

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

import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

case class InTransformer(substraitExprName: String, child: ExpressionTransformer, original: In)
  extends UnaryExpressionTransformer {
  override def doTransform(context: SubstraitContext): ExpressionNode = {
    assert(original.list.forall(_.foldable))
    // Stores the values in a List Literal.
    val values: Set[Any] = original.list.map(_.eval()).toSet
    InExpressionTransformer.toTransformer(child.doTransform(context), values, child.dataType)
  }
}

case class InSetTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: InSet)
  extends UnaryExpressionTransformer {
  override def doTransform(context: SubstraitContext): ExpressionNode = {
    val leftNode = child.doTransform(context)
    val values = original.hset
    val valueType = original.child.dataType

    // Keep raw literal values and defer building LiteralNode objects until toProtobuf().
    val rawValues = new java.util.ArrayList[Object](
      values.toSeq
        // Sort elements for deterministic behaviours.
        .sortBy(Literal(_, valueType).toString())
        .map(_.asInstanceOf[Object])
        .asJava)
    ExpressionBuilder.makeSingularOrListNode(leftNode, rawValues, valueType)
  }
}

object InExpressionTransformer {

  def toTransformer(
      leftNode: ExpressionNode,
      values: Set[Any],
      valueType: DataType): ExpressionNode = {
    val expressionNodes = new java.util.ArrayList[ExpressionNode](
      values.toSeq
        // Sort elements for deterministic behaviours
        .sortBy(Literal(_, valueType).toString())
        .map(value => ExpressionBuilder.makeLiteral(value, valueType, value == null))
        .asJava)

    ExpressionBuilder.makeSingularOrListNode(leftNode, expressionNodes)
  }
}

case class DecimalArithmeticExpressionTransformer(
    substraitExprName: String,
    left: ExpressionTransformer,
    right: ExpressionTransformer,
    resultType: DecimalType,
    original: Expression)
  extends BinaryExpressionTransformer {
  override def dataType: DataType = resultType
}

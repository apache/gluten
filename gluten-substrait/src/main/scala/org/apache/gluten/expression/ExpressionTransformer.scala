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

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types.DataType

import scala.collection.JavaConverters._

// ==== Expression transformer basic interface start ====

trait ExpressionTransformer {
  def substraitExprName: String
  def children: Seq[ExpressionTransformer]
  def original: Expression
  def dataType: DataType = original.dataType
  def nullable: Boolean = original.nullable

  def doTransform(context: SubstraitContext): ExpressionNode = {
    // TODO: the funcName seems can be simplified to `substraitExprName`
    val funcName: String =
      ConverterUtils.makeFuncName(substraitExprName, original.children.map(_.dataType))
    val functionId = context.registerFunction(funcName)
    val childNodes = children.map(_.doTransform(context)).asJava
    val typeNode = ConverterUtils.getTypeNode(dataType, nullable)
    ExpressionBuilder.makeScalarFunction(functionId, childNodes, typeNode)
  }
}

trait LeafExpressionTransformer extends ExpressionTransformer {
  final override def children: Seq[ExpressionTransformer] = Nil
}

trait UnaryExpressionTransformer extends ExpressionTransformer {
  def child: ExpressionTransformer
  final override def children: Seq[ExpressionTransformer] = child :: Nil
}

trait BinaryExpressionTransformer extends ExpressionTransformer {
  def left: ExpressionTransformer
  def right: ExpressionTransformer
  final override def children: Seq[ExpressionTransformer] = left :: right :: Nil
}

// ==== Expression transformer basic interface end ====

case class GenericExpressionTransformer(
    substraitExprName: String,
    children: Seq[ExpressionTransformer],
    original: Expression)
  extends ExpressionTransformer

case class FunctionArgumentExpressionTransformer(
    substraitExprName: String,
    children: Seq[ExpressionTransformer],
    original: Expression,
    functionTypes: Seq[DataType] = Nil)
  extends ExpressionTransformer {
  override def doTransform(context: SubstraitContext): ExpressionNode = {
    val inputTypes = if (functionTypes.nonEmpty) functionTypes else children.map(_.dataType)
    val funcName = ConverterUtils.makeFuncName(substraitExprName, inputTypes)
    val functionId = context.registerFunction(funcName)
    val childNodes = children.map(_.doTransform(context)).asJava
    val typeNode = ConverterUtils.getTypeNode(dataType, nullable)
    ExpressionBuilder.makeScalarFunction(functionId, childNodes, typeNode)
  }
}

object GenericExpressionTransformer {
  def apply(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Expression): GenericExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, child :: Nil, original)
  }
}

case class LiteralTransformer(original: Literal) extends LeafExpressionTransformer {
  override def substraitExprName: String = "literal"
  override def doTransform(context: SubstraitContext): ExpressionNode = {
    ExpressionBuilder.makeLiteral(original.value, original.dataType, original.nullable)
  }
}
object LiteralTransformer {
  def apply(v: Any): LiteralTransformer = {
    LiteralTransformer(Literal(v))
  }
}

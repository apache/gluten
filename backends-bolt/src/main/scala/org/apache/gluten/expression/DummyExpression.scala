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

import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.DataType

abstract class DummyExpression(child: Expression) extends UnaryExpression with Serializable {
  private val accessor: (InternalRow, Int) => Any = InternalRow.getAccessor(dataType, nullable)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, c => c)

  override def dataType: DataType = child.dataType

  override def eval(input: InternalRow): Any = {
    assert(input.numFields == 1, "The input row of DummyExpression should have only 1 field.")
    accessor(input, 0)
  }
}

// Can be used as a wrapper to force fall back the original expression to mock the fallback behavior
// of an supported expression in Gluten which fails native validation.
case class BoltDummyExpression(child: Expression)
  extends DummyExpression(child)
  with Transformable {
  override def getTransformer(
      childrenTransformers: Seq[ExpressionTransformer]): ExpressionTransformer = {
    if (childrenTransformers.size != children.size) {
      throw new IllegalStateException(
        this.getClass.getSimpleName +
          ": getTransformer called before children transformer initialized.")
    }

    GenericExpressionTransformer(
      BoltDummyExpression.BOLT_DUMMY_EXPRESSION,
      childrenTransformers,
      this)
  }

  override protected def withNewChildInternal(newChild: Expression): Expression = copy(newChild)
}

object BoltDummyExpression {
  val BOLT_DUMMY_EXPRESSION = "bolt_dummy_expression"

  private val identifier = new FunctionIdentifier(BOLT_DUMMY_EXPRESSION)

  def registerFunctions(registry: FunctionRegistry): Unit = {
    registry.registerFunction(
      identifier,
      new ExpressionInfo(classOf[BoltDummyExpression].getName, BOLT_DUMMY_EXPRESSION),
      (e: Seq[Expression]) => BoltDummyExpression(e.head)
    )
  }

  def unregisterFunctions(registry: FunctionRegistry): Unit = {
    registry.dropFunction(identifier)
  }
}

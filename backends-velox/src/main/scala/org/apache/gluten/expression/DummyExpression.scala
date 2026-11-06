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
import org.apache.spark.sql.catalyst.catalog.{CatalogFunction, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
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
case class VeloxDummyExpression(child: Expression)
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
      VeloxDummyExpression.VELOX_DUMMY_EXPRESSION,
      childrenTransformers,
      this)
  }

  override protected def withNewChildInternal(newChild: Expression): Expression = copy(newChild)
}

object VeloxDummyExpression {
  val VELOX_DUMMY_EXPRESSION = "velox_dummy_expression"

  private val identifier = new FunctionIdentifier(VELOX_DUMMY_EXPRESSION)

  // Register through SessionCatalog: since Spark 4.2 (SPARK-55964) FunctionRegistry rejects
  // unqualified identifiers, while SessionCatalog qualifies them on every Spark version.
  def registerFunctions(catalog: SessionCatalog): Unit = {
    catalog.registerFunction(
      CatalogFunction(identifier, classOf[VeloxDummyExpression].getName, Seq.empty),
      overrideIfExists = true,
      Some((e: Seq[Expression]) => VeloxDummyExpression(e.head))
    )
  }

  def unregisterFunctions(catalog: SessionCatalog): Unit = {
    catalog.unregisterFunction(identifier)
  }
}

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
package org.apache.spark.sql.catalyst.expressions

import org.apache.gluten.expression._

import org.apache.spark.sql.GlutenTestsTrait
import org.apache.spark.sql.catalyst.analysis.{Analyzer, EmptyFunctionRegistry, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.types._

class GlutenDecimalPrecisionSuite extends GlutenTestsTrait {
  private val catalog = new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry)
  private val analyzer = new Analyzer(catalog)

  private val relation = LocalRelation(
    AttributeReference("i", IntegerType)(),
    AttributeReference("d1", DecimalType(2, 1))(),
    AttributeReference("d2", DecimalType(5, 2))(),
    AttributeReference("u", DecimalType.SYSTEM_DEFAULT)(),
    AttributeReference("f", FloatType)(),
    AttributeReference("b", DoubleType)()
  )

  private val i: Expression = UnresolvedAttribute("i")
  private val d1: Expression = UnresolvedAttribute("d1")
  private val d2: Expression = UnresolvedAttribute("d2")
  private val u: Expression = UnresolvedAttribute("u")
  private val f: Expression = UnresolvedAttribute("f")
  private val b: Expression = UnresolvedAttribute("b")

  private def checkType(expression: Expression, expectedType: DataType): Unit = {
    val plan = analyzer.execute(Project(Seq(Alias(expression, "c")()), relation))
    assert(plan.isInstanceOf[Project])
    val expr = plan.asInstanceOf[Project].projectList.head
    assert(expr.dataType == expectedType)
    val transformedExpr =
      ExpressionConverter.replaceWithExpressionTransformer(expr, plan.inputSet.toSeq)
    assert(transformedExpr.dataType == expectedType)
  }

  private def stripAlias(expr: Expression): Expression = {
    expr match {
      case a: Alias => stripAlias(a.child)
      case _ => expr
    }
  }

  private def checkComparison(expression: Expression, expectedType: DataType): Unit = {
    val plan = analyzer.execute(Project(Alias(expression, "c")() :: Nil, relation))
    assert(plan.isInstanceOf[Project])
    val expr = stripAlias(plan.asInstanceOf[Project].projectList.head)
    val transformedExpr =
      ExpressionConverter.replaceWithExpressionTransformer(expr, plan.inputSet.toSeq)
    assert(transformedExpr.isInstanceOf[GenericExpressionTransformer])
    val binaryComparison = transformedExpr.asInstanceOf[GenericExpressionTransformer]
    assert(binaryComparison.original.isInstanceOf[BinaryComparison])
    assert(binaryComparison.children.size == 2)
    assert(binaryComparison.children.forall(_.dataType == expectedType))
  }

  test("basic operations") {
    checkType(Add(d1, d2), DecimalType(6, 2))
    checkType(Subtract(d1, d2), DecimalType(6, 2))
    checkType(Multiply(d1, d2), DecimalType(8, 3))
    checkType(Divide(d1, d2), DecimalType(10, 7))
    checkType(Divide(d2, d1), DecimalType(10, 6))

    checkType(Add(Add(d1, d2), d1), DecimalType(7, 2))
    checkType(Add(Add(d1, d1), d1), DecimalType(4, 1))
    checkType(Add(d1, Add(d1, d1)), DecimalType(4, 1))
    checkType(Add(Add(Add(d1, d2), d1), d2), DecimalType(8, 2))
    checkType(Add(Add(d1, d2), Add(d1, d2)), DecimalType(7, 2))
    checkType(Subtract(Subtract(d2, d1), d1), DecimalType(7, 2))
    checkType(Multiply(Multiply(d1, d1), d2), DecimalType(11, 4))
    checkType(Divide(d2, Add(d1, d1)), DecimalType(10, 6))
  }

  test("Comparison operations") {
    checkComparison(EqualTo(i, d1), DecimalType(11, 1))
    checkComparison(EqualNullSafe(d2, d1), DecimalType(5, 2))
    checkComparison(LessThan(i, d1), DecimalType(11, 1))
    checkComparison(LessThanOrEqual(d1, d2), DecimalType(5, 2))
    checkComparison(GreaterThan(d2, u), DecimalType.SYSTEM_DEFAULT)
    checkComparison(GreaterThanOrEqual(d1, f), DoubleType)
    checkComparison(GreaterThan(d2, d2), DecimalType(5, 2))
  }

  test("bringing in primitive types") {
    checkType(Add(d1, i), DecimalType(12, 1))
    checkType(Add(d1, f), DoubleType)
    checkType(Add(i, d1), DecimalType(12, 1))
    checkType(Add(f, d1), DoubleType)
    checkType(Add(d1, Cast(i, LongType)), DecimalType(22, 1))
    checkType(Add(d1, Cast(i, ShortType)), DecimalType(7, 1))
    checkType(Add(d1, Cast(i, ByteType)), DecimalType(5, 1))
    checkType(Add(d1, Cast(i, DoubleType)), DoubleType)
  }

  test("maximum decimals") {
    for (expr <- Seq(d1, d2, i, u)) {
      checkType(Add(expr, u), DecimalType(38, 17))
      checkType(Subtract(expr, u), DecimalType(38, 17))
    }

    checkType(Multiply(d1, u), DecimalType(38, 16))
    checkType(Multiply(d2, u), DecimalType(38, 14))
    checkType(Multiply(i, u), DecimalType(38, 7))
    checkType(Multiply(u, u), DecimalType(38, 6))

    checkType(Divide(u, d1), DecimalType(38, 17))
    checkType(Divide(u, d2), DecimalType(38, 16))
    checkType(Divide(u, i), DecimalType(38, 18))
    checkType(Divide(u, u), DecimalType(38, 6))

    for (expr <- Seq(f, b)) {
      checkType(Add(expr, u), DoubleType)
      checkType(Subtract(expr, u), DoubleType)
      checkType(Multiply(expr, u), DoubleType)
      checkType(Divide(expr, u), DoubleType)
    }
  }
}

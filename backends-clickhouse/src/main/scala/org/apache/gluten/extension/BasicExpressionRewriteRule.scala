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
package org.apache.gluten.extension

import org.apache.gluten.backendsapi.clickhouse.CHBackendSettings
import org.apache.gluten.execution._
import org.apache.gluten.expression._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/*
 * This file includes some rules to repace expressions in more efficient way.
 */

// Try to replace `from_json` with `get_json_object` if possible.

object PlanResolvedChecker {
  def check(plan: LogicalPlan): Boolean = {
    plan match {
      case isnert: InsertIntoStatement => isnert.query.resolved
      case _ => plan.resolved
    }
  }
}

case class RepalceFromJsonWithGetJsonObject(spark: SparkSession)
  extends Rule[LogicalPlan]
  with Logging {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (
      !CHBackendSettings.enableReplaceFromJsonWithGetJsonObject || !PlanResolvedChecker.check(plan)
    ) {
      plan
    } else {
      visitPlan(plan)
    }
  }

  def visitPlan(plan: LogicalPlan): LogicalPlan = {
    val newPlan = plan match {
      case project: Project =>
        val newProjectList =
          project.projectList.map(expr => visitExpression(expr).asInstanceOf[NamedExpression])
        project.copy(projectList = newProjectList, child = visitPlan(project.child))
      case filter: Filter =>
        val newCondition = visitExpression(filter.condition)
        Filter(newCondition, visitPlan(filter.child))
      case other =>
        other.withNewChildren(other.children.map(visitPlan))
    }
    // Some plan nodes have tags, we need to copy the tags to the new ones.
    newPlan.copyTagsFrom(plan)
    newPlan
  }

  def visitExpression(expr: Expression): Expression = {
    expr match {
      case getMapValue: GetMapValue
          if getMapValue.child.isInstanceOf[JsonToStructs] &&
            getMapValue.child.dataType.isInstanceOf[MapType] &&
            getMapValue.child.dataType.asInstanceOf[MapType].valueType.isInstanceOf[StringType] &&
            getMapValue.key.isInstanceOf[Literal] &&
            getMapValue.key.dataType.isInstanceOf[StringType] =>
        val child = visitExpression(getMapValue.child.asInstanceOf[JsonToStructs].child)
        val key = UTF8String.fromString(s"$$.${getMapValue.key.asInstanceOf[Literal].value}")
        GetJsonObject(child, Literal(key, StringType))
      case literal: Literal => literal
      case attr: Attribute => attr
      case other =>
        other.withNewChildren(other.children.map(visitExpression))
    }
  }
}

/**
 * We don't apply this rule on LogicalPlan, because there may be fallback in this query. If the
 * replacement was in a fallback node, it could cause the query to fail.
 */
case class ReplaceSubStringComparison(spark: SparkSession) extends Rule[SparkPlan] with Logging {
  val udfFunctionName = "compare_substrings"
  override def apply(plan: SparkPlan): SparkPlan = {
    if (UDFMappings.scalaUDFMap.get(udfFunctionName).isDefined) {
      visitPlan(plan)
    } else {
      plan
    }
  }

  def visitPlan(plan: SparkPlan): SparkPlan = {
    val newPlan = plan match {
      case project: ProjectExecTransformer =>
        val newProjectList =
          project.projectList.map(expr => visitExpression(expr).asInstanceOf[NamedExpression])
        project.copy(projectList = newProjectList, child = visitPlan(project.child))
      case chFilter: CHFilterExecTransformer =>
        val newCondition = visitExpression(chFilter.condition)
        CHFilterExecTransformer(newCondition, visitPlan(chFilter.child))
      case filter: FilterExecTransformer =>
        val newCondition = visitExpression(filter.condition)
        FilterExecTransformer(newCondition, visitPlan(filter.child))
      case other =>
        other.withNewChildren(other.children.map(visitPlan))
    }
    newPlan.copyTagsFrom(plan)
    newPlan
  }

  def validateSubstring(expr: Expression): Boolean = {
    expr match {
      case substring: Substring =>
        substring.pos.isInstanceOf[Literal] && substring.len.isInstanceOf[Literal] &&
        substring.pos.asInstanceOf[Literal].value.asInstanceOf[Int] > 0
      case _ => false
    }
  }

  def visitExpression(expr: Expression): Expression = {
    expr match {
      case binary: BinaryComparison
          if validateSubstring(binary.left) || validateSubstring(binary.right) =>
        rewriteSubStringComparison(binary)
      case other =>
        other.withNewChildren(other.children.map(visitExpression))
    }
  }

  def buildSubstringComparison(
      leftExpression: Expression,
      rightExpression: Expression): Option[Expression] = {
    def getSubStringPositionAndLength(e: Expression): (Integer, Integer, Expression) = {
      e match {
        case subString: Substring =>
          val pos = subString.pos.asInstanceOf[Literal].value.asInstanceOf[Int]
          val len = subString.len.asInstanceOf[Literal].value.asInstanceOf[Int]
          (pos - 1, len, subString.str)

        case literal: Literal if literal.dataType.isInstanceOf[StringType] =>
          val str = literal.value.toString
          (0, str.length, e)
        case _ => (0, Integer.MAX_VALUE, e)
      }
    }
    val (leftPos, leftLen, leftInnerExpr) = getSubStringPositionAndLength(leftExpression)
    val (rightPos, rightLen, rightInnerExpr) = getSubStringPositionAndLength(rightExpression)
    if (leftLen != rightLen) {
      return None
    }

    val udf = ScalaUDF(
      null,
      IntegerType,
      Seq(leftInnerExpr, rightInnerExpr, Literal(leftPos), Literal(rightPos), Literal(rightLen)),
      Nil,
      None,
      Some(udfFunctionName),
      leftExpression.nullable || rightExpression.nullable
    )
    Some(udf)
  }

  def rewriteSubStringComparison(binaryExpression: BinaryComparison): Expression = {
    val zeroLiteral = Literal(0, IntegerType)
    val subStringCompareExpression =
      buildSubstringComparison(binaryExpression.left, binaryExpression.right)
    subStringCompareExpression match {
      case Some(expr) =>
        binaryExpression match {
          case _: EqualTo => EqualTo(expr, zeroLiteral)
          case _: GreaterThan => GreaterThan(expr, zeroLiteral)
          case _: GreaterThanOrEqual => GreaterThanOrEqual(expr, zeroLiteral)
          case _: LessThan => LessThan(expr, zeroLiteral)
          case _: LessThanOrEqual => LessThanOrEqual(expr, zeroLiteral)
          case _ => binaryExpression
        }
      case _ => binaryExpression
    }
  }
}

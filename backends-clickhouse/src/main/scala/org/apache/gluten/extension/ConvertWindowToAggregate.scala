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
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.execution._
import org.apache.gluten.expression.WindowFunctionsBuilder

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.rules.Rule
// import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types._

// When to find the first rows of partitions by window function, we can convert it to aggregate
// function.
case class ConverRowNumbertWindowToAggregateRule(spark: SparkSession)
  extends Rule[SparkPlan]
  with Logging {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (
      !spark.conf
        .get(CHBackendSettings.GLUTEN_ENABLE_WINDOW_GROUP_LIMIT_TO_AGGREGATE, "true")
        .toBoolean
    ) {
      return plan
    }
    plan.transformUp {
      case filter @ FilterExecTransformer(
            condition,
            window @ WindowExecTransformer(
              windowExpressions,
              partitionSpec,
              orderSpec,
              sort @ SortExecTransformer(_, _, _, _))) =>
        if (
          !isSupportedWindowFunction(windowExpressions) || !isTopKLimitFilter(
            condition,
            windowExpressions(0)) || hasSamePartitionAndOrderKeys(partitionSpec, orderSpec)
        ) {
          logDebug(
            s"xxx Not Supported case for converting window to aggregate. is topk limit: " +
              s"${isTopKLimitFilter(condition, windowExpressions(0))}. is supported window " +
              s"function: ${isSupportedWindowFunction(windowExpressions)}. " +
              s"has same partition and order keys: " +
              s"${hasSamePartitionAndOrderKeys(partitionSpec, orderSpec)}")
          filter
        } else {
          val limit = getLimit(condition.asInstanceOf[BinaryComparison])
          if (limit < 1 || limit > 100) {
            filter
          } else {
            val groupLimit = CHAggregateGroupLimitExecTransformer(
              partitionSpec,
              orderSpec,
              extractWindowFunction(windowExpressions(0)),
              sort.child.output ++ Seq(windowExpressions(0).toAttribute),
              limit,
              sort.child
            )
            groupLimit
          }
        }
    }
  }

  def getLimit(e: BinaryComparison): Int = {
    e match {
      case _: EqualTo => evalIntLiteral(e.right).get
      case _: LessThanOrEqual => evalIntLiteral(e.right).get
      case _: LessThan => evalIntLiteral(e.right).get - 1
      case _ => throw new GlutenException(s"Unsupported comparison: $e")
    }
  }

  def evalIntLiteral(expr: Expression): Option[Int] = {
    expr match {
      case int @ Literal(value, IntegerType) => Some(value.asInstanceOf[Int])
      case long @ Literal(value, LongType) => Some(value.asInstanceOf[Long].toInt)
      case _ => None
    }
  }

  def extractWindowFunction(windowExpression: NamedExpression): Expression = {
    val aliasExpr = windowExpression.asInstanceOf[Alias]
    WindowFunctionsBuilder.extractWindowExpression(aliasExpr.child).windowFunction
  }

  def isSupportedWindowFunction(windowExpressions: Seq[NamedExpression]): Boolean = {
    if (windowExpressions.length != 1) {
      return false
    }
    val windowFunction = extractWindowFunction(windowExpressions(0))
    windowFunction match {
      case _: RowNumber => true
      case _ => false
    }
  }

  private def hasSamePartitionAndOrderKeys(
      partitionSpec: Seq[Expression],
      orderSpec: Seq[SortOrder]): Boolean = {
    partitionSpec.nonEmpty && partitionSpec.length == orderSpec.length &&
    partitionSpec.zip(orderSpec).forall {
      case (partitionExpr, sortOrder) =>
        partitionExpr.semanticEquals(sortOrder.child)
    }
  }

  // If the filter condition is a constant limit condition, return the limit value.
  def isTopKLimitFilter(condition: Expression, windowExpression: NamedExpression): Boolean = {
    def isWindowFunctionResult(
        condition: Expression,
        windowExpression: NamedExpression): Boolean = {
      condition match {
        case attr: Attribute =>
          attr.semanticEquals(windowExpression.toAttribute)
        case _ => false
      }
    }
    if (!condition.isInstanceOf[BinaryComparison]) {
      return false
    }
    val binaryComparison = condition.asInstanceOf[BinaryComparison]
    val constLimit = evalIntLiteral(binaryComparison.right)
    if (!isWindowFunctionResult(binaryComparison.left, windowExpression) || !constLimit.isDefined) {
      return false
    }
    binaryComparison match {
      case _: EqualTo => constLimit.get == 1
      case _: LessThanOrEqual | _: LessThan => true
      case _ => false
    }
  }

}

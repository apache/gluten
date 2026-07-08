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
package org.apache.gluten.extension.columnar.rewrite

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.utils.PullOutProjectHelper

import org.apache.spark.sql.catalyst.expressions.{Expression, If, IsNull, Literal, NamedExpression, Or}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count, Partial}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.types.IntegerType

/**
 * This Rule is used to rewrite multi-children count for both partial aggregates and window
 * functions, since Velox only supports `count()` and `count(T)` signatures.
 *
 * For partial aggregates the rewrite triggers when:
 *   - the count is partial
 *   - the count has more than one child
 *
 * For window functions the rewrite triggers whenever the inner aggregate function of the window
 * expression is a `Count` with more than one child. The aggregate mode for window functions is
 * always `Complete`, so we don't gate on mode there.
 *
 * A rewrite count multi-children example:
 * {{{
 *   Count(c1, c2)
 *   =>
 *   Count(
 *     If(
 *       Or(IsNull(c1), IsNull(c2)),
 *       Literal(null),
 *       Literal(1)
 *     )
 *   )
 * }}}
 *
 * TODO: Remove this rule when Velox support multi-children Count.
 */
object RewriteMultiChildrenCount extends RewriteSingleNode with PullOutProjectHelper {
  private lazy val shouldRewriteCount = BackendsApiManager.getSettings.shouldRewriteCount()

  override def isRewritable(plan: SparkPlan): Boolean = {
    plan match {
      case _: BaseAggregateExec => true
      case _: WindowExec => true
      case _ => false
    }
  }

  private def extractCountForRewrite(aggExpr: AggregateExpression): Option[Count] = {
    val isPartialCountWithMoreThanOneChild = aggExpr.mode == Partial && {
      aggExpr.aggregateFunction match {
        case c: Count => c.children.size > 1
        case _ => false
      }
    }
    if (isPartialCountWithMoreThanOneChild) {
      Option(aggExpr.aggregateFunction.asInstanceOf[Count])
    } else {
      None
    }
  }

  /** Window functions don't have Partial/Final modes; rewrite any multi-child Count. */
  private def extractWindowCountForRewrite(aggExpr: AggregateExpression): Option[Count] = {
    aggExpr.aggregateFunction match {
      case c: Count if c.children.size > 1 => Option(c)
      case _ => None
    }
  }

  private def shouldRewrite(aggregateExpressions: Seq[AggregateExpression]): Boolean = {
    aggregateExpressions.exists(extractCountForRewrite(_).isDefined)
  }

  private def shouldRewriteWindow(windowExpressions: Seq[NamedExpression]): Boolean = {
    windowExpressions.exists(_.find {
      case ae: AggregateExpression => extractWindowCountForRewrite(ae).isDefined
      case _ => false
    }.isDefined)
  }

  private def buildSingleChildCount(count: Count): Count = {
    // Follow vanilla Spark Count semantics: count rows where every argument is non-null.
    val nullableChildren = count.children.filter(_.nullable)
    val newChild: Expression = if (nullableChildren.isEmpty) {
      Literal.create(1, IntegerType)
    } else {
      If(
        nullableChildren.map(IsNull).reduce(Or),
        Literal.create(null, IntegerType),
        Literal.create(1, IntegerType))
    }
    count.copy(children = newChild :: Nil)
  }

  private def rewriteCount(
      aggregateExpressions: Seq[AggregateExpression]): Seq[AggregateExpression] = {
    aggregateExpressions.map {
      aggExpr =>
        val countOpt = extractCountForRewrite(aggExpr)
        countOpt
          .map(count => aggExpr.copy(aggregateFunction = buildSingleChildCount(count)))
          .getOrElse(aggExpr)
    }
  }

  private def rewriteWindowExpressions(
      windowExpressions: Seq[NamedExpression]): Seq[NamedExpression] = {
    windowExpressions.map {
      expr =>
        expr
          .transformDown {
            case ae: AggregateExpression =>
              extractWindowCountForRewrite(ae)
                .map(count => ae.copy(aggregateFunction = buildSingleChildCount(count)))
                .getOrElse(ae)
          }
          .asInstanceOf[NamedExpression]
    }
  }

  override def rewrite(plan: SparkPlan): SparkPlan = {
    if (!shouldRewriteCount) {
      return plan
    }

    plan match {
      case agg: BaseAggregateExec if shouldRewrite(agg.aggregateExpressions) =>
        val newAggExprs = rewriteCount(agg.aggregateExpressions)
        copyBaseAggregateExec(agg)(newAggregateExpressions = newAggExprs)

      case window: WindowExec if shouldRewriteWindow(window.windowExpression) =>
        val newWindow =
          window.copy(windowExpression = rewriteWindowExpressions(window.windowExpression))
        newWindow.copyTagsFrom(window)
        newWindow

      case _ => plan
    }
  }
}

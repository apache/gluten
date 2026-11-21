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

import org.apache.spark.sql.catalyst.expressions.{Expression, If, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.window.WindowExec

/**
 * Native window operators have no per-function mask, so the FILTER clause of a window aggregate
 * would otherwise be dropped and the aggregate would run over every row of the frame.
 *
 * For aggregate functions that skip null inputs, the filter can be folded into the inputs:
 * {{{
 *   agg(c1, c2) FILTER (WHERE p) OVER w
 *   =>
 *   agg(If(p, c1, null), If(p, c2, null)) OVER w
 * }}}
 * A row whose predicate is false or null then contributes only null inputs, which these functions
 * ignore, matching Spark's AggregateProcessor that leaves the buffer unchanged for such rows.
 *
 * Aggregates with a FILTER that cannot be rewritten this way are left untouched and must be
 * rejected by backend validation.
 */
object RewriteWindowAggregateFilter extends RewriteSingleNode {

  override def isRewritable(plan: SparkPlan): Boolean = plan.isInstanceOf[WindowExec]

  private def ignoresNullInputs(func: AggregateFunction): Boolean = func match {
    case _: Count | _: Sum | _: Average | _: Min | _: Max => true
    case f: First => f.ignoreNulls
    case l: Last => l.ignoreNulls
    case _: CentralMomentAgg | _: Covariance | _: PearsonCorrelation => true
    case _: BitAggregate => true
    case _ => false
  }

  private def canRewrite(ae: AggregateExpression): Boolean =
    ae.filter.isDefined && !ae.isDistinct && ignoresNullInputs(ae.aggregateFunction)

  private def rewriteAggregate(ae: AggregateExpression): AggregateExpression = {
    val predicate = ae.filter.get
    val newChildren = ae.aggregateFunction.children.map {
      child => If(predicate, child, Literal.create(null, child.dataType)): Expression
    }
    val newFunc = ae.aggregateFunction.withNewChildren(newChildren).asInstanceOf[AggregateFunction]
    val newAe = ae.copy(aggregateFunction = newFunc, filter = None)
    newAe.copyTagsFrom(ae)
    newAe
  }

  override def rewrite(plan: SparkPlan): SparkPlan = plan match {
    case window: WindowExec
        if window.windowExpression.exists(_.find {
          case ae: AggregateExpression => canRewrite(ae)
          case _ => false
        }.isDefined) =>
      val newWindowExpressions = window.windowExpression.map {
        _.transformDown { case ae: AggregateExpression if canRewrite(ae) => rewriteAggregate(ae) }
          .asInstanceOf[NamedExpression]
      }
      val newWindow = window.copy(windowExpression = newWindowExpressions)
      newWindow.copyTagsFrom(window)
      newWindow
    case _ => plan
  }
}

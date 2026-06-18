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

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

import scala.collection.mutable

// If you want to debug CommonSubexpressionEliminateRule, you can:
// 1. replace all `logTrace` to `logError`
// 2. append two options to spark config
//    --conf spark.sql.planChangeLog.level=error
//    --conf spark.sql.planChangeLog.batches=all
case class CommonSubexpressionEliminateRule(spark: SparkSession)
  extends Rule[LogicalPlan]
  with Logging {

  private var lastPlan: LogicalPlan = null

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val newPlan =
      if (
        plan.resolved && GlutenConfig.get.enableGluten
        && GlutenConfig.get.enableCommonSubexpressionEliminate && !plan.fastEquals(lastPlan)
      ) {
        lastPlan = plan
        visitPlan(plan)
      } else {
        plan
      }
    newPlan
  }

  private case class AliasAndAttribute(alias: Alias, attribute: Attribute)

  private case class RewriteContext(exprs: Seq[Expression], child: LogicalPlan)

  private def visitPlan(plan: LogicalPlan): LogicalPlan = {
    var newPlan = plan match {
      case project: Project => visitProject(project)
      // TODO: CSE in Filter doesn't work for unknown reason, need to fix it later
      // case filter: Filter => visitFilter(filter)
      case window: Window => visitWindow(window)
      case aggregate: Aggregate => visitAggregate(aggregate)
      case sort: Sort => visitSort(sort)
      case other =>
        val children = other.children.map(visitPlan)
        other.withNewChildren(children)
    }

    if (newPlan.output.size == plan.output.size) {
      return newPlan
    }

    // Add a Project to trim unnecessary attributes(which are always at the end of the output)
    val postProjectList = newPlan.output.take(plan.output.size)
    Project(postProjectList, newPlan)
  }

  private def replaceCommonExprWithAttribute(
      expr: Expression,
      commonExprMap: mutable.HashMap[ExpressionEquals, AliasAndAttribute]): Expression = {
    val exprEquals = commonExprMap.get(ExpressionEquals(expr))
    if (exprEquals.isDefined) {
      exprEquals.get.attribute
    } else {
      expr.mapChildren(replaceCommonExprWithAttribute(_, commonExprMap))
    }
  }

  private def replaceAggCommonExprWithAttribute(
      expr: Expression,
      commonExprMap: mutable.HashMap[ExpressionEquals, AliasAndAttribute],
      inAgg: Boolean = false): Expression = {
    val exprEquals = commonExprMap.get(ExpressionEquals(expr))
    expr match {
      case _ if exprEquals.isDefined && inAgg =>
        exprEquals.get.attribute
      case _: AggregateExpression =>
        expr.mapChildren(replaceAggCommonExprWithAttribute(_, commonExprMap, true))
      case _ =>
        expr.mapChildren(replaceAggCommonExprWithAttribute(_, commonExprMap, inAgg))
    }
  }

  private def isValidCommonExpr(expr: Expression): Boolean = {
    if (
      (expr.isInstanceOf[Unevaluable] && !expr.isInstanceOf[AttributeReference])
      || expr.isInstanceOf[AggregateFunction]
      || (expr.isInstanceOf[AttributeReference]
        && expr.asInstanceOf[AttributeReference].name == VirtualColumn.groupingIdName)
    ) {
      logTrace(s"Check common expression failed $expr class ${expr.getClass.toString}")
      return false
    }

    expr.children.forall(isValidCommonExpr(_))
  }

  private def addToEquivalentExpressions(
      expr: Expression,
      equivalentExpressions: EquivalentExpressions): Unit = {
    logTrace(s"addToEquivalentExpressions $expr class ${expr.getClass.toString}")
    if (expr.isInstanceOf[AggregateExpression]) {
      equivalentExpressions.addExprTree(expr)
    } else {
      expr.children.foreach(addToEquivalentExpressions(_, equivalentExpressions))
    }
  }

  private def rewrite(inputCtx: RewriteContext): RewriteContext = {
    logTrace(s"Start rewrite with input exprs:${inputCtx.exprs} input child:${inputCtx.child}")
    val equivalentExpressions = new EquivalentExpressions
    inputCtx.exprs.foreach(
      expr => {
        // For input exprs that contains AggregateExpression, add the AggregateExpression directly
        // to equivalentExpressions. Otherwise add the whole expression.
        // This fix issue: https://github.com/oap-project/gluten/issues/4642
        if (expr.find(_.isInstanceOf[AggregateExpression]).isDefined) {
          addToEquivalentExpressions(expr, equivalentExpressions)
        } else {
          expr match {
            case alias: Alias =>
              equivalentExpressions.addExprTree(alias.child)
            case _ =>
              equivalentExpressions.addExprTree(expr)
          }
        }
      })

    // Get all the expressions that appear at least twice
    val newChild = visitPlan(inputCtx.child)
    val commonExprs = equivalentExpressions.getCommonSubexpressions

    // Put the common expressions into a hash map
    val commonExprMap = mutable.HashMap.empty[ExpressionEquals, AliasAndAttribute]
    commonExprs.foreach {
      expr =>
        if (!expr.foldable && !expr.isInstanceOf[Attribute] && isValidCommonExpr(expr)) {
          logTrace(s"Common subexpression $expr class ${expr.getClass.toString}")
          val exprEquals = ExpressionEquals(expr)
          val alias = Alias(expr, expr.toString)()
          val attribute = alias.toAttribute
          commonExprMap.put(exprEquals, AliasAndAttribute(alias, attribute))
        }
    }

    if (commonExprMap.isEmpty) {
      logTrace(s"commonExprMap is empty, all exprs: ${equivalentExpressions.debugString(true)}")
      return RewriteContext(inputCtx.exprs, newChild)
    }

    // Generate pre-project as new child
    var preProjectList = newChild.output ++ commonExprMap.values.map(_.alias)
    val preProject = Project(preProjectList, newChild)
    logTrace(s"newChild after rewrite: $preProject")

    // Replace the common expressions with the first expression that produces it.
    try {
      var newExprs = inputCtx.exprs
        .map(
          expr => {
            if (expr.find(_.isInstanceOf[AggregateExpression]).isDefined) {
              replaceAggCommonExprWithAttribute(expr, commonExprMap)
            } else {
              replaceCommonExprWithAttribute(expr, commonExprMap)
            }
          })
      logTrace(s"newExprs after rewrite: $newExprs")
      RewriteContext(newExprs, preProject)
    } catch {
      case e: Exception =>
        logWarning(
          s"Common subexpression eliminate failed with exception: ${e.getMessage}" +
            s" while replace ${inputCtx.exprs} with $commonExprMap, fallback now")
        RewriteContext(inputCtx.exprs, newChild)
    }
  }

  private def visitProject(project: Project): Project = {
    val inputCtx = RewriteContext(project.projectList, project.child)
    val outputCtx = rewrite(inputCtx)
    Project(outputCtx.exprs.map(_.asInstanceOf[NamedExpression]), outputCtx.child)
  }

  private def visitFilter(filter: Filter): Filter = {
    val inputCtx = RewriteContext(Seq(filter.condition), filter.child)
    val outputCtx = rewrite(inputCtx)
    Filter(outputCtx.exprs.head, outputCtx.child)
  }

  private def visitWindow(window: Window): Window = {
    val inputCtx = RewriteContext(window.windowExpressions, window.child)
    val outputCtx = rewrite(inputCtx)
    Window(
      outputCtx.exprs.map(_.asInstanceOf[NamedExpression]),
      window.partitionSpec,
      window.orderSpec,
      outputCtx.child)
  }

  private def visitAggregate(aggregate: Aggregate): Aggregate = {
    logTrace(
      s"aggregate groupingExpressions: ${aggregate.groupingExpressions} " +
        s"aggregateExpressions: ${aggregate.aggregateExpressions}")
    // Only extract common subexpressions from aggregateExpressions
    // that contains AggregateExpression.
    // Fix issue: https://github.com/oap-project/gluten/issues/4642
    val exprsWithIndex = aggregate.aggregateExpressions.zipWithIndex
      .filter(_._1.find(_.isInstanceOf[AggregateExpression]).isDefined)
    val inputCtx = RewriteContext(exprsWithIndex.map(_._1), aggregate.child)
    val outputCtx = rewrite(inputCtx)
    val newExprs = outputCtx.exprs
    val indexToNewExpr = exprsWithIndex.map(_._2).zip(newExprs).toMap
    val updatedAggregateExpressions = aggregate.aggregateExpressions.indices.map {
      index => indexToNewExpr.getOrElse(index, aggregate.aggregateExpressions(index))
    }
    Aggregate(
      aggregate.groupingExpressions,
      updatedAggregateExpressions.toSeq.map(_.asInstanceOf[NamedExpression]),
      outputCtx.child)
  }

  private def visitSort(sort: Sort): Sort = {
    val exprs = sort.order.flatMap(_.children)
    val inputCtx = RewriteContext(exprs, sort.child)
    val outputCtx = rewrite(inputCtx)

    var start = 0
    var newOrder = Seq.empty[SortOrder]
    sort.order.foreach(
      order => {
        val childrenSize = order.children.size
        val newChildren = outputCtx.exprs.slice(start, start + childrenSize)
        newOrder = newOrder :+ order.withNewChildren(newChildren).asInstanceOf[SortOrder]
        start += childrenSize
      })

    Sort(newOrder, sort.global, outputCtx.child)
  }
}

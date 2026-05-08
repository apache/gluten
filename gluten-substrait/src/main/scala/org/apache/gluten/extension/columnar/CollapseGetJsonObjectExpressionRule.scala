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
package org.apache.gluten.extension.columnar

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * The rule is aimed to collapse nested `get_json_object` functions as one for optimization, e.g.
 * get_json_object(get_json_object(d, '$.a'), '$.b') => get_json_object(d, '$.a.b'). And we should
 * notice that some case can not be applied to this rule:
 *   - get_json_object(get_json_object({"a":"{\\\"x\\\":5}"}', '$.a'), '$.x'), the json string has
 *     backslashes to escape quotes ;
 *   - get_json_object(get_json_object('{"a.b": 0}', '$.a), '$.b'), the json key contains dot
 *     character(.) and it's same as the collapsed json path;
 */
case class CollapseGetJsonObjectExpressionRule(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (
      plan.resolved
      && GlutenConfig.get.enableCollapseNestedGetJsonObject
    ) {
      visitPlan(plan)
    } else {
      plan
    }
  }

  private def visitPlan(plan: LogicalPlan): LogicalPlan = plan match {
    case p: Project =>
      var newProjectList = Seq.empty[NamedExpression]
      p.projectList.foreach {
        case a: Alias if a.child.isInstanceOf[GetJsonObject] =>
          newProjectList :+= optimizeNestedFunctions(a).asInstanceOf[NamedExpression]
        case p =>
          newProjectList :+= p
      }
      val newChild = visitPlan(p.child)
      Project(newProjectList, newChild)
    case f: Filter =>
      val newCond = optimizeNestedFunctions(f.condition)
      val newChild = visitPlan(f.child)
      Filter(newCond, newChild)
    case other =>
      val children = other.children.map(visitPlan)
      plan.withNewChildren(children)
  }

  private def optimizeNestedFunctions(
      expr: Expression,
      path: String = "",
      isNested: Boolean = false): Expression = {

    def getPathLiteral(path: Expression): Option[String] = path match {
      case l: Literal =>
        Option.apply(l.value.toString)
      case _ =>
        Option.empty
    }

    expr match {
      case g: GetJsonObject =>
        val gPath = getPathLiteral(g.path).orNull
        var newPath = ""
        if (gPath != null) {
          newPath = gPath.replace("$", "") + path
        }
        val res = optimizeNestedFunctions(g.json, newPath, isNested = true)
        if (gPath != null) {
          res
        } else {
          var newChildren = Seq.empty[Expression]
          newChildren :+= res
          newChildren :+= g.path
          val newExpr = g.withNewChildren(newChildren)
          if (path.nonEmpty) {
            GetJsonObject(newExpr, Literal.apply("$" + path))
          } else {
            newExpr
          }
        }
      case _ =>
        // Do not propagate the accumulated path into children of non-GJO expressions.
        // A non-GJO function (e.g. regexp_extract, substring) transforms the JSON string,
        // so we cannot merge the outer GJO path with any inner GJO path across it.
        // Each child starts fresh optimization with empty path.
        val newChildren = expr.children.map(x => optimizeNestedFunctions(x))
        val newExpr = expr.withNewChildren(newChildren)
        if (isNested && path.nonEmpty) {
          val pathExpr = Literal.apply("$" + path)
          GetJsonObject(newExpr, pathExpr)
        } else {
          newExpr
        }
    }
  }
}

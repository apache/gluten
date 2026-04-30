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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

// If there is an expression (not a attribute) in an aggregation function's
// parameters. It will introduce a pre-projection to calculate the expression
// at first, and make all the parameters be attributes.
// If it's a aggregation with grouping set, this pre-projection is placed after
// expand operator. This is not efficient, we cannot move this pre-projection
// before the expand operator.
case class PushdownAggregatePreProjectionAheadExpand(session: SparkSession)
  extends Rule[SparkPlan]
  with Logging {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!CHBackendSettings.enablePushdownPreProjectionAheadExpand) {
      return plan
    }
    plan.transformUp {
      case hashAggregate: CHHashAggregateExecTransformer =>
        tryPushdownAggregatePreProject(hashAggregate)
    }
  }

  def isGroupingColumn(e: NamedExpression): Boolean = {
    e.isInstanceOf[AttributeReference] && e
      .asInstanceOf[AttributeReference]
      .name
      .startsWith(VirtualColumn.groupingIdName, 0)
  }

  def dropGroupingColumn(expressions: Seq[NamedExpression]): Seq[NamedExpression] = {
    expressions.filter(!isGroupingColumn(_))
  }

  def isAttributeOrLiteral(e: Expression): Boolean = {
    e match {
      case _: Attribute | _: BoundReference | _: Literal => true
      case _ => false
    }
  }

  def tryPushdownAggregatePreProject(plan: SparkPlan): SparkPlan = {
    val hashAggregate = plan.asInstanceOf[CHHashAggregateExecTransformer]
    val originGroupingKeys = dropGroupingColumn(hashAggregate.groupingExpressions)
    // make things simple, if any grouping key is not attribute, don't change anything
    if (!originGroupingKeys.forall(isAttributeOrLiteral(_))) {
      return hashAggregate
    }
    hashAggregate.child match {
      case project @ ProjectExecTransformer(expressions, expand: ExpandExecTransformer) =>
        val rootChild = expand.child

        // This could not happen
        if (rootChild.output.exists(isGroupingColumn(_))) {
          return hashAggregate
        }
        // drop the goruping id column
        val aheadProjectExprs = dropGroupingColumn(project.projectList)
        val originInputAttributes = aheadProjectExprs.filter(e => isAttributeOrLiteral(e))

        val preProjectExprs = aheadProjectExprs.filter(e => !isAttributeOrLiteral(e))
        if (preProjectExprs.isEmpty) {
          return hashAggregate
        }

        // If the expression involves grouping keys, don't change anything
        // This should not happen.
        if (preProjectExprs.exists(e => originGroupingKeys.exists(e.references.contains(_)))) {
          return hashAggregate
        }

        def projectInputExists(expr: Expression, inputs: Seq[Attribute]): Boolean = {
          expr.children.foreach {
            case a: Attribute =>
              return inputs.exists(i => i.name.equals(a.name) && i.exprId.equals(a.exprId))
            case p: Expression =>
              return projectInputExists(p, inputs)
            case _ =>
              return true
          }
          true
        }

        val couldPushDown = preProjectExprs.forall {
          case p: Expression => projectInputExists(p, rootChild.output)
          case _ => true
        }

        if (!couldPushDown) {
          return hashAggregate
        }

        // The new ahead project node will take rootChild's output and preProjectExprs as the
        // the projection expressions.
        val aheadProject = ProjectExecTransformer(rootChild.output ++ preProjectExprs, rootChild)
        val aheadProjectOuput = aheadProject.output
        val preProjectOutputAttrs = aheadProjectOuput.filter(
          e =>
            !originInputAttributes.exists(_.exprId.equals(e.asInstanceOf[NamedExpression].exprId)))

        val newExpandProjections = expand.projections.map {
          exprs => exprs ++ preProjectOutputAttrs
        }
        val newExpandOutput = expand.output ++ preProjectOutputAttrs
        val newExpand = ExpandExecTransformer(newExpandProjections, newExpandOutput, aheadProject)

        hashAggregate.withNewChildren(Seq(newExpand))
      case _ => plan
    }
  }
}

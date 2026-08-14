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

import org.apache.gluten.expression.ExpressionMappings
import org.apache.gluten.expression.aggregate.{BoltCollectList, BoltCollectSet}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, AGGREGATE_EXPRESSION}

import scala.reflect.{classTag, ClassTag}

/**
 * Bolt's collect_list / collect_set use array as intermediate data type so aren't compatible with
 * vanilla Spark. We here replace the two functions with bolt_collect_list / bolt_collect_set to
 * distinguish.
 */
case class CollectRewriteRule(spark: SparkSession) extends Rule[LogicalPlan] {
  import CollectRewriteRule._
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!has[BoltCollectSet] && !has[BoltCollectList]) {
      return plan
    }

    val newPlan = plan.transformUpWithPruning(_.containsPattern(AGGREGATE)) {
      case node =>
        replaceAggCollect(node)
    }
    if (newPlan.fastEquals(plan)) {
      return plan
    }
    newPlan
  }

  private def replaceAggCollect(node: LogicalPlan): LogicalPlan = {
    node match {
      case agg: Aggregate =>
        agg.transformExpressionsWithPruning(_.containsPattern(AGGREGATE_EXPRESSION)) {
          case ToBoltCollect(newAggExpr) =>
            newAggExpr
        }
      case other => other
    }
  }
}

object CollectRewriteRule {
  private object ToBoltCollect {
    def unapply(expr: Expression): Option[Expression] = expr match {
      case aggExpr @ AggregateExpression(s: CollectSet, _, _, _, _) if has[BoltCollectSet] =>
        val newAggExpr =
          aggExpr.copy(aggregateFunction = BoltCollectSet(s.child))
        Some(newAggExpr)
      case aggExpr @ AggregateExpression(l: CollectList, _, _, _, _) if has[BoltCollectList] =>
        val newAggExpr = aggExpr.copy(BoltCollectList(l.child))
        Some(newAggExpr)
      case _ => None
    }
  }

  private def has[T <: Expression: ClassTag]: Boolean =
    ExpressionMappings.expressionsMap.contains(classTag[T].runtimeClass)
}

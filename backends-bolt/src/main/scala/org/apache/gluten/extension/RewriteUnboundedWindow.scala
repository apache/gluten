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

import org.apache.gluten.config.BoltConfig

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, And, EqualNullSafe, NamedExpression, SpecifiedWindowFrame, WindowExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.WINDOW

/**
 * For unbounded window, bolt needs to load the entire partition's data into memory for calculation,
 * which can easily cause OOM when the partition is too large. This rule rewrites unbounded window
 * to an equivalent aggregate join operation to avoid OOM.
 *
 * Input query:
 * {{{
 *    SELECT *, SUM(c0) OVER (PARTITION BY c1) AS sum FROM t
 * }}}
 *
 * Rewritten query:
 * {{{
 *    SELECT t.*, t1.sum FROM t LEFT JOIN (SELECT c1, SUM(c0) AS sum FROM t GROUP BY c1) t1
 *     ON t.c1 <=> t1.c1
 * }}}
 */
case class RewriteUnboundedWindow(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!BoltConfig.get.enableRewriteUnboundedWindow) {
      return plan
    }
    plan.transformUpWithPruning(_.containsPattern(WINDOW)) {
      case w: Window if w.orderSpec.isEmpty && isUnboundedWindow(w.windowExpressions) =>
        val partitionAliases = w.partitionSpec.zipWithIndex.map {
          case (expr, idx) => Alias(expr, s"part_$idx")()
        }
        val aggregateExprs = partitionAliases ++ w.windowExpressions.map {
          case alias @ Alias(WindowExpression(agg: AggregateExpression, _), _) =>
            alias.copy(child = agg)(
              alias.exprId,
              alias.qualifier,
              alias.explicitMetadata,
              alias.nonInheritableMetadataKeys)
        }
        val aggregate = Aggregate(w.partitionSpec, aggregateExprs, w.child)
        val joinCondition = w.partitionSpec
          .zip(partitionAliases)
          .map(exprs => EqualNullSafe(exprs._1, exprs._2.toAttribute))
          .reduceOption(And)
        val join = Join(w.child, aggregate, Inner, joinCondition, JoinHint(None, None))
        Project(w.output, join)
    }
  }

  private def isUnboundedWindow(windowExpressions: Seq[NamedExpression]): Boolean = {
    windowExpressions.forall {
      case Alias(WindowExpression(_: AggregateExpression, windowSpec), _) =>
        windowSpec.frameSpecification match {
          case f: SpecifiedWindowFrame => f.isUnbounded
          case _ => false
        }
      case _ => false
    }
  }
}

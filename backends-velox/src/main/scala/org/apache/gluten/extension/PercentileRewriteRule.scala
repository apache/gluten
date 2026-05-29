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
import org.apache.gluten.expression.aggregate.VeloxApproximatePercentile

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, AGGREGATE_EXPRESSION}

import scala.reflect.{classTag, ClassTag}

/**
 * Rewrite Spark native ApproximatePercentile to VeloxApproximatePercentile:
 *   - Velox uses a 9-field StructType intermediate (KLL sketch), incompatible with Spark's
 *     TypedImperativeAggregate (single BinaryType buffer).
 *   - Accuracy is passed as-is (Spark's original integer value, e.g. 10000). Velox C++
 *     SparkAccuracyPolicy internally computes epsilon = 1.0 / accuracy.
 */
case class ApproxPercentileRewriteRule(spark: SparkSession) extends Rule[LogicalPlan] {
  import ApproxPercentileRewriteRule._
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!has[VeloxApproximatePercentile]) {
      return plan
    }

    val newPlan = plan.transformUpWithPruning(_.containsPattern(AGGREGATE)) {
      case node =>
        replaceApproxPercentile(node)
    }
    if (newPlan.fastEquals(plan)) {
      return plan
    }
    newPlan
  }

  private def replaceApproxPercentile(node: LogicalPlan): LogicalPlan = {
    node match {
      case agg: Aggregate =>
        agg.transformExpressionsWithPruning(_.containsPattern(AGGREGATE_EXPRESSION)) {
          case ToVeloxApproxPercentile(newAggExpr) =>
            newAggExpr
        }
      case other => other
    }
  }
}

object ApproxPercentileRewriteRule {

  /**
   * Maximum accuracy value that the Velox KLL sketch can handle without OOM. When
   * relativeError=0.0, Spark sets accuracy=Int.MaxValue which causes kFromAccuracy to compute
   * k=9,314,283,425, leading to multi-GiB memory allocation. We cap at 100,000 which gives
   * k≈6,600 — still very accurate but memory-safe.
   */
  private val MAX_SUPPORTED_ACCURACY: Int = 100000

  private object ToVeloxApproxPercentile {
    def unapply(expr: Expression): Option[Expression] = expr match {
      case aggExpr @ AggregateExpression(ap: ApproximatePercentile, _, _, _, _) =>
        // 当 accuracy 过大时（如 relativeError=0.0 导致 accuracy=Int.MaxValue），
        // 不替换为 VeloxApproximatePercentile，让 Spark 原生实现处理。
        val accuracyTooLarge = ap.accuracyExpression match {
          case Literal(v: Number, _) if v != null => v.intValue() > MAX_SUPPORTED_ACCURACY
          case _ => false
        }
        if (accuracyTooLarge) {
          None
        } else {
          val newAggExpr = aggExpr.copy(
            aggregateFunction =
              VeloxApproximatePercentile(ap.child, ap.percentageExpression, ap.accuracyExpression))
          Some(newAggExpr)
        }
      case _ => None
    }
  }

  private def has[T <: Expression: ClassTag]: Boolean =
    ExpressionMappings.expressionsMap.contains(classTag[T].runtimeClass)
}

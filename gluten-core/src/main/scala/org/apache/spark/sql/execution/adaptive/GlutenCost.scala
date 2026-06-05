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
package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.SparkPlan

/** Since https://github.com/apache/gluten/pull/6143. */
class GlutenCost(val eval: CostEvaluator, val plan: SparkPlan) extends Cost {
  override def compare(that: Cost): Int = that match {
    case that: GlutenCost if plan eq that.plan =>
      0
    case that: GlutenCost if plan == that.plan =>
      // Plans are identical. Considers the newer one as having lower cost.
      -(plan.id - that.plan.id)
    case that: GlutenCost =>
      // Plans are different. Use the delegated cost evaluator.
      assert(eval == that.eval)
      eval.evaluateCost(plan).compare(eval.evaluateCost(that.plan))
    case _ =>
      throw QueryExecutionErrors.cannotCompareCostWithTargetCostError(that.toString)
  }

  override def hashCode(): Int = throw new UnsupportedOperationException()

  override def equals(obj: Any): Boolean = obj match {
    case that: Cost => compare(that) == 0
    case _ => false
  }
}

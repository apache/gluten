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
package org.apache.gluten.extension.columnar.heuristic

import org.apache.gluten.extension.caller.CallerInfo
import org.apache.gluten.extension.columnar.{ColumnarRuleApplier, ColumnarRuleExecutor}
import org.apache.gluten.extension.columnar.ColumnarRuleApplier.ColumnarRuleCall
import org.apache.gluten.logging.LogLevelUtil

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

/**
 * Columnar rule applier that optimizes, implements Spark plan into Gluten plan by heuristically
 * applying columnar rules in fixed order.
 */
class HeuristicApplier(
    session: SparkSession,
    transformBuilders: Seq[ColumnarRuleCall => Rule[SparkPlan]],
    fallbackPolicyBuilders: Seq[ColumnarRuleCall => SparkPlan => Rule[SparkPlan]],
    postBuilders: Seq[ColumnarRuleCall => Rule[SparkPlan]],
    finalBuilders: Seq[ColumnarRuleCall => Rule[SparkPlan]],
    ruleWrappers: Seq[Rule[SparkPlan] => Rule[SparkPlan]])
  extends ColumnarRuleApplier
  with Logging
  with LogLevelUtil {
  override def apply(plan: SparkPlan, outputsColumnar: Boolean): SparkPlan = {
    val call = new ColumnarRuleCall(session, CallerInfo.create(), outputsColumnar)
    makeRule(call).apply(plan)
  }

  private def makeRule(call: ColumnarRuleCall): Rule[SparkPlan] = {
    originalPlan =>
      val suggestedPlan = transformPlan("transform", transformRules(call), originalPlan)
      val finalPlan = transformPlan(
        "fallback",
        fallbackPolicies(call).map(_(originalPlan)),
        suggestedPlan) match {
        case FallbackNode(fallbackPlan) =>
          // we should use vanilla c2r rather than native c2r,
          // and there should be no `GlutenPlan` anymore,
          // so skip the `postRules()`.
          fallbackPlan
        case plan =>
          transformPlan("post", postRules(call), plan)
      }
      transformPlan("final", finalRules(call), finalPlan)
  }

  private def transformPlan(
      phase: String,
      rules: Seq[Rule[SparkPlan]],
      plan: SparkPlan): SparkPlan = {
    val wrappedRules = ruleWrappers.foldLeft(rules) {
      case (rules, wrapper) =>
        rules.map(wrapper)
    }
    new ColumnarRuleExecutor(phase, wrappedRules).execute(plan)
  }

  /**
   * Rules to let planner create a suggested Gluten plan being sent to `fallbackPolicies` in which
   * the plan will be breakdown and decided to be fallen back or not.
   */
  private def transformRules(call: ColumnarRuleCall): Seq[Rule[SparkPlan]] = {
    transformBuilders.map(b => b.apply(call))
  }

  /**
   * Rules to add wrapper `FallbackNode`s on top of the input plan, as hints to make planner fall
   * back the whole input plan to the original vanilla Spark plan.
   */
  private def fallbackPolicies(call: ColumnarRuleCall): Seq[SparkPlan => Rule[SparkPlan]] = {
    fallbackPolicyBuilders.map(b => b.apply(call))
  }

  /**
   * Rules applying to non-fallen-back Gluten plans. To do some post cleanup works on the plan to
   * make sure it be able to run and be compatible with Spark's execution engine.
   */
  private def postRules(call: ColumnarRuleCall): Seq[Rule[SparkPlan]] = {
    postBuilders.map(b => b.apply(call))
  }

  /*
   * Rules consistently applying to all input plans after all other rules have been applied, despite
   * whether the input plan is fallen back or not.
   */
  private def finalRules(call: ColumnarRuleCall): Seq[Rule[SparkPlan]] = {
    finalBuilders.map(b => b.apply(call))
  }
}

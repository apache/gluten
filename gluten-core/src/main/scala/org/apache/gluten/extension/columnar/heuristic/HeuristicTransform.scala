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

import org.apache.gluten.component.Component
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.extension.caller.CallerInfo
import org.apache.gluten.extension.columnar.ColumnarRuleApplier.ColumnarRuleCall
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.extension.columnar.offload.OffloadSingleNode
import org.apache.gluten.extension.columnar.offload.OffloadSingleNode._
import org.apache.gluten.extension.columnar.rewrite.RewriteSingleNode
import org.apache.gluten.extension.columnar.validator.Validator
import org.apache.gluten.extension.injector.Injector
import org.apache.gluten.logging.LogLevelUtil

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

/**
 * Rule to offload Spark query plan to Gluten query plan using programed heuristics.
 *
 * The procedure consists of 3 stages:
 *
 *   1. Rewrite 2. Validate 3. Offload
 *
 * In the rewrite stage, planner will try converting the Spark query plan to various forms of
 * possible alternative Spark query plans, then choose the optimal one to send to next stage. During
 * which, the same validation code that is about to be used in stage 2 might be invoked early to
 * predict on the estimate "cost" of an alternative Spark query plan.
 *
 * Once the plan is rewritten, query planner will call native validation code in stage 2 to
 * determine which part of the plan is offload-able or not, then add fallback tags to the
 * non-offload-able parts.
 *
 * In stage 3, query planner will convert the offload-able Spark plan nodes into Gluten plan nodes.
 */
class HeuristicTransform private (all: Seq[Rule[SparkPlan]])
  extends Rule[SparkPlan]
  with LogLevelUtil {
  override def apply(plan: SparkPlan): SparkPlan = {
    all.foldLeft(plan) {
      case (plan, single) =>
        single(plan)
    }
  }
}

object HeuristicTransform {
  def withRules(all: Seq[Rule[SparkPlan]]): HeuristicTransform = {
    new HeuristicTransform(all)
  }

  /**
   * A simple heuristic transform rule with a validator and some offload rules.
   *
   * Validator will be called before applying the offload rules.
   */
  case class Simple(validator: Validator, offloadRules: Seq[OffloadSingleNode])
    extends Rule[SparkPlan]
    with Logging {
    override def apply(plan: SparkPlan): SparkPlan = {
      offloadRules.foldLeft(plan) {
        case (p, rule) =>
          p.transformUp {
            node =>
              validator.validate(node) match {
                case Validator.Passed =>
                  rule.offloadAndPropagateTag(node)
                case Validator.Failed(reason) =>
                  logDebug(s"Validation failed by reason: $reason on query plan: ${node.nodeName}")
                  if (FallbackTags.maybeOffloadable(node)) {
                    FallbackTags.add(node, reason)
                  }
                  node
              }
          }
      }
    }
  }

  /**
   * A heuristic transform rule with given rewrite rules. Fallback tags will be used in the
   * procedure to determine which part of the plan is or is not eligible to be offloaded. The tags
   * should also be correctly handled in the offload rules.
   *
   * TODO: Handle tags internally. Remove tag handling code in user offload rules.
   */
  case class WithRewrites(
      validator: Validator,
      rewriteRules: Seq[RewriteSingleNode],
      offloadRules: Seq[OffloadSingleNode])
    extends Rule[SparkPlan] {
    private val validate = AddFallbackTags(validator)
    private val rewrite = RewriteSparkPlanRulesManager(validate, rewriteRules)
    private val offload = LegacyOffload(offloadRules)

    override def apply(plan: SparkPlan): SparkPlan = {
      Seq(rewrite, validate, offload).foldLeft(plan) {
        case (plan, stage) =>
          stage(plan)
      }
    }
  }

  // Creates a static HeuristicTransform rule for use in certain
  // places that requires to emulate the offloading of a Spark query plan.
  //
  // TODO: Avoid using this and eventually remove the API.
  def static(): HeuristicTransform = {
    val exts = new SparkSessionExtensions()
    val dummyInjector = new Injector(exts)
    // Components should override Backend's rules. Hence, reversed injection order is applied.
    Component.sorted().reverse.foreach(_.injectRules(dummyInjector))
    val session = SparkSession.getActiveSession.getOrElse(
      throw new GlutenException(
        "HeuristicTransform#static can only be called when an active Spark session exists"))
    val call = new ColumnarRuleCall(session, CallerInfo.create(), false)
    dummyInjector.gluten.legacy.createHeuristicTransform(call)
  }
}

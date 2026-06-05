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
package org.apache.gluten.extension.injector

import org.apache.gluten.config.GlutenCoreConfig
import org.apache.gluten.extension.GlutenColumnarRule
import org.apache.gluten.extension.columnar.ColumnarRuleApplier
import org.apache.gluten.extension.columnar.ColumnarRuleApplier.ColumnarRuleCall
import org.apache.gluten.extension.columnar.heuristic.{HeuristicApplier, HeuristicTransform}

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.mutable

/** Injector used to inject query planner rules into Gluten. */
class GlutenInjector private[injector] (control: InjectorControl) {
  import GlutenInjector._
  val legacy: LegacyInjector = new LegacyInjector()

  private[injector] def inject(extensions: SparkSessionExtensions): Unit = {
    extensions.injectColumnar(
      control.disabler().wrapColumnarRule(s => new GlutenColumnarRule(s, applier)))
  }

  private def applier(session: SparkSession): ColumnarRuleApplier = {
    val conf = new GlutenCoreConfig(session.sessionState.conf)
    legacy.createApplier(session)
  }
}

object GlutenInjector {
  class LegacyInjector {
    private val preTransformBuilders = mutable.Buffer.empty[ColumnarRuleCall => Rule[SparkPlan]]
    private val transformBuilders = mutable.Buffer.empty[ColumnarRuleCall => Rule[SparkPlan]]
    private val postTransformBuilders = mutable.Buffer.empty[ColumnarRuleCall => Rule[SparkPlan]]
    private val fallbackPolicyBuilders =
      mutable.Buffer.empty[ColumnarRuleCall => SparkPlan => Rule[SparkPlan]]
    private val postBuilders = mutable.Buffer.empty[ColumnarRuleCall => Rule[SparkPlan]]
    private val finalBuilders = mutable.Buffer.empty[ColumnarRuleCall => Rule[SparkPlan]]
    private val ruleWrappers = mutable.Buffer.empty[Rule[SparkPlan] => Rule[SparkPlan]]

    def injectPreTransform(builder: ColumnarRuleCall => Rule[SparkPlan]): Unit = {
      preTransformBuilders += builder
    }

    def injectTransform(builder: ColumnarRuleCall => Rule[SparkPlan]): Unit = {
      transformBuilders += builder
    }

    def injectPostTransform(builder: ColumnarRuleCall => Rule[SparkPlan]): Unit = {
      postTransformBuilders += builder
    }

    def injectFallbackPolicy(builder: ColumnarRuleCall => SparkPlan => Rule[SparkPlan]): Unit = {
      fallbackPolicyBuilders += builder
    }

    def injectPost(builder: ColumnarRuleCall => Rule[SparkPlan]): Unit = {
      postBuilders += builder
    }

    def injectFinal(builder: ColumnarRuleCall => Rule[SparkPlan]): Unit = {
      finalBuilders += builder
    }

    def injectRuleWrapper(wrapper: Rule[SparkPlan] => Rule[SparkPlan]): Unit = {
      ruleWrappers += wrapper
    }

    private[injector] def createApplier(session: SparkSession): ColumnarRuleApplier = {
      new HeuristicApplier(
        session,
        (preTransformBuilders ++ Seq(
          c => createHeuristicTransform(c)) ++ postTransformBuilders).toSeq,
        fallbackPolicyBuilders.toSeq,
        postBuilders.toSeq,
        finalBuilders.toSeq,
        ruleWrappers.toSeq
      )
    }

    def createHeuristicTransform(call: ColumnarRuleCall): HeuristicTransform = {
      val all = transformBuilders.map(_(call))
      HeuristicTransform.withRules(all.toSeq)
    }
  }
}

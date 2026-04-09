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
package org.apache.spark.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

object SparkPlanRules extends Logging {
  // Since https://github.com/apache/gluten/pull/1523
  def extendedColumnarRule(ruleNamesStr: String): SparkSession => Rule[SparkPlan] =
    (session: SparkSession) => {
      val ruleNames = ruleNamesStr.split(",").filter(_.nonEmpty)
      val rules = ruleNames.flatMap {
        ruleName =>
          try {
            val ruleClass = Utils.classForName(ruleName)
            val rule =
              ruleClass
                .getConstructor(classOf[SparkSession])
                .newInstance(session)
                .asInstanceOf[Rule[SparkPlan]]
            Some(rule)
          } catch {
            // Ignore the error if we cannot find the class or when the class has the wrong type.
            case e @ (_: ClassCastException | _: ClassNotFoundException |
                _: NoClassDefFoundError) =>
              logWarning(s"Cannot create extended rule $ruleName", e)
              None
          }
      }
      new OrderedRules(rules)
    }

  object EmptyRule extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = plan
  }

  class AbortRule(message: String) extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan =
      throw new IllegalStateException(
        "AbortRule is being executed, this should not happen. Reason: " + message)
  }

  class OrderedRules(rules: Seq[Rule[SparkPlan]]) extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = {
      rules.foldLeft(plan) {
        case (plan, rule) =>
          rule.apply(plan)
      }
    }
  }
}

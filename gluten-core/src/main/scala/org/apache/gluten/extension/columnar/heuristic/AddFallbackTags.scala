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

import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.extension.columnar.validator.Validator

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

// Add fallback tags when validator returns negative outcome.
case class AddFallbackTags(validator: Validator) extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    plan.foreachUp {
      case p if FallbackTags.maybeOffloadable(p) => addFallbackTag(p)
      case _ =>
    }
    plan
  }

  private def addFallbackTag(plan: SparkPlan): Unit = {
    val outcome = validator.validate(plan)
    outcome match {
      case Validator.Failed(reason) =>
        FallbackTags.add(plan, reason)
      case Validator.Passed =>
    }
  }
}

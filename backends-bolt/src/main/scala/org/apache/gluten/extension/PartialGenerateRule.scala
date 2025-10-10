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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{ColumnarPartialGenerateExec, GenerateExecTransformer}
import org.apache.gluten.utils.PlanUtil

import org.apache.spark.sql.catalyst.expressions.UserDefinedExpression
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{GenerateExec, SparkPlan}

case class PartialGenerateRule() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!GlutenConfig.get.enableColumnarPartialGenerate) {
      return plan
    }
    val newPlan = plan match {
      // If the root node of the plan is a GenerateExec and its child is a gluten columnar op,
      // we try to add a ColumnarPartialGenerateExec
      case plan: GenerateExec if PlanUtil.isGlutenColumnarOp(plan.child) =>
        tryAddColumnarPartialGenerateExec(plan)
      case _ => plan
    }
    newPlan.transformUp {
      case parent: SparkPlan
          if parent.children.exists(_.isInstanceOf[GenerateExec]) &&
            PlanUtil.isGlutenColumnarOp(parent) =>
        parent.mapChildren {
          case plan: GenerateExec if PlanUtil.isGlutenColumnarOp(plan.child) =>
            tryAddColumnarPartialGenerateExec(plan)
          case other => other
        }
    }
  }

  private def tryAddColumnarPartialGenerateExec(plan: GenerateExec): SparkPlan = {
    if (GenerateExecTransformer.supportsGenerate(plan.generator)) {
      return plan
    }
    if (!plan.generator.isInstanceOf[UserDefinedExpression]) {
      return plan
    }
    val transformer = ColumnarPartialGenerateExec.create(plan)
    if (transformer.doValidate().ok()) {
      transformer
    } else plan
  }
}

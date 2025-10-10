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
import org.apache.gluten.execution.ColumnarPartialProjectExec
import org.apache.gluten.utils.PlanUtil

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}

case class PartialProjectRule(spark: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!GlutenConfig.get.enableColumnarPartialProject) {
      return plan
    }

    val newPlan = plan match {
      // If the root node of the plan is a ProjectExec and its child is a gluten columnar op,
      // we try to add a ColumnarPartialProjectExec
      case p: ProjectExec if PlanUtil.isGlutenColumnarOp(p.child) =>
        tryAddColumnarPartialProjectExec(p)
      case _ => plan
    }

    newPlan.transformUp {
      case parent: SparkPlan
          if parent.children.exists(_.isInstanceOf[ProjectExec]) &&
            PlanUtil.isGlutenColumnarOp(parent) =>
        parent.mapChildren {
          case p: ProjectExec if PlanUtil.isGlutenColumnarOp(p.child) =>
            tryAddColumnarPartialProjectExec(p)
          case other => other
        }
    }
  }

  private def tryAddColumnarPartialProjectExec(plan: ProjectExec): SparkPlan = {
    val transformer = ColumnarPartialProjectExec.create(plan)
    if (
      transformer.doValidate().ok() &&
      transformer.child.asInstanceOf[ColumnarPartialProjectExec].doValidate().ok()
    ) {
      transformer
    } else plan
  }
}

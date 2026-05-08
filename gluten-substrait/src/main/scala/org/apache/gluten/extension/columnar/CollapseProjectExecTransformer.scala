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
package org.apache.gluten.extension.columnar

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.ProjectExecTransformer

import org.apache.spark.sql.catalyst.expressions.{Alias, CreateNamedStruct, Expression, If, NamedExpression}
import org.apache.spark.sql.catalyst.optimizer.CollapseProjectShim
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

object CollapseProjectExecTransformer extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!GlutenConfig.get.enableColumnarProjectCollapse) {
      return plan
    }
    plan.transformUp {
      case p1 @ ProjectExecTransformer(_, p2: ProjectExecTransformer)
          if !containsNamedStructAlias(p2.projectList)
            && CollapseProjectShim.canCollapseExpressions(
              p1.projectList,
              p2.projectList,
              alwaysInline = false) =>
        val collapsedProject = p2.copy(projectList =
          CollapseProjectShim.buildCleanedProjectList(p1.projectList, p2.projectList))
        val validationResult = collapsedProject.doValidate()
        if (validationResult.ok()) {
          logDebug(s"Collapse project $p1 and $p2.")
          // copy tags from p1 first
          collapsedProject.copyTagsFrom(p1)
          // if p1 has no tags, copy p2's tags
          collapsedProject.copyTagsFrom(p2)
          collapsedProject
        } else {
          logDebug(s"Failed to collapse project, due to ${validationResult.reason()}")
          p1
        }
    }
  }

  /**
   * In Velox, CreateNamedStruct will generate a special output named obj, We cannot collapse such
   * project transformer, otherwise it will result in a bind reference failure. Checks for
   * CreateNamedStruct as direct Alias child or wrapped in If (null-guard pattern from
   * reconcileFieldNames), but not deeply nested inside arbitrary expressions.
   */
  private def containsNamedStructAlias(projectList: Seq[NamedExpression]): Boolean = {
    projectList.exists {
      case a: Alias => isOrWrapsCreateNamedStruct(a.child)
      case _ => false
    }
  }

  private def isOrWrapsCreateNamedStruct(expr: Expression): Boolean = {
    expr match {
      case _: CreateNamedStruct => true
      case If(_, trueValue, falseValue) =>
        isOrWrapsCreateNamedStruct(trueValue) || isOrWrapsCreateNamedStruct(falseValue)
      case _ => false
    }
  }
}

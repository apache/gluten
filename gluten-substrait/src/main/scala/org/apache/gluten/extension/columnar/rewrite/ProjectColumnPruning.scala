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
package org.apache.gluten.extension.columnar.rewrite

import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan, UnaryExecNode}

/**
 * After applying the PullOutPreProject rule, there may be some projects that contain columns not
 * consumed by the parent. These columns will be removed by this rewrite rule.
 */
object ProjectColumnPruning extends RewriteSingleNode {
  override def isRewritable(plan: SparkPlan): Boolean = {
    plan match {
      case parent: UnaryExecNode if parent.child.isInstanceOf[ProjectExec] => true
      case _ => false
    }
  }

  private def getReferences(plan: SparkPlan): AttributeSet = {
    // SPARK-55979 - aggregate.references is unreliable.
    AttributeSet(plan.expressions) -- (plan.producedAttributes -- plan.children.flatMap(
      _.outputSet))
  }

  override def rewrite(plan: SparkPlan): SparkPlan = plan match {
    case parent: UnaryExecNode if parent.child.isInstanceOf[ProjectExec] =>
      val project = parent.child.asInstanceOf[ProjectExec]
      val unusedAttribute = project.outputSet -- (getReferences(parent) ++ parent.outputSet)

      if (unusedAttribute.nonEmpty) {
        val newProject = project.copy(projectList = project.projectList.diff(unusedAttribute.toSeq))
        newProject.copyTagsFrom(project)
        parent.withNewChildren(Seq(newProject))
      } else {
        parent
      }
    case _ => plan
  }
}

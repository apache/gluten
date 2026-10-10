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

import org.apache.gluten.execution.DeltaProjectExecTransformer
import org.apache.gluten.extension.DeltaPostTransformRules.containsIncrementMetricExpr
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.extension.columnar.offload.OffloadSingleNode

import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}

case class OffloadDeltaProject() extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case project @ ProjectExec(projectList, child)
        if projectList.exists(containsIncrementMetricExpr) =>
      val nativeCounting = IncrementMetricOffload.nativeCounting
      if (IncrementMetricOffload.canOffloadProject(projectList, nativeCounting)) {
        DeltaProjectExecTransformer(projectList, child)
      } else {
        val reason = if (nativeCounting) {
          IncrementMetricOffload.nativeConjunctionProjectReason
        } else {
          IncrementMetricOffload.conditionalProjectReason
        }
        FallbackTags.add(project, reason)
        project
      }
    case p => p
  }
}

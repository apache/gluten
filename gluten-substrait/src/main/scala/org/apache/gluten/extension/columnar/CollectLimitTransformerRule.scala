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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{CollectLimitExec, SparkPlan}

case class CollectLimitTransformerRule() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!GlutenConfig.get.enableColumnarCollectLimit) {
      return plan
    }

    val transformed = plan.transformUp {
      case exec: CollectLimitExec
          if exec.child.supportsColumnar &&
            (exec.child.output.nonEmpty ||
              BackendsApiManager.getSettings.supportEmptySchemaColumnarShuffle()) =>
        val offset = exec.offset
        BackendsApiManager.getSparkPlanExecApiInstance
          .genColumnarCollectLimitExec(exec.limit, exec.child, offset)
    }

    transformed
  }
}

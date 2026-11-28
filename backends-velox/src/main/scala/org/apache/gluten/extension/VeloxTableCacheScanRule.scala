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

import org.apache.gluten.utils.PlanUtil

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.columnar.{InMemoryTableScanExec, InMemoryTableScanExecTransformer}

/**
 * Replaces an [[InMemoryTableScanExec]] that reads a Velox columnar table cache with
 * [[InMemoryTableScanExecTransformer]], which reports cache read metrics.
 *
 * Under AQE, the scan is wrapped in a `TableCacheQueryStageExec` before Gluten's columnar rules
 * run, and those rules are not applied inside the stage. The rule is therefore registered both as a
 * query stage preparation rule, which runs on the whole plan before stages are created, and as a
 * Gluten columnar rule for plans executed without AQE.
 */
case class VeloxTableCacheScanRule() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case scan: InMemoryTableScanExec
        if !scan.isInstanceOf[InMemoryTableScanExecTransformer] && PlanUtil.isGlutenTableCache(
          scan) =>
      InMemoryTableScanExecTransformer(scan)
  }
}

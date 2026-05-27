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
package org.apache.spark.sql.delta.stats

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, V2WriteCommand}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.{DeltaTable, OptimisticTransaction, PreprocessTableWithDVs}
import org.apache.spark.sql.delta.sources.DeltaSQLConf

/**
 * Delta 3.3 compatibility shim around Delta's PrepareDeltaScan.
 *
 * This preserves Delta's normal scan preparation first, including stats skipping, metadata-query
 * optimization, and transaction read tracking. After that, Gluten runs DV preprocessing so the scan
 * exposes Delta's internal DV row-deleted column and row-index metadata. The native physical rule
 * later strips Spark's synthetic DV predicate once the materialized DV payload is attached to the
 * native split, so Velox applies the DV filter exactly once.
 */
class PrepareDeltaScan(protected val spark: SparkSession)
  extends Rule[LogicalPlan]
  with PrepareDeltaScanBase
  with PreprocessTableWithDVs {

  override def apply(plan0: LogicalPlan): LogicalPlan = {
    var plan = plan0

    val isSubquery = isSubqueryRoot(plan)
    val isDataSourceV2 = plan.isInstanceOf[V2WriteCommand]
    if (isSubquery || isDataSourceV2) {
      return plan
    }

    val updatedPlan = if (spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_STATS_SKIPPING)) {
      if (spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_METADATA_QUERY_ENABLED)) {
        plan = optimizeQueryWithMetadata(plan)
      }
      prepareDeltaScan(plan)
    } else {
      OptimisticTransaction.getActive.foreach {
        txn =>
          val logsInPlan = plan.collect { case DeltaTable(fileIndex) => fileIndex.deltaLog }
          if (logsInPlan.exists(_.isSameLogAs(txn.deltaLog))) {
            txn.readWholeTable()
          }
      }
      plan
    }

    preprocessTablesWithDVs(updatedPlan)
  }
}

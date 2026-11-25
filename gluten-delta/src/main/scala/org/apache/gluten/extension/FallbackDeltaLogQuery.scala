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

import org.apache.gluten.extension.columnar.FallbackTags

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.DeltaLogFileIndex
import org.apache.spark.sql.execution.{FileSourceScanExec, RDDScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

/**
 * Keeps queries that read nothing but Delta's own transaction log on Spark.
 *
 * Delta answers planning questions (which files to scan, data skipping, metadata-only aggregates)
 * with Spark jobs over its log: the `_delta_log` files, read through a [[DeltaLogFileIndex]], and
 * the RDD it caches after replaying them, which Delta names "Delta Table State #..." or "Delta
 * Table State with Stats #..." (Snapshot.stateReconstruction and DataSkippingReader.withStats).
 * Those rows come from the JVM and the jobs map them through Scala objects, so offloading them only
 * wraps small native steps in row/columnar conversions, and every query plans slower than on Spark.
 *
 * A plan qualifies only when every source it reads is such a log source, so no query over table
 * data is affected.
 */
object FallbackDeltaLogQuery extends Rule[SparkPlan] {
  private val deltaStateRddPrefix = "Delta Table State"

  override def apply(plan: SparkPlan): SparkPlan = {
    if (readsOnlyDeltaLog(plan)) {
      plan.foreach(FallbackTags.add(_, "query over the Delta transaction log"))
    }
    plan
  }

  private def readsOnlyDeltaLog(plan: SparkPlan): Boolean = {
    val leaves = sources(plan)
    leaves.nonEmpty && leaves.forall(isDeltaLogSource)
  }

  // Looks through AQE query stages and reused exchanges, so a later stage of a log query is
  // recognized like its first one.
  private def sources(plan: SparkPlan): Seq[SparkPlan] = plan match {
    case stage: QueryStageExec => sources(stage.plan)
    case reused: ReusedExchangeExec => sources(reused.child)
    case leaf if leaf.children.isEmpty => Seq(leaf)
    case other => other.children.flatMap(sources)
  }

  private def isDeltaLogSource(plan: SparkPlan): Boolean = plan match {
    case scan: RDDScanExec => Option(scan.rdd.name).exists(_.startsWith(deltaStateRddPrefix))
    case scan: FileSourceScanExec => scan.relation.location.isInstanceOf[DeltaLogFileIndex]
    case _ => false
  }
}

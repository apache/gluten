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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, DynamicPruning}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{ColumnarToRowExec, FilterExec, LeafExecNode, ProjectExec, SparkPlan, SparkStrategy}

import org.apache.iceberg.spark.source.CompactIcebergScan

/**
 * Spark's V2 strategy probes supportsColumnar while constructing the physical plan. That probe
 * materializes the vanilla input partitions before Gluten can offload the scan. Defer it and always
 * add the projection needed for safe rows if the scan later falls back.
 */
object CompactIcebergStrategy extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(project, filters, relation: DataSourceV2ScanRelation)
        if relation.scan.isInstanceOf[CompactIcebergScan] &&
          relation.scan.asInstanceOf[CompactIcebergScan].usesCompactPlanning &&
          relation.keyGroupedPartitioning.isEmpty &&
          !filters.exists(_.isInstanceOf[DynamicPruning]) =>
      val scan = BatchScanExec(
        relation.output,
        relation.scan,
        Seq.empty,
        relation.ordering,
        relation.relation.table,
        StoragePartitionJoinParams(None))
      val deferred = DeferredIcebergScanExec(scan)
      val filtered = filters.reduceOption(And).map(FilterExec(_, deferred)).getOrElse(deferred)
      ProjectExec(project, filtered) :: Nil
    case _ => Nil
  }
}

case class DeferredIcebergScanExec(scanExec: BatchScanExec) extends LeafExecNode {
  override def output: Seq[Attribute] = scanExec.output

  override protected def doExecute(): RDD[InternalRow] = {
    val rows = if (scanExec.supportsColumnar) ColumnarToRowExec(scanExec) else scanExec
    ProjectExec(output, rows).execute()
  }
}

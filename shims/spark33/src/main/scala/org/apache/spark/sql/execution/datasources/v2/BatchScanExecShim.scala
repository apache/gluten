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

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.KeyGroupedPartitioning
import org.apache.spark.sql.catalyst.util.InternalRowSet
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{HasPartitionKey, InputPartition, Scan, SupportsRuntimeFiltering}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class BatchScanExecShim(
    override val output: Seq[AttributeReference],
    @transient override val scan: Scan,
    override val runtimeFilters: Seq[Expression],
    val keyGroupedPartitioning: Option[Seq[Expression]] = None,
    val ordering: Option[Seq[SortOrder]] = None,
    @transient val table: Table,
    val commonPartitionValues: Option[Seq[(InternalRow, Int)]] = None,
    val applyPartialClustering: Boolean = false,
    val replicatePartitions: Boolean = false)
  extends AbstractBatchScanExec(output, scan, runtimeFilters) {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] = Map()

  lazy val metadataColumns: Seq[AttributeReference] = output.collect {
    case FileSourceMetadataAttribute(attr) => attr
  }

  def hasUnsupportedColumns: Boolean = {
    // TODO, fallback if user define same name column due to we can't right now
    // detect which column is metadata column which is user defined column.
    val metadataColumnsNames = metadataColumns.map(_.name)
    output
      .filterNot(metadataColumns.toSet)
      .exists(v => metadataColumnsNames.contains(v.name))
  }

  def postDriverMetrics(): Unit = {}

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException("Need to implement this method")
  }

  @transient protected lazy val filteredPartitions: Seq[Seq[InputPartition]] = {
    val dataSourceFilters = runtimeFilters.flatMap {
      case DynamicPruningExpression(e) => DataSourceStrategy.translateRuntimeFilter(e)
      case _ => None
    }

    if (dataSourceFilters.nonEmpty) {
      val originalPartitioning = outputPartitioning

      // the cast is safe as runtime filters are only assigned if the scan can be filtered
      val filterableScan = scan.asInstanceOf[SupportsRuntimeFiltering]
      filterableScan.filter(dataSourceFilters.toArray)

      // call toBatch again to get filtered partitions
      val newPartitions = scan.toBatch.planInputPartitions()

      originalPartitioning match {
        case p: KeyGroupedPartitioning =>
          if (newPartitions.exists(!_.isInstanceOf[HasPartitionKey])) {
            throw new SparkException(
              "Data source must have preserved the original partitioning " +
                "during runtime filtering: not all partitions implement HasPartitionKey after " +
                "filtering")
          }

          val newRows = new InternalRowSet(p.expressions.map(_.dataType))
          newRows ++= newPartitions.map(_.asInstanceOf[HasPartitionKey].partitionKey())
          val oldRows = p.partitionValuesOpt.get

          if (oldRows.size != newRows.size) {
            throw new SparkException(
              "Data source must have preserved the original partitioning " +
                "during runtime filtering: the number of unique partition values obtained " +
                s"through HasPartitionKey changed: before ${oldRows.size}, after ${newRows.size}")
          }

          if (!oldRows.forall(newRows.contains)) {
            throw new SparkException(
              "Data source must have preserved the original partitioning " +
                "during runtime filtering: the number of unique partition values obtained " +
                s"through HasPartitionKey remain the same but do not exactly match")
          }

          groupPartitions(newPartitions).get.map(_._2)

        case _ =>
          // no validation is needed as the data source did not report any specific partitioning
          newPartitions.map(Seq(_))
      }

    } else {
      partitions
    }
  }

  @transient lazy val pushedAggregate: Option[Aggregation] = {
    scan match {
      case s: ParquetScan => s.pushedAggregate
      case o: OrcScan => o.pushedAggregate
      case _ => None
    }
  }

  final override protected def otherCopyArgs: Seq[AnyRef] = {
    Seq(
      ordering,
      table,
      commonPartitionValues,
      // Box boolean to match `AnyRef`
      Boolean.box(applyPartialClustering),
      Boolean.box(replicatePartitions))
  }
}

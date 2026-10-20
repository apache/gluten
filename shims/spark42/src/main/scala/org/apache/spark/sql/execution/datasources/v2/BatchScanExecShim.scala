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
import org.apache.spark.sql.catalyst.plans.physical.KeyedPartitioning
import org.apache.spark.sql.catalyst.util.InternalRowComparableWrapper
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.functions.Reducer
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{HasPartitionKey, InputPartition, Scan}
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

// Spark 4.2 removed `StoragePartitionJoinParams` and no longer accepts the SPJ parameters
// (`joinKeyPositions`, `commonPartitionValues`, `reducers`, `applyPartialClustering`,
// `replicatePartitions`) on the scan node -- that grouping/replication now happens in
// `GroupPartitionsExec`. To keep the public constructor identical to the other Spark shims
// (Gluten's own planner reads these vals), they are kept here as shim-local fields and are simply
// not forwarded into the Spark superclass, which now only takes `keyGroupedPartitioning`.
abstract class BatchScanExecShim(
    output: Seq[AttributeReference],
    @transient scan: Scan,
    runtimeFilters: Seq[Expression],
    keyGroupedPartitioning: Option[Seq[Expression]] = None,
    ordering: Option[Seq[SortOrder]] = None,
    @transient val table: Table,
    val joinKeyPositions: Option[Seq[Int]] = None,
    val commonPartitionValues: Option[Seq[(InternalRow, Int)]] = None,
    val reducers: Option[Seq[Option[Reducer[_, _]]]] = None,
    val applyPartialClustering: Boolean = false,
    val replicatePartitions: Boolean = false)
  extends AbstractBatchScanExec(
    output,
    scan,
    runtimeFilters,
    ordering,
    table,
    keyGroupedPartitioning
  ) {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] = Map()

  lazy val metadataColumns: Seq[AttributeReference] = output.collect {
    case FileSourceConstantMetadataAttribute(attr) => attr
    case FileSourceGeneratedMetadataAttribute(attr, _) => attr
  }

  def hasUnsupportedColumns: Boolean = {
    // TODO, fallback if user define same name column due to we can't right now
    // detect which column is metadata column which is user defined column.
    val metadataColumnsNames = metadataColumns.map(_.name)
    output
      .filterNot(metadataColumns.toSet)
      .exists(v => metadataColumnsNames.contains(v.name))
  }

  // Spark 4.2 moved `postDriverMetrics` to SupportsCustomDriverMetrics and made the reported
  // task metrics an explicit argument (see BatchScanExec in Spark 4.2).
  def doPostDriverMetrics(): Unit = {
    postDriverMetrics(scan.reportDriverMetrics())
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException("Need to implement this method")
  }

  @transient protected lazy val filteredPartitions: Seq[Seq[InputPartition]] = {
    val originalPartitioning = outputPartitioning

    val filtered = PushDownUtils.pushRuntimeFilters(scan, runtimeFilters, table, output)
    // call toBatch again to get filtered partitions if any runtime filter was pushed
    val newPartitions =
      if (filtered) scan.toBatch.planInputPartitions().toSeq else inputPartitions

    originalPartitioning match {
      case k: KeyedPartitioning =>
        if (newPartitions.exists(!_.isInstanceOf[HasPartitionKey])) {
          throw new SparkException(
            "Data source must have preserved the original partitioning " +
              "during runtime filtering: not all partitions implement HasPartitionKey after " +
              "filtering")
        }

        if (filtered) {
          // Validate that runtime filtering only removed partition keys, never introduced new ones.
          val newPartitionKeys = newPartitions
            .map(
              partition =>
                InternalRowComparableWrapper(
                  partition.asInstanceOf[HasPartitionKey].partitionKey(),
                  k.expressions))
            .toSet
          val oldPartitionKeys = k.partitionKeys.toSet
          // We require the new number of partition keys to be equal or less than the old number.
          if (oldPartitionKeys.size < newPartitionKeys.size) {
            throw new SparkException(
              "During runtime filtering, data source must either report " +
                "the same number of partition values, or a subset of partition values from the " +
                s"original. Before: ${oldPartitionKeys.size} partition values. " +
                s"After: ${newPartitionKeys.size} partition values")
          }
          if (!newPartitionKeys.forall(oldPartitionKeys.contains)) {
            throw new SparkException(
              "During runtime filtering, data source must not report new " +
                "partition values that are not present in the original partitioning.")
          }
        }

        // Group the splits that share the same partition key and sort the groups by key ascending.
        // This reproduces the key-grouped layout Spark 4.1's `BatchScanExec` produced and that
        // Gluten's native `SparkShims.orderPartitions` still expects.
        //
        // FIXME(Spark 4.2, apache/gluten#13139): this collapsing is incorrect for Spark 4.2, which
        // keeps splits ungrouped and defers grouping to `GroupPartitionsExec` -- e.g. keys
        // [A, A, B] advertise 3 partitions but build only 2, so `GroupPartitionsExec` requests an
        // index the reduced RDD lacks. It is currently unreachable (only `KeyGroupedPartitioning`
        // /SPJ sources hit it, and no such source -- e.g. Iceberg -- is enabled on Spark 4.2), so
        // it is dormant today. It must be reworked before native keyed DSv2 is enabled on 4.2; see
        // the issue for the two fix options.
        newPartitions
          .map(part => (part.asInstanceOf[HasPartitionKey].partitionKey(), part))
          .groupBy { case (key, _) => InternalRowComparableWrapper(key, k.expressions) }
          .toSeq
          .sortBy(_._1)(k.keyOrdering)
          .map { case (_, keyedParts) => keyedParts.map(_._2) }

      case _ =>
        // no validation is needed as the data source did not report any specific partitioning
        newPartitions.map(Seq(_))
    }
  }

  @transient lazy val pushedAggregate: Option[Aggregation] = {
    scan match {
      case s: ParquetScan => s.pushedAggregate
      case o: OrcScan => o.pushedAggregate
      case _ => None
    }
  }
}

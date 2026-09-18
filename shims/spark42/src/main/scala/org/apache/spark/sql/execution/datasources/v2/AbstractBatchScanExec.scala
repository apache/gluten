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
import org.apache.spark.sql.catalyst.plans.physical.{KeyedPartitioning, SinglePartition}
import org.apache.spark.sql.catalyst.util.{truncatedString, InternalRowComparableWrapper}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read._
import org.apache.spark.util.ArrayImplicits._

import com.google.common.base.Objects

/**
 * Physical plan node for scanning a batch of data from a data source v2. Please ref BatchScanExec
 * in Spark.
 *
 * Ported to the Spark 4.2 storage-partitioned-join API: Spark 4.2 removed
 * `StoragePartitionJoinParams` / `KeyGroupedPartitioning` and replaced them with the single
 * `keyGroupedPartitioning` expression list plus `KeyedPartitioning`. The partition
 * grouping/replication that BatchScanExec used to perform in `inputRDD` was moved out of the scan
 * into `GroupPartitionsExec`, so this class now mirrors Spark 4.2's `BatchScanExec`.
 */
abstract class AbstractBatchScanExec(
    output: Seq[AttributeReference],
    @transient scan: Scan,
    val runtimeFilters: Seq[Expression],
    ordering: Option[Seq[SortOrder]] = None,
    @transient table: Table,
    val keyGroupedPartitioning: Option[Seq[Expression]] = None
) extends DataSourceV2ScanExecBase {

  @transient lazy val batch: Batch = if (scan == null) null else scan.toBatch

  // TODO: unify the equal/hashCode implementation for all data source v2 query plans.
  override def equals(other: Any): Boolean = other match {
    case other: AbstractBatchScanExec =>
      this.batch != null && this.batch == other.batch &&
      this.runtimeFilters == other.runtimeFilters &&
      this.keyGroupedPartitioning == other.keyGroupedPartitioning
    case _ =>
      false
  }

  override def hashCode(): Int = Objects.hashCode(batch, runtimeFilters)

  @transient override lazy val inputPartitions: Seq[InputPartition] = inputPartitionsShim

  @transient protected lazy val inputPartitionsShim: Seq[InputPartition] =
    batch.planInputPartitions().toImmutableArraySeq

  @transient private lazy val filteredPartitions: Seq[Option[InputPartition]] = {
    val originalPartitioning = outputPartitioning

    val filtered = PushDownUtils.pushRuntimeFilters(scan, runtimeFilters, table, output)
    if (filtered) {
      // call toBatch again to get filtered partitions
      val newPartitions = scan.toBatch.planInputPartitions()

      originalPartitioning match {
        case k: KeyedPartitioning =>
          if (newPartitions.exists(!_.isInstanceOf[HasPartitionKey])) {
            throw new SparkException(
              "Data source must have preserved the original partitioning " +
                "during runtime filtering: not all partitions implement HasPartitionKey after " +
                "filtering")
          }

          val inputMap = k.partitionKeys.groupBy(identity).view.mapValues(_.size)
          val comparableKeyWrapperFactory = InternalRowComparableWrapper
            .getInternalRowComparableWrapperFactory(k.expressionDataTypes)
          val filteredMap = newPartitions.groupBy(
            p => comparableKeyWrapperFactory(p.asInstanceOf[HasPartitionKey].partitionKey()))

          if (!filteredMap.keySet.subsetOf(inputMap.keySet)) {
            throw new SparkException(
              "During runtime filtering, data source must not report new " +
                "partition keys that are not present in the original partitioning.")
          }

          inputMap.toSeq
            .sortBy(_._1)(k.keyOrdering)
            .flatMap {
              case (key, size) =>
                // We require the new number of partitions to be equal or less than the old number
                // of partitions for a given key. In the case of less than, empty partitions are
                // added.
                val fps = filteredMap.getOrElse(key, Array.empty)

                if (fps.size > size) {
                  throw new SparkException(
                    "During runtime filtering, data source must not report " +
                      s"new partitions for a given key. Before: $size partitions. " +
                      s"After: ${fps.size} partitions")
                }

                fps.map(Some(_)).padTo(size, None)
            }

        case _ =>
          // no validation is needed as the data source did not report any specific partitioning
          newPartitions.toSeq.map(Some(_))
      }

    } else {
      (originalPartitioning match {
        case k: KeyedPartitioning =>
          inputPartitions.sortBy(_.asInstanceOf[HasPartitionKey].partitionKey())(k.keyRowOrdering)

        case _ => inputPartitions
      }).map(Some(_))
    }
  }

  override lazy val readerFactory: PartitionReaderFactory = batch.createReaderFactory()

  override lazy val inputRDD: RDD[InternalRow] = {
    val rdd = if (filteredPartitions.isEmpty && outputPartitioning == SinglePartition) {
      // return an empty RDD with 1 partition if dynamic filtering removed the only split
      sparkContext.parallelize(Array.empty[InternalRow].toImmutableArraySeq, 1)
    } else {
      new DataSourceRDD(
        sparkContext,
        filteredPartitions,
        readerFactory,
        supportsColumnar,
        customMetrics)
    }
    postDriverMetrics(scan.reportDriverMetrics())
    rdd
  }

  override def simpleString(maxFields: Int): String = {
    val truncatedOutputString = truncatedString(output, "[", ", ", "]", maxFields)
    val runtimeFiltersString = s"RuntimeFilters: ${runtimeFilters.mkString("[", ",", "]")}"
    val result = s"$nodeName$truncatedOutputString ${scan.description()} $runtimeFiltersString"
    redact(result)
  }
}

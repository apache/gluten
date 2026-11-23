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
package org.apache.spark.sql.execution

import org.apache.gluten.metrics.GlutenTimeMetric

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, BoundReference, Expression, FileSourceConstantMetadataAttribute, FileSourceGeneratedMetadataAttribute, PlanExpression, Predicate}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.BitSet

abstract class FileSourceScanExecShim(
    @transient relation: HadoopFsRelation,
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    optionalNumCoalescedBuckets: Option[Int],
    dataFilters: Seq[Expression],
    tableIdentifier: Option[TableIdentifier],
    disableBucketedScan: Boolean = false)
  extends AbstractFileSourceScanExec(
    relation,
    output,
    requiredSchema,
    partitionFilters,
    optionalBucketSet,
    optionalNumCoalescedBuckets,
    dataFilters,
    tableIdentifier,
    disableBucketedScan) {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] = Map()

  lazy val metadataColumns: Seq[AttributeReference] = output.collect {
    case FileSourceConstantMetadataAttribute(attr) => attr
    case FileSourceGeneratedMetadataAttribute(attr, _) => attr
  }

  protected lazy val driverMetricsAlias = driverMetrics

  def hasUnsupportedColumns: Boolean = {
    // TODO, fallback if user define same name column due to we can't right now
    // detect which column is metadata column which is user defined column.
    val metadataColumnsNames = metadataColumns.map(_.name)
    output
      .filterNot(metadataColumns.toSet)
      .exists(v => metadataColumnsNames.contains(v.name))
  }

  def isMetadataColumn(attr: Attribute): Boolean = metadataColumns.contains(attr)

  def hasFieldIds: Boolean = ParquetUtils.hasFieldIds(requiredSchema)

  protected def isDynamicPruningFilter(e: Expression): Boolean =
    e.find(_.isInstanceOf[PlanExpression[_]]).isDefined

  protected def setFilesNumAndSizeMetric(
      partitions: Seq[PartitionDirectory],
      static: Boolean): Unit = {
    val filesNum = partitions.map(_.files.size.toLong).sum
    val filesSize = partitions.map(_.files.map(_.getLen).sum).sum
    if (!static || !partitionFilters.exists(isDynamicPruningFilter)) {
      driverMetrics("numFiles").set(filesNum)
      driverMetrics("filesSize").set(filesSize)
    } else {
      driverMetrics("staticFilesNum").set(filesNum)
      driverMetrics("staticFilesSize").set(filesSize)
    }
    if (relation.partitionSchema.nonEmpty) {
      driverMetrics("numPartitions").set(partitions.length)
    }
  }

  @transient override lazy val dynamicallySelectedPartitions: Array[PartitionDirectory] = {
    val dynamicPartitionFilters =
      partitionFilters.filter(isDynamicPruningFilter)
    val selected = if (dynamicPartitionFilters.nonEmpty) {
      GlutenTimeMetric.withMillisTime {
        // call the file index for the files matching all filters except dynamic partition filters
        val boundedFilters = dynamicPartitionFilters.map {
          dynamicPartitionFilter =>
            dynamicPartitionFilter.transform {
              case a: AttributeReference =>
                val index = relation.partitionSchema.indexWhere(a.name == _.name)
                BoundReference(index, relation.partitionSchema(index).dataType, nullable = true)
            }
        }
        val boundPredicate = Predicate.create(boundedFilters.reduce(And), Nil)
        val ret = selectedPartitions.filter(p => boundPredicate.eval(p.values))
        setFilesNumAndSizeMetric(ret, static = false)
        ret
      }(t => driverMetrics("pruningTime").set(t))
    } else {
      selectedPartitions
    }
    sendDriverMetrics()
    selected
  }

  def getPartitionArray: Array[PartitionDirectory] = {
    dynamicallySelectedPartitions
  }

  def getPartitionsSeq(): Seq[Partition] = {
    Seq()
  }
}

abstract class ArrowFileSourceScanLikeShim(original: FileSourceScanExec)
  extends FileSourceScanLike {
  override val nodeNamePrefix: String = "ArrowFile"

  override def tableIdentifier: Option[TableIdentifier] = original.tableIdentifier

  override def inputRDDs(): Seq[RDD[InternalRow]] = original.inputRDDs()

  override def dataFilters: Seq[Expression] = original.dataFilters

  override def disableBucketedScan: Boolean = original.disableBucketedScan

  override def optionalBucketSet: Option[BitSet] = original.optionalBucketSet

  override def optionalNumCoalescedBuckets: Option[Int] = original.optionalNumCoalescedBuckets

  override def partitionFilters: Seq[Expression] = original.partitionFilters

  override def relation: HadoopFsRelation = original.relation

  override def requiredSchema: StructType = original.requiredSchema
}

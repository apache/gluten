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
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.Partition
import org.apache.spark.internal.LogKeys.{COUNT, MAX_SPLIT_BYTES, OPEN_COST_IN_BYTES}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, BoundReference, Expression, FileSourceConstantMetadataAttribute, FileSourceGeneratedMetadataAttribute, PlanExpression, Predicate}
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.{BucketingUtils, FilePartition, HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.collection.BitSet

import org.apache.hadoop.fs.Path

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

  def dataFiltersInScan: Seq[Expression] = dataFilters.filterNot(_.references.exists {
    attr => SparkShimLoader.getSparkShims.isRowIndexMetadataColumn(attr.name)
  })

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

  protected def setFilesNumAndSizeMetric(partitions: ScanFileListing, static: Boolean): Unit = {
    val filesNum = partitions.totalNumberOfFiles
    val filesSize = partitions.totalFileSize
    if (!static || !partitionFilters.exists(isDynamicPruningFilter)) {
      driverMetrics("numFiles").set(filesNum)
      driverMetrics("filesSize").set(filesSize)
    } else {
      driverMetrics("staticFilesNum").set(filesNum)
      driverMetrics("staticFilesSize").set(filesSize)
    }
    if (relation.partitionSchema.nonEmpty) {
      driverMetrics("numPartitions").set(partitions.partitionCount)
    }
  }

  @transient override lazy val dynamicallySelectedPartitions: ScanFileListing = {
    val dynamicDataFilters = dataFilters.filter(isDynamicPruningFilter)
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
        val returnedFiles =
          selectedPartitions.filterAndPruneFiles(boundPredicate, dynamicDataFilters)
        setFilesNumAndSizeMetric(returnedFiles, false)
        returnedFiles
      }(t => driverMetrics("pruningTime").set(t))
    } else {
      selectedPartitions
    }
    sendDriverMetrics()
    selected
  }

  def getPartitionArray: Array[PartitionDirectory] = {
    // Use dynamicallySelectedPartitions which triggers selectedPartitions evaluation,
    // ensuring driver metrics (numFiles, filesSize, etc.) are properly set.
    // Convert ScanFileListing to Array[PartitionDirectory] via filePartitionIterator.
    dynamicallySelectedPartitions.filePartitionIterator.map {
      listing => PartitionDirectory(listing.values, listing.files.toSeq)
    }.toArray
  }

  /**
   * Create an RDD for bucketed reads. The non-bucketed variant of this function is
   * [[createReadRDD]].
   *
   * The algorithm is pretty simple: each RDD partition being returned should include all the files
   * with the same bucket id from all the given Hive partitions.
   *
   * @param bucketSpec
   *   the bucketing spec.
   * @param selectedPartitions
   *   Hive-style partition that are part of the read.
   */
  private def createBucketedReadPartition(
      bucketSpec: BucketSpec,
      selectedPartitions: ScanFileListing): Seq[FilePartition] = {
    logInfo(log"Planning with ${MDC(COUNT, bucketSpec.numBuckets)} buckets")
    val partitionArray = selectedPartitions.toPartitionArray
    val filesGroupedToBuckets = partitionArray.groupBy {
      f =>
        BucketingUtils
          .getBucketId(f.toPath.getName)
          .getOrElse(throw QueryExecutionErrors.invalidBucketFile(f.urlEncodedPath))
    }

    val prunedFilesGroupedToBuckets = if (optionalBucketSet.isDefined) {
      val bucketSet = optionalBucketSet.get
      filesGroupedToBuckets.filter(f => bucketSet.get(f._1))
    } else {
      filesGroupedToBuckets
    }

    val filePartitions = optionalNumCoalescedBuckets
      .map {
        numCoalescedBuckets =>
          logInfo(log"Coalescing to ${MDC(COUNT, numCoalescedBuckets)} buckets")
          val coalescedBuckets = prunedFilesGroupedToBuckets.groupBy(_._1 % numCoalescedBuckets)
          Seq.tabulate(numCoalescedBuckets) {
            bucketId =>
              val partitionedFiles = coalescedBuckets
                .get(bucketId)
                .map {
                  _.values.flatten.toArray
                }
                .getOrElse(Array.empty)
              FilePartition(bucketId, partitionedFiles)
          }
      }
      .getOrElse {
        Seq.tabulate(bucketSpec.numBuckets) {
          bucketId =>
            FilePartition(bucketId, prunedFilesGroupedToBuckets.getOrElse(bucketId, Array.empty))
        }
      }
    filePartitions
  }

  /**
   * Create an RDD for non-bucketed reads. The bucketed variant of this function is
   * [[createBucketedReadRDD]].
   *
   * @param selectedPartitions
   *   Hive-style partition that are part of the read.
   */
  private def createReadPartitions(selectedPartitions: ScanFileListing): Seq[FilePartition] = {
    val openCostInBytes = relation.sparkSession.sessionState.conf.filesOpenCostInBytes
    val maxSplitBytes =
      FilePartition.maxSplitBytes(relation.sparkSession, selectedPartitions)
    logInfo(log"Planning scan with bin packing, max size: ${MDC(MAX_SPLIT_BYTES, maxSplitBytes)} " +
      log"bytes, open cost is considered as scanning ${MDC(OPEN_COST_IN_BYTES, openCostInBytes)} " +
      log"bytes.")

    // Filter files with bucket pruning if possible
    val bucketingEnabled = relation.sparkSession.sessionState.conf.bucketingEnabled
    val shouldProcess: Path => Boolean = optionalBucketSet match {
      case Some(bucketSet) if bucketingEnabled =>
        // Do not prune the file if bucket file name is invalid
        filePath => BucketingUtils.getBucketId(filePath.getName).forall(bucketSet.get)
      case _ =>
        _ => true
    }

    val splitFiles = selectedPartitions.filePartitionIterator
      .flatMap {
        partition =>
          val ListingPartition(partitionVals, _, fileStatusIterator) = partition
          fileStatusIterator.flatMap {
            file =>
              // getPath() is very expensive so we only want to call it once in this block:
              val filePath = file.getPath
              if (shouldProcess(filePath)) {
                val isSplitable =
                  relation.fileFormat.isSplitable(relation.sparkSession, relation.options, filePath)
                PartitionedFileUtil.splitFiles(
                  file = file,
                  filePath = filePath,
                  isSplitable = isSplitable,
                  maxSplitBytes = maxSplitBytes,
                  partitionValues = partitionVals
                )
              } else {
                Seq.empty
              }
          }
      }
      .toArray
      .sortBy(_.length)(implicitly[Ordering[Long]].reverse)

    val partitions = FilePartition
      .getFilePartitions(relation.sparkSession, splitFiles.toImmutableArraySeq, maxSplitBytes)
    partitions
  }

  def getPartitionsSeq(): Seq[Partition] = {
    if (bucketedScan) {
      createBucketedReadPartition(relation.bucketSpec.get, dynamicallySelectedPartitions)
    } else {
      createReadPartitions(dynamicallySelectedPartitions)
    }
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

  override def getStream: Option[SparkDataStream] = original.stream
}

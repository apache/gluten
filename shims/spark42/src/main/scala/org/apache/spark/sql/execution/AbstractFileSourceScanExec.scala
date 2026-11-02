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

import org.apache.gluten.execution.PartitionedFileUtilShim

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{FileSourceOptions, InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat => ParquetSource}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.collection.BitSet

import org.apache.hadoop.fs.Path

import java.util.concurrent.TimeUnit._

/**
 * Physical plan node for scanning data from HadoopFsRelations.
 *
 * @param relation
 *   The file-based relation to scan.
 * @param output
 *   Output attributes of the scan, including data attributes and partition attributes.
 * @param requiredSchema
 *   Required schema of the underlying relation, excluding partition columns.
 * @param partitionFilters
 *   Predicates to use for partition pruning.
 * @param optionalBucketSet
 *   Bucket ids for bucket pruning.
 * @param optionalNumCoalescedBuckets
 *   Number of coalesced buckets.
 * @param dataFilters
 *   Filters on non-partition columns.
 * @param tableIdentifier
 *   Identifier for the table in the metastore.
 * @param disableBucketedScan
 *   Disable bucketed scan based on physical query plan, see rule [[DisableUnnecessaryBucketedScan]]
 *   for details.
 */
abstract class AbstractFileSourceScanExec(
    @transient override val relation: HadoopFsRelation,
    override val output: Seq[Attribute],
    override val requiredSchema: StructType,
    override val partitionFilters: Seq[Expression],
    override val optionalBucketSet: Option[BitSet],
    override val optionalNumCoalescedBuckets: Option[Int],
    override val dataFilters: Seq[Expression],
    override val tableIdentifier: Option[TableIdentifier],
    override val disableBucketedScan: Boolean = false)
  extends FileSourceScanLike {

  override def supportsColumnar: Boolean = {
    // The value should be defined in GlutenPlan.
    throw new UnsupportedOperationException(
      "Unreachable code from org.apache.spark.sql.execution.AbstractFileSourceScanExec" +
        ".supportsColumnar")
  }

  private lazy val needsUnsafeRowConversion: Boolean = {
    if (relation.fileFormat.isInstanceOf[ParquetSource]) {
      conf.parquetVectorizedReaderEnabled
    } else {
      false
    }
  }

  lazy val inputRDD: RDD[InternalRow] = {
    val options = relation.options +
      (FileFormat.OPTION_RETURNING_BATCH -> supportsColumnar.toString)
    val readFile: (PartitionedFile) => Iterator[InternalRow] =
      relation.fileFormat.buildReaderWithPartitionValues(
        sparkSession = relation.sparkSession,
        dataSchema = relation.dataSchema,
        partitionSchema = relation.partitionSchema,
        requiredSchema = requiredSchema,
        filters = pushedDownFilters,
        options = options,
        hadoopConf = relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options)
      )

    val readRDD = if (bucketedScan) {
      createBucketedReadRDD(relation.bucketSpec.get, readFile, dynamicallySelectedPartitions)
    } else {
      createReadRDD(readFile, dynamicallySelectedPartitions)
    }
    sendDriverMetrics()
    readRDD
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    inputRDD :: Nil
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    if (needsUnsafeRowConversion) {
      inputRDD.mapPartitionsWithIndexInternal {
        (index, iter) =>
          val toUnsafe = UnsafeProjection.create(schema)
          toUnsafe.initialize(index)
          iter.map {
            row =>
              numOutputRows += 1
              toUnsafe(row)
          }
      }
    } else {
      inputRDD.mapPartitionsInternal {
        iter =>
          iter.map {
            row =>
              numOutputRows += 1
              row
          }
      }
    }
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val scanTime = longMetric("scanTime")
    inputRDD.asInstanceOf[RDD[ColumnarBatch]].mapPartitionsInternal {
      batches =>
        new Iterator[ColumnarBatch] {

          override def hasNext: Boolean = {
            // The `FileScanRDD` returns an iterator which scans the file during the `hasNext` call.
            val startNs = System.nanoTime()
            val res = batches.hasNext
            scanTime += NANOSECONDS.toMillis(System.nanoTime() - startNs)
            res
          }

          override def next(): ColumnarBatch = {
            val batch = batches.next()
            numOutputRows += batch.numRows()
            batch
          }
        }
    }
  }

  override val nodeNamePrefix: String = "File"

  /**
   * Create an RDD for bucketed reads. The non-bucketed variant of this function is
   * [[createReadRDD]].
   *
   * The algorithm is pretty simple: each RDD partition being returned should include all the files
   * with the same bucket id from all the given Hive partitions.
   *
   * @param bucketSpec
   *   the bucketing spec.
   * @param readFile
   *   a function to read each (part of a) file.
   * @param selectedPartitions
   *   Hive-style partition that are part of the read.
   */
  private def createBucketedReadRDD(
      bucketSpec: BucketSpec,
      readFile: (PartitionedFile) => Iterator[InternalRow],
      selectedPartitions: ScanFileListing): RDD[InternalRow] = {
    logInfo(s"Planning with ${bucketSpec.numBuckets} buckets")
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
          logInfo(s"Coalescing to $numCoalescedBuckets buckets")
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

    new FileScanRDD(
      relation.sparkSession,
      readFile,
      filePartitions,
      new StructType(requiredSchema.fields ++ relation.partitionSchema.fields),
      fileConstantMetadataColumns,
      relation.fileFormat.fileConstantMetadataExtractors,
      new FileSourceOptions(CaseInsensitiveMap(relation.options))
    )
  }

  /**
   * Create an RDD for non-bucketed reads. The bucketed variant of this function is
   * [[createBucketedReadRDD]].
   *
   * @param readFile
   *   a function to read each (part of a) file.
   * @param selectedPartitions
   *   Hive-style partition that are part of the read.
   */
  private def createReadRDD(
      readFile: (PartitionedFile) => Iterator[InternalRow],
      selectedPartitions: ScanFileListing): RDD[InternalRow] = {
    val openCostInBytes = relation.sparkSession.sessionState.conf.filesOpenCostInBytes
    val maxSplitBytes =
      FilePartition.maxSplitBytes(relation.sparkSession, selectedPartitions)
    logInfo(
      s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
        s"open cost is considered as scanning $openCostInBytes bytes.")

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
          partition.files.flatMap {
            file =>
              if (shouldProcess(file.getPath)) {
                val isSplitable = relation.fileFormat.isSplitable(
                  relation.sparkSession,
                  relation.options,
                  file.getPath)
                PartitionedFileUtilShim.splitFiles(
                  sparkSession = relation.sparkSession,
                  file = file,
                  isSplitable = isSplitable,
                  maxSplitBytes = maxSplitBytes,
                  partitionValues = partition.values
                )
              } else {
                Seq.empty
              }
          }
      }
      .toArray
      .sortBy(_.length)(implicitly[Ordering[Long]].reverse)

    val partitions =
      FilePartition.getFilePartitions(relation.sparkSession, splitFiles, maxSplitBytes)

    new FileScanRDD(
      relation.sparkSession,
      readFile,
      partitions,
      new StructType(requiredSchema.fields ++ relation.partitionSchema.fields),
      fileConstantMetadataColumns,
      relation.fileFormat.fileConstantMetadataExtractors,
      new FileSourceOptions(CaseInsensitiveMap(relation.options))
    )
  }

  // Filters unused DynamicPruningExpression expressions - one which has been replaced
  // with DynamicPruningExpression(Literal.TrueLiteral) during Physical Planning
  protected def filterUnusedDynamicPruningExpressions(
      predicates: Seq[Expression]): Seq[Expression] = {
    predicates.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral))
  }
}

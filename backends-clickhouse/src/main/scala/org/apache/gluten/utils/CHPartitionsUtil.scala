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
package org.apache.gluten.utils

import org.apache.gluten.backendsapi.clickhouse.CHBackendSettings
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.Partition
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.util.SparkResourceUtil
import org.apache.spark.util.collection.BitSet

import org.apache.hadoop.fs.Path

import scala.collection.mutable.ArrayBuffer

case class CHPartitionsUtil(
    relation: HadoopFsRelation,
    selectedPartitions: Array[PartitionDirectory],
    bucketedScan: Boolean,
    optionalBucketSet: Option[BitSet],
    optionalNumCoalescedBuckets: Option[Int])
  extends Logging {

  def genPartitionSeq(): Seq[Partition] = {
    if (bucketedScan) {
      genBucketedPartitionSeq()
    } else {
      genNonBuckedPartitionSeq()
    }
  }

  private def genNonBuckedPartitionSeq(): Seq[Partition] = {
    val maxSplitBytes =
      FilePartition.maxSplitBytes(relation.sparkSession, selectedPartitions)

    // Filter files with bucket pruning if possible
    val bucketingEnabled = relation.sparkSession.sessionState.conf.bucketingEnabled
    val shouldProcess: Path => Boolean = optionalBucketSet match {
      case Some(bucketSet) if bucketingEnabled =>
        // Do not prune the file if bucket file name is invalid
        filePath => BucketingUtils.getBucketId(filePath.getName).forall(bucketSet.get)
      case _ =>
        _ => true
    }

    val splitFiles = selectedPartitions
      .flatMap {
        partition =>
          SparkShimLoader.getSparkShims.getFileStatus(partition).flatMap {
            file =>
              // getPath() is very expensive so we only want to call it once in this block:
              val filePath = file._1.getPath

              if (shouldProcess(filePath)) {
                val isSplitable =
                  relation.fileFormat.isSplitable(relation.sparkSession, relation.options, filePath)
                SparkShimLoader.getSparkShims.splitFiles(
                  relation.sparkSession,
                  file._1,
                  filePath,
                  isSplitable,
                  maxSplitBytes,
                  partition.values,
                  file._2
                )
              } else {
                Seq.empty
              }
          }
      }
      .sortBy(_.length)(implicitly[Ordering[Long]].reverse)

    val totalCores = SparkResourceUtil.getTotalCores(relation.sparkSession.sessionState.conf)
    val isAllSmallFiles = splitFiles.forall(_.length < maxSplitBytes)
    val fileCntThreshold = relation.sparkSession.sessionState.conf
      .getConfString(
        CHBackendSettings.GLUTEN_CLICKHOUSE_FILES_PER_PARTITION_THRESHOLD,
        CHBackendSettings.GLUTEN_CLICKHOUSE_FILES_PER_PARTITION_THRESHOLD_DEFAULT
      )
      .toInt

    // calculate the file count for each partition according to the parameter
    val totalFilesThreshold = totalCores * fileCntThreshold
    if (fileCntThreshold > 0 && isAllSmallFiles && splitFiles.size <= totalFilesThreshold) {
      var fileCnt = math.round((splitFiles.size * 1.0) / totalCores).toInt
      if (fileCnt < 1) fileCnt = 1
      getFilePartitionsByFileCnt(splitFiles, fileCnt)
    } else {
      FilePartition.getFilePartitions(relation.sparkSession, splitFiles, maxSplitBytes)
    }
  }

  private def genBucketedPartitionSeq(): Seq[Partition] = {
    val bucketSpec = relation.bucketSpec.get
    logInfo(s"Planning with ${bucketSpec.numBuckets} buckets")
    val filesGroupedToBuckets =
      SparkShimLoader.getSparkShims.filesGroupedToBuckets(selectedPartitions)

    val prunedFilesGroupedToBuckets = if (optionalBucketSet.isDefined) {
      val bucketSet = optionalBucketSet.get
      filesGroupedToBuckets.filter(f => bucketSet.get(f._1))
    } else {
      filesGroupedToBuckets
    }

    optionalNumCoalescedBuckets
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
  }

  /** Generate `Seq[FilePartition]` according to the file count */
  private def getFilePartitionsByFileCnt(
      partitionedFiles: Seq[PartitionedFile],
      fileCntPerPartition: Int): Seq[FilePartition] = {
    val partitions = new ArrayBuffer[FilePartition]
    val currentFiles = new ArrayBuffer[PartitionedFile]
    var currentFileCnt = 0L

    /** Close the current partition and move to the next. */
    def closePartition(): Unit = {
      if (currentFiles.nonEmpty) {
        // Copy to a new Array.
        val newPartition = FilePartition(partitions.size, currentFiles.toArray)
        partitions += newPartition
      }
      currentFiles.clear()
      currentFileCnt = 0L
    }

    partitionedFiles.foreach {
      file =>
        if (currentFileCnt >= fileCntPerPartition) {
          closePartition()
        }
        // Add the given file to the current partition.
        currentFileCnt += 1L
        currentFiles += file
    }
    closePartition()
    partitions.toSeq
  }
}

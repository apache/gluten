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
package org.apache.gluten.delta

import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.rel.DeltaLocalFilesNode
import org.apache.gluten.substrait.rel.DeltaLocalFilesNode.DeltaFileReadOptions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaParquetFileFormat
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArrayFormat, StoredBitmap}
import org.apache.spark.sql.delta.storage.dv.{DeletionVectorStore, HadoopFileSystemDVStore}
import org.apache.spark.sql.execution.datasources.PartitionedFile

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.io.DataInputStream
import java.util.{Map => JMap}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object DeltaDeletionVectorScanInfo {
  object RowIndexFilterType extends Enumeration {
    type RowIndexFilterType = Value
    val KEEP_ALL, IF_CONTAINED, IF_NOT_CONTAINED = Value
  }

  import RowIndexFilterType._

  final case class DeletionVectorInfo(
      hasDeletionVector: Boolean,
      rowIndexFilterType: RowIndexFilterType,
      cardinality: Long,
      serializedDeletionVector: Array[Byte])

  final case class PartitionFileScanInfo(
      normalizedOtherMetadataColumns: Map[String, Object],
      deletionVectorInfo: DeletionVectorInfo)

  private val RowIndexFilterIdEncoded =
    DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED
  private val RowIndexFilterTypeKey =
    DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_TYPE

  /**
   * Materializes per-file Delta DV read options for a split, alongside each file's metadata with
   * the DV bookkeeping keys stripped. Returns None when no file in the split carries a deletion
   * vector, so callers can keep the generic split representation.
   *
   * `tablePath` is the Delta table root, supplied by the caller from `TahoeFileIndex.path`, and is
   * used to resolve on-disk DV locations. A single Hadoop Configuration is reused across all files
   * in the partition.
   */
  def normalize(
      partitionFiles: Seq[PartitionedFile],
      tablePath: Path)
      : Option[(Seq[JMap[String, Object]], Seq[DeltaFileReadOptions])] = {
    if (partitionFiles.isEmpty) {
      return None
    }
    val spark = activeSparkSession
    // Create a single Hadoop Configuration for the entire partition.
    val hadoopConf = spark.sessionState.newHadoopConf()

    val scanInfos = partitionFiles.map(file => extract(file, hadoopConf, tablePath))
    if (scanInfos.exists(_.deletionVectorInfo.hasDeletionVector)) {
      Some(
        (
          scanInfos.map(_.normalizedOtherMetadataColumns.asJava),
          scanInfos.map(info => toDeltaFileReadOptions(info.deletionVectorInfo))))
    } else {
      None
    }
  }

  /** Public entry point for extracting DV info from a single file (used by tests). */
  def extract(
      spark: SparkSession,
      file: PartitionedFile,
      tablePath: Path): PartitionFileScanInfo = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    extract(file, hadoopConf, tablePath)
  }

  private def extract(
      file: PartitionedFile,
      hadoopConf: Configuration,
      tablePath: Path): PartitionFileScanInfo = {
    val metadata = otherMetadataColumns(file)
    val normalizedMetadata = metadata -- Seq(RowIndexFilterIdEncoded, RowIndexFilterTypeKey)
    val dvInfo = extractDeletionVectorInfo(metadata, hadoopConf, tablePath)
    PartitionFileScanInfo(normalizedMetadata, dvInfo)
  }

  private def toDeltaFileReadOptions(dvInfo: DeletionVectorInfo): DeltaFileReadOptions = {
    new DeltaFileReadOptions(
      toSubstraitRowIndexFilterType(dvInfo.rowIndexFilterType),
      dvInfo.hasDeletionVector,
      dvInfo.cardinality,
      dvInfo.serializedDeletionVector)
  }

  private def toSubstraitRowIndexFilterType(
      filterType: RowIndexFilterType): DeltaLocalFilesNode.RowIndexFilterType = {
    filterType match {
      case IF_CONTAINED => DeltaLocalFilesNode.RowIndexFilterType.IF_CONTAINED
      case IF_NOT_CONTAINED => DeltaLocalFilesNode.RowIndexFilterType.IF_NOT_CONTAINED
      case _ => DeltaLocalFilesNode.RowIndexFilterType.KEEP_ALL
    }
  }

  private def activeSparkSession: SparkSession = {
    SparkSession.getActiveSession
      .orElse(SparkSession.getDefaultSession)
      .getOrElse {
        throw new IllegalStateException(
          "Active SparkSession is required to materialize Delta deletion vectors")
      }
  }

  private def extractDeletionVectorInfo(
      metadata: Map[String, Object],
      hadoopConf: Configuration,
      tablePath: Path): DeletionVectorInfo = {
    val descriptorValue = metadata.get(RowIndexFilterIdEncoded)
    val filterTypeValue = metadata.get(RowIndexFilterTypeKey)

    (descriptorValue, filterTypeValue) match {
      case (None, None) =>
        DeletionVectorInfo(false, KEEP_ALL, 0L, Array.emptyByteArray)
      case (Some(encodedDescriptor), Some(filterType)) =>
        val descriptor = parseDescriptor(encodedDescriptor.toString)
        val serializedPayload = serializePayload(hadoopConf, tablePath, descriptor)
        DeletionVectorInfo(
          true,
          parseRowIndexFilterType(filterType.toString),
          descriptor.cardinality,
          serializedPayload)
      case _ =>
        throw new IllegalStateException(
          s"Both $RowIndexFilterIdEncoded and $RowIndexFilterTypeKey must either be present or absent")
    }
  }

  private def otherMetadataColumns(file: PartitionedFile): Map[String, Object] = {
    val otherMetadata =
      SparkShimLoader.getSparkShims.getOtherConstantMetadataColumnValues(file)
    if (otherMetadata == null) {
      Map.empty
    } else {
      otherMetadata.asScala.toMap
    }
  }

  private def parseDescriptor(encodedDescriptor: String): DeletionVectorDescriptor = {
    try {
      DeletionVectorDescriptor.deserializeFromBase64(encodedDescriptor)
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException("Unable to parse Delta deletion vector descriptor", e)
    }
  }

  private def parseRowIndexFilterType(filterType: String): RowIndexFilterType = {
    filterType match {
      case "IF_CONTAINED" => IF_CONTAINED
      case "IF_NOT_CONTAINED" => IF_NOT_CONTAINED
      case "KEEP_ALL" => KEEP_ALL
      case unexpected =>
        throw new IllegalStateException(s"Unexpected row index filter type: $unexpected")
    }
  }

  /**
   * Reads the DV payload bytes for the native engine. For on-disk DVs, reads the raw bytes directly
   * from the DV file using Delta's `DeletionVectorStore.readRangeFromStream`, which includes
   * checksum verification. The on-disk format is already Portable Roaring Bitmap Array (the format
   * the native Velox side expects), so this skips the expensive
   * deserialize-into-Java-Roaring-objects + re-serialize round-trip.
   *
   * Falls back to the standard load+serialize path for inline DVs (small payloads embedded in Delta
   * metadata) which don't have a file to read from.
   */
  private def serializePayload(
      hadoopConf: Configuration,
      tablePath: Path,
      descriptor: DeletionVectorDescriptor): Array[Byte] = {
    if (tablePath == null) {
      throw new IllegalStateException(
        "Unable to resolve Delta table path while materializing deletion vector payload")
    }
    if (descriptor.storageType != "i") {
      // On-disk DV (storageType "u" for UUID or "p" for path): read raw bytes directly.
      readRawDvBytes(hadoopConf, tablePath, descriptor)
    } else {
      // Inline DV (storageType "i"): bytes are in the descriptor metadata.
      val dvStore = new HadoopFileSystemDVStore(hadoopConf)
      StoredBitmap
        .create(descriptor, tablePath)
        .load(dvStore)
        .serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
    }
  }

  /**
   * Reads raw DV bytes directly from the DV file on disk. The file layout per entry is: [4 bytes
   * BE] data_size, [N bytes] payload (Portable Roaring), [4 bytes BE] CRC32 checksum.
   * `DeletionVectorStore.readRangeFromStream` handles all of this including checksum verification,
   * and returns the raw payload bytes.
   */
  private def readRawDvBytes(
      hadoopConf: Configuration,
      tablePath: Path,
      descriptor: DeletionVectorDescriptor): Array[Byte] = {
    val dvPath = descriptor.absolutePath(tablePath)
    val fs = dvPath.getFileSystem(hadoopConf)
    val stream = new DataInputStream(fs.open(dvPath))
    try {
      val offset = descriptor.offset.getOrElse(0)
      if (offset > 0) {
        stream.skipBytes(offset)
      }
      DeletionVectorStore.readRangeFromStream(stream, descriptor.sizeInBytes)
    } finally {
      stream.close()
    }
  }

}

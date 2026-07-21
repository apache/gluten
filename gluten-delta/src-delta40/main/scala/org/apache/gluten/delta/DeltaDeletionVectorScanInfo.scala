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
import scala.util.Try
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
   * Performance: reuses a single Hadoop Configuration instance across all files in the partition.
   * The table root is taken from `tablePath` when the caller can supply it (e.g. from
   * `TahoeFileIndex.path`); otherwise it is derived once from the first file, which requires a
   * `_delta_log` existence probe. Passing `tablePath` avoids that filesystem round-trip.
   */
  def normalize(
      partitionColumnCount: Int,
      partitionFiles: Seq[PartitionedFile],
      tablePath: Option[Path] = None)
      : Option[(Seq[JMap[String, Object]], Seq[DeltaFileReadOptions])] = {
    if (partitionFiles.isEmpty) {
      return None
    }
    val spark = activeSparkSession
    val hadoopConf = spark.sessionState.newHadoopConf()
    // Prefer the caller-supplied table root (TahoeFileIndex.path). Fall back to deriving it from
    // the first file -- which probes the filesystem for _delta_log -- only when unavailable.
    val cachedTablePath =
      tablePath.getOrElse(resolveTablePath(hadoopConf, partitionColumnCount, partitionFiles.head))

    val scanInfos = partitionFiles.map {
      file => extract(partitionColumnCount, file, hadoopConf, cachedTablePath)
    }
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
      partitionColumnCount: Int,
      file: PartitionedFile): PartitionFileScanInfo = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    val tablePath = resolveTablePath(hadoopConf, partitionColumnCount, file)
    extract(partitionColumnCount, file, hadoopConf, tablePath)
  }

  private def extract(
      partitionColumnCount: Int,
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

  /** Cached reflective methods for parsing DV descriptors (Delta 4.0 API compatibility). */
  private lazy val descriptorParseMethods: Seq[java.lang.reflect.Method] = {
    val methods = Seq("deserializeFromBase64", "fromJson")
    val found = methods.flatMap {
      methodName =>
        Try(DeletionVectorDescriptor.getClass.getMethod(methodName, classOf[String])).toOption
    }
    if (found.isEmpty) {
      throw new IllegalStateException(
        "Unable to find DeletionVectorDescriptor parse method (tried: " +
          methods.mkString(", ") + ")")
    }
    found
  }

  private def parseDescriptor(encodedDescriptor: String): DeletionVectorDescriptor = {
    var lastException: Throwable = null
    for (method <- descriptorParseMethods) {
      try {
        return method
          .invoke(DeletionVectorDescriptor, encodedDescriptor)
          .asInstanceOf[DeletionVectorDescriptor]
      } catch {
        case NonFatal(e) => lastException = e
      }
    }
    throw new IllegalArgumentException(
      "Unable to parse Delta deletion vector descriptor",
      lastException)
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

  private def serializePayload(
      hadoopConf: Configuration,
      tablePath: Path,
      descriptor: DeletionVectorDescriptor): Array[Byte] = {
    if (tablePath == null) {
      throw new IllegalStateException(
        "Unable to resolve Delta table path while materializing deletion vector payload")
    }
    if (descriptor.storageType != "i") {
      // On-disk DV: read raw bytes directly (already in Portable Roaring format).
      readRawDvBytes(hadoopConf, tablePath, descriptor)
    } else {
      // Inline DV: bytes are in the descriptor metadata.
      val dvStore = new HadoopFileSystemDVStore(hadoopConf)
      StoredBitmap
        .create(descriptor, tablePath)
        .load(dvStore)
        .serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
    }
  }

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

  private def resolveTablePath(
      hadoopConf: org.apache.hadoop.conf.Configuration,
      partitionColumnCount: Int,
      file: PartitionedFile): Path = {
    val fileParent = new Path(unescapePathName(file.filePath.toString)).getParent
    var tablePath = fileParent
    for (_ <- 0 until partitionColumnCount) {
      tablePath = tablePath.getParent
    }
    if (tablePath != null && isDeltaTablePath(hadoopConf, tablePath)) {
      return tablePath
    }

    var candidate = fileParent
    while (candidate != null && !isDeltaTablePath(hadoopConf, candidate)) {
      candidate = candidate.getParent
    }
    if (candidate != null) candidate else tablePath
  }

  private def isDeltaTablePath(
      hadoopConf: org.apache.hadoop.conf.Configuration,
      tablePath: Path): Boolean = {
    val deltaLogPath = new Path(tablePath, "_delta_log")
    try {
      deltaLogPath.getFileSystem(hadoopConf).exists(deltaLogPath)
    } catch {
      case NonFatal(_) => false
    }
  }

  private def unescapePathName(path: String): String = {
    if (path == null || path.indexOf('%') < 0) {
      path
    } else {
      val builder = new StringBuilder(path.length)
      var index = 0
      while (index < path.length) {
        if (path.charAt(index) == '%' && index + 2 < path.length) {
          val high = Character.digit(path.charAt(index + 1), 16)
          val low = Character.digit(path.charAt(index + 2), 16)
          if (high >= 0 && low >= 0) {
            builder.append(((high << 4) | low).toChar)
            index += 3
          } else {
            builder.append(path.charAt(index))
            index += 1
          }
        } else {
          builder.append(path.charAt(index))
          index += 1
        }
      }
      builder.toString()
    }
  }
}

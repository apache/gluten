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
import org.apache.gluten.substrait.rel.DeltaLocalFilesNode.{DeletionVectorPayload, DeltaFileReadOptions, InMemoryDeletionVectorPayload}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaParquetFileFormat
import org.apache.spark.sql.delta.actions.{AddFile, DeletionVectorDescriptor}
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArrayFormat, StoredBitmap}
import org.apache.spark.sql.delta.storage.dv.{DeletionVectorStore, HadoopFileSystemDVStore}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.util.SerializableConfiguration

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
      deletionVectorPayload: DeletionVectorPayload) {
    def serializedDeletionVector: Array[Byte] = deletionVectorPayload.materialize()
  }

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
   * `tablePath` is the authoritative Delta table root supplied by `TahoeFileIndex.path`. On-disk DV
   * descriptors retain a shared serializable Hadoop configuration but do not open their sidecar
   * until executor-side split serialization. Inline DVs remain eager because their bytes are
   * already present in Delta metadata.
   */
  def normalize(
      partitionFiles: Seq[PartitionedFile],
      tablePath: Path)
      : Option[(Seq[JMap[String, Object]], Seq[DeltaFileReadOptions])] = {
    normalize(partitionFiles, tablePath, None)
  }

  def normalize(
      partitionFiles: Seq[PartitionedFile],
      tablePath: Path,
      readMetrics: Option[DeletionVectorReadMetrics])
      : Option[(Seq[JMap[String, Object]], Seq[DeltaFileReadOptions])] = {
    if (partitionFiles.isEmpty) {
      return None
    }
    val spark = activeSparkSession
    val hadoopConf = spark.sessionState.newHadoopConf()
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)

    val scanInfos = partitionFiles.map {
      file => extract(file, hadoopConf, serializableHadoopConf, tablePath, readMetrics)
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

  /**
   * Materializes normal-table DV options from the AddFiles selected by PreparedDeltaFileIndex.
   * These are authoritative when a data file's DV is replaced by repeated DML; the corresponding
   * PartitionedFile may still contain the previous descriptor in its constant metadata.
   */
  private[gluten] def buildAddFileLookup(
      tablePath: Path,
      addFiles: Seq[AddFile]): DeltaAddFileLookup = {
    DeltaAddFileLookup(tablePath, addFiles, addFiles.exists(_.deletionVector != null))
  }

  private[gluten] def normalizeFromAddFiles(
      partitionFiles: Seq[PartitionedFile],
      tablePath: Path,
      addFileLookup: DeltaAddFileLookup)
      : Option[(Seq[JMap[String, Object]], Seq[DeltaFileReadOptions])] = {
    normalizeFromAddFiles(partitionFiles, tablePath, addFileLookup, None)
  }

  private[gluten] def normalizeFromAddFiles(
      partitionFiles: Seq[PartitionedFile],
      tablePath: Path,
      addFileLookup: DeltaAddFileLookup,
      readMetrics: Option[DeletionVectorReadMetrics])
      : Option[(Seq[JMap[String, Object]], Seq[DeltaFileReadOptions])] = {
    if (partitionFiles.isEmpty) {
      return None
    }
    val spark = activeSparkSession
    val hadoopConf = spark.sessionState.newHadoopConf()
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    val hadDvMetadata = partitionFiles.exists {
      file =>
        val metadata = otherMetadataColumns(file)
        metadata.contains(RowIndexFilterIdEncoded) || metadata.contains(RowIndexFilterTypeKey)
    }
    if (!hadDvMetadata && !addFileLookup.hasDeletionVector) {
      return None
    }
    val scanInfos = partitionFiles.map {
      file =>
        val addFile = addFileLookup.find(file)
        extract(file, hadoopConf, serializableHadoopConf, tablePath, addFile, readMetrics)
    }
    if (hadDvMetadata || scanInfos.exists(_.deletionVectorInfo.hasDeletionVector)) {
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
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    extract(file, hadoopConf, serializableHadoopConf, tablePath, None)
  }

  private def extract(
      file: PartitionedFile,
      hadoopConf: Configuration,
      serializableHadoopConf: SerializableConfiguration,
      tablePath: Path,
      readMetrics: Option[DeletionVectorReadMetrics]): PartitionFileScanInfo = {
    val metadata = otherMetadataColumns(file)
    val normalizedMetadata = metadata -- Seq(RowIndexFilterIdEncoded, RowIndexFilterTypeKey)
    val dvInfo = extractDeletionVectorInfo(
      metadata,
      hadoopConf,
      serializableHadoopConf,
      tablePath,
      readMetrics)
    PartitionFileScanInfo(normalizedMetadata, dvInfo)
  }

  private def extract(
      file: PartitionedFile,
      hadoopConf: Configuration,
      serializableHadoopConf: SerializableConfiguration,
      tablePath: Path,
      addFile: AddFile,
      readMetrics: Option[DeletionVectorReadMetrics]): PartitionFileScanInfo = {
    val metadata = otherMetadataColumns(file)
    val normalizedMetadata = metadata -- Seq(RowIndexFilterIdEncoded, RowIndexFilterTypeKey)
    val dvInfo = Option(addFile.deletionVector) match {
      case Some(descriptor) =>
        DeletionVectorInfo(
          true,
          IF_CONTAINED,
          descriptor.cardinality,
          deletionVectorPayload(
            hadoopConf,
            serializableHadoopConf,
            tablePath,
            descriptor,
            readMetrics))
      case None =>
        DeletionVectorInfo(
          false,
          KEEP_ALL,
          0L,
          new InMemoryDeletionVectorPayload(Array.emptyByteArray))
    }
    PartitionFileScanInfo(normalizedMetadata, dvInfo)
  }

  private def toDeltaFileReadOptions(dvInfo: DeletionVectorInfo): DeltaFileReadOptions = {
    new DeltaFileReadOptions(
      toSubstraitRowIndexFilterType(dvInfo.rowIndexFilterType),
      dvInfo.hasDeletionVector,
      dvInfo.cardinality,
      dvInfo.deletionVectorPayload)
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
      serializableHadoopConf: SerializableConfiguration,
      tablePath: Path,
      readMetrics: Option[DeletionVectorReadMetrics]): DeletionVectorInfo = {
    val descriptorValue = metadata.get(RowIndexFilterIdEncoded)
    val filterTypeValue = metadata.get(RowIndexFilterTypeKey)

    (descriptorValue, filterTypeValue) match {
      case (None, None) =>
        DeletionVectorInfo(
          false,
          KEEP_ALL,
          0L,
          new InMemoryDeletionVectorPayload(Array.emptyByteArray))
      case (Some(encodedDescriptor), Some(filterType)) =>
        val descriptor = parseDescriptor(encodedDescriptor.toString)
        val payload = deletionVectorPayload(
          hadoopConf,
          serializableHadoopConf,
          tablePath,
          descriptor,
          readMetrics)
        DeletionVectorInfo(
          true,
          parseRowIndexFilterType(filterType.toString),
          descriptor.cardinality,
          payload)
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

  /** Selects a deferred source for on-disk DVs and eager bytes for inline DVs. */
  private def deletionVectorPayload(
      hadoopConf: Configuration,
      serializableHadoopConf: SerializableConfiguration,
      tablePath: Path,
      descriptor: DeletionVectorDescriptor,
      readMetrics: Option[DeletionVectorReadMetrics]): DeletionVectorPayload = {
    if (tablePath == null) {
      throw new IllegalStateException(
        "Unable to resolve Delta table path while preparing deletion vector payload")
    }
    if (descriptor.storageType != "i") {
      val dvPath = descriptor.absolutePath(tablePath)
      new OnDiskDeletionVectorPayload(
        serializableHadoopConf,
        dvPath.toString,
        requiredOffset(descriptor),
        descriptor.sizeInBytes,
        readMetrics)
    } else {
      new InMemoryDeletionVectorPayload(serializeInlinePayload(hadoopConf, tablePath, descriptor))
    }
  }

  private def requiredOffset(descriptor: DeletionVectorDescriptor): Long = {
    descriptor.offset
      .map(_.toLong)
      .getOrElse {
        throw new IllegalStateException(
          s"On-disk Delta deletion vector '${descriptor.storageType}' is missing its offset")
      }
  }

  /**
   * Decodes an inline DV already embedded in Delta metadata into Velox's portable bitmap format.
   */
  private def serializeInlinePayload(
      hadoopConf: Configuration,
      tablePath: Path,
      descriptor: DeletionVectorDescriptor): Array[Byte] = {
    if (tablePath == null) {
      throw new IllegalStateException(
        "Unable to resolve Delta table path while materializing deletion vector payload")
    }
    val dvStore = new HadoopFileSystemDVStore(hadoopConf)
    StoredBitmap
      .create(descriptor, tablePath)
      .load(dvStore)
      .serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
  }

  /**
   * Reads raw DV bytes directly from the DV file on disk. The file layout per entry is: [4 bytes
   * BE] data_size, [N bytes] payload (Portable Roaring), [4 bytes BE] CRC32 checksum.
   * `DeletionVectorStore.readRangeFromStream` handles all of this including checksum verification,
   * and returns the raw payload bytes.
   */
  private def readRawDvBytes(
      hadoopConf: Configuration,
      dvPath: Path,
      offset: Long,
      sizeInBytes: Int): Array[Byte] = {
    val fs = dvPath.getFileSystem(hadoopConf)
    // Positioned absolute seek, matching Delta's own `HadoopFileSystemDVStore.read`. `seek` is a
    // single positioned reposition (a ranged read on object stores), whereas `DataInputStream.
    // skipBytes` is best-effort -- it can skip fewer bytes than requested without error, which would
    // then fail the CRC check in `readRangeFromStream`.
    val fileStream = fs.open(dvPath)
    try {
      fileStream.seek(offset)
      DeletionVectorStore.readRangeFromStream(new DataInputStream(fileStream), sizeInBytes)
    } finally {
      fileStream.close()
    }
  }

  /**
   * Executor-side on-disk payload source. Successful materialization is memoized for repeated split
   * serialization; failed reads remain retryable.
   */
  @SerialVersionUID(1L)
  final private class OnDiskDeletionVectorPayload(
      serializableHadoopConf: SerializableConfiguration,
      absolutePath: String,
      offset: Long,
      sizeInBytes: Int,
      readMetrics: Option[DeletionVectorReadMetrics])
    extends DeletionVectorPayload {
    require(offset >= 0, s"Deletion vector offset must be non-negative: $offset")
    require(sizeInBytes >= 0, s"Deletion vector size must be non-negative: $sizeInBytes")

    @transient @volatile private var cachedPayload: Array[Byte] = _

    override def materialize(): Array[Byte] = {
      var payload = cachedPayload
      if (payload == null) {
        this.synchronized {
          payload = cachedPayload
          if (payload == null) {
            val startedAt = System.nanoTime()
            readMetrics.foreach(_.registerForCurrentTask())
            readMetrics.foreach(_.readAttempts.add(1L))
            try {
              payload = readRawDvBytes(
                serializableHadoopConf.value,
                new Path(absolutePath),
                offset,
                sizeInBytes)
              readMetrics.foreach(_.readBytes.add(payload.length.toLong))
              cachedPayload = payload
            } finally {
              readMetrics.foreach(_.readTimeNanos.add(System.nanoTime() - startedAt))
            }
          }
        }
      }
      payload
    }

    override def isMaterialized(): Boolean = cachedPayload != null
  }

}

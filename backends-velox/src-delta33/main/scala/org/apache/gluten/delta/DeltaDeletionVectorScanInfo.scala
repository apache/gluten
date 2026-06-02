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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.GlutenDeltaParquetFileFormat
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArrayFormat, StoredBitmap}
import org.apache.spark.sql.delta.storage.dv.HadoopFileSystemDVStore
import org.apache.spark.sql.execution.datasources.PartitionedFile

import org.apache.hadoop.fs.Path

import java.util.{ArrayList => JArrayList}

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
    GlutenDeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED
  private val RowIndexFilterTypeKey =
    GlutenDeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_TYPE

  def extract(
      spark: SparkSession,
      partitionColumnCount: Int,
      file: PartitionedFile): PartitionFileScanInfo = {
    val metadata = otherMetadataColumns(file)
    val normalizedMetadata = metadata -- Seq(RowIndexFilterIdEncoded, RowIndexFilterTypeKey)
    val dvInfo = extractDeletionVectorInfo(spark, partitionColumnCount, file, metadata)
    PartitionFileScanInfo(normalizedMetadata, dvInfo)
  }

  def extractAll(
      spark: SparkSession,
      partitionColumnCount: Int,
      files: Seq[PartitionedFile]): Seq[PartitionFileScanInfo] = {
    files.map(extract(spark, partitionColumnCount, _))
  }

  def extractAllFromJava(
      spark: SparkSession,
      partitionColumnCount: Int,
      files: java.util.List[PartitionedFile]): java.util.List[PartitionFileScanInfo] = {
    new JArrayList(extractAll(spark, partitionColumnCount, files.asScala.toSeq).asJava)
  }

  private def extractDeletionVectorInfo(
      spark: SparkSession,
      partitionColumnCount: Int,
      file: PartitionedFile,
      metadata: Map[String, Object]): DeletionVectorInfo = {
    val descriptorValue = metadata.get(RowIndexFilterIdEncoded)
    val filterTypeValue = metadata.get(RowIndexFilterTypeKey)

    (descriptorValue, filterTypeValue) match {
      case (None, None) =>
        DeletionVectorInfo(false, KEEP_ALL, 0L, Array.emptyByteArray)
      case (Some(encodedDescriptor), Some(filterType)) =>
        val descriptor = parseDescriptor(encodedDescriptor.toString)
        val serializedPayload = serializePayload(spark, partitionColumnCount, file, descriptor)
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

  private def serializePayload(
      spark: SparkSession,
      partitionColumnCount: Int,
      file: PartitionedFile,
      descriptor: DeletionVectorDescriptor): Array[Byte] = {
    val tablePath = resolveTablePath(spark, partitionColumnCount, file)
    if (tablePath == null) {
      throw new IllegalStateException(
        "Unable to resolve Delta table path while materializing deletion vector payload")
    }
    val dvStore = new HadoopFileSystemDVStore(spark.sessionState.newHadoopConf())
    StoredBitmap
      .create(descriptor, tablePath)
      .load(dvStore)
      .serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
  }

  private def resolveTablePath(
      spark: SparkSession,
      partitionColumnCount: Int,
      file: PartitionedFile): Path = {
    val fileParent = new Path(unescapePathName(file.filePath.toString)).getParent
    var tablePath = fileParent
    for (_ <- 0 until partitionColumnCount) {
      tablePath = tablePath.getParent
    }
    if (tablePath != null && isDeltaTablePath(spark, tablePath)) {
      return tablePath
    }

    var candidate = fileParent
    while (candidate != null && !isDeltaTablePath(spark, candidate)) {
      candidate = candidate.getParent
    }
    if (candidate != null) candidate else tablePath
  }

  private def isDeltaTablePath(spark: SparkSession, tablePath: Path): Boolean = {
    val deltaLogPath = new Path(tablePath, "_delta_log")
    try {
      deltaLogPath.getFileSystem(spark.sessionState.newHadoopConf()).exists(deltaLogPath)
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

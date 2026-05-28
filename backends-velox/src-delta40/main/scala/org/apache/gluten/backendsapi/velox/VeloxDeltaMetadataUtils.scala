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
package org.apache.gluten.backendsapi.velox

import org.apache.gluten.backendsapi.velox.VeloxIteratorApi.unescapePathName
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.rel.DeltaLocalFilesNode.{DeltaFileReadOptions, RowIndexFilterType}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArrayFormat, StoredBitmap}
import org.apache.spark.sql.delta.storage.dv.HadoopFileSystemDVStore
import org.apache.spark.sql.execution.datasources.PartitionedFile

import org.apache.hadoop.fs.Path

import java.util.{ArrayList => JArrayList, HashMap => JHashMap, List => JList, Map => JMap}

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

object VeloxDeltaMetadataUtils {
  private val RowIndexFilterIdEncoded = "row_index_filter_id_encoded"
  private val RowIndexFilterTypeKey = "row_index_filter_type"
  private val RowIndexFilterTypeIfContained = "IF_CONTAINED"
  private val RowIndexFilterTypeIfNotContained = "IF_NOT_CONTAINED"

  final class NormalizedSplitMetadata(
      val otherMetadataColumns: JList[JMap[String, Object]],
      val deltaReadOptions: JList[DeltaFileReadOptions])
    extends Serializable

  private def decodeDescriptor(
      normalizedMetadata: JMap[String, Object]): Option[DeletionVectorDescriptor] = {
    Option(normalizedMetadata.get(RowIndexFilterIdEncoded))
      .map(_.toString)
      .filter(_.nonEmpty)
      .flatMap(parseDescriptor)
  }

  private def parseDescriptor(encodedDescriptor: String): Option[DeletionVectorDescriptor] = {
    val methods = Seq("deserializeFromBase64", "fromJson")
    methods.iterator
      .map {
        methodName =>
          Try {
            val method = DeletionVectorDescriptor.getClass.getMethod(methodName, classOf[String])
            method
              .invoke(DeletionVectorDescriptor, encodedDescriptor)
              .asInstanceOf[DeletionVectorDescriptor]
          }.toOption
      }
      .collectFirst { case Some(descriptor) => descriptor }
  }

  private def serializePayload(
      dvStore: HadoopFileSystemDVStore,
      tablePath: Path,
      descriptor: DeletionVectorDescriptor): Array[Byte] = {
    if (tablePath == null) {
      throw new IllegalStateException(
        "Unable to resolve Delta table path while materializing deletion vector payload")
    }
    StoredBitmap
      .create(descriptor, tablePath)
      .load(dvStore)
      .serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
  }

  private def normalizeMetadata(metadata: JMap[String, Object]): JMap[String, Object] = {
    val normalized = new JHashMap[String, Object]()
    if (metadata != null) {
      normalized.putAll(metadata)
    }
    normalized.remove(RowIndexFilterIdEncoded)
    normalized.remove(RowIndexFilterTypeKey)
    normalized
  }

  private def parseRowIndexFilterType(
      metadata: JMap[String, Object]): RowIndexFilterType = {
    Option(metadata.get(RowIndexFilterTypeKey)).map(_.toString) match {
      case Some(RowIndexFilterTypeIfContained) => RowIndexFilterType.IF_CONTAINED
      case Some(RowIndexFilterTypeIfNotContained) => RowIndexFilterType.IF_NOT_CONTAINED
      case _ => RowIndexFilterType.KEEP_ALL
    }
  }

  def normalizeSplitMetadata(
      partitionColumnCount: Int,
      files: JList[PartitionedFile]): NormalizedSplitMetadata = {
    val dvStore = new HadoopFileSystemDVStore(activeSpark.sessionState.newHadoopConf())
    val normalizedMetadataColumns = new JArrayList[JMap[String, Object]](files.size())
    val deltaReadOptions = new JArrayList[DeltaFileReadOptions](files.size())
    var hasDeletionVectors = false

    files.asScala.foreach {
      file =>
        val otherMetadata =
          SparkShimLoader.getSparkShims.getOtherConstantMetadataColumnValues(file)
        val metadataWithDecodedPayload = new JHashMap[String, Object]()
        if (otherMetadata != null) {
          metadataWithDecodedPayload.putAll(otherMetadata)
        }

        val descriptor = decodeDescriptor(metadataWithDecodedPayload)
        val rowIndexFilterType = parseRowIndexFilterType(metadataWithDecodedPayload)
        val normalizedMetadata = normalizeMetadata(metadataWithDecodedPayload)

        descriptor match {
          case Some(descriptor) =>
            hasDeletionVectors = true
            val payloadTablePath = resolveTablePath(partitionColumnCount, file)
            val serializedPayload = serializePayload(dvStore, payloadTablePath, descriptor)
            deltaReadOptions.add(
              new DeltaFileReadOptions(
                rowIndexFilterType,
                true,
                descriptor.cardinality,
                serializedPayload))
            normalizedMetadataColumns.add(normalizedMetadata)
          case None =>
            deltaReadOptions.add(
              new DeltaFileReadOptions(rowIndexFilterType, false, 0L, Array.emptyByteArray))
            normalizedMetadataColumns.add(normalizedMetadata)
        }
    }

    val deltaOptions = if (hasDeletionVectors) {
      deltaReadOptions
    } else {
      new JArrayList[DeltaFileReadOptions]()
    }
    new NormalizedSplitMetadata(normalizedMetadataColumns, deltaOptions)
  }

  private def activeSpark: SparkSession = {
    SparkSession.getActiveSession
      .orElse(SparkSession.getDefaultSession)
      .getOrElse {
        throw new IllegalStateException(
          "Active SparkSession is required to materialize Delta deletion vectors")
      }
  }

  private def resolveTablePath(partitionColumnCount: Int, file: PartitionedFile): Path = {
    val fileParent = new Path(unescapePathName(file.filePath.toString)).getParent
    var tablePath = fileParent
    for (_ <- 0 until partitionColumnCount) {
      tablePath = tablePath.getParent
    }
    val spark = activeSpark
    if (tablePath != null && isDeltaTablePath(spark, tablePath)) {
      return tablePath
    }

    // Spark can report a partition column count that does not map 1:1 to path depth for
    // prepared Delta scans. Find the nearest ancestor of the file path that has _delta_log.
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
}

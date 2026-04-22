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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArrayFormat, StoredBitmap}
import org.apache.spark.sql.delta.storage.dv.HadoopFileSystemDVStore
import org.apache.spark.sql.execution.datasources.PartitionedFile

import org.apache.hadoop.fs.Path

import java.util.{ArrayList => JArrayList, HashMap => JHashMap, List => JList, Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try
import scala.util.control.NonFatal

object VeloxDeltaMetadataUtils {
  val DeltaDvCardinality = "delta_dv_cardinality"
  val DeltaDvPayloadIndex = "delta_dv_payload_index"

  private val RowIndexFilterIdEncoded = "row_index_filter_id_encoded"
  private val RowIndexFilterType = "row_index_filter_type"
  private val RowIndexFilterTypeIfContained = "IF_CONTAINED"

  final class NormalizedSplitMetadata(
      val otherMetadataColumns: JList[JMap[String, Object]],
      val deletionVectorPayloads: Array[Array[Byte]])
    extends Serializable

  private def loadDeletionVectorsByRelativePath(
      tablePath: Path): Map[String, DeletionVectorDescriptor] = {
    val spark = activeSpark
    if (
      tablePath == null || !isDeltaSessionConfigured(spark) || !isDeltaTablePath(spark, tablePath)
    ) {
      return Map.empty
    }

    try {
      DeltaLog
        .forTable(spark, tablePath)
        .update()
        .allFiles
        .collect()
        .collect {
          case addFile: AddFile if addFile.deletionVector != null =>
            normalizePath(addFile.path) -> addFile.deletionVector
        }
        .toMap
    } catch {
      case NonFatal(_) => Map.empty
    }
  }

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
      descriptor: DeletionVectorDescriptor): Option[Array[Byte]] = {
    if (tablePath == null) {
      return None
    }
    Some(
      StoredBitmap
        .create(descriptor, tablePath)
        .load(dvStore)
        .serializeAsByteArray(RoaringBitmapArrayFormat.Portable))
  }

  private def normalizeMetadataWithDescriptor(
      metadata: JMap[String, Object],
      descriptor: DeletionVectorDescriptor): JMap[String, Object] = {
    val normalized = new JHashMap[String, Object]()
    if (metadata != null) {
      normalized.putAll(metadata)
    }
    normalized.put(DeltaDvCardinality, Long.box(descriptor.cardinality))
    normalized.remove(RowIndexFilterIdEncoded)
    if (!normalized.containsKey(RowIndexFilterType)) {
      normalized.put(RowIndexFilterType, RowIndexFilterTypeIfContained)
    }
    normalized
  }

  private def findDescriptorForFile(
      file: PartitionedFile,
      descriptorsByRelativePath: Map[String, DeletionVectorDescriptor])
      : Option[DeletionVectorDescriptor] = {
    val normalizedFilePath = normalizePath(unescapePathName(file.filePath.toString))
    descriptorsByRelativePath.collectFirst {
      case (relativePath, descriptor)
          if normalizedFilePath == relativePath || normalizedFilePath.endsWith(s"/$relativePath") =>
        descriptor
    }
  }

  def normalizeSplitMetadata(
      partitionColumnCount: Int,
      files: JList[PartitionedFile]): NormalizedSplitMetadata = {
    val dvStore = new HadoopFileSystemDVStore(activeSpark.sessionState.newHadoopConf())
    val normalizedMetadataColumns = new JArrayList[JMap[String, Object]](files.size())
    val deletionVectorPayloads = scala.collection.mutable.ArrayBuffer.empty[Array[Byte]]
    val deletionVectorsByTablePath =
      mutable.HashMap.empty[String, Map[String, DeletionVectorDescriptor]]

    files.asScala.foreach {
      file =>
        val otherMetadata =
          SparkShimLoader.getSparkShims.getOtherConstantMetadataColumnValues(file)
        val metadataWithDecodedPayload = new JHashMap[String, Object]()
        if (otherMetadata != null) {
          metadataWithDecodedPayload.putAll(otherMetadata)
        }

        val (tablePath, descriptor) = decodeDescriptor(metadataWithDecodedPayload) match {
          case descriptor @ Some(_) =>
            (null, descriptor)
          case None =>
            val tablePath = resolveTablePath(partitionColumnCount, file)
            val descriptorsByRelativePath =
              if (tablePath == null) {
                Map.empty[String, DeletionVectorDescriptor]
              } else {
                deletionVectorsByTablePath.getOrElseUpdate(
                  tablePath.toString,
                  loadDeletionVectorsByRelativePath(tablePath))
              }
            (tablePath, findDescriptorForFile(file, descriptorsByRelativePath))
        }

        descriptor match {
          case Some(descriptor) =>
            val normalized = normalizeMetadataWithDescriptor(metadataWithDecodedPayload, descriptor)
            val payloadTablePath =
              Option(tablePath).getOrElse(resolveTablePath(partitionColumnCount, file))
            val serializedPayload = serializePayload(dvStore, payloadTablePath, descriptor)
            serializedPayload.foreach {
              payload =>
                normalized.put(DeltaDvPayloadIndex, Int.box(deletionVectorPayloads.length))
                deletionVectorPayloads += payload
            }
            normalizedMetadataColumns.add(normalized)
          case None =>
            normalizedMetadataColumns.add(metadataWithDecodedPayload)
        }
    }

    new NormalizedSplitMetadata(normalizedMetadataColumns, deletionVectorPayloads.toArray)
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

  private def normalizePath(path: String): String = {
    path.replace('\\', '/').stripPrefix("/")
  }

  private def isDeltaSessionConfigured(spark: SparkSession): Boolean = {
    val extensions =
      spark.conf
        .getOption("spark.sql.extensions")
        .exists(_.split(",").exists(_.trim == "io.delta.sql.DeltaSparkSessionExtension"))
    val catalog =
      spark.conf
        .getOption("spark.sql.catalog.spark_catalog")
        .contains("org.apache.spark.sql.delta.catalog.DeltaCatalog")
    extensions && catalog
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

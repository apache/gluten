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

import org.apache.gluten.config.VeloxDeltaConfig

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArrayFormat, StoredBitmap}
import org.apache.spark.sql.delta.storage.dv.HadoopFileSystemDVStore
import org.apache.spark.sql.execution.datasources.PartitionedFile

import org.apache.hadoop.fs.Path

import java.util.{ArrayList => JArrayList, HashMap => JHashMap, List => JList, Map => JMap}

import scala.collection.JavaConverters._

object VeloxDeltaMetadataUtils {
  val DeltaDvStorageType = "delta_dv_storage_type"
  val DeltaDvPathOrInline = "delta_dv_path_or_inline"
  val DeltaDvOffset = "delta_dv_offset"
  val DeltaDvSizeInBytes = "delta_dv_size_in_bytes"
  val DeltaDvCardinality = "delta_dv_cardinality"
  val DeltaDvSerializedPayload = "delta_dv_serialized_payload"
  val DeltaDvPayloadIndex = "delta_dv_payload_index"

  private val RowIndexFilterIdEncoded = "row_index_filter_id_encoded"

  final class NormalizedSplitMetadata(
      val otherMetadataColumns: JList[JMap[String, Object]],
      val deletionVectorPayloads: Array[Array[Byte]])
    extends Serializable

  def normalizeOtherMetadataColumns(
      partitionColumnCount: Int,
      file: PartitionedFile,
      otherConstantMetadataColumnValues: JMap[String, Object]): JMap[String, Object] = {
    val normalized = new JHashMap[String, Object]()
    if (otherConstantMetadataColumnValues != null) {
      normalized.putAll(otherConstantMetadataColumnValues)
    }

    Option(normalized.get(RowIndexFilterIdEncoded)).map(_.toString).foreach {
      encodedDescriptor =>
        val descriptor = DeletionVectorDescriptor.deserializeFromBase64(encodedDescriptor)
        val tablePath = resolveTablePath(partitionColumnCount, file)
        descriptor.storageType match {
          case "i" =>
            normalized.put(DeltaDvStorageType, descriptor.storageType)
            normalized.put(DeltaDvPathOrInline, descriptor.pathOrInlineDv)
          case _ =>
            val absolutePath =
              descriptor.absolutePath(tablePath)
            normalized.put(DeltaDvStorageType, "p")
            normalized.put(DeltaDvPathOrInline, absolutePath.toUri.toASCIIString)
        }
        descriptor.offset.foreach(offset => normalized.put(DeltaDvOffset, Int.box(offset)))
        normalized.put(DeltaDvSizeInBytes, Int.box(descriptor.sizeInBytes))
        normalized.put(DeltaDvCardinality, Long.box(descriptor.cardinality))
        if (VeloxDeltaConfig.get.enableJvmDeletionVectorPayloadHandoff) {
          val dvStore = new HadoopFileSystemDVStore(activeSpark.sessionState.newHadoopConf())
          val serializedPayload = StoredBitmap
            .create(descriptor, tablePath)
            .load(dvStore)
            .serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
          normalized.put(DeltaDvSerializedPayload, serializedPayload)
        }
        normalized.remove(RowIndexFilterIdEncoded)
    }

    normalized
  }

  def normalizeSplitMetadata(
      partitionColumnCount: Int,
      files: JList[PartitionedFile]): NormalizedSplitMetadata = {
    val normalizedMetadataColumns = new JArrayList[JMap[String, Object]](files.size())
    val deletionVectorPayloads = scala.collection.mutable.ArrayBuffer.empty[Array[Byte]]

    files.asScala.foreach {
      file =>
        val normalized = normalizeOtherMetadataColumns(
          partitionColumnCount,
          file,
          file.otherConstantMetadataColumnValues.asJava.asInstanceOf[JMap[String, Object]])

        normalized.get(DeltaDvSerializedPayload) match {
          case payload: Array[Byte] =>
            normalized.remove(DeltaDvSerializedPayload)
            normalized.put(DeltaDvPayloadIndex, Int.box(deletionVectorPayloads.length))
            deletionVectorPayloads += payload
          case _ =>
        }

        normalizedMetadataColumns.add(normalized)
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
    var tablePath = new Path(file.filePath.toString).getParent
    for (_ <- 0 until partitionColumnCount) {
      tablePath = tablePath.getParent
    }
    tablePath
  }
}

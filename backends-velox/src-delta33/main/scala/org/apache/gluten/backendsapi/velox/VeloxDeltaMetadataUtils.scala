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

import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.spark.sql.execution.datasources.PartitionedFile

import org.apache.hadoop.fs.Path

import java.util.{HashMap => JHashMap, Map => JMap}

object VeloxDeltaMetadataUtils {
  val DeltaDvStorageType = "delta_dv_storage_type"
  val DeltaDvPathOrInline = "delta_dv_path_or_inline"
  val DeltaDvOffset = "delta_dv_offset"
  val DeltaDvSizeInBytes = "delta_dv_size_in_bytes"
  val DeltaDvCardinality = "delta_dv_cardinality"

  private val RowIndexFilterIdEncoded = "row_index_filter_id_encoded"

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
        descriptor.storageType match {
          case "i" =>
            normalized.put(DeltaDvStorageType, descriptor.storageType)
            normalized.put(DeltaDvPathOrInline, descriptor.pathOrInlineDv)
          case _ =>
            val absolutePath =
              descriptor.absolutePath(resolveTablePath(partitionColumnCount, file))
            normalized.put(DeltaDvStorageType, "p")
            normalized.put(DeltaDvPathOrInline, absolutePath.toUri.toASCIIString)
        }
        descriptor.offset.foreach(offset => normalized.put(DeltaDvOffset, Int.box(offset)))
        normalized.put(DeltaDvSizeInBytes, Int.box(descriptor.sizeInBytes))
        normalized.put(DeltaDvCardinality, Long.box(descriptor.cardinality))
        normalized.remove(RowIndexFilterIdEncoded)
    }

    normalized
  }

  private def resolveTablePath(partitionColumnCount: Int, file: PartitionedFile): Path = {
    var tablePath = new Path(file.filePath.toString).getParent
    for (_ <- 0 until partitionColumnCount) {
      tablePath = tablePath.getParent
    }
    tablePath
  }
}

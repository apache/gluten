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
package org.apache.gluten.execution

import org.apache.gluten.vectorized.HashJoinBuilder

import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.execution.unsafe.JniUnsafeByteBuffer
import org.apache.spark.sql.execution.unsafe.UnsafeByteArray

import java.io.{Externalizable, ObjectInput, ObjectOutput}

/**
 * Serialized broadcast hash table that can be efficiently broadcast to executors. This is built on
 * the driver and contains the serialized hash table data.
 *
 * @param broadcastId
 *   Identity of the driver-side broadcast this table was built for. Several broadcast hash joins
 *   can share one broadcast exchange (reused exchange), in which case they all see the same
 *   `broadcastId` while having distinct per-join hash table ids. Executors key the (expensive)
 *   deserialization on this id so that the table is materialized once and then shared.
 * @param buildSideRelation
 *   The raw build side relation. Driver-only: it backs [[BuildSideRelation.transform]] for DPP and
 *   [[BuildSideRelation.deserialized]] for broadcast-mode conversion, both of which run on the
 *   driver. It is deliberately excluded from the wire format, since shipping it would broadcast the
 *   whole raw build side alongside the hash table.
 */
class SerializedBroadcastHashTable(
    var serializedData: UnsafeByteArray,
    var numRows: Long,
    var ignoreNullKeys: Boolean,
    var joinHasNullKeys: Boolean,
    var droppedDuplicates: Boolean,
    var bloomFilterBlocksByteSize: Long,
    var hashProbeDynamicFiltersProduced: Long,
    var broadcastId: String,
    @transient var buildSideRelation: BuildSideRelation)
  extends Externalizable {

  def this() = this(null, 0, false, false, false, 0, 0, null, null) // Required for Externalizable

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeLong(numRows)
    out.writeBoolean(ignoreNullKeys)
    out.writeBoolean(joinHasNullKeys)
    out.writeBoolean(droppedDuplicates)
    out.writeLong(bloomFilterBlocksByteSize)
    out.writeLong(hashProbeDynamicFiltersProduced)
    out.writeUTF(if (broadcastId == null) "" else broadcastId)
    serializedData.writeExternal(out)
    // 'buildSideRelation' is intentionally not written. See the class doc.
  }

  override def readExternal(in: ObjectInput): Unit = {
    numRows = in.readLong()
    ignoreNullKeys = in.readBoolean()
    joinHasNullKeys = in.readBoolean()
    droppedDuplicates = in.readBoolean()
    bloomFilterBlocksByteSize = in.readLong()
    hashProbeDynamicFiltersProduced = in.readLong()
    broadcastId = in.readUTF() match {
      case "" => null
      case id => id
    }
    val data = new UnsafeByteArray()
    data.readExternal(in)
    serializedData = data
    buildSideRelation = null
  }

  /**
   * Deserialize the hash table on executor side. The serialized Velox hash table is already in a
   * prepared, probe-ready form, so executor side only needs deserialization without re-running
   * prepareJoinTable.
   *
   * @return
   *   Hash table builder handle
   */
  def deserialize(cacheKey: String): Long = {
    if (serializedData == null || serializedData.isReleased) {
      throw new IllegalStateException(
        s"Serialized hash table bytes for broadcast $broadcastId have already been released. " +
          "They are freed once the native table has been materialized from them, so this " +
          "instance cannot be deserialized again.")
    }
    HashJoinBuilder.deserializeHashTableDirect(
      cacheKey,
      serializedData.address(),
      Math.toIntExact(serializedData.size()),
      ignoreNullKeys,
      joinHasNullKeys)
  }

  /**
   * Frees the off-heap buffer holding the serialized bytes. Only safe once the native hash table
   * has been materialized from it, and only on an executor: on the driver the very same object is
   * still owned by the broadcast variable and by
   * [[VeloxBroadcastBuildSideCache.buildAndSerializeOnDriverInBroadcastExchange]]'s cache.
   */
  def releaseSerializedData(): Unit = {
    if (serializedData != null) {
      serializedData.release()
    }
  }

  /** Get the size of serialized data in bytes. */
  def sizeInBytes: Long = if (serializedData == null) 0L else serializedData.size()
}

object SerializedBroadcastHashTable {
  def apply(
      serializedData: UnsafeByteArray,
      numRows: Long,
      ignoreNullKeys: Boolean,
      joinHasNullKeys: Boolean,
      droppedDuplicates: Boolean,
      bloomFilterBlocksByteSize: Long,
      hashProbeDynamicFiltersProduced: Long,
      broadcastId: String,
      buildSideRelation: BuildSideRelation): SerializedBroadcastHashTable =
    new SerializedBroadcastHashTable(
      serializedData,
      numRows,
      ignoreNullKeys,
      joinHasNullKeys,
      droppedDuplicates,
      bloomFilterBlocksByteSize,
      hashProbeDynamicFiltersProduced,
      broadcastId,
      buildSideRelation
    )

  /**
   * Build and serialize a hash table on the driver.
   *
   * @param hashTableHandle
   *   Handle to the built hash table
   * @param buildSideRelation
   *   The build side relation for metadata
   * @return
   *   Serialized broadcast hash table
   */
  def fromHashTable(
      hashTableHandle: Long,
      cacheKey: String,
      buildSideRelation: BuildSideRelation,
      droppedDuplicates: Boolean,
      numRows: Long): SerializedBroadcastHashTable = {
    try {
      val serializedSize = HashJoinBuilder.serializedHashTableSizeDirect(hashTableHandle)
      val byteBuffer = JniUnsafeByteBuffer.allocate(serializedSize)
      HashJoinBuilder.serializeHashTableDirect(
        hashTableHandle,
        byteBuffer.address(),
        byteBuffer.size())
      val serializedData = byteBuffer.toUnsafeByteArray()
      val ignoreNullKeys = HashJoinBuilder
        .getHashTableIgnoreNullKeys(hashTableHandle)
      val joinHasNullKeys = HashJoinBuilder
        .getHashTableJoinHasNullKeys(hashTableHandle)

      val bloomFilterBlocksByteSize = HashJoinBuilder
        .getHashTableBloomFilterBlocksByteSize(hashTableHandle)
      val hashProbeDynamicFiltersProduced = if (bloomFilterBlocksByteSize > 0) 1L else 0L

      SerializedBroadcastHashTable(
        serializedData,
        numRows,
        ignoreNullKeys,
        joinHasNullKeys,
        droppedDuplicates,
        bloomFilterBlocksByteSize,
        hashProbeDynamicFiltersProduced,
        cacheKey,
        buildSideRelation
      )
    } finally {
      synchronized {
        HashJoinBuilder.clearHashTable(cacheKey, hashTableHandle)
      }
    }
  }
}

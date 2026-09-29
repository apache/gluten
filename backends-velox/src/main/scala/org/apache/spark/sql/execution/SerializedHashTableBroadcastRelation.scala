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
package org.apache.spark.sql.execution

import org.apache.gluten.execution.SerializedBroadcastHashTable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.KnownSizeEstimation

/**
 * Broadcast relation that contains a pre-built and serialized hash table. This is similar to
 * Spark's native HashedRelation broadcast approach where the hash table is built once on the driver
 * and broadcast to executors.
 *
 * Unlike ColumnarBuildSideRelation which broadcasts raw data and builds hash table on each
 * executor, this class broadcasts the serialized hash table directly, saving CPU time on executors.
 *
 * @param serializedHashTable
 *   The serialized hash table built on driver
 * @param mode
 *   The broadcast mode (HashedRelationBroadcastMode or IdentityBroadcastMode)
 * @param output
 *   The output attributes
 * @param buildTimeMs
 *   Time spent building hash table on driver (milliseconds)
 * @param serializeTimeMs
 *   Time spent serializing hash table on driver (milliseconds)
 */
case class SerializedHashTableBroadcastRelation(
    serializedHashTable: SerializedBroadcastHashTable,
    safeBroadcastMode: SafeBroadcastMode,
    output: Seq[Attribute],
    buildTimeMs: Long,
    serializeTimeMs: Long)
  extends BuildSideRelation
  with KnownSizeEstimation {

  // Rebuild the real BroadcastMode on demand; never serialize it.
  @transient override lazy val mode: BroadcastMode =
    BroadcastModeUtils.fromSafe(safeBroadcastMode, output)

  /**
   * Returns an iterator of deserialized columnar batches. Note: This is not the primary use case
   * for this class. The main purpose is to provide the serialized hash table directly to the join
   * operator.
   */
  override def deserialized: Iterator[ColumnarBatch] = {
    serializedHashTable.buildSideRelation match {
      case _: SerializedHashTableBroadcastRelation =>
        throw new IllegalStateException(
          "Unexpected nested SerializedHashTableBroadcastRelation in SerializedBroadcastHashTable")
      case other =>
        other.deserialized
    }
  }

  override def asReadOnlyCopy(): SerializedHashTableBroadcastRelation = this

  /**
   * Get the serialized hash table for use in join operations. This is the primary interface for
   * consuming this broadcast relation.
   */
  def getSerializedHashTable: SerializedBroadcastHashTable = serializedHashTable

  /**
   * Transform is used for DPP (Dynamic Partition Pruning) to extract keys. We delegate to the
   * underlying buildSideRelation in the serialized hash table.
   */
  override def transform(key: Expression): Array[InternalRow] = {
    serializedHashTable.buildSideRelation.transform(key)
  }

  override def estimatedSize: Long = {
    serializedHashTable.sizeInBytes
  }

  /**
   * Get metrics for monitoring.
   */
  def getMetrics: (Long, Long, Long, Long) = {
    (
      serializedHashTable.numRows,
      serializedHashTable.sizeInBytes,
      buildTimeMs,
      serializeTimeMs
    )
  }
}

object SerializedHashTableBroadcastRelation {

  /**
   * Create SerializedHashTableBroadcastRelation from ColumnarBuildSideRelation by building and
   * serializing the hash table on the driver.
   */
  def fromColumnarRelation(
      columnarRelation: ColumnarBuildSideRelation,
      broadcastContext: org.apache.gluten.execution.BroadcastHashJoinContext,
      numRows: Long)
      : SerializedHashTableBroadcastRelation = {

    val startBuildTime = System.currentTimeMillis()

    // Build and serialize hash table on driver
    val serializedHashTable = org.apache.gluten.execution.VeloxBroadcastBuildSideCache
      .buildAndSerializeOnDriverInBroadcastExchange(columnarRelation, broadcastContext, numRows)

    val buildTimeMs = System.currentTimeMillis() - startBuildTime
    val serializeTimeMs = 0L // This is tracked inside SerializedBroadcastHashTable

    SerializedHashTableBroadcastRelation(
      serializedHashTable,
      columnarRelation.safeBroadcastMode,
      columnarRelation.output,
      buildTimeMs,
      serializeTimeMs
    )
  }
}

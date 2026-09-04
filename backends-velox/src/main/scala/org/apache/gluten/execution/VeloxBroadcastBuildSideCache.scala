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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.backendsapi.velox.VeloxBackendSettings
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.vectorized.HashJoinBuilder

import org.apache.spark.SparkEnv
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.ColumnarBuildSideRelation
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.execution.unsafe.UnsafeColumnarBuildSideRelation
import org.apache.spark.task.TaskResources

import com.github.benmanes.caffeine.cache.{Cache, Caffeine, RemovalCause, RemovalListener}

import java.util.concurrent.TimeUnit

case class BroadcastHashTable(
    pointer: Long,
    relation: BuildSideRelation,
    droppedDuplicates: Boolean)

/**
 * One natively deserialized hash table, shared by every broadcast hash join that reads the same
 * driver-side broadcast.
 */
private case class SharedDeserializedHashTable(pointer: Long)

/**
 * `VeloxBroadcastBuildSideCache` is used for controlling to build bhj hash table once.
 *
 * The complicated part is due to reuse exchange, where multiple BHJ IDs correspond to a
 * `BuildSideRelation`.
 *
 * This implementation supports two modes:
 *   1. Driver-side build (new): Hash table is built and serialized on driver, then broadcast to
 *      executors.
 *   2. Executor-side build (legacy): Each executor builds its own hash table from broadcast data
 */
object VeloxBroadcastBuildSideCache
  extends Logging
  with RemovalListener[String, BroadcastHashTable] {

  private lazy val expiredTime = SparkEnv.get.conf.getLong(
    VeloxBackendSettings.GLUTEN_VELOX_BROADCAST_CACHE_EXPIRED_TIME,
    VeloxBackendSettings.GLUTEN_VELOX_BROADCAST_CACHE_EXPIRED_TIME_DEFAULT
  )

  // Use for controlling to build bhj hash table once.
  // key: hashtable id, value is hashtable backend pointer(long to string).
  private val buildSideRelationCache: Cache[String, BroadcastHashTable] =
    Caffeine.newBuilder
      .expireAfterAccess(expiredTime, TimeUnit.SECONDS)
      .removalListener(this)
      .build[String, BroadcastHashTable]()

  // Executor-side cache of natively deserialized hash tables, keyed by the driver-side broadcast
  // id rather than by the per-join hash table id, so that joins sharing a reused broadcast
  // exchange share one native table. Values are owned here; per-join entries in
  // 'buildSideRelationCache' hold clones.
  private val sharedDeserializedCache: Cache[String, SharedDeserializedHashTable] =
    Caffeine.newBuilder
      .expireAfterAccess(expiredTime, TimeUnit.SECONDS)
      .removalListener(
        new RemovalListener[String, SharedDeserializedHashTable] {
          override def onRemoval(
              key: String,
              value: SharedDeserializedHashTable,
              cause: RemovalCause): Unit = {
            if (value != null) {
              HashJoinBuilder.clearHashTable(key, value.pointer)
            }
          }
        }
      ).build[String, SharedDeserializedHashTable]()

  // Cache for driver-side serialized hash tables to avoid rebuilding for reuse exchange
  private val driverSerializedCache: Cache[String, SerializedBroadcastHashTable] =
    Caffeine.newBuilder
      .expireAfterAccess(expiredTime, TimeUnit.SECONDS)
      .removalListener(
        new RemovalListener[String, SerializedBroadcastHashTable] {
          override def onRemoval(
              key: String,
              value: SerializedBroadcastHashTable,
              cause: RemovalCause): Unit = {
            if (value != null && value.serializedData != null) {
              value.serializedData.release()
            }
          }
        }
      ).build[String, SerializedBroadcastHashTable]()

  def getOrBuildBroadcastHashTable(
      broadcast: Broadcast[BuildSideRelation],
      broadcastContext: BroadcastHashJoinContext): BroadcastHashTable = {

    buildSideRelationCache
      .get(
        broadcastContext.buildHashTableId,
        (_: String) => {
          val (pointer, relation, droppedDuplicates) = broadcast.value match {
            case columnar: ColumnarBuildSideRelation =>
              columnar.buildHashTable(broadcastContext)
            case unsafe: UnsafeColumnarBuildSideRelation =>
              unsafe.buildHashTable(broadcastContext)
          }

          broadcastContext.hashTableMemorySizeMetric.foreach(
            _ += HashJoinBuilder.getHashTableMemoryUsage(pointer))

          BroadcastHashTable(pointer, relation, droppedDuplicates)
        }
      )
  }

  /**
   * Build hash table on driver and serialize for broadcasting. This version is called from
   * BroadcastExchangeExec and doesn't need a broadcast variable.
   *
   * This is the Spark-native approach where hash table is built in BroadcastExchangeExec.
   */
  def buildAndSerializeOnDriverInBroadcastExchange(
      relation: BuildSideRelation,
      broadcastContext: BroadcastHashJoinContext,
      numRows: Long): SerializedBroadcastHashTable = {

    val broadcastId = broadcastContext.buildHashTableId

    val cached = driverSerializedCache.getIfPresent(broadcastId)
    if (cached != null) {
      logInfo(s"Reusing cached serialized hash table for broadcast ID: $broadcastId")
      return cached
    }

    def resetRelation(droppedDuplicates: Boolean): Unit = relation match {
      case r: ColumnarBuildSideRelation => r.reset(droppedDuplicates)
      case r: UnsafeColumnarBuildSideRelation => r.reset(droppedDuplicates)
      case _ =>
    }

    relation.synchronized {
      val cachedAfterLock = driverSerializedCache.getIfPresent(broadcastId)
      if (cachedAfterLock != null) {
        logInfo(s"Reusing cached serialized hash table for broadcast ID: $broadcastId (after lock)")
        return cachedAfterLock
      }

      logInfo(
        s"Building hash table on driver in BroadcastExchangeExec " +
          s"for broadcast ID: $broadcastId")

      val backendName = BackendsApiManager.getBackendName
      TaskResources.runUnsafe {
        val runtime = Runtimes.contextInstance(
          backendName,
          "DriverBroadcastHashTableBuild"
        )

        resetRelation(broadcastContext.droppedDuplicates)
        val (hashTableHandle, _, droppedDuplicates) = relation match {
          case r: ColumnarBuildSideRelation =>
            r.buildHashTableWithRuntime(broadcastContext, runtime)
          case r: UnsafeColumnarBuildSideRelation =>
            r.buildHashTableWithRuntime(broadcastContext, runtime)
          case other =>
            throw new IllegalArgumentException(
              s"Unsupported relation type for driver-side build: ${other.getClass.getName}")
        }
        try {
          val startSerializeTime = System.currentTimeMillis()
          val result =
            SerializedBroadcastHashTable.fromHashTable(
              hashTableHandle,
              broadcastId,
              relation,
              droppedDuplicates,
              numRows)
          val serializeTimeMs = System.currentTimeMillis() - startSerializeTime

          logInfo(
            s"Built and serialized hash table on driver: " +
              s"size=${result.sizeInBytes} bytes, " +
              s"rows=${result.numRows}, " +
              s"serializeTime=${serializeTimeMs}ms " +
              s"for broadcast ID: $broadcastId")

          broadcastContext.serializeHashTableTimeMetric.foreach(_ += serializeTimeMs)
          broadcastContext.serializedHashTableSizeMetric.foreach(_ += result.sizeInBytes)

          driverSerializedCache.put(broadcastId, result)
          result
        } finally {
          resetRelation(droppedDuplicates)
        }
      }
    }
  }

  /**
   * Returns the raw build side relation that the driver-side build of `broadcastId` was made from,
   * if this JVM is the driver that built it.
   *
   * The relation is deliberately kept out of the broadcast payload, so a
   * [[SerializedBroadcastHashTable]] read back from the payload has none. That includes the copy
   * the driver itself gets: the broadcast is created with `serializedOnly = true`, so no
   * deserialized copy is retained on the driver and even a driver-side `broadcast.value` goes
   * through the wire format. Consumers that legitimately need the raw build side all run on the
   * driver (DPP key extraction, broadcast mode conversion), where this lookup finds the original
   * that [[buildAndSerializeOnDriverInBroadcastExchange]] cached. On an executor it finds nothing,
   * which is the correct answer there.
   */
  def driverBuildSideRelation(broadcastId: String): Option[BuildSideRelation] =
    Option(broadcastId)
      .flatMap(id => Option(driverSerializedCache.getIfPresent(id)))
      .flatMap(serialized => Option(serialized.buildSideRelation))

  /**
   * Deserialize hash table on executor from broadcast data.
   *
   * A reused broadcast exchange feeds several broadcast hash joins, each with its own
   * `broadcastHashTableId` (the native side looks the table up by that id via [[get]]). Doing the
   * deserialization per join id would materialize a full copy of the table per join. Instead the
   * table is deserialized once per driver-side broadcast and every join id gets a cheap clone that
   * shares the same native table, mirroring what the executor-side build path does via
   * `cloneHashTable`.
   */
  def deserializeOnExecutor(
      serialized: SerializedBroadcastHashTable,
      broadcastHashTableId: String,
      deserializeHashTableTimeMetric: Option[org.apache.spark.sql.execution.metric.SQLMetric] =
        None): BroadcastHashTable = {

    buildSideRelationCache.get(
      broadcastHashTableId,
      (_: String) => {
        val shared = getOrDeserializeShared(
          serialized,
          broadcastHashTableId,
          deserializeHashTableTimeMetric)
        // Register the shared table under this join's id as well, so that the native probe side
        // can resolve it. The clone holds its own reference to the same native table.
        val hashTableHandle =
          HashJoinBuilder.cloneHashTable(broadcastHashTableId, shared.pointer)
        BroadcastHashTable(
          hashTableHandle,
          serialized.buildSideRelation,
          serialized.droppedDuplicates)
      }
    )
  }

  /**
   * Returns a handle to the native hash table for `serialized`, deserializing it at most once per
   * driver-side broadcast id. The returned handle is owned by [[sharedDeserializedCache]]; callers
   * must clone it rather than releasing it.
   */
  private def getOrDeserializeShared(
      serialized: SerializedBroadcastHashTable,
      broadcastHashTableId: String,
      deserializeHashTableTimeMetric: Option[org.apache.spark.sql.execution.metric.SQLMetric])
      : SharedDeserializedHashTable = {
    // Older payloads, and any path that did not go through the driver-side build, carry no
    // broadcast id. Fall back to keying on the join id, which is what the previous behavior was.
    val sharedKey =
      if (serialized.broadcastId != null) serialized.broadcastId else broadcastHashTableId

    sharedDeserializedCache.get(
      sharedKey,
      (key: String) => {
        logInfo(s"Deserializing hash table on executor for broadcast ID: $key")
        val startTime = System.currentTimeMillis()
        val hashTableHandle = serialized.deserialize(key)
        val timeMs = System.currentTimeMillis() - startTime
        deserializeHashTableTimeMetric.foreach(_ += timeMs)

        // The serialized bytes have been fully consumed into the native table. On an executor
        // nothing else refers to them, so free the off-heap copy instead of waiting for the
        // broadcast object to be collected. On the driver the same object is still owned by the
        // broadcast variable and by driverSerializedCache, so it must be left alone.
        if (!isDriver) {
          serialized.releaseSerializedData()
        }
        SharedDeserializedHashTable(hashTableHandle)
      }
    )
  }

  // Mirrors the private[spark] SparkContext.DRIVER_IDENTIFIER.
  private val driverExecutorId = "driver"

  private def isDriver: Boolean = {
    val env = SparkEnv.get
    env == null || env.executorId == driverExecutorId
  }

  /** This is called from c++ side. */
  def get(broadcastHashtableId: String): Long = {
    Option(buildSideRelationCache.getIfPresent(broadcastHashtableId))
      .map(_.pointer)
      .getOrElse(0)
  }

  def invalidateBroadcastHashtable(broadcastHashtableId: String): Unit = {
    // Cleanup operations on the backend are idempotent.
    buildSideRelationCache.invalidate(broadcastHashtableId)
  }

  /** Only used in UT. */
  def size(): Long = buildSideRelationCache.estimatedSize()

  /** Only used in UT. */
  def driverSerializedCacheSize(): Long = driverSerializedCache.estimatedSize()

  def cleanAll(): Unit = {
    buildSideRelationCache.invalidateAll()
    sharedDeserializedCache.invalidateAll()
    driverSerializedCache.invalidateAll()
  }

  override def onRemoval(
      key: String,
      value: BroadcastHashTable,
      cause: RemovalCause): Unit = {
    synchronized {
      if (value.relation != null) {
        value.relation match {
          case columnar: ColumnarBuildSideRelation =>
            columnar.reset(value.droppedDuplicates)
          case unsafe: UnsafeColumnarBuildSideRelation =>
            unsafe.reset(value.droppedDuplicates)
        }
      }

      HashJoinBuilder.clearHashTable(key, value.pointer)
    }
  }
}

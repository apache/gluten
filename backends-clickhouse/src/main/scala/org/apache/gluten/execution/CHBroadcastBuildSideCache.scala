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

import org.apache.gluten.backendsapi.clickhouse.CHBackendSettings
import org.apache.gluten.vectorized.StorageJoinBuilder

import org.apache.spark.SparkEnv
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.joins.{BuildSideRelation, ClickHouseBuildSideRelation}

import com.github.benmanes.caffeine.cache.{Cache, Caffeine, RemovalCause, RemovalListener}

import java.util.concurrent.TimeUnit

case class BroadcastHashTable(pointer: Long, relation: ClickHouseBuildSideRelation)

/**
 * `CHBroadcastBuildSideCache` is used for controlling to build bhj hash table once.
 *
 * The complicated part is due to reuse exchange, where multiple BHJ IDs correspond to a
 * `ClickHouseBuildSideRelation`.
 */
object CHBroadcastBuildSideCache
  extends Logging
  with RemovalListener[String, BroadcastHashTable] {

  private lazy val expiredTime = SparkEnv.get.conf.getLong(
    CHBackendSettings.GLUTEN_CLICKHOUSE_BROADCAST_CACHE_EXPIRED_TIME,
    CHBackendSettings.GLUTEN_CLICKHOUSE_BROADCAST_CACHE_EXPIRED_TIME_DEFAULT
  )

  // Use for controlling to build bhj hash table once.
  // key: hashtable id, value is hashtable backend pointer(long to string).
  private val buildSideRelationCache: Cache[String, BroadcastHashTable] =
    Caffeine.newBuilder
      .expireAfterAccess(expiredTime, TimeUnit.SECONDS)
      .removalListener(this)
      .build[String, BroadcastHashTable]()

  def getOrBuildBroadcastHashTable(
      broadcast: Broadcast[BuildSideRelation],
      broadcastContext: BroadcastJoinContext): BroadcastHashTable = {

    buildSideRelationCache
      .get(
        broadcastContext.buildTableId,
        (broadcastId: String) => {
          val (pointer, relation) =
            broadcast.value
              .asInstanceOf[ClickHouseBuildSideRelation]
              .buildHashTable(broadcastContext)
          logDebug(s"Create bhj $broadcastId = 0x${pointer.toHexString}")
          BroadcastHashTable(pointer, relation)
        }
      )
  }

  /** This is called from c++ side. */
  def get(broadcastHashtableId: String): Long =
    Option(buildSideRelationCache.getIfPresent(broadcastHashtableId))
      .map(_.pointer)
      .getOrElse(0)

  def invalidateBroadcastHashtable(broadcastHashtableId: String): Unit = {
    // Cleanup operations on the backend are idempotent.
    buildSideRelationCache.invalidate(broadcastHashtableId)
  }

  /** Only used in UT. */
  def size(): Long = buildSideRelationCache.estimatedSize()

  def cleanAll(): Unit = buildSideRelationCache.invalidateAll()

  override def onRemoval(
      key: String,
      value: BroadcastHashTable,
      cause: RemovalCause): Unit = {
    logDebug(s"Remove bhj $key = 0x${value.pointer.toHexString}")
    if (value.relation != null) {
      value.relation.reset()
    }
    StorageJoinBuilder.nativeCleanBuildHashTable(key, value.pointer)
  }
}

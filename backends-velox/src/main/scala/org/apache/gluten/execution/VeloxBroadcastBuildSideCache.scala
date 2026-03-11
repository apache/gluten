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

import org.apache.gluten.backendsapi.velox.VeloxBackendSettings
import org.apache.gluten.vectorized.HashJoinBuilder

import org.apache.spark.SparkEnv
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.ColumnarBuildSideRelation
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.execution.unsafe.UnsafeColumnarBuildSideRelation

import com.github.benmanes.caffeine.cache.{Cache, Caffeine, RemovalCause, RemovalListener}

import java.util.concurrent.TimeUnit

case class BroadcastHashTable(pointer: Long, relation: BuildSideRelation)

/**
 * `VeloxBroadcastBuildSideCache` is used for controlling to build bhj hash table once.
 *
 * The complicated part is due to reuse exchange, where multiple BHJ IDs correspond to a
 * `BuildSideRelation`.
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

  def getOrBuildBroadcastHashTable(
      broadcast: Broadcast[BuildSideRelation],
      broadcastContext: BroadcastHashJoinContext): BroadcastHashTable = synchronized {

    buildSideRelationCache
      .get(
        broadcastContext.buildHashTableId,
        (broadcast_id: String) => {
          val (pointer, relation) = broadcast.value match {
            case columnar: ColumnarBuildSideRelation =>
              columnar.buildHashTable(broadcastContext)
            case unsafe: UnsafeColumnarBuildSideRelation =>
              unsafe.buildHashTable(broadcastContext)
          }

          logWarning(s"Create bhj $broadcast_id = $pointer")
          BroadcastHashTable(pointer, relation)
        }
      )
  }

  /** This is callback from c++ backend. */
  def get(broadcastHashtableId: String): Long =
    synchronized {
      Option(buildSideRelationCache.getIfPresent(broadcastHashtableId))
        .map(_.pointer)
        .getOrElse(0)
    }

  def invalidateBroadcastHashtable(broadcastHashtableId: String): Unit = synchronized {
    // Cleanup operations on the backend are idempotent.
    buildSideRelationCache.invalidate(broadcastHashtableId)
  }

  /** Only used in UT. */
  def size(): Long = buildSideRelationCache.estimatedSize()

  def cleanAll(): Unit = buildSideRelationCache.invalidateAll()

  override def onRemoval(key: String, value: BroadcastHashTable, cause: RemovalCause): Unit = {
    synchronized {
      logWarning(s"Remove bhj $key = ${value.pointer}")
      if (value.relation != null) {
        value.relation match {
          case columnar: ColumnarBuildSideRelation =>
            columnar.reset()
          case unsafe: UnsafeColumnarBuildSideRelation =>
            unsafe.reset()
        }
      }

      HashJoinBuilder.clearHashTable(value.pointer)
    }
  }
}

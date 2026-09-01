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
package org.apache.spark.shuffle

import org.apache.spark.{ShuffleDependency, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.MergedBlockMeta
import org.apache.spark.storage.{BlockId, ShuffleBlockBatchId, ShuffleBlockId, ShuffleMergedBlockId}

/** The internal shuffle manager instance used by GlutenShuffleManager. */
private class ShuffleManagerRouter(lookup: ShuffleManagerLookup)
  extends ShuffleManager
  with Logging {
  import ShuffleManagerRouter._
  private val cache = new Cache()
  private val resolver = new BlockResolver(cache)

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    val manager = lookup.findShuffleManager(dependency)
    cache.store(shuffleId, manager).registerShuffle(shuffleId, dependency)
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    ensureShuffleManagerRegistered(handle)
    cache.get(handle.shuffleId).getWriter(handle, mapId, context, metrics)
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    ensureShuffleManagerRegistered(handle)
    cache
      .get(handle.shuffleId)
      .getReader(handle, startMapIndex, endMapIndex, startPartition, endPartition, context, metrics)
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    // On a multi-executor cluster, Spark broadcasts RemoveShuffle to every executor, so a router
    // that never registered or served this shuffleId still receives unregisterShuffle. Tolerate
    // the miss instead of asserting: report that this router removed nothing.
    cache.remove(shuffleId).exists(_.unregisterShuffle(shuffleId))
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = resolver

  override def stop(): Unit = {
    if (!(cache.size() == 0)) {
      logWarning(
        s"Shuffle router cache is not empty when being stopped. This might be because the " +
          s"shuffle is not unregistered.")
    }
    lookup.all().foreach(_.stop())
  }

  private def ensureShuffleManagerRegistered(handle: ShuffleHandle): Unit = {
    val baseShuffleHandle = handle match {
      case b: BaseShuffleHandle[_, _, _] => b
      case _ =>
        throw new UnsupportedOperationException(
          s"${handle.getClass} is not a BaseShuffleHandle so is not supported by " +
            s"GlutenShuffleManager")
    }
    // store is idempotent and resolves the manager lazily, so several task threads first-touching
    // the same new shuffleId install it exactly once, while an already-cached shuffle skips the
    // lookup entirely.
    cache.store(
      baseShuffleHandle.shuffleId,
      lookup.findShuffleManager(baseShuffleHandle.dependency))
  }
}

private object ShuffleManagerRouter {
  private class Cache {
    private val cache: java.util.Map[Int, ShuffleManager] =
      new java.util.concurrent.ConcurrentHashMap()

    def store(shuffleId: Int, manager: => ShuffleManager): ShuffleManager = {
      // Idempotent: on a multi-core executor several task threads may first-touch the same new
      // shuffleId concurrently. computeIfAbsent lets the first caller install the manager and the
      // rest observe it, instead of racing into an assertion. The by-name manager is resolved only
      // on a miss, so an already-cached shuffle skips the lookup. lookup resolves the same manager
      // for a given dependency, so returning an already-cached entry is safe.
      cache.computeIfAbsent(shuffleId, _ => manager)
    }

    def get(shuffleId: Int): ShuffleManager = {
      val manager = cache.get(shuffleId)
      assert(manager != null, s"Shuffle manager not registered for shuffle id: $shuffleId")
      manager
    }

    def remove(shuffleId: Int): Option[ShuffleManager] = {
      // On a multi-executor cluster, Spark broadcasts RemoveShuffle to every executor, so this
      // router may be asked to remove a shuffleId it never cached. Return None instead of
      // asserting the entry was present.
      Option(cache.remove(shuffleId))
    }

    def size(): Int = {
      cache.size()
    }

    def clear(): Unit = {
      cache.clear()
    }
  }

  private class BlockResolver(cache: Cache) extends ShuffleBlockResolver {
    override def getBlockData(blockId: BlockId, dirs: Option[Array[String]]): ManagedBuffer = {
      val shuffleId = blockId match {
        case id: ShuffleBlockId =>
          id.shuffleId
        case batchId: ShuffleBlockBatchId =>
          batchId.shuffleId
        case _ =>
          throw new IllegalArgumentException(
            "GlutenShuffleManager: Unsupported shuffle block id: " + blockId)
      }
      cache.get(shuffleId).shuffleBlockResolver.getBlockData(blockId, dirs)
    }

    override def getMergedBlockData(
        blockId: ShuffleMergedBlockId,
        dirs: Option[Array[String]]): Seq[ManagedBuffer] = {
      val shuffleId = blockId.shuffleId
      cache.get(shuffleId).shuffleBlockResolver.getMergedBlockData(blockId, dirs)
    }

    override def getMergedBlockMeta(
        blockId: ShuffleMergedBlockId,
        dirs: Option[Array[String]]): MergedBlockMeta = {
      val shuffleId = blockId.shuffleId
      cache.get(shuffleId).shuffleBlockResolver.getMergedBlockMeta(blockId, dirs)
    }

    override def stop(): Unit = {
      throw new UnsupportedOperationException(
        s"BlockResolver ${getClass.getSimpleName} doesn't need to be explicitly stopped")
    }
  }
}

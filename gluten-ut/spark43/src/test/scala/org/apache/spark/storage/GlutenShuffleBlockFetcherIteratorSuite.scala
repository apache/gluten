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
package org.apache.spark.storage

import org.apache.spark.{MapOutputTracker, SparkFunSuite, TaskContext}
import org.apache.spark.MapOutputTracker.SHUFFLE_PUSH_MAP_ID
import org.apache.spark.executor.TempShuffleReadMetrics
import org.apache.spark.network.BlockTransferService
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.{BlockFetchingListener, ExternalBlockStoreClient, MergedBlockMeta, MergedBlocksMetaListener}
import org.apache.spark.storage.BlockManagerId.SHUFFLE_MERGER_IDENTIFIER

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}
import org.roaringbitmap.RoaringBitmap

import java.io.InputStream
import java.util.concurrent.CompletableFuture

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Tests the stale map index check (SPARK-57491) of [[GlutenPushBasedFetchHelper]], adapted from
 * Spark's ShuffleBlockFetcherIteratorSuite.
 */
class GlutenShuffleBlockFetcherIteratorSuite extends SparkFunSuite {

  private var transfer: BlockTransferService = _
  private var mapOutputTracker: MapOutputTracker = _

  private val localBmId = BlockManagerId("test-client", "test-local-host", 1)
  private val localDirs = Array("test-merged-dir-1", "test-merged-dir-2")
  private val mergedBlockId = ShuffleMergedBlockId(0, 0, 2)
  private val originalBlocks = Seq(ShuffleBlockId(0, 0, 2), ShuffleBlockId(0, 1, 2))

  override def beforeEach(): Unit = {
    super.beforeEach()
    transfer = mock(classOf[BlockTransferService])
    mapOutputTracker = mock(classOf[MapOutputTracker])
    when(mapOutputTracker.getMapSizesForMergeResult(any(), any(), any()))
      .thenReturn(Seq.empty.iterator)
    when(mapOutputTracker.getStaleMapIndexes(any())).thenReturn(mutable.Set.empty[Int])
  }

  private def doReturn(value: Any) = org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  private def createMockManagedBuffer(size: Int = 1): ManagedBuffer = {
    val buffer = mock(classOf[ManagedBuffer])
    val in = mock(classOf[InputStream])
    when(in.read(any())).thenReturn(1)
    when(in.read(any(), any(), any())).thenReturn(1)
    when(buffer.createInputStream()).thenReturn(in)
    when(buffer.size()).thenReturn(size)
    buffer
  }

  private def createMockPushMergedBlockMeta(bitmaps: Array[RoaringBitmap]): MergedBlockMeta = {
    val meta = mock(classOf[MergedBlockMeta])
    when(meta.getNumChunks).thenReturn(bitmaps.length)
    when(meta.readChunkBitmaps()).thenReturn(bitmaps)
    doReturn(createMockManagedBuffer()).when(meta).getChunksBitmapBuffer
    meta
  }

  private def bitmap(mapIndexes: Int*): RoaringBitmap = {
    val bitmap = new RoaringBitmap()
    mapIndexes.foreach(bitmap.add)
    bitmap
  }

  /**
   * Creates a block manager that serves the original blocks of map indexes 0 and 1 of reduce
   * partition 2, and the push-merged-local block with the given chunk bitmaps.
   */
  private def createBlockManager(bitmaps: Array[RoaringBitmap]): BlockManager = {
    val blockManager = mock(classOf[BlockManager])
    doReturn(localBmId).when(blockManager).blockManagerId
    val blockStoreClient = mock(classOf[ExternalBlockStoreClient])
    val hostLocalDirManager = new HostLocalDirManager(1, blockStoreClient)
    when(blockManager.hostLocalDirManager).thenReturn(Some(hostLocalDirManager))
    when(blockStoreClient.getHostLocalDirs(any(), any(), any(), any())).thenAnswer {
      invocation =>
        invocation
          .getArgument[CompletableFuture[java.util.Map[String, Array[String]]]](3)
          .complete(Map(SHUFFLE_MERGER_IDENTIFIER -> localDirs).asJava)
    }
    originalBlocks.foreach {
      blockId =>
        doReturn(createMockManagedBuffer(100)).when(blockManager).getLocalBlockData(blockId)
    }
    when(mapOutputTracker.getMapSizesForMergeResult(0, 2)).thenAnswer {
      _ =>
        Seq(
          (
            localBmId,
            Seq[(BlockId, Long, Int)](
              (ShuffleBlockId(0, 0, 2), 1L, 0),
              (ShuffleBlockId(0, 1, 2), 1L, 1)))).iterator
    }
    val meta = createMockPushMergedBlockMeta(bitmaps)
    when(blockManager.getLocalMergedBlockMeta(mergedBlockId, localDirs)).thenReturn(meta)
    doReturn(bitmaps.toSeq.map(_ => createMockManagedBuffer(100)))
      .when(blockManager)
      .getLocalMergedBlockData(mergedBlockId, localDirs)
    blockManager
  }

  private def readAll(
      blockManager: BlockManager,
      mergerBmId: BlockManagerId,
      shuffleMetrics: TempShuffleReadMetrics): Seq[BlockId] = {
    val iterator = new GlutenShuffleBlockFetcherIterator(
      TaskContext.empty(),
      transfer,
      blockManager,
      mapOutputTracker,
      Iterator(
        mergerBmId -> Seq[(BlockId, Long, Int)]((mergedBlockId, 200L, SHUFFLE_PUSH_MAP_ID))),
      (_, in) => in,
      maxBytesInFlight = Long.MaxValue,
      maxReqsInFlight = Int.MaxValue,
      maxBlocksInFlightPerAddress = Int.MaxValue,
      maxReqSizeShuffleToMem = Int.MaxValue,
      maxAttemptsOnNettyOOM = 10,
      detectCorrupt = true,
      detectCorruptUseExtraMemory = true,
      checksumEnabled = true,
      checksumAlgorithm = "ADLER32",
      shuffleMetrics = shuffleMetrics,
      doBatchFetch = false
    )
    iterator.map(_._1).toList
  }

  test("SPARK-57491: push-merged-local block with a stale map index falls back") {
    val mergerBmId = BlockManagerId(SHUFFLE_MERGER_IDENTIFIER, localBmId.host, 1)
    when(mapOutputTracker.getStaleMapIndexes(0)).thenReturn(mutable.Set(1))

    val staleMetrics = TaskContext.empty().taskMetrics.createTempShuffleReadMetrics()
    val staleBlockManager = createBlockManager(Array(bitmap(0, 1)))
    assert(readAll(staleBlockManager, mergerBmId, staleMetrics) === originalBlocks)
    assert(staleMetrics.mergedFetchFallbackCount === 1)

    val currentMetrics = TaskContext.empty().taskMetrics.createTempShuffleReadMetrics()
    val currentBlockManager = createBlockManager(Array(bitmap(0), bitmap(2)))
    val ids = readAll(currentBlockManager, mergerBmId, currentMetrics)
    assert(ids === Seq(ShuffleBlockChunkId(0, 0, 2, 0), ShuffleBlockChunkId(0, 0, 2, 1)))
    assert(currentMetrics.mergedFetchFallbackCount === 0)
  }

  test("SPARK-57491: remote push-merged block with a stale map index falls back") {
    val mergerBmId = BlockManagerId(SHUFFLE_MERGER_IDENTIFIER, "test-remote-host", 2)
    when(mapOutputTracker.getStaleMapIndexes(0)).thenReturn(mutable.Set(1))
    when(transfer.fetchBlocks(any(), any(), any(), any(), any(), any())).thenAnswer {
      invocation =>
        val listener = invocation.getArgument[BlockFetchingListener](4)
        invocation
          .getArgument[Array[String]](3)
          .foreach(blockId => listener.onBlockFetchSuccess(blockId, createMockManagedBuffer(100)))
    }

    var bitmaps: Array[RoaringBitmap] = null
    when(transfer.getMergedBlockMeta(any(), any(), any(), any(), any(), any())).thenAnswer {
      invocation =>
        invocation
          .getArgument[MergedBlocksMetaListener](5)
          .onSuccess(
            invocation.getArgument[Int](2),
            invocation.getArgument[Int](3),
            invocation.getArgument[Int](4),
            createMockPushMergedBlockMeta(bitmaps))
    }

    bitmaps = Array(bitmap(0, 1))
    val staleMetrics = TaskContext.empty().taskMetrics.createTempShuffleReadMetrics()
    assert(readAll(createBlockManager(Array.empty), mergerBmId, staleMetrics) === originalBlocks)
    assert(staleMetrics.mergedFetchFallbackCount === 1)

    bitmaps = Array(bitmap(0), bitmap(2))
    val currentMetrics = TaskContext.empty().taskMetrics.createTempShuffleReadMetrics()
    val ids = readAll(createBlockManager(Array.empty), mergerBmId, currentMetrics)
    assert(ids === Seq(ShuffleBlockChunkId(0, 0, 2, 0), ShuffleBlockChunkId(0, 0, 2, 1)))
    assert(currentMetrics.mergedFetchFallbackCount === 0)
  }
}

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

import org.apache.gluten.config.{BoltConfig, GlutenConfig}
import org.apache.gluten.proto.ShuffleReaderInfo
import org.apache.gluten.vectorized.{ColumnarBatchSerializerInstance, SettableColumnarBatchSerializer, ShuffleStreamReader}

import org.apache.spark._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerId, ShuffleBlockFetcherIterator}
import org.apache.spark.util.CompletionIterator

import java.io.InputStream

/**
 * Wrap shuffle reader iterator so that the down streams can control shuffle reader. If the down
 * stream is whole stage iterator, it would take over the shuffle reader iterator and offload as a
 * Bolt operator
 *
 * @param delegate
 *   the origin iterator
 * @param inner
 *   shuffle reader input streams
 * @param serializer
 *   Serializer object to update metrics
 * @param readerInfo
 *   shuffle reader information
 */
class ShuffleReaderIteratorWrapper(
    delegate: Iterator[Product2[Int, ColumnarBatch]],
    val inner: Iterator[(BlockId, InputStream)],
    serializer: SettableColumnarBatchSerializer,
    readerInfo: Array[Byte])
  extends Iterator[Product2[Int, ColumnarBatch]] {
  // if the shuffle reader is mark offloaded, we should use inner to fetch the blocks
  var markOffloaded = false
  override def hasNext: Boolean = !markOffloaded && delegate.hasNext
  override def next(): Product2[Int, ColumnarBatch] = {
    if (markOffloaded) throw new NoSuchElementException("Iterator is marked as offloaded")
    else delegate.next()
  }
  def markAsOffloaded(): Unit = {
    markOffloaded = true
  }
  def updateMetrics(
      numRows: Long,
      numBatchesTotal: Long,
      decompressTimeInMs: Long,
      deserializeTimeInMs: Long,
      totalReadTimeInMs: Long): Unit = {
    serializer.numOutputRows.add(numRows)
    serializer.decompressTime.add(decompressTimeInMs)
    serializer.deserializeTime.add(deserializeTimeInMs)
    serializer.totalReadTime.add(totalReadTimeInMs)
    serializer.readBatchNumRows.set(if (numBatchesTotal == 0) 0 else numRows / numBatchesTotal)
  }
  def getStreamReader: ShuffleStreamReader = new ShuffleStreamReader(inner)
  def getReaderInfo: Array[Byte] = readerInfo
}

/**
 * Fetches and reads the blocks from a shuffle by requesting them from other nodes' block stores.
 */
class ColumnarShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    blocksByAddress: Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])],
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker,
    shouldBatchFetch: Boolean = false)
  extends ShuffleReader[K, C]
  with Logging {

  private val dep = handle.dependency

  private def fetchContinuousBlocksInBatch: Boolean = {
    val conf = SparkEnv.get.conf
    val serializerRelocatable = dep.serializer.supportsRelocationOfSerializedObjects
    val compressed = conf.get(config.SHUFFLE_COMPRESS)
    val codecConcatenation = if (compressed) {
      CompressionCodec.supportsConcatenationOfSerializedStreams(CompressionCodec.createCodec(conf))
    } else {
      true
    }
    val useOldFetchProtocol = conf.get(config.SHUFFLE_USE_OLD_FETCH_PROTOCOL)
    // SPARK-34790: Fetching continuous blocks in batch is incompatible with io encryption.
    val ioEncryption = conf.get(config.IO_ENCRYPTION_ENABLED)

    val doBatchFetch = shouldBatchFetch && serializerRelocatable &&
      (!compressed || codecConcatenation) && !useOldFetchProtocol && !ioEncryption
    if (shouldBatchFetch && !doBatchFetch) {
      logDebug(
        "The feature tag of continuous shuffle block fetching is set to true, but " +
          "we can not enable the feature because other conditions are not satisfied. " +
          s"Shuffle compress: $compressed, serializer relocatable: $serializerRelocatable, " +
          s"codec concatenation: $codecConcatenation, use old shuffle fetch protocol: " +
          s"$useOldFetchProtocol, io encryption: $ioEncryption.")
    }
    doBatchFetch
  }

  def getReaderInfo(): ShuffleReaderInfo = {
    val conf = SparkEnv.get.conf
    val compressionCodec =
      if (conf.getBoolean("spark.shuffle.compress", true)) {
        GlutenShuffleUtils.getCompressionCodec(conf)
      } else {
        "" // uncompressed
      }
    val compressionCodecBackend =
      GlutenConfig.get.columnarShuffleCodecBackend.getOrElse("none")
    val batchSize = GlutenConfig.get.maxBatchSize
    val shuffleBatchByteSize = BoltConfig.get.maxShuffleBatchByteSize
    val forceShuffleWriterType = BoltConfig.get.forceShuffleWriterType
    val nativePartitioning =
      dep
        .asInstanceOf[ColumnarShuffleDependency[Int, ColumnarBatch, ColumnarBatch]]
        .nativePartitioning
    val builder = ShuffleReaderInfo.newBuilder()
    builder
      .setBatchSize(batchSize)
      .setShuffleBatchByteSize(shuffleBatchByteSize)
      .setNumPartitions(nativePartitioning.getNumPartitions)
      .setPartitionShortName(nativePartitioning.getShortName)
      .setForcedWriterType(forceShuffleWriterType)
      .setCompressionType(compressionCodec)
      .setCodec(compressionCodecBackend)
    builder.build()
  }

  /** Read the combined key-values for this reduce task */
  override def read(): Iterator[Product2[K, C]] = {
    val wrappedStreams = new ShuffleBlockFetcherIterator(
      context,
      blockManager.blockStoreClient,
      blockManager,
      mapOutputTracker,
      blocksByAddress,
      serializerManager.wrapStream,
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.get(config.REDUCER_MAX_SIZE_IN_FLIGHT) * 1024 * 1024,
      SparkEnv.get.conf.get(config.REDUCER_MAX_REQS_IN_FLIGHT),
      SparkEnv.get.conf.get(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS),
      SparkEnv.get.conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM),
      SparkEnv.get.conf.get(config.SHUFFLE_MAX_ATTEMPTS_ON_NETTY_OOM),
      SparkEnv.get.conf.get(config.SHUFFLE_DETECT_CORRUPT),
      SparkEnv.get.conf.get(config.SHUFFLE_DETECT_CORRUPT_MEMORY),
      SparkEnv.get.conf.get(config.SHUFFLE_CHECKSUM_ENABLED),
      SparkEnv.get.conf.get(config.SHUFFLE_CHECKSUM_ALGORITHM),
      readMetrics,
      fetchContinuousBlocksInBatch
    ).toCompletionIterator

    val recordIter = dep match {
      case columnarDep: ColumnarShuffleDependency[K, _, C] =>
        // If the dependency is a ColumnarShuffleDependency, we use the columnar serializer.
        columnarDep.serializer
          .newInstance()
          .asInstanceOf[ColumnarBatchSerializerInstance]
          .deserializeStreams(wrappedStreams)
          .asKeyValueIterator
      case _ =>
        val serializerInstance = dep.serializer.newInstance()
        // Create a key/value iterator for each stream
        wrappedStreams.flatMap {
          case (blockId, wrappedStream) =>
            // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
            // NextIterator. The NextIterator makes sure that close() is called on the
            // underlying InputStream when all records have been read.
            serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
        }
    }

    // Update the context task metrics for each record read.
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map {
        record =>
          readMetrics.incRecordsRead(1)
          record
      },
      context.taskMetrics().mergeShuffleReadMetrics()).asInstanceOf[Iterator[Product2[K, C]]]

    // An interruptible iterator must be used here in order to support task cancellation
    if (BoltConfig.get.shuffleInsideBolt) {
      new ShuffleReaderIteratorWrapper(
        new InterruptibleIterator(context, metricIter)
          .asInstanceOf[Iterator[Product2[Int, ColumnarBatch]]],
        wrappedStreams,
        dep.serializer.asInstanceOf[SettableColumnarBatchSerializer],
        getReaderInfo().toByteArray
      ).asInstanceOf[Iterator[Product2[K, C]]]
    } else {
      new InterruptibleIterator(context, metricIter)
        .asInstanceOf[Iterator[Product2[K, C]]]
    }
  }
}

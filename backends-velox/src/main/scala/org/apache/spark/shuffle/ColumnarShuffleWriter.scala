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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.config.{GlutenConfig, GpuHashShuffleWriterType, HashShuffleWriterType, SortShuffleWriterType}
import org.apache.gluten.memory.memtarget.{MemoryTarget, Spiller}
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.vectorized._

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{SHUFFLE_COMPRESS, SHUFFLE_DISK_WRITE_BUFFER_SIZE, SHUFFLE_FILE_BUFFER_SIZE, SHUFFLE_SORT_INIT_BUFFER_SIZE, SHUFFLE_SORT_USE_RADIXSORT}
import org.apache.spark.memory.SparkMemoryUtil
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{SparkDirectoryUtil, SparkResourceUtil, Utils}

import java.io.IOException

class ColumnarShuffleWriter[K, V](
    shuffleBlockResolver: IndexShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, V],
    mapId: Long,
    writeMetrics: ShuffleWriteMetricsReporter)
  extends ShuffleWriter[K, V]
  with Logging {

  private val dep = handle.dependency.asInstanceOf[ColumnarShuffleDependency[K, V, V]]

  dep.shuffleWriterType match {
    case HashShuffleWriterType | SortShuffleWriterType | GpuHashShuffleWriterType =>
    // Valid shuffle writer types
    case _ =>
      throw new IllegalArgumentException(
        s"Unsupported shuffle writer type: ${dep.shuffleWriterType.name}, " +
          s"expected one of: ${HashShuffleWriterType.name}, ${SortShuffleWriterType.name}")
  }

  protected val isSort: Boolean = dep.shuffleWriterType == SortShuffleWriterType

  private val numPartitions: Int = dep.partitioner.numPartitions

  private val conf = SparkEnv.get.conf

  private val blockManager = SparkEnv.get.blockManager

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false

  private var mapStatus: MapStatus = _

  private val localDirs = SparkDirectoryUtil
    .get()
    .namespace("shuffle-write")
    .all
    .map(_.getAbsolutePath)
    .mkString(",")

  private lazy val nativeBufferSize = {
    val bufferSize = GlutenConfig.get.shuffleWriterBufferSize
    val maxBatchSize = GlutenConfig.get.maxBatchSize
    if (bufferSize > maxBatchSize) {
      logInfo(
        s"${GlutenConfig.SHUFFLE_WRITER_BUFFER_SIZE.key} ($bufferSize) exceeds max " +
          s" batch size. Limited to ${GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key} ($maxBatchSize).")
      maxBatchSize
    } else {
      bufferSize
    }
  }

  private val compressionCodec: Option[String] =
    if (conf.getBoolean(SHUFFLE_COMPRESS.key, SHUFFLE_COMPRESS.defaultValue.get)) {
      Some(GlutenShuffleUtils.getCompressionCodec(conf))
    } else {
      None
    }

  private val compressionLevel = {
    compressionCodec
      .map(codec => GlutenShuffleUtils.getCompressionLevel(conf, codec))
      .getOrElse(GlutenShuffleUtils.DEFAULT_COMPRESSION_LEVEL)
  }

  private val compressionBufferSize = {
    compressionCodec
      .map(codec => GlutenShuffleUtils.getCompressionBufferSize(conf, codec))
      .getOrElse(0)
  }

  private val reallocThreshold = GlutenConfig.get.columnarShuffleReallocThreshold

  private val runtime = Runtimes.contextInstance(BackendsApiManager.getBackendName, "ShuffleWriter")

  private val partitionWriterJniWrapper = LocalPartitionWriterJniWrapper.create(runtime)

  private val shuffleWriterJniWrapper = ShuffleWriterJniWrapper.create(runtime)

  private var nativeShuffleWriter: Long = -1L

  private var splitResult: GlutenSplitResult = _

  private var partitionLengths: Array[Long] = _

  private val taskContext: TaskContext = TaskContext.get()

  private def availableOffHeapPerTask(): Long = {
    val perTask =
      SparkMemoryUtil.getCurrentAvailableOffHeapMemory / SparkResourceUtil.getTaskSlots(conf)
    perTask
  }

  @throws[IOException]
  def internalWrite(records: Iterator[Product2[K, V]]): Unit = {
    if (!records.hasNext) {
      handleEmptyInput()
      return
    }

    val tempDataFile = Utils.tempFileWith(shuffleBlockResolver.getDataFile(dep.shuffleId, mapId))

    while (records.hasNext) {
      val cb = records.next()._2.asInstanceOf[ColumnarBatch]
      if (cb.numRows == 0 || cb.numCols == 0) {
        logInfo(s"Skip ColumnarBatch of ${cb.numRows} rows, ${cb.numCols} cols")
      } else {
        if (nativeShuffleWriter == -1L) {
          val partitionWriterHandle = partitionWriterJniWrapper.createPartitionWriter(
            numPartitions,
            compressionCodec.orNull,
            GlutenConfig.get.columnarShuffleCodecBackend.orNull,
            compressionLevel,
            compressionBufferSize,
            GlutenConfig.get.columnarShuffleCompressionThreshold,
            nativeBufferSize,
            GlutenConfig.get.columnarShuffleMergeThreshold,
            blockManager.subDirsPerLocalDir,
            conf.get(SHUFFLE_FILE_BUFFER_SIZE).toInt,
            tempDataFile.getAbsolutePath,
            localDirs,
            GlutenConfig.get.columnarShuffleEnableDictionary,
            GlutenConfig.get.columnarShuffleEnableTypeAwareCompress
          )

          nativeShuffleWriter = if (isSort) {
            shuffleWriterJniWrapper.createSortShuffleWriter(
              numPartitions,
              dep.nativePartitioning.getShortName,
              GlutenShuffleUtils.getStartPartitionId(
                dep.nativePartitioning,
                taskContext.partitionId),
              conf.get(SHUFFLE_DISK_WRITE_BUFFER_SIZE).toInt,
              conf.get(SHUFFLE_SORT_INIT_BUFFER_SIZE).toInt,
              conf.get(SHUFFLE_SORT_USE_RADIXSORT),
              partitionWriterHandle
            )
          } else if (dep.shuffleWriterType == GpuHashShuffleWriterType) {
            shuffleWriterJniWrapper.createGpuHashShuffleWriter(
              numPartitions,
              dep.nativePartitioning.getShortName,
              GlutenShuffleUtils.getStartPartitionId(
                dep.nativePartitioning,
                taskContext.partitionId),
              nativeBufferSize,
              reallocThreshold,
              partitionWriterHandle
            )
          } else {
            shuffleWriterJniWrapper.createHashShuffleWriter(
              numPartitions,
              dep.nativePartitioning.getShortName,
              GlutenShuffleUtils.getStartPartitionId(
                dep.nativePartitioning,
                taskContext.partitionId),
              nativeBufferSize,
              reallocThreshold,
              partitionWriterHandle
            )
          }

          runtime
            .memoryManager()
            .addSpiller(new Spiller() {
              override def spill(self: MemoryTarget, phase: Spiller.Phase, size: Long): Long =
                phase match {
                  case Spiller.Phase.SPILL =>
                    logInfo(s"Gluten shuffle writer: Trying to spill $size bytes of data")
                    val spilled = shuffleWriterJniWrapper.reclaim(nativeShuffleWriter, size)
                    logInfo(s"Gluten shuffle writer: Spilled $spilled / $size bytes of data")
                    spilled
                  case _ => 0L
                }
            })
        }

        val rows = cb.numRows()
        val columnarBatchHandle =
          ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, cb)
        val startTime = System.nanoTime()
        val bytesWritten = shuffleWriterJniWrapper.write(
          nativeShuffleWriter,
          rows,
          columnarBatchHandle,
          availableOffHeapPerTask())
        dep.metrics("shuffleWallTime").add(System.nanoTime() - startTime)
        dep.metrics("numInputRows").add(rows)
        dep.metrics("inputBatches").add(1)
        writeMetrics.incBytesWritten(bytesWritten)
        // This metric is important, AQE use it to decide if EliminateLimit
        writeMetrics.incRecordsWritten(rows)
      }
      cb.close()
    }

    if (nativeShuffleWriter == -1L) {
      handleEmptyInput()
      return
    }

    val startTime = System.nanoTime()
    assert(nativeShuffleWriter != -1L)
    splitResult = shuffleWriterJniWrapper.stop(nativeShuffleWriter)
    closeShuffleWriter()
    dep.metrics("shuffleWallTime").add(System.nanoTime() - startTime)
    if (!isSort) {
      dep
        .metrics("splitTime")
        .add(
          dep.metrics("shuffleWallTime").value - splitResult.getTotalSpillTime -
            splitResult.getTotalWriteTime -
            splitResult.getTotalCompressTime)
      dep.metrics("avgDictionaryFields").set(splitResult.getAvgDictionaryFields)
      dep.metrics("dictionarySize").add(splitResult.getDictionarySize)
    } else {
      dep.metrics("sortTime").add(splitResult.getSortTime)
      dep.metrics("c2rTime").add(splitResult.getC2RTime)
    }
    dep.metrics("spillTime").add(splitResult.getTotalSpillTime)
    dep.metrics("bytesSpilled").add(splitResult.getTotalBytesSpilled)
    dep.metrics("dataSize").add(splitResult.getRawPartitionLengths.sum)
    dep.metrics("compressTime").add(splitResult.getTotalCompressTime)
    dep.metrics("peakBytes").add(splitResult.getPeakBytes)
    writeMetrics.incBytesWritten(splitResult.getBytesWritten)
    writeMetrics.incWriteTime(splitResult.getTotalWriteTime + splitResult.getTotalSpillTime)
    taskContext.taskMetrics().incMemoryBytesSpilled(splitResult.getBytesToEvict)
    taskContext.taskMetrics().incDiskBytesSpilled(splitResult.getTotalBytesSpilled)

    partitionLengths = splitResult.getPartitionLengths
    try {
      shuffleBlockResolver.writeMetadataFileAndCommit(
        dep.shuffleId,
        mapId,
        partitionLengths,
        Array[Long](),
        tempDataFile)
    } finally {
      if (tempDataFile.exists() && !tempDataFile.delete()) {
        logError(s"Error while deleting temp file ${tempDataFile.getAbsolutePath}")
      }
    }

    // The partitionLength is much more than vanilla spark partitionLengths,
    // almost 3 times than vanilla spark partitionLengths
    // This value is sensitive in rules such as AQE rule OptimizeSkewedJoin DynamicJoinSelection
    // May affect the final plan
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
  }

  private def handleEmptyInput(): Unit = {
    partitionLengths = new Array[Long](dep.partitioner.numPartitions)
    shuffleBlockResolver.writeMetadataFileAndCommit(
      dep.shuffleId,
      mapId,
      partitionLengths,
      Array[Long](),
      null)
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
  }

  @throws[IOException]
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    internalWrite(records)
  }

  private def closeShuffleWriter(): Unit = {
    if (nativeShuffleWriter == -1L) {
      return
    }
    shuffleWriterJniWrapper.close(nativeShuffleWriter)
    nativeShuffleWriter = -1L
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        Option(mapStatus)
      } else {
        None
      }
    } finally {
      closeShuffleWriter()
    }
  }

  def getPartitionLengths: Array[Long] = partitionLengths

}

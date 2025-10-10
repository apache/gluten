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
import org.apache.gluten.backendsapi.bolt.WholeStageIteratorWrapper
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.config.{BoltConfig, GlutenConfig, HashShuffleWriterType, SortShuffleWriterType}
import org.apache.gluten.memory.memtarget.{MemoryTarget, Spiller}
import org.apache.gluten.proto.{ShuffleWriterInfo, ShuffleWriterResult}
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.shuffle.{BoltShuffleWriterJniWrapper, BoltSplitResult}

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.SHUFFLE_COMPRESS
import org.apache.spark.memory.SparkMemoryUtil
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{SparkDirectoryUtil, SparkResourceUtil, Utils}

import java.io.IOException

import scala.collection.JavaConverters._

class ColumnarShuffleWriter[K, V](
    shuffleBlockResolver: IndexShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, V],
    mapId: Long,
    writeMetrics: ShuffleWriteMetricsReporter)
  extends ShuffleWriter[K, V]
  with Logging {

  private val dep = handle.dependency.asInstanceOf[ColumnarShuffleDependency[K, V, V]]

  dep.shuffleWriterType match {
    case HashShuffleWriterType | SortShuffleWriterType =>
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

  private val nativeMergeBufferSize = BoltConfig.get.maxBatchSize

  private val nativeMergeThreshold = BoltConfig.get.columnarShuffleMergeThreshold

  private val compressionCodecBackend =
    BoltConfig.get.columnarShuffleCodecBackend.orNull

  private val bufferCompressThreshold =
    BoltConfig.get.columnarShuffleCompressionThreshold

  private val forceShuffleWriterType =
    BoltConfig.get.forceShuffleWriterType

  private val useV2PreAllocSizeThreshold =
    BoltConfig.get.useV2PreallocSizeThreshold

  private val rowVectorModeCompressionMinColumns =
    BoltConfig.get.rowVectorModeCompressionMinColumns

  private val rowVectorModeCompressionMaxBufferSize =
    BoltConfig.get.rowvectorModeCompressionMaxBufferSize

  private val accumulateBatchMaxColumns =
    BoltConfig.get.accumulateBatchMaxColumns

  private val accumulateBatchMaxBatches =
    BoltConfig.get.accumulateBatchMaxBatches

  private val recommendedColumn2RowSize =
    BoltConfig.get.recommendedColumn2RowSize

  private val shuffleCheckRatio =
    BoltConfig.get.shuffleCheckRatio

  private val enableVectorCombination =
    BoltConfig.get.enableVectorCombination

  private val reAllocThreshold = GlutenConfig.get.columnarShuffleReallocThreshold

  private val runtime = Runtimes.contextInstance(BackendsApiManager.getBackendName, "ShuffleWriter")

  private val shuffleWriterJniWrapper = BoltShuffleWriterJniWrapper.create(runtime)

  private var nativeShuffleWriter: Long = -1L

  private var splitResult: BoltSplitResult = _

  private var partitionLengths: Array[Long] = _

  private val taskContext: TaskContext = TaskContext.get()

  private def availableOffHeapPerTask(): Long = {
    val perTask =
      SparkMemoryUtil.getCurrentAvailableOffHeapMemory / SparkResourceUtil.getTaskSlots(conf)
    perTask
  }

  val dataFile = Utils.tempFileWith(shuffleBlockResolver.getDataFile(dep.shuffleId, mapId))

  private def getShuffleWriterInfo(): ShuffleWriterInfo = {
    val builder = ShuffleWriterInfo.newBuilder()
    builder.setPartitioningName(dep.nativePartitioning.getShortName)
    builder.setNumPartitions(dep.nativePartitioning.getNumPartitions)
    builder.setStartPartitionId(
      GlutenShuffleUtils.getStartPartitionId(dep.nativePartitioning, taskContext.partitionId))
    builder.setTaskAttemptId(taskContext.taskAttemptId())
    builder.setBufferSize(nativeBufferSize)
    builder.setMergeBufferSize(nativeMergeBufferSize)
    builder.setMergeThreshold(nativeMergeThreshold)
    builder.setCompressionCodec(compressionCodec.orNull)
    builder.setCompressionBackend(Option(compressionCodecBackend).getOrElse("none"))
    builder.setCompressionLevel(compressionLevel)
    builder.setCompressionThreshold(bufferCompressThreshold)
    builder.setCompressionMode(BoltConfig.get.columnarShuffleCompressionMode)
    builder.setDataFile(dataFile.getAbsolutePath)
    builder.setNumSubDirs(blockManager.subDirsPerLocalDir)
    builder.setLocalDirs(localDirs)
    builder.setReallocThreshold(reAllocThreshold)
    builder.setMemLimit(availableOffHeapPerTask())
    builder.setPushBufferMaxSize(0)
    builder.setWriterType("local")
    builder.setForcedWriterType(forceShuffleWriterType)
    builder.setUseV2PreallocThreshold(useV2PreAllocSizeThreshold)
    builder.setRowCompressionMinCols(rowVectorModeCompressionMinColumns)
    builder.setRowCompressionMaxBuffer(rowVectorModeCompressionMaxBufferSize)
    builder.setAccumulateBatchMaxColumns(accumulateBatchMaxColumns)
    builder.setAccumulateBatchMaxBatches(accumulateBatchMaxBatches)
    builder.setRecommendedC2RSize(recommendedColumn2RowSize)
    builder.setShuffleCheckRatio(shuffleCheckRatio)

    builder.build()
  }

  @throws[IOException]
  private def combinedWrite(
      wholeStageIteratorWrapper: WholeStageIteratorWrapper[Product2[K, V]]): Unit = {
    val writerInfo = getShuffleWriterInfo()
    val iterHandle = wholeStageIteratorWrapper.getInner.itrHandle()
    shuffleWriterJniWrapper.addShuffleWriter(iterHandle, writerInfo.toByteArray, null)
    if (wholeStageIteratorWrapper.hasNext) {
      wholeStageIteratorWrapper.next()
      assert(wholeStageIteratorWrapper.hasNext)
    }
    val result =
      ShuffleWriterResult.parseFrom(shuffleWriterJniWrapper.getShuffleWriterResult)
    val metrics = result.getMetrics
    writeMetrics.incRecordsWritten(metrics.getInputRowNumber)
    dep.metrics("numInputRows").add(metrics.getInputRowNumber)
    /*
    if (reportAllWebUIMetrics) {
      dep.metrics("inputBatches").add(metrics.getInputBatches)
      dep
        .metrics("splitTime")
        .add(metrics.getSplitTime)
      dep.metrics("spillTime").add(metrics.getSpillTime)
      dep.metrics("bytesSpilled").add(metrics.getSpillBytes)
      dep.metrics("splitBufferSize").add(metrics.getSplitBufferSize)
      dep.metrics("preallocSize").add(metrics.getPreallocSize)
      dep.metrics("rowVectorModeCompress").add(metrics.getRowVectorModeCompress)
      dep.metrics("combinedVectorNumber").set(metrics.getCombinedVectorNumber.toDouble)
      dep.metrics("combineVectorTimes").set(metrics.getCombinedVectorTimes.toDouble)
      dep.metrics("combineVectorCost").add(metrics.getCombineVectorCost)
      dep.metrics("computePidTime").add(metrics.getComputePidTime)
    }
     */
    dep.metrics("compressTime").add(metrics.getCompressTime)
    dep.metrics("useV2").add(metrics.getUseV2)
    dep.metrics("convertTime").add(metrics.getConvertTime)
    dep.metrics("flattenTime").add(metrics.getFlattenTime)
    dep.metrics("dataSize").add(metrics.getDataSize)
    dep.metrics("useRowBased").add(metrics.getUseRowBased)
    partitionLengths = result.getPartitionLengthsList.asScala.toArray.map(l => l.toLong)
    try {
      shuffleBlockResolver.writeMetadataFileAndCommit(
        dep.shuffleId,
        mapId,
        partitionLengths,
        Array[Long](),
        dataFile)
    } finally {
      if (dataFile.exists() && !dataFile.delete()) {
        logError(s"Error while deleting temp file ${dataFile.getAbsolutePath}")
      }
    }
    // The partitionLength is much more than vanilla spark partitionLengths,
    // almost 3 times than vanilla spark partitionLengths
    // This value is sensitive in rules such as AQE rule OptimizeSkewedJoin DynamicJoinSelection
    // May affect the final plan
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
  }
  @throws[IOException]
  private def internalWrite(records: Iterator[Product2[K, V]]): Unit = {
    records match {
      case wrapper: WholeStageIteratorWrapper[Product2[K, V]] =>
        // offload writer into WholeStageIterator and run as a Bolt operator
        combinedWrite(wrapper)
        return
      case _ => ()
    }

    if (!records.hasNext) {
      handleEmptyInput()
      return
    }

    while (records.hasNext) {
      val cb = records.next()._2.asInstanceOf[ColumnarBatch]
      if (cb.numRows == 0 || cb.numCols == 0) {
        logInfo(s"Skip ColumnarBatch of ${cb.numRows} rows, ${cb.numCols} cols")
      } else {
        val rows = cb.numRows()
        val handle = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, cb)
        if (nativeShuffleWriter == -1L) {
          nativeShuffleWriter = shuffleWriterJniWrapper.createShuffleWriter(
            getShuffleWriterInfo().toByteArray,
            handle,
            null
          )
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
        val startTime = System.nanoTime()
        shuffleWriterJniWrapper.write(nativeShuffleWriter, rows, handle, availableOffHeapPerTask())
        val splitTime = System.nanoTime() - startTime
        dep.metrics("numInputRows").add(rows)
        dep.metrics("splitTime").add(splitTime)
        dep.metrics("inputBatches").add(1)
        // This metric is important, AQE use it to decide if EliminateLimit
        writeMetrics.incRecordsWritten(rows)
      }
      cb.close()
    }

    val startTime = System.nanoTime()
    assert(nativeShuffleWriter != -1L)
    splitResult = shuffleWriterJniWrapper.stop(nativeShuffleWriter)
    closeShuffleWriter()
    /* TODO report more metrics
    dep
      .metrics("splitTime")
      .add(
        System.nanoTime() - startTime - splitResult.getTotalSpillTime -
          splitResult.getTotalWriteTime -
          splitResult.getTotalCompressTime -
          splitResult.getConvertTime -
          splitResult.getFlattenTime -
          splitResult.getComputePidTime)
    dep.metrics("spillTime").add(splitResult.getTotalSpillTime)
    dep.metrics("bytesSpilled").add(splitResult.getTotalBytesSpilled)
    dep.metrics("splitBufferSize").add(splitResult.getSplitBufferSize)
    dep.metrics("preallocSize").add(splitResult.getPreAllocSize)
    dep.metrics("rowVectorModeCompress").add(splitResult.rowVectorModeCompress)
    dep.metrics("combinedVectorNumber").set(splitResult.combinedVectorNumber.toDouble)
    dep.metrics("combineVectorTimes").set(splitResult.combineVectorTimes.toDouble)
    dep.metrics("combineVectorCost").add(splitResult.combineVectorCost)
    dep.metrics("computePidTime").add(splitResult.getComputePidTime)
    dep.metrics("compressTime").add(splitResult.getTotalCompressTime)
     */
    dep.metrics("useV2").add(splitResult.getUseV2Count)
    dep.metrics("convertTime").add(splitResult.getConvertTime)
    dep.metrics("flattenTime").add(splitResult.getFlattenTime)
    dep.metrics("dataSize").add(splitResult.getRawPartitionLengths.sum)
    dep.metrics("useRowBased").add(splitResult.getUseRowBased)
    writeMetrics.incBytesWritten(splitResult.getTotalBytesWritten)
    writeMetrics.incWriteTime(splitResult.getTotalWriteTime)
    // TODO report memory bytes spilled
    taskContext.taskMetrics().incMemoryBytesSpilled(0)
    taskContext.taskMetrics().incDiskBytesSpilled(splitResult.getTotalBytesSpilled)
    partitionLengths = splitResult.getPartitionLengths
    try {
      shuffleBlockResolver.writeMetadataFileAndCommit(
        dep.shuffleId,
        mapId,
        partitionLengths,
        Array[Long](),
        dataFile)
    } finally {
      if (dataFile.exists() && !dataFile.delete()) {
        logError(s"Error while deleting temp file ${dataFile.getAbsolutePath}")
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

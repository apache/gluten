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
import org.apache.gluten.config.BoltConfig
import org.apache.gluten.memory.memtarget.{MemoryTarget, Spiller}
import org.apache.gluten.proto.{ShuffleWriterInfo, ShuffleWriterResult}
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.shuffle.{BoltShuffleWriterJniWrapper, BoltSplitResult}

import org.apache.spark._
import org.apache.spark.memory.SparkMemoryUtil
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.celeborn.CelebornShuffleHandle
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SparkResourceUtil

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf

import java.io.IOException

import scala.collection.JavaConverters._

class BoltCelebornColumnarShuffleWriter[K, V](
    shuffleId: Int,
    handle: CelebornShuffleHandle[K, V, V],
    context: TaskContext,
    celebornConf: CelebornConf,
    client: ShuffleClient,
    writeMetrics: ShuffleWriteMetricsReporter)
  extends CelebornColumnarShuffleWriter[K, V](
    shuffleId,
    handle,
    context,
    celebornConf,
    client,
    writeMetrics) {
  private val runtime =
    Runtimes.contextInstance(BackendsApiManager.getBackendName, "CelebornShuffleWriter")

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

  private val shuffleWriterJniWrapper = BoltShuffleWriterJniWrapper.create(runtime)

  private var splitResult: BoltSplitResult = _

  private def availableOffHeapPerTask(): Long = {
    SparkMemoryUtil.getCurrentAvailableOffHeapMemory / SparkResourceUtil.getTaskSlots(conf)
  }

  private def getShuffleWriterInfo(): ShuffleWriterInfo = {
    val builder = ShuffleWriterInfo.newBuilder()
    builder.setPartitioningName(dep.nativePartitioning.getShortName)
    builder.setNumPartitions(dep.nativePartitioning.getNumPartitions)
    builder.setStartPartitionId(
      GlutenShuffleUtils.getStartPartitionId(dep.nativePartitioning, context.partitionId))
    builder.setTaskAttemptId(context.taskAttemptId())
    builder.setBufferSize(nativeBufferSize)
    builder.setMergeBufferSize(0)
    builder.setMergeThreshold(0)
    builder.setCompressionCodec(compressionCodec.orNull)
    builder.setCompressionBackend("none")
    builder.setCompressionLevel(compressionLevel)
    builder.setCompressionThreshold(BoltConfig.get.columnarShuffleCompressionThreshold)
    builder.setCompressionMode(BoltConfig.get.columnarShuffleCompressionMode)

    builder.setDataFile("")
    builder.setNumSubDirs(0)
    builder.setLocalDirs("")
    builder.setReallocThreshold(BoltConfig.get.columnarShuffleReallocThreshold)
    builder.setMemLimit(availableOffHeapPerTask())
    builder.setPushBufferMaxSize(clientPushBufferMaxSize)
    builder.setShuffleBatchByteSize(BoltConfig.get.maxShuffleBatchByteSize)
    builder.setWriterType("celeborn")
    builder.setForcedWriterType(forceShuffleWriterType)
    builder.setUseV2PreallocThreshold(useV2PreAllocSizeThreshold)
    builder.setRowCompressionMinCols(rowVectorModeCompressionMinColumns)
    builder.setRowCompressionMaxBuffer(rowVectorModeCompressionMaxBufferSize)
    builder.setEnableVectorCombination(enableVectorCombination)
    builder.setAccumulateBatchMaxColumns(accumulateBatchMaxColumns)
    builder.setAccumulateBatchMaxBatches(accumulateBatchMaxBatches)
    builder.setRecommendedC2RSize(recommendedColumn2RowSize)
    builder.setShuffleCheckRatio(shuffleCheckRatio)

    builder.build()
  }

  @throws[IOException]
  def combinedWrite(wholeStageIteratorWrapper: WholeStageIteratorWrapper[Product2[K, V]]): Unit = {
    val itrHandle = wholeStageIteratorWrapper.inner.itrHandle()
    shuffleWriterJniWrapper.addShuffleWriter(
      itrHandle,
      getShuffleWriterInfo().toByteArray,
      celebornPartitionPusher)
    if (wholeStageIteratorWrapper.hasNext) {
      wholeStageIteratorWrapper.next()
      assert(wholeStageIteratorWrapper.hasNext)
    }
    val result =
      ShuffleWriterResult.parseFrom(shuffleWriterJniWrapper.getShuffleWriterResult)
    val metrics = result.getMetrics
    if (metrics.getInputRowNumber == 0) {
      handleEmptyIterator()
      return
    }
    writeMetrics.incRecordsWritten(metrics.getInputRowNumber)
    writeMetrics.incWriteTime(metrics.getTotalWriteTime + metrics.getTotalPushTime)
    dep.metrics("numInputRows").add(metrics.getInputRowNumber)
    dep.metrics("dataSize").add(metrics.getDataSize)
    dep.metrics("compressTime").add(metrics.getCompressTime)
    dep.metrics("rssWriteTime").add(metrics.getTotalWriteTime)
    dep.metrics("rssPushTime").add(metrics.getTotalPushTime)
    partitionLengths = result.getPartitionLengthsList.asScala.toArray.map(l => l.toLong)
    val startNs = System.nanoTime
    pushMergedDataToCeleborn()
    dep.metrics("rssCloseWaitTime").add(System.nanoTime() - startNs)
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
  }

  @throws[IOException]
  override def internalWrite(records: Iterator[Product2[K, V]]): Unit = {
    records match {
      case wrapper: WholeStageIteratorWrapper[Product2[K, V]] =>
        // offload writer into WholeStageIterator and run as a Bolt operator
        combinedWrite(wrapper)
        return
      case _ => ()
    }
    if (!records.hasNext) {
      handleEmptyIterator()
      return
    }
    while (records.hasNext) {
      val cb = records.next()._2.asInstanceOf[ColumnarBatch]
      if (cb.numRows == 0 || cb.numCols == 0) {
        logInfo(s"Skip ColumnarBatch of ${cb.numRows} rows, ${cb.numCols} cols")
      } else {
        val columnarBatchHandle =
          ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, cb)
        if (nativeShuffleWriter == -1L) {
          createShuffleWriter(columnarBatchHandle)
        }
        val startTime = System.nanoTime()
        shuffleWriterJniWrapper.write(
          nativeShuffleWriter,
          cb.numRows,
          columnarBatchHandle,
          availableOffHeapPerTask())
        dep.metrics("shuffleWallTime").add(System.nanoTime() - startTime)
        dep.metrics("numInputRows").add(cb.numRows)
        dep.metrics("inputBatches").add(1)
        // This metric is important, AQE use it to decide if EliminateLimit
        writeMetrics.incRecordsWritten(cb.numRows())
      }
    }

    // If all of the ColumnarBatch have empty rows, the nativeShuffleWriter still equals -1
    if (nativeShuffleWriter == -1L) {
      handleEmptyIterator()
      return
    }

    val startTime = System.nanoTime()
    splitResult = shuffleWriterJniWrapper.stop(nativeShuffleWriter)

    dep.metrics("shuffleWallTime").add(System.nanoTime() - startTime)
    dep
      .metrics("splitTime")
      .add(
        dep.metrics("shuffleWallTime").value - splitResult.getTotalPushTime -
          splitResult.getTotalWriteTime -
          splitResult.getTotalCompressTime)
    dep.metrics("dataSize").add(splitResult.getRawPartitionLengths.sum)
    writeMetrics.incBytesWritten(splitResult.getTotalBytesWritten)
    writeMetrics.incWriteTime(splitResult.getTotalWriteTime + splitResult.getTotalPushTime)

    partitionLengths = splitResult.getPartitionLengths

    pushMergedDataToCeleborn()
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)

  }

  def createShuffleWriter(columnarBatchHandler: Long): Unit = {
    nativeShuffleWriter = shuffleWriterJniWrapper.createShuffleWriter(
      getShuffleWriterInfo().toByteArray,
      columnarBatchHandler,
      celebornPartitionPusher
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

  override def closeShuffleWriter(): Unit = {
    shuffleWriterJniWrapper.close(nativeShuffleWriter)
  }
}

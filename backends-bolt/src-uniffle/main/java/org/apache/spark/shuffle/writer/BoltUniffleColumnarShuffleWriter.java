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
package org.apache.spark.shuffle.writer;

import org.apache.gluten.backendsapi.BackendsApiManager;
import org.apache.gluten.columnarbatch.ColumnarBatches;
import org.apache.gluten.config.GlutenConfig;
import org.apache.gluten.config.SortShuffleWriterType$;
import org.apache.gluten.memory.memtarget.MemoryTarget;
import org.apache.gluten.memory.memtarget.Spiller;
import org.apache.gluten.runtime.Runtime;
import org.apache.gluten.runtime.Runtimes;
import org.apache.gluten.vectorized.GlutenSplitResult;
import org.apache.gluten.vectorized.ShuffleWriterJniWrapper;
import org.apache.gluten.vectorized.UnifflePartitionWriterJniWrapper;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.internal.config.package$;
import org.apache.spark.memory.SparkMemoryUtil;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.shuffle.ColumnarShuffleDependency;
import org.apache.spark.shuffle.GlutenShuffleUtils;
import org.apache.spark.shuffle.RssShuffleHandle;
import org.apache.spark.shuffle.RssShuffleManager;
import org.apache.spark.shuffle.RssSparkConfig;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.util.SparkResourceUtil;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.exception.RssException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import scala.Option;
import scala.Product2;
import scala.collection.Iterator;

public class BoltUniffleColumnarShuffleWriter<K, V> extends RssShuffleWriter<K, V, V> {

  private static final Logger LOG = LoggerFactory.getLogger(BoltUniffleColumnarShuffleWriter.class);

  private long nativeShuffleWriter = -1L;

  private boolean stopping = false;
  private final double reallocThreshold = GlutenConfig.get().columnarShuffleReallocThreshold();
  private String compressionCodec;
  private String codecBackend;
  private int compressionLevel;
  private int compressionBufferSize;
  private final int diskWriteBufferSize;
  private final int partitionId;

  private final Runtime runtime =
      Runtimes.contextInstance(BackendsApiManager.getBackendName(), "UniffleShuffleWriter");
  private final UnifflePartitionWriterJniWrapper partitionWriterJniWrapper =
      UnifflePartitionWriterJniWrapper.create(runtime);
  private final ShuffleWriterJniWrapper shuffleWriterJniWrapper =
      ShuffleWriterJniWrapper.create(runtime);
  private final int nativeBufferSize = GlutenConfig.get().maxBatchSize();
  private final int bufferSize;
  private final int numPartitions;

  private final ColumnarShuffleDependency<K, V, V> columnarDep;
  private final SparkConf sparkConf;

  private long availableOffHeapPerTask() {
    return SparkMemoryUtil.getCurrentAvailableOffHeapMemory()
        / SparkResourceUtil.getTaskSlots(sparkConf);
  }

  public BoltUniffleColumnarShuffleWriter(
      int partitionId,
      String appId,
      int shuffleId,
      String taskId,
      long taskAttemptId,
      ShuffleWriteMetrics shuffleWriteMetrics,
      RssShuffleManager shuffleManager,
      SparkConf sparkConf,
      ShuffleWriteClient shuffleWriteClient,
      RssShuffleHandle<K, V, V> rssHandle,
      Function<String, Boolean> taskFailureCallback,
      TaskContext context) {
    super(
        appId,
        shuffleId,
        taskId,
        taskAttemptId,
        shuffleWriteMetrics,
        shuffleManager,
        sparkConf,
        shuffleWriteClient,
        rssHandle,
        taskFailureCallback,
        context);
    columnarDep = (ColumnarShuffleDependency<K, V, V>) rssHandle.getDependency();
    this.partitionId = partitionId;
    this.sparkConf = sparkConf;
    this.numPartitions = columnarDep.nativePartitioning().getNumPartitions();
    bufferSize =
        (int)
            sparkConf.getSizeAsBytes(
                RssSparkConfig.RSS_WRITER_BUFFER_SIZE.key(),
                RssSparkConfig.RSS_WRITER_BUFFER_SIZE.defaultValue().get());
    this.diskWriteBufferSize =
        (int) (long) sparkConf.get(package$.MODULE$.SHUFFLE_DISK_WRITE_BUFFER_SIZE());
    if ((boolean) sparkConf.get(package$.MODULE$.SHUFFLE_COMPRESS())) {
      compressionCodec = GlutenShuffleUtils.getCompressionCodec(sparkConf);
      compressionLevel = GlutenShuffleUtils.getCompressionLevel(sparkConf, compressionCodec);
      compressionBufferSize =
          GlutenShuffleUtils.getCompressionBufferSize(sparkConf, compressionCodec);
      Option<String> codecBackend = GlutenConfig.get().columnarShuffleCodecBackend();
      if (codecBackend.isDefined()) {
        this.codecBackend = codecBackend.get();
      }
    }
  }

  @Override
  protected void writeImpl(Iterator<Product2<K, V>> records) {
    if (!records.hasNext()) {
      sendCommit();
      return;
    }
    // writer already init
    PartitionPusher partitionPusher = new PartitionPusher(this);
    while (records.hasNext()) {
      ColumnarBatch cb = (ColumnarBatch) (records.next()._2());
      if (cb.numRows() == 0 || cb.numCols() == 0) {
        LOG.info("Skip ColumnarBatch of 0 rows or 0 cols");
      } else {
        if (nativeShuffleWriter == -1) {
          long partitionWriterHandle =
              partitionWriterJniWrapper.createPartitionWriter(
                  numPartitions,
                  compressionCodec,
                  codecBackend,
                  compressionLevel,
                  compressionBufferSize,
                  bufferSize,
                  bufferSize,
                  partitionPusher);

          if (columnarDep.shuffleWriterType().equals(SortShuffleWriterType$.MODULE$)) {
            nativeShuffleWriter =
                shuffleWriterJniWrapper.createSortShuffleWriter(
                    numPartitions,
                    columnarDep.nativePartitioning().getShortName(),
                    GlutenShuffleUtils.getStartPartitionId(
                        columnarDep.nativePartitioning(), partitionId),
                    diskWriteBufferSize,
                    (int) (long) sparkConf.get(package$.MODULE$.SHUFFLE_SORT_INIT_BUFFER_SIZE()),
                    (boolean) sparkConf.get(package$.MODULE$.SHUFFLE_SORT_USE_RADIXSORT()),
                    partitionWriterHandle);
          } else {
            nativeShuffleWriter =
                shuffleWriterJniWrapper.createHashShuffleWriter(
                    numPartitions,
                    columnarDep.nativePartitioning().getShortName(),
                    GlutenShuffleUtils.getStartPartitionId(
                        columnarDep.nativePartitioning(), partitionId),
                    nativeBufferSize,
                    reallocThreshold,
                    partitionWriterHandle);
          }

          runtime
              .memoryManager()
              .addSpiller(
                  new Spiller() {
                    @Override
                    public long spill(MemoryTarget self, Spiller.Phase phase, long size) {
                      if (!Spiller.Phase.SPILL.equals(phase)) {
                        return 0L;
                      }
                      LOG.info("Gluten shuffle writer: Trying to push {} bytes of data", size);
                      long pushed = shuffleWriterJniWrapper.reclaim(nativeShuffleWriter, size);
                      LOG.info("Gluten shuffle writer: Pushed {} / {} bytes of data", pushed, size);
                      return pushed;
                    }
                  });
        }
        long startTime = System.nanoTime();
        long columnarBatchHandle =
            ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName(), cb);
        long bytes =
            shuffleWriterJniWrapper.write(
                nativeShuffleWriter, cb.numRows(), columnarBatchHandle, availableOffHeapPerTask());
        LOG.debug("jniWrapper.write rows {}, split bytes {}", cb.numRows(), bytes);
        columnarDep.metrics().get("dataSize").get().add(bytes);
        // this metric replace part of uniffle shuffle write time
        columnarDep.metrics().get("shuffleWallTime").get().add(System.nanoTime() - startTime);
        columnarDep.metrics().get("numInputRows").get().add(cb.numRows());
        columnarDep.metrics().get("inputBatches").get().add(1);
        shuffleWriteMetrics.incRecordsWritten(cb.numRows());
      }
    }

    LOG.info("nativeShuffleWriter value {}", nativeShuffleWriter);
    // If all of the ColumnarBatch have empty rows, the nativeShuffleWriter still equals -1
    if (nativeShuffleWriter == -1L) {
      sendCommit();
      return;
    }
    long startTime = System.nanoTime();
    GlutenSplitResult splitResult;
    try {
      splitResult = shuffleWriterJniWrapper.stop(nativeShuffleWriter);
    } catch (IOException e) {
      throw new RssException(e);
    }
    columnarDep.metrics().get("shuffleWallTime").get().add(System.nanoTime() - startTime);
    columnarDep
        .metrics()
        .get("splitTime")
        .get()
        .add(
            columnarDep.metrics().get("shuffleWallTime").get().value()
                - splitResult.getTotalPushTime()
                - splitResult.getTotalWriteTime()
                - splitResult.getTotalCompressTime());

    // bytesWritten is calculated in uniffle side: WriteBufferManager.createShuffleBlock
    // shuffleWriteMetrics.incBytesWritten(splitResult.getTotalBytesWritten());
    shuffleWriteMetrics.incWriteTime(
        splitResult.getTotalWriteTime()
            + splitResult.getTotalPushTime()
            + splitResult.getTotalCompressTime());
    // partitionLengths is calculate in uniffle side

    long pushMergedDataTime = System.nanoTime();
    // clear all
    sendRestBlockAndWait();
    sendCommit();
    long writeDurationNanos = System.nanoTime() - pushMergedDataTime;
    shuffleWriteMetrics.incWriteTime(writeDurationNanos);
    LOG.info(
        "Finish write shuffle with rest write {} ms",
        TimeUnit.NANOSECONDS.toMillis(writeDurationNanos));
  }

  @Override
  protected void sendCommit() {
    if (!isMemoryShuffleEnabled) {
      super.sendCommit();
    }
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    if (!stopping) {
      stopping = true;
      closeShuffleWriter();
      return super.stop(success);
    }
    return Option.<MapStatus>empty();
  }

  private void closeShuffleWriter() {
    if (nativeShuffleWriter != -1) {
      shuffleWriterJniWrapper.close(nativeShuffleWriter);
      nativeShuffleWriter = -1;
    }
  }

  private void sendRestBlockAndWait() {
    List<ShuffleBlockInfo> shuffleBlockInfos = super.getBufferManager().clear();
    super.processShuffleBlockInfos(shuffleBlockInfos);
    // make checkBlockSendResult no arguments
    super.internalCheckBlockSendResult();
  }

  public int doAddByte(int partitionId, byte[] data, int length) {
    List<ShuffleBlockInfo> shuffleBlockInfos =
        super.getBufferManager()
            .addPartitionData(partitionId, data, length, System.currentTimeMillis());
    super.processShuffleBlockInfos(shuffleBlockInfos);
    return length;
  }
}

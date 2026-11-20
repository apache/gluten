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
package org.apache.spark.shuffle.writer

import org.apache.gluten.backendsapi.bolt.{BoltMetricsApi, BoltSparkPlanExecApi}
import org.apache.gluten.columnarbatch.BoltColumnarBatches
import org.apache.gluten.config.{BoltConfig, GlutenConfig, HashShuffleWriterType, ShuffleWriterType, SortShuffleWriterType}
import org.apache.gluten.vectorized.{ArrowColumnVectors, NativePartitioning}

import org.apache.spark.{HashPartitioner, SparkConf, SparkFunSuite, TaskContext}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.{ColumnarShuffleDependency, RssShuffleHandle, RssShuffleManager}
import org.apache.spark.shuffle.handle.ShuffleHandleInfo
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.task.TaskResources

import org.apache.uniffle.client.api.ShuffleWriteClient
import org.apache.uniffle.common.ShuffleBlockInfo
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyInt, anyLong}
import org.mockito.Mockito.{mock, when}

import java.util.Collections

class BoltUniffleShuffleWriterSuite extends SparkFunSuite with SharedSparkSession {
  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
    .set("spark.memory.offHeap.enabled", "true")
    .set("spark.memory.offHeap.size", "1g")
    .set("spark.gluten.sql.columnar.backend.lib", "bolt")

  private case class Sizes(raw: Long, pushed: Long, pushedBeforeStop: Long)

  private def writeBatches(
      compress: Boolean,
      bufferRows: Int,
      numPartitions: Int = 2,
      writerType: ShuffleWriterType = HashShuffleWriterType): Sizes = {
    var result = Option.empty[Sizes]
    withSQLConf(
      GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key -> bufferRows.toString,
      BoltConfig.FORCE_SHUFFLE_WRITER_TYPE.key -> "1",
      BoltConfig.COMBINED_VECTOR_ENABLED.key -> "false"
    ) {
      TaskResources.runUnsafe {
        val conf = spark.sparkContext.getConf.clone()
          .set("spark.shuffle.compress", compress.toString)
          .set("spark.io.compression.codec", "lz4")
          .set("spark.rss.storage.type", "MEMORY_LOCALFILE")
        val metrics = new BoltMetricsApi()
          .genColumnarShuffleExchangeMetrics(spark.sparkContext, writerType)
        val dep = mock(classOf[ColumnarShuffleDependency[Int, ColumnarBatch, ColumnarBatch]])
        when(dep.metrics).thenReturn(metrics)
        when(dep.shuffleWriterType).thenReturn(writerType)
        when(dep.partitioner).thenReturn(new HashPartitioner(numPartitions))
        when(dep.serializer).thenReturn(new JavaSerializer(conf))
        when(dep.nativePartitioning)
          .thenReturn(new NativePartitioning("hash", numPartitions, null, null))
        val handle = mock(classOf[RssShuffleHandle[Int, ColumnarBatch, ColumnarBatch]])
        when(handle.getDependency).thenReturn(dep)
        val handleInfo = mock(classOf[ShuffleHandleInfo])
        val manager = mock(classOf[RssShuffleManager])
        when(manager.getShuffleHandleInfo(anyInt(), anyInt(), any(), anyBoolean()))
          .thenReturn(handleInfo)
        val writeMetrics = new ShuffleWriteMetrics
        var committed = false
        val writer = new BoltUniffleColumnarShuffleWriter[Int, ColumnarBatch](
          0,
          "test",
          0,
          "test-task",
          TaskContext.get().taskAttemptId(),
          writeMetrics,
          manager,
          conf,
          mock(classOf[ShuffleWriteClient]),
          handle,
          (_: String) => java.lang.Boolean.FALSE,
          TaskContext.get()
        ) {
          override protected def sendCommit(): Unit = {
            super.sendCommit()
            committed = true
          }
        }

        // Keep the real JVM writer, JNI and native buffering/compression. Replace only
        // Uniffle's network buffer so no remote shuffle service is needed.
        var pushed = 0L
        val buffer = mock(classOf[WriteBufferManager])
        when(buffer.addPartitionData(anyInt(), any[Array[Byte]](), anyInt(), anyLong()))
          .thenAnswer(
            (invocation: org.mockito.invocation.InvocationOnMock) => {
              pushed += invocation.getArgument[Int](2)
              Collections.emptyList[ShuffleBlockInfo]()
            })
        val field = classOf[RssShuffleWriter[_, _, _]].getDeclaredField("bufferManager")
        field.setAccessible(true)
        field.set(writer, buffer)

        val schema = new StructType().add("pid", IntegerType).add("value", LongType)
        val batch = BoltColumnarBatches.ensureBoltBatch(
          ArrowColumnVectors.fromRows(
            schema,
            (0 until 128).iterator.map(row => InternalRow(row % numPartitions, 42L))))
        var pushedBeforeStop = -1L
        val records = new Iterator[Product2[Int, ColumnarBatch]] {
          private var remaining = 2
          override def hasNext: Boolean = {
            if (remaining == 0) pushedBeforeStop = pushed
            remaining > 0
          }
          override def next(): Product2[Int, ColumnarBatch] = {
            remaining -= 1
            (0, batch)
          }
        }
        try {
          writer.write(records)
          assert(committed)
          assert(writeMetrics.recordsWritten == 256)
          result = Some(Sizes(metrics("dataSize").value, pushed, pushedBeforeStop))
        } finally {
          writer.stop(false)
          batch.close()
        }
      }
    }
    result.get
  }

  test("Uniffle completes with a dependency above the sort partition threshold") {
    withSQLConf(
      GlutenConfig.COLUMNAR_SHUFFLE_SORT_PARTITIONS_THRESHOLD.key -> "4",
      "spark.shuffle.manager" -> "org.apache.spark.shuffle.gluten.uniffle.UniffleShuffleManager"
    ) {
      val attribute = AttributeReference("value", LongType)()
      val numPartitions = 5
      val writerType = new BoltSparkPlanExecApi().getShuffleWriterType(
        HashPartitioning(Seq(attribute), numPartitions),
        Seq(attribute))
      assert(writerType == SortShuffleWriterType)
      val sizes = writeBatches(
        compress = true,
        bufferRows = 4096,
        numPartitions = numPartitions,
        writerType = writerType)
      assert(sizes.raw == 256L * java.lang.Long.BYTES)
      assert(sizes.pushed > 0)
    }
  }

  test("Uniffle dataSize includes small batches flushed only by native stop") {
    val uncompressed = writeBatches(compress = false, bufferRows = 4096)
    val compressed = writeBatches(compress = true, bufferRows = 4096)
    info(s"uncompressed=$uncompressed, compressed=$compressed")
    Seq(uncompressed, compressed).foreach {
      sizes =>
        assert(sizes.pushedBeforeStop == 0)
        assert(sizes.pushed > 0)
        // 256 non-null longs, excluding the partition-id column.
        assert(sizes.raw == 256L * java.lang.Long.BYTES)
    }
    assert(compressed.pushed < uncompressed.pushed)
    assert(compressed.raw == uncompressed.raw)
  }

  test("Uniffle dataSize counts early pushes and the final flush exactly once") {
    val buffered = writeBatches(compress = true, bufferRows = 4096)
    val early = writeBatches(compress = true, bufferRows = 64)
    info(s"buffered=$buffered, early=$early")
    assert(early.pushedBeforeStop > 0)
    assert(early.pushed > early.pushedBeforeStop)
    assert(early.raw == buffered.raw)
    assert(early.raw == 256L * java.lang.Long.BYTES)
  }
}

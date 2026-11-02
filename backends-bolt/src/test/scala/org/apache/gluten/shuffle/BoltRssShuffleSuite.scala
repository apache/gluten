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
package org.apache.gluten.shuffle

import org.apache.gluten.columnarbatch.{BoltColumnarBatches, ColumnarBatches}
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.proto.{ShuffleReaderInfo, ShuffleWriterInfo}
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.vectorized.{ArrowColumnVectors, ColumnarBatchOutIterator, ShuffleStreamReader}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.task.TaskResources
import org.apache.spark.unsafe.types.UTF8String

import org.apache.arrow.c.ArrowSchema

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

/** The RSS callback belongs to the actual transport, not necessarily Celeborn. */
class BoltRssShuffleSuite extends SparkFunSuite with SharedSparkSession {
  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
    .set("spark.memory.offHeap.enabled", "true")
    .set("spark.memory.offHeap.size", "1g")
    .set("spark.gluten.sql.columnar.backend.lib", "bolt")

  class LocalPusher {
    val partitions: Array[ByteArrayOutputStream] = Array.fill(2)(new ByteArrayOutputStream())

    def pushPartitionData(partitionId: Int, bytes: Array[Byte], length: Int): Int = {
      partitions(partitionId).write(bytes, 0, length)
      length
    }
  }

  for (
    codec <- Seq("uncompressed", "lz4");
    readerCodec <- (if (codec == "uncompressed") Seq("", "uncompressed") else Seq(codec));
    batchMode <- Seq("bolt", "v2", "arrow")
  ) {
    val rowVectorMode = batchMode == "v2"
    val writerType = if (rowVectorMode) 2 else if (batchMode == "arrow") 0 else 1
    val mode = batchMode match {
      case "v2" => " below the rowvector compression threshold"
      case "arrow" => " from an Arrow batch with adaptive sizing"
      case _ => ""
    }
    val readerEncoding = if (readerCodec.isEmpty) "legacy empty" else readerCodec
    test(s"RSS native writer round trips $codec payloads$mode with $readerEncoding reader codec") {
      TaskResources.runUnsafe {
        val inputSchema = new StructType()
          .add("pid", IntegerType)
          .add("value", LongType)
          .add("text", StringType)
        val texts = Seq("ten", "twenty", null, "forty")
        val batch = ColumnarBatches.ensureOffloaded(
          ArrowBufferAllocators.contextInstance(),
          ArrowColumnVectors.fromRows(
            inputSchema,
            (0 until 4).iterator.map {
              row => InternalRow(row % 2, (row + 1) * 10L, UTF8String.fromString(texts(row)))
            })
        )
        if (batchMode != "arrow") {
          BoltColumnarBatches.ensureBoltBatch(batch)
        }
        val batchHandle = ColumnarBatches.getNativeHandle("bolt", batch)
        val runtime = Runtimes.contextInstance("bolt", "RSS roundtrip")
        val writer = BoltShuffleWriterJniWrapper.create(runtime)
        val pusher = new LocalPusher
        val writerInfo = ShuffleWriterInfo.newBuilder()
          .setPartitioningName("hash")
          .setNumPartitions(2)
          .setTaskAttemptId(org.apache.spark.TaskContext.get().taskAttemptId())
          .setBufferSize(2)
          .setReallocThreshold(0.25)
          .setMemLimit(64L << 20)
          .setCompressionCodec(codec)
          .setCompressionBackend("none")
          .setCompressionMode(if (rowVectorMode) "rowvector" else "buffer")
          .setCompressionThreshold(if (rowVectorMode) 1024 else 1)
          .setShuffleBatchByteSize(1 << 20)
          .setRowCompressionMinCols(if (rowVectorMode) 1 else 9)
          .setRowCompressionMaxBuffer(1 << 20)
          .setPushBufferMaxSize(1024)
          .setWriterType("celeborn")
          .setForcedWriterType(writerType)
          .setRowFormat("compact")
          .build()
        try {
          val writerHandle = writer.createShuffleWriter(writerInfo.toByteArray, batchHandle, pusher)
          try {
            writer.write(writerHandle, 4, batchHandle, 64L << 20)
            val result = writer.stop(writerHandle)
            assert(result.getTotalBytesWritten > 0)
            assert(result.getPartitionLengths.sum == pusher.partitions.map(_.size().toLong).sum)
          } finally {
            writer.close(writerHandle)
          }
        } finally {
          batch.close()
        }

        val outputSchema = new StructType().add("value", LongType).add("text", StringType)
        val allocator = ArrowBufferAllocators.contextInstance()
        val schema = ArrowSchema.allocateNew(allocator)
        val reader = BoltShuffleReaderJniWrapper.create(runtime)
        try {
          org.apache.gluten.utils.ArrowAbiUtil.exportSchema(
            allocator,
            SparkArrowUtil.toArrowSchema(outputSchema, "UTC"),
            schema)
          val readerInfo = ShuffleReaderInfo.newBuilder()
            .setBatchSize(4)
            .setNumPartitions(2)
            .setPartitionShortName("hash")
            .setForcedWriterType(writerType)
            .setCompressionType(readerCodec)
            .setCodec("none")
            .setRowFormat("compact")
            .build()
          val readerHandle = reader.make(schema.memoryAddress(), readerInfo.toByteArray)
          try {
            val expected = Seq(Seq((10L, "ten"), (30L, null)), Seq((20L, "twenty"), (40L, "forty")))
            pusher.partitions.zipWithIndex.foreach {
              case (bytes, pid) =>
                val streams = ShuffleStreamReader(Iterator(
                  (ShuffleBlockId(0, 0, pid), new ByteArrayInputStream(bytes.toByteArray))))
                val iterator =
                  new ColumnarBatchOutIterator(runtime, reader.read(readerHandle, streams))
                val actual = scala.collection.mutable.ArrayBuffer.empty[(Long, String)]
                try {
                  while (iterator.hasNext) {
                    val output = ColumnarBatches.load(allocator, iterator.next())
                    try {
                      (0 until output.numRows()).foreach {
                        row =>
                          actual += ((
                            output.column(0).getLong(row),
                            if (output.column(1).isNullAt(row)) null
                            else output.column(1).getUTF8String(row).toString))
                      }
                    } finally {
                      output.close()
                    }
                  }
                } finally {
                  iterator.close()
                }
                assert(actual.toSeq == expected(pid))
            }
          } finally {
            reader.close(readerHandle)
          }
        } finally {
          schema.close()
        }
      }
    }
  }
}

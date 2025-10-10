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
package org.apache.gluten.vectorized

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.{BoltConfig, GlutenConfig}
import org.apache.gluten.iterator.ClosableIterator
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.proto.ShuffleReaderInfo
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.shuffle.{BoltShuffleReaderJniWrapper, BoltShuffleReaderMetrics}
import org.apache.gluten.utils.ArrowAbiUtil

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, SerializerInstance}
import org.apache.spark.shuffle.GlutenShuffleUtils
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.SparkSchemaUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.BlockId
import org.apache.spark.task.{TaskResource, TaskResources}

import org.apache.arrow.c.ArrowSchema
import org.apache.arrow.memory.BufferAllocator

import java.io._
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

import scala.reflect.ClassTag

class ColumnarBatchSerializer(
    schema: StructType,
    readBatchNumRows: SQLMetric,
    numOutputRows: SQLMetric,
    decompressTime: SQLMetric,
    deserializeTime: SQLMetric,
    totalReadTime: SQLMetric)
  extends SettableColumnarBatchSerializer(
    readBatchNumRows,
    numOutputRows,
    decompressTime,
    deserializeTime,
    totalReadTime)
  with Serializable {

  /** Creates a new [[SerializerInstance]]. */
  override def newInstance(): SerializerInstance = {
    new ColumnarBatchSerializerInstanceImpl(
      schema,
      readBatchNumRows,
      numOutputRows,
      decompressTime,
      deserializeTime,
      totalReadTime,
      numPartitions,
      partitionShortName)
  }

  override def supportsRelocationOfSerializedObjects: Boolean = true
}

private class ColumnarBatchSerializerInstanceImpl(
    schema: StructType,
    readBatchNumRows: SQLMetric,
    numOutputRows: SQLMetric,
    decompressTime: SQLMetric,
    deserializeTime: SQLMetric,
    totalReadTime: SQLMetric,
    numPartitions: Int,
    partitionShortName: String)
  extends ColumnarBatchSerializerInstance
  with Logging {

  private val runtime =
    Runtimes.contextInstance(BackendsApiManager.getBackendName, "ShuffleReader")

  private val jniWrapper = BoltShuffleReaderJniWrapper.create(runtime)

  private val shuffleReaderHandle = {
    val allocator: BufferAllocator = ArrowBufferAllocators
      .contextInstance(classOf[ColumnarBatchSerializerInstance].getSimpleName)
      .newChildAllocator("GlutenColumnarBatch deserialize", 0, Long.MaxValue)
    val arrowSchema =
      SparkSchemaUtil.toArrowSchema(schema, SQLConf.get.sessionLocalTimeZone)
    val cSchema = ArrowSchema.allocateNew(allocator)
    ArrowAbiUtil.exportSchema(allocator, arrowSchema, cSchema)
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

    val builder = ShuffleReaderInfo.newBuilder()
    builder
      .setBatchSize(batchSize)
      .setShuffleBatchByteSize(shuffleBatchByteSize)
      .setNumPartitions(numPartitions)
      .setPartitionShortName(partitionShortName)
      .setForcedWriterType(forceShuffleWriterType)
      .setCompressionType(compressionCodec)
      .setCodec(compressionCodecBackend)
    val shuffleReaderHandle = jniWrapper.make(
      cSchema.memoryAddress(),
      builder.build().toByteArray
    )
    // Close shuffle reader instance as lately as the end of task processing,
    // since the native reader could hold a reference to memory pool that
    // was used to create all buffers read from shuffle reader. The pool
    // should keep alive before all buffers finish consuming.
    TaskResources.addRecycler(s"ShuffleReaderHandle_$shuffleReaderHandle", 50) {
      // Collect Metrics
      val readerMetrics = new BoltShuffleReaderMetrics()
      jniWrapper.populateMetrics(shuffleReaderHandle, readerMetrics)
      deserializeTime += readerMetrics.getDeserializeTime
      decompressTime += readerMetrics.getDecompressTime

      jniWrapper.close(shuffleReaderHandle)
      cSchema.release()
      cSchema.close()
      allocator.close()
    }
    shuffleReaderHandle
  }

  override def deserializeStream(in: InputStream): DeserializationStream = {
    val startTime = System.nanoTime()
    val r = new TaskDeserializationStream(Iterator((null, in)))
    totalReadTime += (System.nanoTime() - startTime)
    r
  }

  override def deserializeStreams(
      streams: Iterator[(BlockId, InputStream)]): DeserializationStream = {
    val startTime = System.nanoTime()
    val r = new TaskDeserializationStream(streams)
    totalReadTime += (System.nanoTime() - startTime)
    r
  }

  private class TaskDeserializationStream(streams: Iterator[(BlockId, InputStream)])
    extends DeserializationStream
    with TaskResource {
    private val streamReader = ShuffleStreamReader(streams)

    private val wrappedOut: ClosableIterator[ColumnarBatch] = new ColumnarBatchOutIterator(
      runtime,
      jniWrapper
        .read(shuffleReaderHandle, streamReader))

    private var cb: ColumnarBatch = _

    private var numBatchesTotal: Long = _
    private var numRowsTotal: Long = _

    // Otherwise calling close() twice would cause resource ID not found error.
    private val closeCalled: AtomicBoolean = new AtomicBoolean(false)

    // Otherwise calling release() twice would cause #close0() to be called twice.
    private val releaseCalled: AtomicBoolean = new AtomicBoolean(false)

    private val resourceId = UUID.randomUUID().toString

    TaskResources.addResource(resourceId, this)

    override def asIterator: Iterator[Any] = {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException
    }

    override def readKey[T: ClassTag](): T = {
      // We skipped serialization of the key in writeKey(), so just return a dummy value since
      // this is going to be discarded anyways.
      null.asInstanceOf[T]
    }

    @throws(classOf[EOFException])
    override def readValue[T: ClassTag](): T = {
      val startTime = System.nanoTime()
      if (cb != null) {
        cb.close()
        cb = null
      }
      val batch = {
        val maybeBatch =
          try {
            wrappedOut.next()
          } catch {
            case ioe: IOException =>
              this.close()
              logError("Failed to load next RecordBatch", ioe)
              throw ioe
          }
        if (maybeBatch == null) {
          // EOF reached
          this.close()
          totalReadTime += (System.nanoTime() - startTime)
          throw new EOFException
        }
        maybeBatch
      }
      totalReadTime += (System.nanoTime() - startTime)
      val numRows = batch.numRows()
      logDebug(s"Read ColumnarBatch of $numRows rows")
      numBatchesTotal += 1
      numRowsTotal += numRows
      cb = batch
      cb.asInstanceOf[T]
    }

    override def readObject[T: ClassTag](): T = {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException
    }

    override def close(): Unit = {
      if (!closeCalled.compareAndSet(false, true)) {
        return
      }
      // Would remove the resource object from registry to lower GC pressure.
      TaskResources.releaseResource(resourceId)
    }

    // We don't yet have a path to propagate `close` calls from Bolt's value stream
    // to Spark-side's endpoint like this place.
    //
    // E.g. A Bolt limit operator may suddenly drop the input stream after emitting enough
    // rows. In the case DeserializationStream#close() will not be called. Spark doesn't
    // call close() either. So we should handle the case especially.
    override def release(): Unit = {
      if (!releaseCalled.compareAndSet(false, true)) {
        return
      }
      close0()
    }

    private def close0(): Unit = {
      if (numBatchesTotal > 0) {
        readBatchNumRows.set(numRowsTotal.toDouble / numBatchesTotal)
      }
      numOutputRows += numRowsTotal
      wrappedOut.close()
      streamReader.close()
      if (cb != null) {
        cb.close()
      }
    }

    override def resourceName(): String = getClass.getName
  }

  // Columnar shuffle write process don't need this.
  override def serializeStream(s: OutputStream): SerializationStream =
    throw new UnsupportedOperationException

  // These methods are never called by shuffle code.
  override def serialize[T: ClassTag](t: T): ByteBuffer = throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException
}

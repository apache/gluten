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
import org.apache.gluten.config.{GlutenConfig, ShuffleWriterType, VeloxConfig}
import org.apache.gluten.iterator.ClosableIterator
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.utils.ArrowAbiUtil

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{DeserializationStream, Serializer, SerializerInstance}
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
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

import scala.reflect.ClassTag

class ColumnarBatchSerializer(
    schema: StructType,
    readBatchNumRows: SQLMetric,
    numOutputRows: SQLMetric,
    deserializeTime: SQLMetric,
    decompressTime: SQLMetric,
    shuffleWriterType: ShuffleWriterType)
  extends Serializer
  with Serializable {

  /** Creates a new [[SerializerInstance]]. */
  override def newInstance(): SerializerInstance = {
    new ColumnarBatchSerializerInstanceImpl(
      schema,
      readBatchNumRows,
      numOutputRows,
      deserializeTime,
      decompressTime,
      shuffleWriterType)
  }

  override def supportsRelocationOfSerializedObjects: Boolean = true
}

private class ColumnarBatchSerializerInstanceImpl(
    schema: StructType,
    readBatchNumRows: SQLMetric,
    numOutputRows: SQLMetric,
    deserializeTime: SQLMetric,
    decompressTime: SQLMetric,
    shuffleWriterType: ShuffleWriterType)
  extends ColumnarBatchSerializerInstance
  with Logging {

  private val runtime =
    Runtimes.contextInstance(BackendsApiManager.getBackendName, "ShuffleReader")

  private val jniWrapper = ShuffleReaderJniWrapper.create(runtime)

  private val shuffleReaderHandle = {
    val allocator: BufferAllocator = ArrowBufferAllocators
      .contextInstance(classOf[ColumnarBatchSerializerInstance].getSimpleName)
      .newChildAllocator("GlutenColumnarBatch deserialize", 0, Long.MaxValue)
    val arrowSchema =
      SparkSchemaUtil.toArrowSchema(schema, SQLConf.get.sessionLocalTimeZone)
    val cSchema = ArrowSchema.allocateNew(allocator)
    ArrowAbiUtil.exportSchema(allocator, arrowSchema, cSchema)

    val conf = SparkEnv.get.conf
    val glutenConfig = GlutenConfig.get
    val veloxConfig = VeloxConfig.get
    val compressionCodec =
      if (conf.getBoolean("spark.shuffle.compress", true)) {
        GlutenShuffleUtils.getCompressionCodec(conf)
      } else {
        null // uncompressed
      }
    val compressionCodecBackend =
      glutenConfig.columnarShuffleCodecBackend.orNull
    val shuffleReaderHandle = jniWrapper.make(
      shuffleWriterType.name,
      cSchema.memoryAddress(),
      compressionCodec,
      compressionCodecBackend,
      glutenConfig.maxBatchSize,
      glutenConfig.columnarShuffleReaderBufferSize,
      glutenConfig.columnarSortShuffleDeserializerBufferSize,
      veloxConfig.enableHashShuffleReaderStreamMerge,
      veloxConfig.enableGpuAsyncShuffleReader,
      veloxConfig.gpuAsyncReaderMaxPrefetchBytes
    )
    // Close shuffle reader instance as lately as the end of task processing,
    // since the native reader could hold a reference to memory pool that
    // was used to create all buffers read from shuffle reader. The pool
    // should keep alive before all buffers finish consuming.
    TaskResources.addRecycler(s"ShuffleReaderHandle_$shuffleReaderHandle", 50) {
      // Collect Metrics
      val readerMetrics = new ShuffleReaderMetrics()
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

  // `deserializeStream` is currently still used by uniffle shuffle reader.
  override def deserializeStream(in: InputStream): DeserializationStream = {
    new TaskDeserializationStream(Iterator((null, in)))
  }

  override def deserializeStreams(
      streams: Iterator[(BlockId, InputStream)],
      onComplete: () => Unit): DeserializationStream = {
    new TaskDeserializationStream(streams, Some(onComplete))
  }

  private class TaskDeserializationStream(
      streams: Iterator[(BlockId, InputStream)],
      onComplete: Option[() => Unit] = None)
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
          throw new EOFException
        }
        maybeBatch
      }
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

    // We don't yet have a path to propagate `close` calls from Velox's value stream
    // to Spark-side's endpoint like this place.
    //
    // E.g. A Velox limit operator may suddenly drop the input stream after emitting enough
    // rows. In the case DeserializationStream#close() will not be called. Spark doesn't
    // call close() either. So we should handle the case especially.
    override def release(): Unit = {
      if (!releaseCalled.compareAndSet(false, true)) {
        return
      }
      close0()
    }

    private def close0(): Unit = {
      // Stop the native reader from reading more streams.
      jniWrapper.stop(shuffleReaderHandle)
      onComplete.foreach(_())

      if (numBatchesTotal > 0) {
        readBatchNumRows.set(numRowsTotal.toDouble / numBatchesTotal)
      }
      numOutputRows += numRowsTotal
      wrappedOut.close()
      if (cb != null) {
        cb.close()
      }
    }

    override def resourceName(): String = getClass.getName
  }
}

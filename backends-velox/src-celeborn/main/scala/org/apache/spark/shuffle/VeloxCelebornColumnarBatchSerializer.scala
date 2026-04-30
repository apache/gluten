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
import org.apache.gluten.config.{GlutenConfig, ShuffleWriterType}
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.utils.ArrowAbiUtil
import org.apache.gluten.vectorized._

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.SHUFFLE_COMPRESS
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.SparkSchemaUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.task.{TaskResource, TaskResources}

import org.apache.arrow.c.ArrowSchema
import org.apache.arrow.memory.BufferAllocator
import org.apache.celeborn.client.read.CelebornInputStream

import java.io._
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

import scala.reflect.ClassTag

class CelebornColumnarBatchSerializer(
    schema: StructType,
    readBatchNumRows: SQLMetric,
    numOutputRows: SQLMetric,
    shuffleWriterType: ShuffleWriterType)
  extends Serializer
  with Serializable {

  /** Creates a new [[SerializerInstance]]. */
  override def newInstance(): SerializerInstance = {
    new CelebornColumnarBatchSerializerInstance(
      schema,
      readBatchNumRows,
      numOutputRows,
      shuffleWriterType)
  }
}

private class CelebornColumnarBatchSerializerInstance(
    schema: StructType,
    readBatchNumRows: SQLMetric,
    numOutputRows: SQLMetric,
    shuffleWriterType: ShuffleWriterType)
  extends SerializerInstance
  with Logging {

  private val runtime =
    Runtimes.contextInstance(BackendsApiManager.getBackendName, "CelebornShuffleReader")

  private val shuffleReaderHandle = {
    val allocator: BufferAllocator = ArrowBufferAllocators
      .contextInstance(classOf[CelebornColumnarBatchSerializerInstance].getSimpleName)
      .newChildAllocator("GlutenColumnarBatch deserialize", 0, Long.MaxValue)
    val arrowSchema =
      SparkSchemaUtil.toArrowSchema(schema, SQLConf.get.sessionLocalTimeZone)
    val cSchema = ArrowSchema.allocateNew(allocator)
    ArrowAbiUtil.exportSchema(allocator, arrowSchema, cSchema)
    val conf = SparkEnv.get.conf
    val compressionCodec =
      if (conf.getBoolean(SHUFFLE_COMPRESS.key, SHUFFLE_COMPRESS.defaultValue.get)) {
        GlutenShuffleUtils.getCompressionCodec(conf)
      } else {
        null // uncompressed
      }
    val compressionCodecBackend =
      GlutenConfig.get.columnarShuffleCodecBackend.orNull
    val jniWrapper = ShuffleReaderJniWrapper.create(runtime)
    val batchSize = GlutenConfig.get.maxBatchSize
    val readerBufferSize = GlutenConfig.get.columnarShuffleReaderBufferSize
    val deserializerBufferSize = GlutenConfig.get.columnarSortShuffleDeserializerBufferSize
    val handle = jniWrapper
      .make(
        cSchema.memoryAddress(),
        compressionCodec,
        compressionCodecBackend,
        batchSize,
        readerBufferSize,
        deserializerBufferSize,
        shuffleWriterType.name
      )
    // Close shuffle reader instance as lately as the end of task processing,
    // since the native reader could hold a reference to memory pool that
    // was used to create all buffers read from shuffle reader. The pool
    // should keep alive before all buffers to finish consuming.
    TaskResources.addRecycler(s"CelebornShuffleReaderHandle_$handle", 50) {
      jniWrapper.close(handle)
      cSchema.release()
      cSchema.close()
      allocator.close()
    }
    handle
  }

  override def deserializeStream(in: InputStream): DeserializationStream = {
    new TaskDeserializationStream(in)
  }

  private class TaskDeserializationStream(in: InputStream)
    extends DeserializationStream
    with TaskResource {
    private val streamReader = ShuffleStreamReader(Iterator((null, in)))

    private var wrappedOut: ColumnarBatchOutIterator = _

    private var cb: ColumnarBatch = _

    private var numBatchesTotal: Long = _
    private var numRowsTotal: Long = _

    private val isEmptyStream: Boolean = in.equals(CelebornInputStream.empty())

    // Otherwise calling close() twice would cause resource ID not found error.
    private val closeCalled: AtomicBoolean = new AtomicBoolean(false)

    // Otherwise calling release() twice would cause #close0() to be called twice.
    private val releaseCalled: AtomicBoolean = new AtomicBoolean(false)

    private val resourceId = UUID.randomUUID().toString

    TaskResources.addResource(resourceId, this)

    override def asKeyValueIterator: Iterator[(Any, Any)] = new Iterator[(Any, Any)] {
      private var gotNext = false
      private var nextValue: (Any, Any) = _
      private var finished = false

      def getNext: (Any, Any) = {
        try {
          (readKey[Any](), readValue[Any]())
        } catch {
          case eof: EOFException =>
            finished = true
            null
        }
      }

      override def hasNext: Boolean = {
        if (!isEmptyStream && !finished) {
          if (!gotNext) {
            nextValue = getNext
            gotNext = true
          }
        }
        !isEmptyStream && !finished
      }

      override def next(): (Any, Any) = {
        if (!hasNext) {
          throw new NoSuchElementException("End of stream")
        }
        gotNext = false
        nextValue
      }
    }

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
      initStream()
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
      if (wrappedOut != null) {
        wrappedOut.close()
      }
      streamReader.close()
      if (cb != null) {
        cb.close()
      }
    }

    private def initStream(): Unit = {
      if (wrappedOut == null) {
        wrappedOut = new ColumnarBatchOutIterator(
          runtime,
          ShuffleReaderJniWrapper
            .create(runtime)
            .read(shuffleReaderHandle, streamReader))
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

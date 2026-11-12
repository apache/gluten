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
package org.apache.gluten.connector.write

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.{ColumnarBatches, ColumnarBatchJniWrapper}
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.execution.metric.CustomMetrics
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.arrow.c.{ArrowArray, ArrowArrayStream, ArrowSchema, CDataDictionaryProvider, Data}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.lance.{Fragment, FragmentMetadata}
import org.lance.spark.write.{FragmentWriteSpec, LanceBatchWrite}
import org.lance.spark.write.metric.LanceWriteMetrics

import java.io.IOException
import java.util.{Collections, List => JList}
import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._

/**
 * Writes one task's Velox batches into uncommitted Lance fragments.
 *
 * Spark pushes batches through [[write]], while Lance pulls batches from an Arrow C stream until it
 * ends. A background thread bridges the two, as lance-spark's own row writer does: each batch is
 * exported to Arrow C structs on the task thread and queued, and the thread writes fragments with
 * the parameters of the [[FragmentWriteSpec]] from a stream that drains the queue.
 *
 * The stream advertises the commit schema, so its names, nullability and timestamp zone are the
 * ones `LanceBatchWrite.commit` uses. The first exported batch is checked against it, so a type
 * Velox exports differently fails the task instead of writing a corrupt table. Fixed-size list
 * columns arrive as plain lists and are converted by [[LanceFixedSizeLists]], and the values Velox
 * leaves under nulls are zeroed by [[LanceNullSlots]] so that Lance encodes as compactly as it does
 * for the row writer.
 *
 * It reports the metrics lance-spark's row writer reports, under the same names, and fills the
 * native write metrics of the write node.
 */
class LanceColumnarBatchDataWriter(spec: FragmentWriteSpec, commitSchema: Schema)
  extends DataWriter[ColumnarBatch]
  with Logging {
  import LanceColumnarBatchDataWriter._

  private val allocator: BufferAllocator =
    ArrowBufferAllocators.contextInstance(classOf[LanceColumnarBatchDataWriter].getSimpleName)
  private val queue = new ArrayBlockingQueue[Item](QueueCapacity)
  private val exportedSchema = LanceFixedSizeLists.exportedSchema(commitSchema)

  private var stream: ArrowArrayStream = _
  private var thread: Thread = _
  private var schemaChecked = false
  @volatile private var fragments: JList[FragmentMetadata] = _
  @volatile private var failure: Throwable = _
  @volatile private var writeWallNs = 0L
  // Time the stream waits for the task to queue its next batch, which is not time spent writing.
  private val queueWaitNs = new AtomicLong()
  // Set by the stream when Lance reads its end.
  @volatile private var streamEnded = false
  // Set by the task to make the stream fail, so Lance stops writing.
  @volatile private var stopRequested = false
  private var stopWaited = false

  private var recordsWritten = 0L
  private var bytesWritten = 0L
  private var filesWritten = 0L

  // Spark forwards bytesWritten and recordsWritten to the task's output metrics, as for the row
  // writer. Lance only reports file sizes once its fragments complete, inside commit().
  private val taskMetrics: Array[CustomTaskMetric] = Array(
    taskMetric(LanceWriteMetrics.RECORDS_WRITTEN, recordsWritten),
    taskMetric(LanceWriteMetrics.BYTES_WRITTEN, bytesWritten),
    taskMetric("numWrittenBytes", bytesWritten),
    taskMetric("numWrittenFiles", filesWritten),
    taskMetric("writeWallNs", writeWallNs)
  )

  override def write(batch: ColumnarBatch): Unit = {
    val numRows = batch.numRows()
    if (numRows == 0) {
      return
    }
    if (thread == null) {
      start()
    } else {
      // Once Lance has failed, it reads no more batches, so do not export them.
      checkWriter()
    }
    enqueue(exportBatch(batch))
    recordsWritten += numRows
  }

  override def commit(): WriterCommitMessage = {
    if (thread == null) {
      // No rows in this task: commit nothing, as lance-spark's row writer does.
      return LanceBatchWrite.taskCommit(Collections.emptyList())
    }
    enqueue(End)
    thread.join()
    throwIfFailed()
    if (!streamEnded) {
      throw new IllegalStateException("Lance finished writing before the end of the stream")
    }
    fragments.asScala.foreach {
      fragment =>
        fragment.getFiles.asScala.foreach {
          file =>
            filesWritten += 1
            Option(file.getFileSizeBytes).foreach(size => bytesWritten += size)
        }
    }
    LanceBatchWrite.taskCommit(fragments)
  }

  override def currentMetricsValues(): Array[CustomTaskMetric] = taskMetrics

  override def abort(): Unit = {
    // A failed stream makes Lance delete the files it wrote for this task, finished or not, as
    // Spark expects of abort(). lance-spark's row writer ends its stream instead, which leaves them.
    stopWriter()
    // Spark adds a failed attempt's output metrics to the stage totals, so the retry would count
    // these rows twice. Clear them, as lance-spark's row writer does.
    recordsWritten = 0
    bytesWritten = 0
    CustomMetrics.updateMetrics(taskMetrics.toSeq, Map.empty)
  }

  override def close(): Unit = {
    stopWriter()
    // A writer thread that did not stop may still read the stream, so leave it rather than free it.
    // The task then reports the stream's struct as leaked Arrow memory.
    if (stream != null && (thread == null || !thread.isAlive)) {
      // Lance has taken the stream by now, unless it failed before reading it; release() then
      // closes the reader, and frees nothing otherwise.
      stream.release()
      stream.close()
      stream = null
    }
  }

  private def start(): Unit = {
    stream = ArrowArrayStream.allocateNew(allocator)
    Data.exportArrayStream(allocator, new QueueReader, stream)
    thread = new Thread(() => runWriter(), "gluten-lance-write")
    thread.setDaemon(true)
    thread.start()
  }

  /**
   * Runs on the writer thread until Lance has read the stream to its end, or failed. The write time
   * leaves out the time the stream waits for batches, which the operators producing them spend, as
   * the time of write of Velox's own writers does.
   */
  private def runWriter(): Unit = {
    val startNs = System.nanoTime()
    try {
      fragments = Fragment
        .write()
        .datasetUri(spec.getDatasetUri)
        .data(stream)
        .writeParams(spec.toWriteParams())
        .execute()
    } catch {
      case t: Throwable => failure = t
    } finally {
      writeWallNs = System.nanoTime() - startNs - queueWaitNs.get()
    }
  }

  /** Exports a Velox batch to Arrow C structs, checking the first batch's schema. */
  private def exportBatch(batch: ColumnarBatch): ExportedBatch = {
    val handle = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, batch)
    val exported = new ExportedBatch(ArrowArray.allocateNew(allocator))
    val cSchema = ArrowSchema.allocateNew(allocator)
    // An exported batch holds Velox memory, so it must be released on every path.
    try {
      ColumnarBatchJniWrapper.exportToArrow(
        handle,
        cSchema.memoryAddress(),
        exported.array.memoryAddress())
      if (schemaChecked) {
        cSchema.release()
      } else {
        val provider = new CDataDictionaryProvider()
        // Importing the schema releases and frees the struct, even when it fails.
        try checkExportedSchema(Data.importSchema(allocator, cSchema, provider))
        finally provider.close()
        schemaChecked = true
      }
      exported
    } catch {
      case t: Throwable =>
        exported.close()
        throw t
    } finally {
      cSchema.close()
    }
  }

  /** Queues an item, failing fast if the Lance writer has died instead of blocking forever. */
  private def enqueue(item: Item): Unit = {
    try {
      while (!queue.offer(item, PollIntervalMs, TimeUnit.MILLISECONDS)) {
        checkWriter()
      }
    } catch {
      case t: Throwable =>
        item.close()
        throw t
    }
  }

  /**
   * Makes the stream fail so Lance stops writing, then waits for the writer thread, once: abort()
   * and close() both stop it, and a task should not wait twice for a writer stuck in I/O, even when
   * the first wait is interrupted.
   */
  private def stopWriter(): Unit = {
    stopRequested = true
    drain()
    if (thread != null && thread.isAlive && !stopWaited) {
      stopWaited = true
      try {
        thread.join(StopTimeoutMs)
      } catch {
        // Spark interrupts a task it kills. Keep the interrupt for Spark to see.
        case _: InterruptedException => Thread.currentThread().interrupt()
      }
      if (thread.isAlive) {
        logWarning("Lance write thread is still running after the task stopped it")
      }
    }
  }

  private def drain(): Unit = {
    var item = queue.poll()
    while (item != null) {
      item.close()
      item = queue.poll()
    }
  }

  private def throwIfFailed(): Unit = {
    if (failure != null) {
      throw new IOException("Lance native write failed", failure)
    }
  }

  /** Fails the task if Lance has failed, or stopped reading before the end of the stream. */
  private def checkWriter(): Unit = {
    throwIfFailed()
    if (!thread.isAlive) {
      throw new IllegalStateException("Lance write thread exited before the stream ended")
    }
  }

  private def checkExportedSchema(exported: Schema): Unit = {
    if (!exportedSchema.getFields.asScala.corresponds(exported.getFields.asScala)(compatible)) {
      throw new IllegalStateException(
        s"Velox exported schema $exported does not match the Lance commit schema $commitSchema")
    }
  }

  private def compatible(expected: Field, actual: Field): Boolean = {
    val typesMatch = (expected.getType, actual.getType) match {
      // Velox exports timestamps without a time zone; Lance accepts that.
      case (e: ArrowType.Timestamp, a: ArrowType.Timestamp) => e.getUnit == a.getUnit
      case (_: ArrowType.Map, _: ArrowType.Map) => true
      case (e, a) => e == a
    }
    typesMatch && actual.getDictionary == null &&
    expected.getChildren.asScala.corresponds(actual.getChildren.asScala)(compatible)
  }

  /** The stream's source: imports queued batches into a root shaped like the commit schema. */
  private class QueueReader extends ArrowReader(allocator) {
    // Batches with fixed-size list columns are imported as Velox exports them, then converted.
    private val staging: Option[VectorSchemaRoot] =
      if (exportedSchema == commitSchema) None
      else
        Some(VectorSchemaRoot.create(exportedSchema, LanceColumnarBatchDataWriter.this.allocator))

    override protected def readSchema(): Schema = commitSchema

    override def loadNextBatch(): Boolean = {
      if (streamEnded) {
        return false
      }
      prepareLoadNextBatch()
      val waitStartNs = System.nanoTime()
      var item: Item = null
      while (item == null) {
        // Checked between polls, so a stop reaches Lance even when the queue is empty.
        if (stopRequested) {
          throw new IOException("Lance write aborted")
        }
        item = queue.poll(PollIntervalMs, TimeUnit.MILLISECONDS)
      }
      queueWaitNs.addAndGet(System.nanoTime() - waitStartNs)
      item match {
        case End =>
          streamEnded = true
          false
        case batch: ExportedBatch =>
          val writerAllocator = LanceColumnarBatchDataWriter.this.allocator
          staging match {
            case None => batch.importInto(writerAllocator, getVectorSchemaRoot)
            case Some(root) =>
              batch.importInto(writerAllocator, root)
              LanceFixedSizeLists.moveInto(root, getVectorSchemaRoot)
          }
          LanceNullSlots.zero(getVectorSchemaRoot, writerAllocator)
          true
      }
    }

    override def bytesRead(): Long = 0

    override protected def closeReadSource(): Unit = {
      drain()
      staging.foreach(_.close())
    }
  }
}

object LanceColumnarBatchDataWriter {
  private val QueueCapacity = 4
  private val PollIntervalMs = 100L
  // As long as lance-spark's row writer waits for its writer thread in abort().
  private val StopTimeoutMs = TimeUnit.MINUTES.toMillis(5)

  private def taskMetric(metricName: String, read: => Long): CustomTaskMetric =
    new CustomTaskMetric {
      override def name(): String = metricName
      override def value(): Long = read
    }

  sealed private trait Item {
    def close(): Unit = {}
  }

  /** An exported batch, released and freed by [[close]] unless it was imported. */
  final private class ExportedBatch(val array: ArrowArray) extends Item {
    private var owned = true

    /**
     * Moves the batch into `root`. Importing takes the data even when it fails, and some Arrow
     * versions also free the struct, so only the idempotent close() is left to do.
     */
    def importInto(allocator: BufferAllocator, root: VectorSchemaRoot): Unit = {
      owned = false
      try Data.importIntoVectorSchemaRoot(allocator, array, root, null)
      finally array.close()
    }

    override def close(): Unit = {
      if (owned) {
        owned = false
        array.release()
        array.close()
      }
    }
  }

  private case object End extends Item
}

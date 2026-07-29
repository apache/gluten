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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.config.{GlutenConfig, VeloxConfig}
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.utils.ArrowAbiUtil
import org.apache.gluten.vectorized._

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.{BroadcastUtils, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.task.TaskResources
import org.apache.spark.unsafe.Platform

import org.apache.arrow.c.ArrowSchema
import org.apache.arrow.memory.ArrowBuf

import scala.collection.mutable.ListBuffer

case class RowToVeloxColumnarExec(child: SparkPlan) extends RowToColumnarExecBase(child = child) {
  override def doExecuteColumnarInternal(): RDD[ColumnarBatch] = {
    val numInputRows = longMetric("numInputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val convertTime = longMetric("convertTime")
    val numRows = GlutenConfig.get.maxBatchSize
    val numBytes = VeloxConfig.get.veloxPreferredBatchBytes
    // This avoids calling `schema` in the RDD closure, so that we don't need to include the entire
    // plan (this) in the closure.
    val localSchema = schema
    child.execute().mapPartitions {
      rowIterator =>
        RowToVeloxColumnarExec.toColumnarBatchIterator(
          rowIterator,
          localSchema,
          numInputRows,
          numOutputBatches,
          convertTime,
          numRows,
          numBytes)
    }
  }

  override def doExecuteBroadcast[T](): Broadcast[T] = {
    val numInputRows = longMetric("numInputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val convertTime = longMetric("convertTime")
    val numRows = GlutenConfig.get.maxBatchSize
    val numBytes = VeloxConfig.get.veloxPreferredBatchBytes
    val mode = BroadcastUtils.getBroadcastMode(outputPartitioning)
    val relation = child.executeBroadcast()
    BroadcastUtils.sparkToVeloxUnsafe(
      sparkContext,
      mode,
      schema,
      relation,
      itr =>
        RowToVeloxColumnarExec.toColumnarBatchIterator(
          itr,
          schema,
          numInputRows,
          numOutputBatches,
          convertTime,
          numRows,
          numBytes)
    )
  }

  protected def withNewChildInternal(newChild: SparkPlan): RowToVeloxColumnarExec =
    copy(child = newChild)
}

object RowToVeloxColumnarExec {

  def toColumnarBatchIterator(
      it: Iterator[InternalRow],
      schema: StructType,
      columnBatchSize: Int,
      columnBatchBytes: Long): Iterator[ColumnarBatch] = {
    val numInputRows = new SQLMetric("numInputRows")
    val numOutputBatches = new SQLMetric("numOutputBatches")
    val convertTime = new SQLMetric("convertTime")
    RowToVeloxColumnarExec.toColumnarBatchIterator(
      it,
      schema,
      numInputRows,
      numOutputBatches,
      convertTime,
      columnBatchSize,
      columnBatchBytes)
  }

  def toColumnarBatchIterator(
      it: Iterator[InternalRow],
      schema: StructType,
      numInputRows: SQLMetric,
      numOutputBatches: SQLMetric,
      convertTime: SQLMetric,
      columnBatchSize: Int,
      columnBatchBytes: Long): Iterator[ColumnarBatch] = {
    if (it.isEmpty) {
      return Iterator.empty
    }

    val arrowSchema =
      SparkArrowUtil.toArrowSchema(schema, SQLConf.get.sessionLocalTimeZone)
    val runtime = Runtimes.contextInstance(BackendsApiManager.getBackendName, "RowToColumnar")
    val jniWrapper = NativeRowToColumnarJniWrapper.create(runtime)
    val arrowAllocator = ArrowBufferAllocators.contextInstance()
    val cSchema = ArrowSchema.allocateNew(arrowAllocator)
    val factory = UnsafeProjection
    val converter = factory.create(schema)
    val r2cHandle =
      try {
        ArrowAbiUtil.exportSchema(arrowAllocator, arrowSchema, cSchema)
        jniWrapper.init(cSchema.memoryAddress())
      } finally {
        cSchema.close()
      }

    val res: Iterator[ColumnarBatch] = new Iterator[ColumnarBatch] {
      var finished = false

      override def hasNext: Boolean = {
        if (finished) {
          false
        } else {
          it.hasNext
        }
      }

      def convertToUnsafeRow(row: InternalRow): UnsafeRow = {
        row match {
          case unsafeRow: UnsafeRow => unsafeRow
          case _ =>
            converter.apply(row)
        }
      }

      override def next(): ColumnarBatch = {
        var arrowBuf: ArrowBuf = null
        TaskResources.addRecycler("RowToColumnar_arrowBuf", 100) {
          if (arrowBuf != null && arrowBuf.refCnt() != 0) {
            arrowBuf.close()
          }
        }
        val rowLength = new ListBuffer[Long]()
        var rowCount = 0
        var offset = 0L
        while (rowCount < columnBatchSize && offset < columnBatchBytes && !finished) {
          if (!it.hasNext) {
            finished = true
          } else {
            val row = it.next()
            val start = System.currentTimeMillis()
            val unsafeRow = convertToUnsafeRow(row)
            val sizeInBytes = unsafeRow.getSizeInBytes

            // allocate buffer based on first row
            if (rowCount == 0) {
              // allocate buffer based on 1st row, but if first row is very big, this will cause OOM
              // maybe we should optimize to list ArrayBuf to native to avoid buf close and allocate
              // 31760L origins from BaseVariableWidthVector.lastValueAllocationSizeInBytes
              // experimental value
              val estimatedBufSize = Math.min(
                Math.max(
                  Math.min(sizeInBytes.toDouble * columnBatchSize * 1.2, 31760L * columnBatchSize),
                  sizeInBytes.toDouble * 10),
                // Limit the size of the buffer to columnBatchBytes or the size of the first row,
                // whichever is greater so we always have enough space for the first row.
                Math.max(columnBatchBytes, sizeInBytes)
              )
              arrowBuf = arrowAllocator.buffer(estimatedBufSize.toLong)
            }

            if ((offset + sizeInBytes) > arrowBuf.capacity()) {
              val bufSize = if (offset + sizeInBytes > columnBatchBytes) {
                // If adding the current row causes the batch size to exceed columnBatchBytes add
                // just enough space to add the current row.
                offset + sizeInBytes
              } else {
                Math.min((offset + sizeInBytes * 2), columnBatchBytes)
              }
              val tmpBuf = arrowAllocator.buffer(bufSize)
              tmpBuf.setBytes(0, arrowBuf, 0, offset)
              arrowBuf.close()
              arrowBuf = tmpBuf
            }
            Platform.copyMemory(
              unsafeRow.getBaseObject,
              unsafeRow.getBaseOffset,
              null,
              arrowBuf.memoryAddress() + offset,
              sizeInBytes)
            offset += sizeInBytes
            rowLength += sizeInBytes.toLong
            rowCount += 1
            convertTime += System.currentTimeMillis() - start
          }
        }
        numInputRows += rowCount
        numOutputBatches += 1
        val startNative = System.currentTimeMillis()
        try {
          val handle = jniWrapper
            .nativeConvertRowToColumnar(r2cHandle, rowLength.toArray, arrowBuf.memoryAddress())
          val cb = ColumnarBatches.create(handle)
          convertTime += System.currentTimeMillis() - startNative
          cb
        } finally {
          arrowBuf.close()
          arrowBuf = null
        }
      }
    }
    Iterators
      .wrap(res)
      .protectInvocationFlow()
      .recycleIterator {
        jniWrapper.close(r2cHandle)
      }
      .recyclePayload(_.close())
      .create()
  }
}

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
import org.apache.gluten.columnarbatch.{BoltColumnarBatches, ColumnarBatches}
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.vectorized.{NativeColumnarToRowInfo, NativeColumnarToRowJniWrapper}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.{BroadcastUtils, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters._

case class BoltColumnarToRowExec(child: SparkPlan) extends ColumnarToRowExecBase(child = child) {

  override protected def doValidateInternal(): ValidationResult = {
    val schema = child.schema
    // Depending on the input type, BoltColumnarToRowConverter.
    for (field <- schema.fields) {
      field.dataType match {
        case _: BooleanType =>
        case _: ByteType =>
        case _: ShortType =>
        case _: IntegerType =>
        case _: LongType =>
        case _: FloatType =>
        case _: DoubleType =>
        case _: StringType =>
        case _: TimestampType =>
        case _: DateType =>
        case _: BinaryType =>
        case _: DecimalType =>
        case _: ArrayType =>
        case _: MapType =>
        case _: StructType =>
        case YearMonthIntervalType.DEFAULT =>
        case _: NullType =>
        case _ =>
          throw new GlutenNotSupportException(
            s"${field.dataType} is unsupported in " +
              s"BoltColumnarToRowExec.")
      }
    }
    ValidationResult.succeeded
  }

  override def doExecuteInternal(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val numInputBatches = longMetric("numInputBatches")
    val convertTime = longMetric("convertTime")
    child.executeColumnar().mapPartitions {
      it =>
        BoltColumnarToRowExec
          .toRowIterator(it, numOutputRows, numInputBatches, convertTime)
    }
  }

  override def doExecuteBroadcast[T](): Broadcast[T] = {
    val numOutputRows = longMetric("numOutputRows")
    val numInputBatches = longMetric("numInputBatches")
    val convertTime = longMetric("convertTime")

    val mode = BroadcastUtils.getBroadcastMode(outputPartitioning)
    val relation = child.executeBroadcast()
    BroadcastUtils.boltToSparkUnsafe(
      sparkContext,
      mode,
      relation,
      BoltColumnarToRowExec.toRowIterator(_, numOutputRows, numInputBatches, convertTime))
  }

  protected def withNewChildInternal(newChild: SparkPlan): BoltColumnarToRowExec =
    copy(child = newChild)
}

object BoltColumnarToRowExec {

  def toRowIterator(batches: Iterator[ColumnarBatch]): Iterator[InternalRow] = {
    val numOutputRows = new SQLMetric("numOutputRows")
    val numInputBatches = new SQLMetric("numInputBatches")
    val convertTime = new SQLMetric("convertTime")
    toRowIterator(
      batches,
      numOutputRows,
      numInputBatches,
      convertTime
    )
  }

  def toRowIterator(
      batches: Iterator[ColumnarBatch],
      numOutputRows: SQLMetric,
      numInputBatches: SQLMetric,
      convertTime: SQLMetric): Iterator[InternalRow] = {
    if (batches.isEmpty) {
      return Iterator.empty
    }

    val converter = new Converter(convertTime)

    val res: Iterator[Iterator[InternalRow]] = new Iterator[Iterator[InternalRow]] {
      override def hasNext: Boolean = {
        batches.hasNext
      }

      override def next(): Iterator[InternalRow] = {
        val batch = batches.next()
        numInputBatches += 1
        numOutputRows += batch.numRows()
        converter.toRowIterator(batch)
      }
    }
    Iterators
      .wrap(res.flatten)
      .protectInvocationFlow() // Spark may call `hasNext()` again after a false output which
      // is not allowed by Gluten iterators. E.g. GroupedIterator#fetchNextGroupIterator
      .recycleIterator {
        converter.close()
      }
      .create()
  }

  /**
   * A convenient C2R API to allow caller converts batches on demand without having to pass in an
   * Iterator[ColumnarBatch].
   */
  class Converter(convertTime: SQLMetric) {
    private val runtime =
      Runtimes.contextInstance(BackendsApiManager.getBackendName, "BoltColumnarToRow")
    // TODO: Pass the jni jniWrapper and arrowSchema and serializeSchema method by broadcast.
    private val jniWrapper = NativeColumnarToRowJniWrapper.create(runtime)
    private val c2rId = jniWrapper.nativeColumnarToRowInit()

    def toRowIterator(batch: ColumnarBatch): Iterator[InternalRow] = {
      if (batch.numRows() == 0) {
        return Iterator.empty
      }

      if (batch.numCols() == 0) {
        val rows = ColumnarBatches.emptyRowIterator(batch.numRows()).asScala
        return rows
      }

      BoltColumnarBatches.checkBoltBatch(batch)

      new Iterator[InternalRow] {
        private val cols = batch.numCols()
        private val rows = batch.numRows()
        private val batchHandle =
          ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, batch)

        // Mutable members.
        private var rowId = 0
        private var baseLength = 0
        private val row = new UnsafeRow(cols)
        private var info: NativeColumnarToRowInfo = _

        override def hasNext: Boolean = {
          rowId < rows
        }

        override def next(): InternalRow = {
          if (rowId == 0 || rowId == baseLength + info.lengths.length) {
            baseLength = if (info == null) {
              baseLength
            } else {
              baseLength + info.lengths.length
            }
            val before = System.currentTimeMillis()
            info = jniWrapper.nativeColumnarToRowConvert(c2rId, batchHandle, rowId)
            convertTime += (System.currentTimeMillis() - before)
          }
          val (offset, length) =
            (info.offsets(rowId - baseLength), info.lengths(rowId - baseLength))
          row.pointTo(null, info.memoryAddress + offset, length)
          rowId += 1
          row
        }
      }
    }

    def close(): Unit = {
      jniWrapper.nativeClose(c2rId)
    }
  }
}

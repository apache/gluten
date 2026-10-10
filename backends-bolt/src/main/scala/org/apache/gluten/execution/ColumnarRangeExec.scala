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

import org.apache.gluten.backendsapi.arrow.ArrowBatchTypes.ArrowJavaBatchType
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.vectorized.ArrowWritableColumnVector

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical.{Range => LogicalRange}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, RangePartitioning, SinglePartition, UnknownPartitioning}
import org.apache.spark.sql.execution.{ColumnarRangeBaseExec, SparkPlan}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * ColumnarRangeExec is a concrete implementation of ColumnarRangeBaseExec that executes the Range
 * operation and supports columnar processing. It generates columnar batches for the specified
 * range.
 *
 * @param range
 *   The logical Range plan containing start, end, step, and numSlices information.
 */
case class ColumnarRangeExec(range: LogicalRange) extends ColumnarRangeBaseExec {

  override def outputOrdering: Seq[SortOrder] = range.outputOrdering

  override def outputPartitioning: Partitioning = {
    if (numElements > 0) {
      if (numSlices == 1) {
        SinglePartition
      } else {
        RangePartitioning(outputOrdering, numSlices)
      }
    } else {
      UnknownPartitioning(0)
    }
  }

  override def doCanonicalize(): SparkPlan = {
    ColumnarRangeExec(range.canonicalized.asInstanceOf[LogicalRange])
  }

  override def batchType(): Convention.BatchType = {
    ArrowJavaBatchType
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (isEmptyRange) {
      sparkContext.emptyRDD[ColumnarBatch]
    } else {
      sparkContext
        .parallelize(0 until numSlices, numSlices)
        .mapPartitionsWithIndex {
          (partitionIndex, _) =>
            val batchSize = 1000
            val safePartitionStart = (partitionIndex) * numElements / numSlices * step + start
            val safePartitionEnd = (partitionIndex + 1) * numElements / numSlices * step + start

            def getSafeMargin(value: BigInt): Long =
              if (value.isValidLong) value.toLong
              else if (value > 0) Long.MaxValue
              else Long.MinValue

            /**
             * Generates the columnar batches for the specified range. Each batch contains a subset
             * of the range values, managed using Arrow column vectors.
             */
            val iterator = new Iterator[ColumnarBatch] {
              var current = safePartitionStart

              override def hasNext: Boolean = {
                if (step > 0) {
                  current < safePartitionEnd
                } else {
                  current > safePartitionEnd
                }
              }

              override def next(): ColumnarBatch = {
                val numRows = math.min(
                  ((safePartitionEnd - current) / step).toInt.max(1),
                  batchSize
                )

                val vectors = ArrowWritableColumnVector.allocateColumns(numRows, schema)

                for (i <- 0 until numRows) {
                  val value = current + i * step
                  vectors(0).putLong(i, getSafeMargin(value))
                }
                vectors.foreach(_.setValueCount(numRows))
                current += numRows * step

                val batch = new ColumnarBatch(vectors.asInstanceOf[Array[ColumnVector]], numRows)
                batch
              }
            }
            Iterators
              .wrap(iterator)
              .recyclePayload(
                batch => {
                  batch.close()
                })
              .create()

        }
    }
  }

  override protected def doExecute(): RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    throw new UnsupportedOperationException("doExecute is not supported for this operator")
  }
}

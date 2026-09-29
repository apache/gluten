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

import org.apache.gluten.columnarbatch.BoltColumnarBatches
import org.apache.gluten.columnarbatch.ColumnarBatches

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.vectorized.ColumnarBatch

case class ColumnarCollectLimitExec(
    limit: Int,
    child: SparkPlan,
    offset: Int = 0
) extends ColumnarCollectLimitBaseExec(limit, child, offset) {

  /**
   * Returns an iterator that gives offset to limit rows in total from the input partitionIter.
   * Either retain the entire batch if it fits within the remaining limit, or prune it if it
   * partially exceeds the remaining limit/offset.
   */
  override def collectWithOffsetAndLimit(
      inputIter: Iterator[ColumnarBatch],
      offset: Int,
      limit: Int): Iterator[ColumnarBatch] = {

    val unlimited = limit < 0
    var rowsToSkip = math.max(offset, 0)
    var rowsToCollect = if (unlimited) Int.MaxValue else limit

    new Iterator[ColumnarBatch] {
      private var nextBatch: Option[ColumnarBatch] = None

      override def hasNext: Boolean = {
        nextBatch.isDefined || fetchNextBatch()
      }

      override def next(): ColumnarBatch = {
        if (!hasNext) throw new NoSuchElementException("No more batches available.")
        val batch = nextBatch.get
        nextBatch = None
        batch
      }

      /**
       * Advance the iterator until we find a batch (possibly sliced) that we can return, or exhaust
       * the input.
       */
      private def fetchNextBatch(): Boolean = {

        if (rowsToCollect <= 0) return false

        while (inputIter.hasNext) {
          val batch = inputIter.next()
          val batchSize = batch.numRows()

          if (rowsToSkip >= batchSize) {
            rowsToSkip -= batchSize
          } else {
            val startIndex = rowsToSkip
            val leftoverAfterSkip = batchSize - startIndex
            rowsToSkip = 0

            val needed = math.min(rowsToCollect, leftoverAfterSkip)

            val prunedBatch =
              if (startIndex == 0 && needed == batchSize) {
                ColumnarBatches.retain(batch)
                batch
              } else {
                BoltColumnarBatches.slice(batch, startIndex, needed)
              }

            rowsToCollect -= needed
            nextBatch = Some(prunedBatch)
            return true
          }
        }
        false
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

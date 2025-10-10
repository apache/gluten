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

import scala.collection.mutable
import scala.util.control.Breaks._

case class ColumnarCollectTailExec(
    limit: Int,
    child: SparkPlan
) extends ColumnarCollectTailBaseExec(limit, child) {

  override protected def collectTailRows(
      partitionIter: Iterator[ColumnarBatch],
      limit: Int
  ): Iterator[ColumnarBatch] = {
    if (!partitionIter.hasNext || limit <= 0) {
      return Iterator.empty
    }

    val tailQueue = new mutable.ListBuffer[ColumnarBatch]()
    var totalRowsInTail = 0L

    while (partitionIter.hasNext) {
      val batch = partitionIter.next()
      val batchRows = batch.numRows()
      ColumnarBatches.retain(batch)
      tailQueue += batch
      totalRowsInTail += batchRows

      breakable {
        while (tailQueue.nonEmpty) {
          val front = tailQueue.head
          val frontRows = front.numRows()

          if (totalRowsInTail - frontRows >= limit) {
            val dropped = tailQueue.remove(0)
            dropped.close()
            totalRowsInTail -= frontRows
          } else {
            break
          }
        }
      }
    }

    val overflow = totalRowsInTail - limit
    if (overflow > 0) {
      val first = tailQueue.remove(0)
      val keep = first.numRows() - overflow
      val sliced = BoltColumnarBatches.slice(first, overflow.toInt, keep.toInt)
      tailQueue.prepend(sliced)
      first.close()
    }

    tailQueue.iterator
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

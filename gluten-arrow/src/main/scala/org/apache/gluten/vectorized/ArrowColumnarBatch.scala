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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.vectorized.ColumnVector

/**
 * A `ColumnarBatch`-like container of [[ArrowWritableColumnVector]]s. Originally forked from
 * Spark's `ColumnarBatch` before Gluten could depend on that class directly; it is kept as a
 * standalone class rather than extending `ColumnarBatch` to preserve the writable-column contract
 * and existing call sites.
 *
 * @param writableColumns
 *   the columns this class wraps
 * @param rowNumbers
 *   the number of rows this batch contains
 */
class ArrowColumnarBatch(writableColumns: Array[ArrowWritableColumnVector], var rowNumbers: Int) {
  private val arrowColumnarRow = new ArrowColumnarRow(writableColumns)

  /**
   * Called to close all the columns in this batch. It is not valid to access the data after calling
   * this. This must be called at the end to clean up memory allocations.
   */
  def close(): Unit = {
    for (c <- writableColumns) {
      c.close()
    }
  }

  /** Returns an iterator over the rows in this batch. */
  def rowIterator: Iterator[InternalRow] = {
    val maxRows = numRows
    val row = new ArrowColumnarRow(writableColumns)
    new Iterator[InternalRow]() {
      var rowId = 0

      override def hasNext: Boolean = rowId < maxRows

      override def next: InternalRow = {
        if (rowId >= maxRows) {
          throw new NoSuchElementException()
        }
        row.rowId = rowId
        rowId = rowId + 1
        row
      }
    }
  }

  /** Sets the number of rows in this batch. */
  def setNumRows(numRows: Int): Unit = {
    this.rowNumbers = numRows
  }

  /** Returns the number of columns that make up this batch. */
  def numCols: Int = writableColumns.length

  /** Returns the number of rows for read, including filtered rows. */
  def numRows: Int = this.rowNumbers

  /** Returns the column at `ordinal`. */
  def column(ordinal: Int): ColumnVector = writableColumns(ordinal)

  def getRow(rowId: Int): InternalRow = {
    assert(rowId >= 0 && rowId < this.numRows)
    arrowColumnarRow.rowId = rowId
    arrowColumnarRow
  }
}

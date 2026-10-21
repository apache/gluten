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

import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}
import org.apache.arrow.vector.{BaseFixedWidthVector, BitVector, FieldVector, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.message.ArrowFieldNode

import scala.collection.JavaConverters._

/**
 * Zeroes the values under nulls in fixed-width columns.
 *
 * Arrow leaves the value of a null slot undefined, and Velox exports whatever its buffers hold
 * there. Lance counts those slots when it picks an encoding, so leftover values widen bit-packed
 * integers and defeat dictionaries, and the files grow. Spark's ArrowWriter, which lance-spark's
 * row writer uses, leaves zeros. The exported buffers belong to Velox and may be shared with other
 * vectors, so they are never written: a column with a non-zero null slot is copied, and the copy is
 * zeroed.
 */
object LanceNullSlots {

  /** Zeroes the null slots of the fixed-width vectors in `root`, nested ones included. */
  def zero(root: VectorSchemaRoot, allocator: BufferAllocator): Unit =
    root.getFieldVectors.asScala.foreach(zero(_, allocator))

  /**
   * The bytes of a validity bitmap of `bits` bits. Arrow 19 renamed BitVectorHelper's
   * getValidityBufferSize, so no one name works across the Arrow versions Gluten builds with.
   */
  private[write] def validityBytes(bits: Int): Int = ((bits + 7L) / 8).toInt

  private def zero(vector: FieldVector, allocator: BufferAllocator): Unit = {
    vector match {
      // Booleans are single bits, so leftover values cost Lance next to nothing.
      case _: BitVector =>
      case fixed: BaseFixedWidthVector if fixed.getNullCount > 0 && hasNonZeroNullSlot(fixed) =>
        replaceWithZeroedCopy(fixed, allocator)
      case _ =>
    }
    vector.getChildrenFromFields.asScala.foreach(zero(_, allocator))
  }

  private def hasNonZeroNullSlot(vector: BaseFixedWidthVector): Boolean = {
    val data = vector.getDataBuffer
    val width = vector.getTypeWidth
    var found = false
    foreachNull(vector.getValidityBuffer, vector.getValueCount) {
      row =>
        found = !isZero(data, row.toLong * width, width)
        !found
    }
    found
  }

  private def isZero(buffer: ArrowBuf, offset: Long, width: Int): Boolean = {
    var i = 0
    while (i + 8 <= width) {
      if (buffer.getLong(offset + i) != 0) {
        return false
      }
      i += 8
    }
    while (i < width) {
      if (buffer.getByte(offset + i) != 0) {
        return false
      }
      i += 1
    }
    true
  }

  /** Swaps the buffers of `vector` for a copy whose null slots are zero. */
  private def replaceWithZeroedCopy(
      vector: BaseFixedWidthVector,
      allocator: BufferAllocator): Unit = {
    val rows = vector.getValueCount
    val width = vector.getTypeWidth.toLong
    val bitmapBytes = validityBytes(rows)
    val copy = vector.getField.createVector(allocator).asInstanceOf[BaseFixedWidthVector]
    // Raw buffers rather than allocateNew(), which would zero the data only for it to be copied over.
    val validity = allocator.buffer(bitmapBytes)
    val data = allocator.buffer(rows * width)
    try {
      validity.setBytes(0, vector.getValidityBuffer, 0, bitmapBytes)
      data.setBytes(0, vector.getDataBuffer, 0, rows * width)
      // Zeroes each run of consecutive nulls at once.
      var runStart = -1
      var runEnd = -1
      foreachNull(validity, rows) {
        row =>
          if (row != runEnd) {
            if (runStart >= 0) {
              data.setZero(runStart * width, (runEnd - runStart) * width)
            }
            runStart = row
          }
          runEnd = row + 1
          true
      }
      if (runStart >= 0) {
        data.setZero(runStart * width, (runEnd - runStart) * width)
      }
      copy.loadFieldBuffers(
        new ArrowFieldNode(rows, vector.getNullCount),
        List(validity, data).asJava)
      // Releases the exported buffers and takes the copy's.
      copy.makeTransferPair(vector).transfer()
    } finally {
      // loadFieldBuffers() took its own references.
      validity.close()
      data.close()
      copy.close()
    }
  }

  /**
   * Calls `f` with each null slot of a bitmap of `rows` bits, in order, until it returns false. The
   * bitmap is read a word at a time, so words without nulls cost one read.
   */
  private def foreachNull(validity: ArrowBuf, rows: Int)(f: Int => Boolean): Unit = {
    val bytes = validityBytes(rows)
    var word = 0
    while (word * 64 < rows) {
      val first = word * 64
      var nulls = ~readWord(validity, word * 8L, bytes)
      if (rows - first < 64) {
        nulls &= (1L << (rows - first)) - 1
      }
      while (nulls != 0) {
        if (!f(first + java.lang.Long.numberOfTrailingZeros(nulls))) {
          return
        }
        nulls &= nulls - 1
      }
      word += 1
    }
  }

  /** The 8 bitmap bytes at `offset`, little endian, reading none past the bitmap's `bytes`. */
  private def readWord(bitmap: ArrowBuf, offset: Long, bytes: Int): Long = {
    if (offset + 8 <= bytes) {
      bitmap.getLong(offset)
    } else {
      var value = 0L
      var i = 0
      while (offset + i < bytes) {
        value |= (bitmap.getByte(offset + i) & 0xffL) << (8 * i)
        i += 1
      }
      value
    }
  }
}

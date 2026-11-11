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

import org.apache.arrow.vector.{BaseFixedWidthVector, BitVectorHelper, VectorSchemaRoot}
import org.apache.arrow.vector.complex.{FixedSizeListVector, ListVector}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import scala.collection.JavaConverters._

/**
 * Velox has no fixed-size list type, so it exports Lance's fixed-size list columns, such as
 * vectors, as plain lists. A batch with such columns is imported with the schema Velox exports and
 * its lists are copied into the fixed-size list vectors of the commit schema. Only top-level
 * fixed-size lists are converted; the write falls back for nested ones.
 */
object LanceFixedSizeLists {

  /** The schema Velox exports for `commitSchema`: its top-level fixed-size lists as plain lists. */
  def exportedSchema(commitSchema: Schema): Schema = {
    val fields = commitSchema.getFields.asScala.map {
      field =>
        field.getType match {
          case _: ArrowType.FixedSizeList =>
            val fieldType =
              new FieldType(field.isNullable, ArrowType.List.INSTANCE, null, field.getMetadata)
            new Field(field.getName, fieldType, field.getChildren)
          case _ => field
        }
    }
    new Schema(fields.asJava, commitSchema.getCustomMetadata)
  }

  /**
   * Moves the rows of `from`, shaped like [[exportedSchema]], into `to`, shaped like the commit
   * schema. Plain lists are copied into fixed-size lists, and every other column is transferred.
   */
  def moveInto(from: VectorSchemaRoot, to: VectorSchemaRoot): Unit = {
    val rows = from.getRowCount
    from.getFieldVectors.asScala.zip(to.getFieldVectors.asScala).foreach {
      case (list: ListVector, fixed: FixedSizeListVector) =>
        copyLists(list, fixed, rows)
        // Free the exported lists now rather than at the next import, so a batch does not hold
        // both copies of its vectors while Lance encodes it.
        list.clear()
      case (source, target) => source.makeTransferPair(target).transfer()
    }
    to.setRowCount(rows)
  }

  private def copyLists(source: ListVector, target: FixedSizeListVector, rows: Int): Unit = {
    val size = target.getListSize
    target.clear()
    target.setInitialCapacity(rows)
    target.allocateNew()
    // When every row holds a list, the elements are one contiguous run and can be moved as is.
    var contiguous = true
    var row = 0
    while (row < rows) {
      if (source.isNull(row)) {
        contiguous = false
      } else {
        val length = source.getElementEndIndex(row) - source.getElementStartIndex(row)
        if (length != size) {
          throw new IllegalArgumentException(
            s"Column ${target.getName} holds lists of $size elements, " +
              s"but row $row has $length elements")
        }
        target.setNotNull(row)
      }
      row += 1
    }
    (source.getDataVector, target.getDataVector) match {
      case (sourceData, targetData) if contiguous =>
        if (rows > 0) {
          sourceData
            .makeTransferPair(targetData)
            .splitAndTransfer(source.getElementStartIndex(0), rows * size)
        }
      case (sourceData: BaseFixedWidthVector, targetData: BaseFixedWidthVector)
          if sourceData.getNullCount == 0 =>
        copyRows(source, sourceData, targetData, rows, size)
      case (sourceData, targetData) =>
        // A null row still takes `size` element slots, left null.
        row = 0
        while (row < rows) {
          if (!source.isNull(row)) {
            val start = source.getElementStartIndex(row)
            var i = 0
            while (i < size) {
              targetData.copyFromSafe(start + i, row * size + i, sourceData)
              i += 1
            }
          }
          row += 1
        }
    }
    target.setValueCount(rows)
  }

  /**
   * Copies the elements of each row with one memory copy, for elements without nulls. The elements
   * of a null row are zero and null, as lance-spark's writer leaves them.
   */
  private def copyRows(
      lists: ListVector,
      source: BaseFixedWidthVector,
      target: BaseFixedWidthVector,
      rows: Int,
      size: Int): Unit = {
    val rowBytes = size.toLong * source.getTypeWidth
    val sourceBuffer = source.getDataBuffer
    val targetBuffer = target.getDataBuffer
    val validity = target.getValidityBuffer
    validity.setOne(0, LanceNullSlots.validityBytes(rows * size))
    var row = 0
    while (row < rows) {
      if (lists.isNull(row)) {
        targetBuffer.setZero(row * rowBytes, rowBytes)
        var element = row * size
        while (element < (row + 1) * size) {
          BitVectorHelper.unsetBit(validity, element)
          element += 1
        }
      } else {
        val start = lists.getElementStartIndex(row) * source.getTypeWidth.toLong
        targetBuffer.setBytes(row * rowBytes, sourceBuffer, start, rowBytes)
      }
      row += 1
    }
  }
}

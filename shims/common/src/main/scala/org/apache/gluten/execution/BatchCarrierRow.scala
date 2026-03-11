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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * An internal-row abstraction that is designed for columnar-based computations to bypass Spark's
 * row-based APIs / SPIs with zero copy.
 *
 * Two implementations are pre-defined:
 *
 *   - TerminalRow
 *   - PlaceholderRow
 *
 * To bypass Spark's row APIs, one single columnar batch will be converted to a series of
 * PlaceholderRows, followed by one TerminalRow that actually wraps that columnar batch. The total
 * number of PlaceholderRows + the TerminalRow equates to the size of the original columnar batch.
 */
sealed abstract class BatchCarrierRow extends InternalRowSparkCompatible {
  override def numFields: Int = throw unsupported()

  override def setNullAt(i: Int): Unit = throw unsupported()

  override def update(i: Int, value: Any): Unit = throw unsupported()

  override def copy(): InternalRow = throw unsupported()

  override def isNullAt(ordinal: Int): Boolean = throw unsupported()

  override def getBoolean(ordinal: Int): Boolean = throw unsupported()

  override def getByte(ordinal: Int): Byte = throw unsupported()

  override def getShort(ordinal: Int): Short = throw unsupported()

  override def getInt(ordinal: Int): Int = throw unsupported()

  override def getLong(ordinal: Int): Long = throw unsupported()

  override def getFloat(ordinal: Int): Float = throw unsupported()

  override def getDouble(ordinal: Int): Double = throw unsupported()

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = throw unsupported()

  override def getUTF8String(ordinal: Int): UTF8String = throw unsupported()

  override def getBinary(ordinal: Int): Array[Byte] = throw unsupported()

  override def getInterval(ordinal: Int): CalendarInterval = throw unsupported()

  override def getStruct(ordinal: Int, numFields: Int): InternalRow = throw unsupported()

  override def getArray(ordinal: Int): ArrayData = throw unsupported()

  override def getMap(ordinal: Int): MapData = throw unsupported()

  override def get(ordinal: Int, dataType: DataType): AnyRef = throw unsupported()

  private def unsupported() = {
    new UnsupportedOperationException(
      "Underlying columnar data is inaccessible from BatchCarrierRow")
  }
}

object BatchCarrierRow {
  def unwrap(row: InternalRow): Option[ColumnarBatch] = row match {
    case _: PlaceholderRow => None
    case t: TerminalRow => Some(t.batch())
    case _ =>
      throw new UnsupportedOperationException(
        s"Row $row is not a ${classOf[BatchCarrierRow].getSimpleName}")
  }
}

/**
 * A [[BatchCarrierRow]] implementation that is backed by a
 * [[org.apache.spark.sql.vectorized.ColumnarBatch]].
 *
 * Serialization code originated since https://github.com/apache/incubator-gluten/issues/9270.
 */
abstract class TerminalRow extends BatchCarrierRow {
  def batch(): ColumnarBatch
  def withNewBatch(batch: ColumnarBatch): TerminalRow
}

/**
 * A [[BatchCarrierRow]] implementation with no data. The only function of this row implementation
 * is to provide row metadata to the receiver and to support correct row-counting.
 */
class PlaceholderRow extends BatchCarrierRow {
  override def copy(): InternalRow = new PlaceholderRow()
}

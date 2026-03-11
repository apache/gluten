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

import org.apache.gluten.exception.GlutenException
import org.apache.gluten.execution.InternalRowSparkCompatible

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

import java.math.BigDecimal

// Copy from Spark MutableColumnarRow mostly but class member columns' type is
// ArrowWritableColumnVector. And support string and binary type to write,
// Arrow writer does not need to setNotNull before writing a value.
final class ArrowColumnarRow(writableColumns: Array[ArrowWritableColumnVector], var rowId: Int = 0)
  extends InternalRowSparkCompatible {

  private val columns: Array[ArrowWritableColumnVector] = writableColumns

  override def numFields(): Int = columns.length

  override def copy(): InternalRow = {
    val row = new GenericInternalRow(columns.length)
    var i = 0
    while (i < numFields()) {
      if (isNullAt(i)) {
        row.setNullAt(i)
      } else {
        val dt = columns(i).dataType()
        dt match {
          case _: BooleanType => row.setBoolean(i, getBoolean(i))
          case _: ByteType => row.setByte(i, getByte(i))
          case _: ShortType => row.setShort(i, getShort(i))
          case _: IntegerType => row.setInt(i, getInt(i))
          case _: LongType => row.setLong(i, getLong(i))
          case _: FloatType => row.setFloat(i, getFloat(i))
          case _: DoubleType => row.setDouble(i, getDouble(i))
          case _: StringType => row.update(i, getUTF8String(i).copy())
          case _: BinaryType => row.update(i, getBinary(i))
          case t: DecimalType => row.setDecimal(i, getDecimal(i, t.precision, t.scale), t.precision)
          case _: DateType => row.setInt(i, getInt(i))
          case _: TimestampType => row.setLong(i, getLong(i))
          case s: StructType => row.update(i, getStruct(i, s.fields.length).copy())
          case _: ArrayType => row.update(i, getArray(i).copy())
          case _: MapType => row.update(i, getMap(i).copy())
          case _ => throw new RuntimeException(s"Not implemented. $dt")
        }
      }
      i += 1
    }
    row
  }

  override def anyNull(): Boolean =
    throw new UnsupportedOperationException()

  override def isNullAt(ordinal: Int): Boolean =
    columns(ordinal).isNullAt(rowId)

  override def getBoolean(ordinal: Int): Boolean =
    columns(ordinal).getBoolean(rowId)

  override def getByte(ordinal: Int): Byte =
    columns(ordinal).getByte(rowId)

  override def getShort(ordinal: Int): Short =
    columns(ordinal).getShort(rowId)

  override def getInt(ordinal: Int): Int =
    columns(ordinal).getInt(rowId)

  override def getLong(ordinal: Int): Long =
    columns(ordinal).getLong(rowId)

  override def getFloat(ordinal: Int): Float =
    columns(ordinal).getFloat(rowId)

  override def getDouble(ordinal: Int): Double =
    columns(ordinal).getDouble(rowId)

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal =
    columns(ordinal).getDecimal(rowId, precision, scale)

  override def getUTF8String(ordinal: Int): UTF8String =
    columns(ordinal).getUTF8String(rowId)

  override def getBinary(ordinal: Int): Array[Byte] =
    columns(ordinal).getBinary(rowId)

  override def getInterval(ordinal: Int): CalendarInterval =
    columns(ordinal).getInterval(rowId)

  override def getStruct(ordinal: Int, numFields: Int): ArrowColumnarRow =
    columns(ordinal).getStructInternal(rowId)

  override def getArray(ordinal: Int): ArrayData =
    columns(ordinal).getArrayInternal(rowId)

  override def getMap(ordinal: Int): MapData =
    columns(ordinal).getMapInternal(rowId)

  override def get(ordinal: Int, dataType: DataType): AnyRef = {
    if (isNullAt(ordinal)) {
      return null
    }
    dataType match {
      case _: BooleanType => java.lang.Boolean.valueOf(getBoolean(ordinal))
      case _: ByteType => java.lang.Byte.valueOf(getByte(ordinal))
      case _: ShortType => java.lang.Short.valueOf(getShort(ordinal))
      case _: IntegerType => java.lang.Integer.valueOf(getInt(ordinal))
      case _: LongType => java.lang.Long.valueOf(getLong(ordinal))
      case _: FloatType => java.lang.Float.valueOf(getFloat(ordinal))
      case _: DoubleType => java.lang.Double.valueOf(getDouble(ordinal))
      case _: StringType => getUTF8String(ordinal)
      case _: BinaryType => getBinary(ordinal)
      case t: DecimalType => getDecimal(ordinal, t.precision, t.scale)
      case _: DateType => java.lang.Integer.valueOf(getInt(ordinal))
      case _: TimestampType => java.lang.Long.valueOf(getLong(ordinal))
      case _: ArrayType => getArray(ordinal)
      case s: StructType => getStruct(ordinal, s.fields.length)
      case _: MapType => getMap(ordinal)
      case _ => throw new UnsupportedOperationException(s"Datatype not supported $dataType")
    }
  }

  override def update(ordinal: Int, value: Any): Unit = {
    if (value == null) {
      setNullAt(ordinal)
    } else {
      val dt = columns(ordinal).dataType()
      dt match {
        case _: BooleanType =>
          setBoolean(ordinal, value.asInstanceOf[Boolean])
        case _: IntegerType =>
          setInt(ordinal, value.asInstanceOf[Int])
        case _: ShortType =>
          setShort(ordinal, value.asInstanceOf[Short])
        case _: LongType =>
          setLong(ordinal, value.asInstanceOf[Long])
        case _: FloatType =>
          setFloat(ordinal, value.asInstanceOf[Float])
        case _: DoubleType =>
          setDouble(ordinal, value.asInstanceOf[Double])
        case t: DecimalType =>
          val d = Decimal.apply(value.asInstanceOf[BigDecimal], t.precision, t.scale)
          setDecimal(ordinal, d, t.precision)
        case _: CalendarIntervalType =>
          setInterval(ordinal, value.asInstanceOf[CalendarInterval])
        case _: StringType =>
          setUTF8String(ordinal, value.asInstanceOf[UTF8String])
        case _: BinaryType =>
          setBinary(ordinal, value.asInstanceOf[Array[Byte]])
        case _ =>
          throw new UnsupportedOperationException(s"Datatype not supported $dt")
      }
    }
  }

  override def setNullAt(ordinal: Int): Unit =
    columns(ordinal).putNull(rowId)

  override def setBoolean(ordinal: Int, value: Boolean): Unit =
    columns(ordinal).putBoolean(rowId, value)

  override def setByte(ordinal: Int, value: Byte): Unit =
    columns(ordinal).putByte(rowId, value)

  override def setShort(ordinal: Int, value: Short): Unit =
    columns(ordinal).putShort(rowId, value)

  override def setInt(ordinal: Int, value: Int): Unit =
    columns(ordinal).putInt(rowId, value)

  override def setLong(ordinal: Int, value: Long): Unit =
    columns(ordinal).putLong(rowId, value)

  override def setFloat(ordinal: Int, value: Float): Unit =
    columns(ordinal).putFloat(rowId, value)

  override def setDouble(ordinal: Int, value: Double): Unit =
    columns(ordinal).putDouble(rowId, value)

  override def setDecimal(ordinal: Int, value: Decimal, precision: Int): Unit =
    columns(ordinal).putDecimal(rowId, value, precision)

  override def setInterval(ordinal: Int, value: CalendarInterval): Unit =
    columns(ordinal).putInterval(rowId, value)

  def setUTF8String(ordinal: Int, value: UTF8String): Unit =
    columns(ordinal).putBytes(rowId, value.numBytes(), value.getBytes, 0)

  def setBinary(ordinal: Int, value: Array[Byte]): Unit =
    columns(ordinal).putBytes(rowId, value.length, value, 0)

  def writeRow(input: GenericInternalRow): Unit = {
    if (input.numFields != columns.length) {
      throw new GlutenException(
        "The numFields of input row should be equal to the number of column vector!"
      )
    }
    var i = 0
    while (i < input.numFields) {
      columns(i).write(input, i)
      i += 1
    }
  }

  def writeRowUnsafe(input: InternalRow): Unit = {
    if (input.numFields != columns.length) {
      throw new GlutenException(
        "The numFields of input row should be equal to the number of column vector!")
    }
    for (i <- 0 until input.numFields) {
      columns(i).writeUnsafe(input, i)
    }
  }

  def finishWriteRow(): Unit = {
    var i = 0
    while (i < columns.length) {
      columns(i).finishWrite()
      i += 1
    }
  }
}

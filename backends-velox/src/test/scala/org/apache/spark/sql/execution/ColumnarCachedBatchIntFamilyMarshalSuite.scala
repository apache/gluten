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
package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{ByteType, DateType, DayTimeIntervalType, IntegerType, LongType, ShortType, StructField, StructType, TimestampNTZType, TimestampType, YearMonthIntervalType}

import org.scalatest.funsuite.AnyFunSuite

/**
 * Integer-family marshal tests (INT / SMALLINT / TINYINT / Date / interval). JVM-only; pins
 * serialize/deserialize dispatch by source-column dataType.
 */
class ColumnarCachedBatchIntFamilyMarshalSuite extends AnyFunSuite {

  test("INT round-trip 4B LE preserves value") {
    val lo: Integer = Int.box(-2147483)
    val hi: Integer = Int.box(2147483)
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](lo, hi, 0, 100, 400L))
    val schema = StructType(Seq(
      StructField("k.lowerBound", IntegerType, nullable = true),
      StructField("k.upperBound", IntegerType, nullable = true),
      StructField("k.nullCount", IntegerType, nullable = false),
      StructField("k.count", IntegerType, nullable = false),
      StructField("k.sizeInBytes", LongType, nullable = false)
    ))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, schema)
    val read = CachedColumnarBatchKryoSerializer.deserializeStats(blob, schema)
    assert(read.getInt(0) == lo, s"lower bound corrupted: expected $lo got ${read.getInt(0)}")
    assert(read.getInt(1) == hi, s"upper bound corrupted: expected $hi got ${read.getInt(1)}")
    assert(read.getInt(2) == 0, "nullCount roundtrip")
    assert(read.getInt(3) == 100, "count roundtrip")
    assert(read.getLong(4) == 400L, "sizeInBytes roundtrip")
  }

  test("SMALLINT round-trip 2B LE preserves value (incl negative)") {
    val lo: java.lang.Short = Short.box((-12345).toShort)
    val hi: java.lang.Short = Short.box(12345.toShort)
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](lo, hi, 0, 200, 400L))
    val schema = StructType(Seq(
      StructField("k.lowerBound", ShortType, nullable = true),
      StructField("k.upperBound", ShortType, nullable = true),
      StructField("k.nullCount", IntegerType, nullable = false),
      StructField("k.count", IntegerType, nullable = false),
      StructField("k.sizeInBytes", LongType, nullable = false)
    ))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, schema)
    val read = CachedColumnarBatchKryoSerializer.deserializeStats(blob, schema)
    assert(read.getShort(0) == lo, s"lower: expected $lo got ${read.getShort(0)}")
    assert(read.getShort(1) == hi, s"upper: expected $hi got ${read.getShort(1)}")
    assert(read.getInt(3) == 200, "count roundtrip")
  }

  test("TINYINT round-trip 1B preserves value (incl negative)") {
    val lo: java.lang.Byte = Byte.box((-128).toByte)
    val hi: java.lang.Byte = Byte.box(127.toByte)
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](lo, hi, 0, 50, 50L))
    val schema = StructType(
      Seq(
        StructField("k.lowerBound", ByteType, nullable = true),
        StructField("k.upperBound", ByteType, nullable = true),
        StructField("k.nullCount", IntegerType, nullable = false),
        StructField("k.count", IntegerType, nullable = false),
        StructField("k.sizeInBytes", LongType, nullable = false)
      ))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, schema)
    val read = CachedColumnarBatchKryoSerializer.deserializeStats(blob, schema)
    assert(read.getByte(0) == lo, s"lower: expected $lo got ${read.getByte(0)}")
    assert(read.getByte(1) == hi, s"upper: expected $hi got ${read.getByte(1)}")
    assert(read.getInt(3) == 50, "count roundtrip")
  }

  test("YearMonthInterval round-trip 4B LE (months as Int)") {
    val lo: Integer = Int.box(-12)
    val hi: Integer = Int.box(36)
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](lo, hi, 0, 10, 40L))
    val schema = StructType(
      Seq(
        StructField("k.lowerBound", YearMonthIntervalType(), nullable = true),
        StructField("k.upperBound", YearMonthIntervalType(), nullable = true),
        StructField("k.nullCount", IntegerType, nullable = false),
        StructField("k.count", IntegerType, nullable = false),
        StructField("k.sizeInBytes", LongType, nullable = false)
      ))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, schema)
    val read = CachedColumnarBatchKryoSerializer.deserializeStats(blob, schema)
    assert(read.getInt(0) == lo, s"lower: expected $lo got ${read.getInt(0)}")
    assert(read.getInt(1) == hi, s"upper: expected $hi got ${read.getInt(1)}")
  }

  test("DayTimeInterval round-trip 8B LE (microseconds as Long)") {
    val lo: java.lang.Long = Long.box(-86400000000L)
    val hi: java.lang.Long = Long.box(86400000000L)
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](lo, hi, 0, 10, 80L))
    val schema = StructType(
      Seq(
        StructField("k.lowerBound", DayTimeIntervalType(), nullable = true),
        StructField("k.upperBound", DayTimeIntervalType(), nullable = true),
        StructField("k.nullCount", IntegerType, nullable = false),
        StructField("k.count", IntegerType, nullable = false),
        StructField("k.sizeInBytes", LongType, nullable = false)
      ))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, schema)
    val read = CachedColumnarBatchKryoSerializer.deserializeStats(blob, schema)
    assert(read.getLong(0) == lo, s"lower: expected $lo got ${read.getLong(0)}")
    assert(read.getLong(1) == hi, s"upper: expected $hi got ${read.getLong(1)}")
  }

  test("Date round-trip 4B LE (days since epoch as Int)") {
    val lo: Integer = Int.box(0) // 1970-01-01
    val hi: Integer = Int.box(20000) // ~2024
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](lo, hi, 0, 100, 400L))
    val schema = StructType(
      Seq(
        StructField("d.lowerBound", DateType, nullable = true),
        StructField("d.upperBound", DateType, nullable = true),
        StructField("d.nullCount", IntegerType, nullable = false),
        StructField("d.count", IntegerType, nullable = false),
        StructField("d.sizeInBytes", LongType, nullable = false)
      ))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, schema)
    val read = CachedColumnarBatchKryoSerializer.deserializeStats(blob, schema)
    assert(read.getInt(0) == lo)
    assert(read.getInt(1) == hi)
  }

  test("Timestamp round-trip 8B LE (microseconds as Long)") {
    val lo: java.lang.Long = Long.box(1700000000000000L)
    val hi: java.lang.Long = Long.box(1800000000000000L)
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](lo, hi, 0, 100, 800L))
    val schema = StructType(
      Seq(
        StructField("ts.lowerBound", TimestampType, nullable = true),
        StructField("ts.upperBound", TimestampType, nullable = true),
        StructField("ts.nullCount", IntegerType, nullable = false),
        StructField("ts.count", IntegerType, nullable = false),
        StructField("ts.sizeInBytes", LongType, nullable = false)
      ))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, schema)
    val read = CachedColumnarBatchKryoSerializer.deserializeStats(blob, schema)
    assert(read.getLong(0) == lo)
    assert(read.getLong(1) == hi)
  }

  test("TimestampNTZ round-trip 8B LE (microseconds as Long)") {
    val lo: java.lang.Long = Long.box(0L)
    val hi: java.lang.Long = Long.box(2000000000000000L)
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](lo, hi, 0, 100, 800L))
    val schema = StructType(
      Seq(
        StructField("tsntz.lowerBound", TimestampNTZType, nullable = true),
        StructField("tsntz.upperBound", TimestampNTZType, nullable = true),
        StructField("tsntz.nullCount", IntegerType, nullable = false),
        StructField("tsntz.count", IntegerType, nullable = false),
        StructField("tsntz.sizeInBytes", LongType, nullable = false)
      ))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, schema)
    val read = CachedColumnarBatchKryoSerializer.deserializeStats(blob, schema)
    assert(read.getLong(0) == lo)
    assert(read.getLong(1) == hi)
  }
}

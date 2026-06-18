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
import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, Decimal, DecimalType}
import org.apache.spark.sql.types.{DoubleType, FloatType, ShortType, TimestampType}
import org.apache.spark.sql.types.{StructField, StructType}

import org.scalatest.funsuite.AnyFunSuite

import java.nio.{ByteBuffer, ByteOrder}

/**
 * Tests for statsBlob binary framing (LE throughout, cpp-aligned).
 *
 * Wire (BIGINT 1-col): [ numCols: u32 LE ] per col [ supported: u8 | nullCount: u32 | count: u32 |
 * sizeInBytes: u64 ] if supported [ lowerLen: u32 | lower bytes | upperLen: u32 | upper bytes ].
 */
class ColumnarCachedBatchStatsBlobSuite extends AnyFunSuite {

  test("statsBlob LE numCols + BIGINT cell round-trip byte-for-byte") {
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](42L, 100L, 0, 10, 64L))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, null)

    // Hand-compute expected wire:
    //   numCols=1 LE = 01 00 00 00
    //   supported=1 = 01
    //   nullCount=0 LE = 00 00 00 00
    //   count=10 LE = 0A 00 00 00
    //   sizeInBytes=64 LE = 40 00 00 00 00 00 00 00
    //   lowerBoundLen=8 LE = 08 00 00 00
    //   lowerBound=42 LE  = 2A 00 00 00 00 00 00 00
    //   upperBoundLen=8 LE = 08 00 00 00
    //   upperBound=100 LE = 64 00 00 00 00 00 00 00
    val expected = Array[Byte](
      0x01, 0x00, 0x00, 0x00,
      0x01,
      0x00, 0x00, 0x00, 0x00,
      0x0a, 0x00, 0x00, 0x00,
      0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x08, 0x00, 0x00, 0x00,
      0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x08, 0x00, 0x00, 0x00,
      0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)
    assert(
      blob === expected,
      "statsBlob must match cpp-aligned LE wire format byte-for-byte")
  }

  test("serializeStats then deserializeStats round-trip BIGINT 1-col") {
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](-7L, 999L, 3, 100, 1024L))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, null)
    val read = CachedColumnarBatchKryoSerializer.deserializeStats(blob, null)

    assert(read.numFields === 5)
    assert(read.getLong(0) === -7L, "lowerBound at slot 0")
    assert(read.getLong(1) === 999L, "upperBound at slot 1")
    assert(read.getInt(2) === 3, "nullCount at slot 2")
    assert(read.getInt(3) === 100, "count at slot 3")
    assert(read.getLong(4) === 1024L, "sizeInBytes at slot 4")
  }

  test("corrupt statsBlob (numCols out of range) fails eagerly") {
    // Craft a blob claiming numCols=Int.MaxValue: 0xFF FF FF 7F
    val corruptNumCols = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
      .putInt(Int.MaxValue).array()
    val ex = intercept[IllegalArgumentException] {
      CachedColumnarBatchKryoSerializer.deserializeStats(corruptNumCols, null)
    }
    assert(
      ex.getMessage.contains("numCols"),
      s"expected numCols range guard, got: ${ex.getMessage}")
  }

  test("unsupported col round-trip preserves null bounds + metrics") {
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](null, null, 5, 50, 200L))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, null)
    val read = CachedColumnarBatchKryoSerializer.deserializeStats(blob, null)

    assert(read.isNullAt(0), "lowerBound must be null for unsupported col")
    assert(read.isNullAt(1), "upperBound must be null for unsupported col")
    assert(read.getInt(2) === 5)
    assert(read.getInt(3) === 50)
    assert(read.getLong(4) === 200L)
  }

  // SMALLINT: byte-for-byte wire pin.
  // upperBound=32767 -> LE 0xFF 0x7F.
  test("statsBlob SMALLINT cell round-trip byte-for-byte") {
    val stats: InternalRow = new GenericInternalRow(
      Array[Any]((-32768).toShort, 32767.toShort, 0, 5, 10L))
    val schema = StructType(Seq(StructField("a", ShortType)))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, schema)

    // numCols=1 + supported=1 + nullCount=0 + count=5 + sizeInBytes=10 +
    // lowerLen=2 + lowerBound=-32768 (LE 0x00 0x80) +
    // upperLen=2 + upperBound=32767 (LE 0xFF 0x7F)
    val expected = Array[Int](
      0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x0a, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x80, 0x02, 0x00, 0x00,
      0x00, 0xff, 0x7f
    ).map(_.toByte)
    assert(blob === expected, "SMALLINT statsBlob byte-for-byte")
  }

  // TINYINT wrong upperBound byte (0x7E instead of 0x7F).
  test("statsBlob TINYINT cell round-trip byte-for-byte") {
    val stats: InternalRow = new GenericInternalRow(
      Array[Any]((-128).toByte, 127.toByte, 0, 3, 8L))
    val schema = StructType(Seq(StructField("a", ByteType)))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, schema)

    // numCols=1 + supported=1 + nullCount=0 + count=3 + sizeInBytes=8 +
    // lowerLen=1 + lowerBound=-128 (0x80) +
    // upperLen=1 + upperBound=127 (0x7F)
    val expected = Array[Int](
      0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x08, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x80, 0x01, 0x00, 0x00, 0x00,
      0x7f
    ).map(_.toByte)
    assert(blob === expected, "TINYINT statsBlob byte-for-byte")
  }

  // HUGEINT (16B LE int128, two's-complement). DecimalType(20,0) lower=1,
  // upper=-1 exercises both sign-extension extremes. RED: wrong upper byte 0xFE.
  test("statsBlob HUGEINT cell round-trip byte-for-byte") {
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](Decimal(1L, 20, 0), Decimal(-1L, 20, 0), 0, 7, 32L))
    val schema = StructType(Seq(StructField("a", DecimalType(20, 0))))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, schema)

    // numCols=1 + supported=1 + nullCount=0 + count=7 + sizeInBytes=32 +
    // lowerLen=16 + lower=+1 as int128 LE (01 then 15x00) +
    // upperLen=16 + upper=-1 as int128 LE (16x FF)
    val expected = Array[Int](
      0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x20, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00,
      0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
      0xff
    ).map(_.toByte)
    assert(blob === expected, "HUGEINT statsBlob byte-for-byte")
  }

  // REAL (4B LE bit-pattern via Float.floatToRawIntBits).
  // 1.5f -> 0x3FC00000 -> LE 00 00 C0 3F.
  // -2.5f -> 0xC0200000 -> LE 00 00 20 C0.
  test("statsBlob REAL cell round-trip byte-for-byte") {
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](1.5f, -2.5f, 0, 2, 8L))
    val schema = StructType(Seq(StructField("a", FloatType)))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, schema)

    val expected = Array[Int](
      0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x08, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0xc0, 0x3f, 0x04,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0xc0
    ).map(_.toByte)
    assert(blob === expected, "REAL statsBlob byte-for-byte")
  }

  // DOUBLE (8B LE bit-pattern via Double.doubleToRawLongBits).
  // 1.5d -> 0x3FF8000000000000 -> LE 00 00 00 00 00 00 F8 3F.
  // -2.5d -> 0xC004000000000000 -> LE 00 00 00 00 00 00 04 C0.
  test("statsBlob DOUBLE cell round-trip byte-for-byte") {
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](1.5d, -2.5d, 0, 2, 16L))
    val schema = StructType(Seq(StructField("a", DoubleType)))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, schema)

    val expected = Array[Int](
      0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x10, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0xf8, 0x3f, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0xc0
    ).map(_.toByte)
    assert(blob === expected, "DOUBLE statsBlob byte-for-byte")
  }

  // BOOLEAN (1B 0/1). lower=false (0x00), upper=true (0x01).
  test("statsBlob BOOLEAN cell round-trip byte-for-byte") {
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](false, true, 0, 4, 1L))
    val schema = StructType(Seq(StructField("a", BooleanType)))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, schema)

    val expected = Array[Byte](
      0x01, 0x00, 0x00, 0x00,
      0x01,
      0x00, 0x00, 0x00, 0x00,
      0x04, 0x00, 0x00, 0x00,
      0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x01, 0x00, 0x00, 0x00,
      0x00, // lower=false
      0x01, 0x00, 0x00, 0x00,
      0x01)
    assert(blob === expected, "BOOLEAN statsBlob byte-for-byte")
  }

  // TIMESTAMP (8B LE micros via stats.getLong, dispatched via
  // LongType | TimestampType branch). lower=1_000_000L (1s) -> LE
  // 40 42 0F 00 00 00 00 00; upper=2_000_000L (2s) -> LE 80 84 1E 00 00 00 00 00.
  test("statsBlob TIMESTAMP cell round-trip byte-for-byte") {
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](1000000L, 2000000L, 0, 2, 16L))
    val schema = StructType(Seq(StructField("a", TimestampType)))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, schema)

    val expected = Array[Int](
      0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x10, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x40, 0x42, 0x0f, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x80, 0x84, 0x1e, 0x00, 0x00, 0x00, 0x00, 0x00
    ).map(_.toByte)
    assert(blob === expected, "TIMESTAMP statsBlob byte-for-byte")
  }

  // DATE (4B LE days-since-epoch via stats.getInt, dispatched via
  // IntegerType | DateType branch). lower=0 (1970-01-01); upper=20000 ->
  // 0x00004E20 -> LE 20 4E 00 00.
  test("statsBlob DATE cell round-trip byte-for-byte") {
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](0, 20000, 0, 3, 8L))
    val schema = StructType(Seq(StructField("a", DateType)))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, schema)

    val expected = Array[Byte](
      0x01, 0x00, 0x00, 0x00,
      0x01,
      0x00, 0x00, 0x00, 0x00,
      0x03, 0x00, 0x00, 0x00,
      0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x04, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00,
      0x04, 0x00, 0x00, 0x00,
      0x20, 0x4e, 0x00, 0x00)
    assert(blob === expected, "DATE statsBlob byte-for-byte")
  }

}

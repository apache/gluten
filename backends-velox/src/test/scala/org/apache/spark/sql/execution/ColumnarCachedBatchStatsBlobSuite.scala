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
}

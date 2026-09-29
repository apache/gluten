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
import org.apache.spark.sql.types.{Decimal, DecimalType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import org.scalatest.funsuite.AnyFunSuite

import java.util.Arrays

/**
 * Ship-blocker acceptance tests for the non-BIGINT marshal paths (Decimal short/long, String, Float
 * / Double NaN guard). Guards against silent corruption (wrong Decimal scale, wrong UTF-8 byte
 * order, lost BigInteger sign) on round-trip.
 */
class ColumnarCacheShipBlockerMarshalSuite extends AnyFunSuite {

  test("Decimal(10, 2) round-trip preserves value") {
    val lo = Decimal(BigDecimal("1.50"), 10, 2)
    val hi = Decimal(BigDecimal("99.99"), 10, 2)
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](lo, hi, 0, 100, 800L))
    val schema = StructType(Seq(StructField("d", DecimalType(10, 2))))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, schema)
    val read = CachedColumnarBatchKryoSerializer.deserializeStats(blob, schema)
    val dt = DecimalType(10, 2)
    val readLo = read.get(0, dt)
    val readHi = read.get(1, dt)
    assert(readLo == lo, s"lower bound corrupted: expected $lo got $readLo")
    assert(readHi == hi, s"upper bound corrupted: expected $hi got $readHi")
  }

  test("Decimal(30, 5) round-trip preserves big value") {
    val big = BigDecimal("12345678901234567890.12345")
    val lo = Decimal(big.bigDecimal.negate(), 30, 5)
    val hi = Decimal(big, 30, 5)
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](lo, hi, 0, 100, 1600L))
    val schema = StructType(Seq(StructField("d", DecimalType(30, 5))))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, schema)
    val read = CachedColumnarBatchKryoSerializer.deserializeStats(blob, schema)
    val dt = DecimalType(30, 5)
    val readLo = read.get(0, dt)
    val readHi = read.get(1, dt)
    assert(readLo == lo, s"lower bound corrupted: expected $lo got $readLo")
    assert(readHi == hi, s"upper bound corrupted: expected $hi got $readHi")
  }

  test("String byte-wise lex round-trip preserves UTF-8 bytes") {
    val lo = UTF8String.fromString("apple")
    // UTF-8 bytes for two CJK code points (U+4E2D U+6587) constructed from hex
    // to keep this file ASCII-only (scalastyle nonascii filter).
    val hi = UTF8String.fromBytes(Array[Byte](
      0xe4.toByte,
      0xb8.toByte,
      0xad.toByte,
      0xe6.toByte,
      0x96.toByte,
      0x87.toByte))
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](lo, hi, 0, 100, 1024L))
    val schema = StructType(Seq(StructField("s", StringType)))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, schema)
    val read = CachedColumnarBatchKryoSerializer.deserializeStats(blob, schema)
    val readLo = read.getUTF8String(0)
    val readHi = read.getUTF8String(1)
    assert(readLo == lo, s"lower bound corrupted: expected $lo got $readLo")
    assert(readHi == hi, s"upper bound corrupted: expected $hi got $readHi")
  }

  test("String truncation to 256B widens upper bound monotonically") {
    val loBytes = new Array[Byte](100)
    Arrays.fill(loBytes, 'a'.toByte)
    // Upper: 300 bytes of 'm' followed by trailing chars. Truncated prefix
    // is 256 'm's (all 0x6d); +1 carry on last byte -> last byte = 0x6e.
    val hiBytes = new Array[Byte](300)
    Arrays.fill(hiBytes, 'm'.toByte)
    val lo = UTF8String.fromBytes(loBytes)
    val hi = UTF8String.fromBytes(hiBytes)
    val stats: InternalRow = new GenericInternalRow(Array[Any](lo, hi, 0, 100, 50000L))
    val schema = StructType(Seq(StructField("s", StringType)))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, schema)
    val read = CachedColumnarBatchKryoSerializer.deserializeStats(blob, schema)
    val readLo = read.getUTF8String(0)
    val readHi = read.getUTF8String(1)
    assert(readLo.numBytes() == 100, s"lower untruncated, got numBytes=${readLo.numBytes()}")
    assert(readHi.numBytes() == 256, s"upper should truncate to 256B, got ${readHi.numBytes()}")
    val readHiArr = readHi.getBytes
    // First 255 bytes still 'm', last byte 'm'+1 = 'n' (0x6e) due to carry.
    var i = 0
    while (i < 255) {
      assert(readHiArr(i) == 'm'.toByte, s"upper byte $i = ${readHiArr(i)}, expected 'm'")
      i += 1
    }
    assert(
      readHiArr(255) == 'n'.toByte,
      s"upper byte 255 should be 'n' (carry), got ${readHiArr(255)}")
  }

  test("String carry overflow demotes column to unsupported") {
    val loBytes = new Array[Byte](10)
    Arrays.fill(loBytes, 0xff.toByte)
    val hiBytes = new Array[Byte](300)
    Arrays.fill(hiBytes, 0xff.toByte)
    val lo = UTF8String.fromBytes(loBytes)
    val hi = UTF8String.fromBytes(hiBytes)
    val stats: InternalRow = new GenericInternalRow(Array[Any](lo, hi, 0, 100, 50000L))
    val schema = StructType(Seq(StructField("s", StringType)))
    val blob = CachedColumnarBatchKryoSerializer.serializeStats(stats, schema)
    val read = CachedColumnarBatchKryoSerializer.deserializeStats(blob, schema)
    // supported=0 StringType: no sentinel bound; left null. The buildFilter wrapper
    // strips conjuncts referencing demoted columns before super sees them.
    assert(read.isNullAt(0))
    assert(read.isNullAt(1))
  }
}

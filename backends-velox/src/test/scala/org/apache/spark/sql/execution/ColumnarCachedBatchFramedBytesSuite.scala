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

import java.util.Locale

/**
 * Tests for JNI `serializeWithStats` framed-byte parser. Pure JVM; crafts framed bytes by hand via
 * serializeStats, then exercises parseFramedBytes round-trip + corrupt-magic / truncated-blob
 * guards.
 *
 * Layout: `[ STATS_FRAMED_MAGIC: 4B 0xFE 0xCA 0x53 0x02 ] [ statsLen: u32 LE ] [ statsBlob ] [
 * bytesLen: u32 LE ] [ bytesBlob ]`.
 */
class ColumnarCachedBatchFramedBytesSuite extends AnyFunSuite {

  private def craftFramed(stats: InternalRow, bytesBlob: Array[Byte]): Array[Byte] = {
    val statsBlob = CachedColumnarBatchKryoSerializer.serializeStats(stats, null)
    val out = new java.io.ByteArrayOutputStream()
    out.write(CachedColumnarBatchKryoSerializer.STATS_FRAMED_MAGIC)
    writeU32LE(out, statsBlob.length)
    out.write(statsBlob)
    writeU32LE(out, bytesBlob.length)
    out.write(bytesBlob)
    out.toByteArray
  }

  private def writeU32LE(out: java.io.ByteArrayOutputStream, v: Int): Unit = {
    out.write(v & 0xff)
    out.write((v >>> 8) & 0xff)
    out.write((v >>> 16) & 0xff)
    out.write((v >>> 24) & 0xff)
  }

  test("parseFramedBytes round-trip BIGINT 1-col stats + bytesBlob") {
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](42L, 100L, 0, 10, 64L))
    val payload = Array[Byte](1, 2, 3, 4, 5)
    val framed = craftFramed(stats, payload)

    val (parsedStats, parsedBytes) =
      CachedColumnarBatchKryoSerializer.parseFramedBytes(framed, null)

    assert(parsedBytes === payload, "bytesBlob round-trip exact")
    assert(parsedStats !== null, "stats round-trip non-null")
    assert(parsedStats.numFields === 5)
    assert(parsedStats.getLong(0) === 42L)
    assert(parsedStats.getLong(1) === 100L)
    assert(parsedStats.getInt(2) === 0)
    assert(parsedStats.getInt(3) === 10)
    assert(parsedStats.getLong(4) === 64L)
  }

  test("corrupt magic fails eagerly with clear message") {
    val payload = Array[Byte](1, 2, 3)
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](0L, 0L, 0, 1, 1L))
    val good = craftFramed(stats, payload)
    val bad = good.clone()
    bad(0) = 0x00.toByte // corrupt magic byte 0

    val ex = intercept[IllegalArgumentException] {
      CachedColumnarBatchKryoSerializer.parseFramedBytes(bad, null)
    }
    assert(
      ex.getMessage.toLowerCase(Locale.ROOT).contains("magic"),
      s"expected 'magic' in error, got: ${ex.getMessage}")
  }

  test("truncated framed bytes fails eagerly") {
    val payload = Array[Byte](1, 2, 3)
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](0L, 0L, 0, 1, 1L))
    val good = craftFramed(stats, payload)
    val truncated = good.take(8) // magic + statsLen only, no statsBlob

    intercept[Exception] {
      CachedColumnarBatchKryoSerializer.parseFramedBytes(truncated, null)
    }
  }
}

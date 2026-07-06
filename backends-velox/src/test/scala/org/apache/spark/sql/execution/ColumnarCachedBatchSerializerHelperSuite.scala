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

import org.apache.gluten.vectorized.ColumnarBatchSerializerJniWrapper

import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.types.{LongType, StructField, StructType}

import org.mockito.ArgumentMatchers.anyLong
import org.mockito.Mockito.{mock, when}
import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for `ColumnarCachedBatchSerializer` serialization helpers. Exercises the fast-path /
 * fallback catch arms with a Mockito-stubbed JNI wrapper, without requiring a Velox runtime or
 * native libraries.
 */
class ColumnarCachedBatchSerializerHelperSuite extends AnyFunSuite {

  private val structSchema: StructType =
    StructType(Seq(StructField("k", LongType, nullable = true)))

  // A canned legacy CachedBatch that the fallback closure produces.
  private val legacyCachedBytes: Array[Byte] = Array[Byte](7, 7, 7)

  private def newFallbackProbe(): (() => CachedBatch, () => Boolean) = {
    @volatile var called = false
    val closure: () => CachedBatch = () => {
      called = true
      CachedColumnarBatch(1, legacyCachedBytes.length, legacyCachedBytes, null, null)
    }
    (closure, () => called)
  }

  private def writeU32LE(out: java.io.ByteArrayOutputStream, v: Int): Unit = {
    out.write(v & 0xff)
    out.write((v >>> 8) & 0xff)
    out.write((v >>> 16) & 0xff)
    out.write((v >>> 24) & 0xff)
  }

  test("corrupt magic frame is absorbed into legacy fallback (stats=null)") {
    // 12 bytes: 4 bogus magic + 4-byte statsLen=0 + 4-byte bytesLen=0
    val corruptFramed: Array[Byte] = Array[Byte](0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    val jni = mock(classOf[ColumnarBatchSerializerJniWrapper])
    when(jni.serializeWithStats(anyLong())).thenReturn(corruptFramed)
    val (fallback, wasCalled) = newFallbackProbe()
    val cb = ColumnarCachedBatchSerializer.serializeOneBatchWithStats(
      jni,
      0L,
      1,
      structSchema,
      fallback)
    assert(wasCalled(), "fallback closure should be invoked on corrupt magic")
    assert(
      cb.asInstanceOf[CachedColumnarBatch].stats == null,
      "fallback CachedBatch should carry stats=null")
  }

  test("truncated framed bytes absorbed into legacy fallback") {
    val truncated: Array[Byte] = Array[Byte](
      0xfe.toByte,
      0xca.toByte,
      0x53.toByte,
      0x02.toByte,
      0,
      0,
      0,
      0
    ) // magic + statsLen only; no statsBlob, no bytesLen
    val jni = mock(classOf[ColumnarBatchSerializerJniWrapper])
    when(jni.serializeWithStats(anyLong())).thenReturn(truncated)
    val (fallback, wasCalled) = newFallbackProbe()
    val cb = ColumnarCachedBatchSerializer.serializeOneBatchWithStats(
      jni,
      0L,
      1,
      structSchema,
      fallback)
    assert(wasCalled(), "fallback closure should be invoked on truncated frame")
    assert(cb.asInstanceOf[CachedColumnarBatch].stats == null)
  }

  test("Kryo-corrupt statsBlob absorbed into legacy fallback") {
    // Build a framed payload with valid magic, plausible statsLen=8, then random bytes that
    // are not a valid Kryo InternalRow serialization. Kryo throws KryoException (RuntimeException
    // subclass, so NonFatal) which the current ULE-only catch does not handle.
    val statsBlob: Array[Byte] = Array.fill(8)(0xff.toByte) // garbage
    val bytesBlob: Array[Byte] = Array[Byte](1, 2, 3)
    val out = new java.io.ByteArrayOutputStream()
    out.write(Array[Byte](0xfe.toByte, 0xca.toByte, 0x53.toByte, 0x02.toByte))
    def writeU32LE(v: Int): Unit = {
      out.write(v & 0xff)
      out.write((v >>> 8) & 0xff)
      out.write((v >>> 16) & 0xff)
      out.write((v >>> 24) & 0xff)
    }
    writeU32LE(statsBlob.length)
    out.write(statsBlob)
    writeU32LE(bytesBlob.length)
    out.write(bytesBlob)
    val framed = out.toByteArray

    val jni = mock(classOf[ColumnarBatchSerializerJniWrapper])
    when(jni.serializeWithStats(anyLong())).thenReturn(framed)
    val (fallback, wasCalled) = newFallbackProbe()
    val cb = ColumnarCachedBatchSerializer.serializeOneBatchWithStats(
      jni,
      0L,
      1,
      structSchema,
      fallback)
    assert(wasCalled(), "fallback closure should be invoked on Kryo-corrupt statsBlob")
    assert(cb.asInstanceOf[CachedColumnarBatch].stats == null)
  }

  test("UnsatisfiedLinkError still trips capability latch (regression)") {
    // Reset latch via reflection so this test doesn't depend on prior test ordering.
    val flagField =
      ColumnarCachedBatchSerializer.getClass.getDeclaredField("statsExtAvailableFlag")
    flagField.setAccessible(true)
    flagField.setBoolean(ColumnarCachedBatchSerializer, true)
    assert(
      ColumnarCachedBatchSerializer.statsExtAvailable,
      "precondition: capability latch must start true")

    val jni = mock(classOf[ColumnarBatchSerializerJniWrapper])
    when(jni.serializeWithStats(anyLong()))
      .thenThrow(new UnsatisfiedLinkError("serializeWithStats (test injection)"))
    val (fallback, wasCalled) = newFallbackProbe()
    val cb = ColumnarCachedBatchSerializer.serializeOneBatchWithStats(
      jni,
      0L,
      1,
      structSchema,
      fallback)
    assert(wasCalled(), "fallback closure should be invoked on ULE")
    assert(cb.asInstanceOf[CachedColumnarBatch].stats == null)
    assert(
      !ColumnarCachedBatchSerializer.statsExtAvailable,
      "ULE should trip the JVM-lifetime capability latch")

    // Restore for subsequent tests / suites in the same JVM.
    flagField.setBoolean(ColumnarCachedBatchSerializer, true)
  }

  test("V3 null JNI return trips V3 capability latch and falls back") {
    ColumnarCachedBatchSerializer.withStatsExtV3AvailabilityForBenchmark(true) {
      val jni = mock(classOf[ColumnarBatchSerializerJniWrapper])
      when(jni.serializeV3(anyLong())).thenReturn(null)
      val (fallback, wasCalled) = newFallbackProbe()

      val cb = ColumnarCachedBatchSerializer.serializeOneBatchV3(
        jni,
        0L,
        1,
        structSchema,
        includeStats = false,
        fallback)

      assert(wasCalled(), "fallback closure should be invoked when V3 returns null")
      assert(cb.asInstanceOf[CachedColumnarBatch].stats == null)
      assert(
        !ColumnarCachedBatchSerializer.statsExtV3Available,
        "null V3 JNI return should trip the V3 JVM-lifetime capability latch")
    }
  }

  test("V3 UnsatisfiedLinkError trips V3 capability latch and falls back") {
    ColumnarCachedBatchSerializer.withStatsExtV3AvailabilityForBenchmark(true) {
      val jni = mock(classOf[ColumnarBatchSerializerJniWrapper])
      when(jni.serializeWithStatsV3(anyLong()))
        .thenThrow(new UnsatisfiedLinkError("serializeWithStatsV3 (test injection)"))
      val (fallback, wasCalled) = newFallbackProbe()

      val cb = ColumnarCachedBatchSerializer.serializeOneBatchV3(
        jni,
        0L,
        1,
        structSchema,
        includeStats = true,
        fallback)

      assert(wasCalled(), "fallback closure should be invoked on V3 ULE")
      assert(cb.asInstanceOf[CachedColumnarBatch].stats == null)
      assert(
        !ColumnarCachedBatchSerializer.statsExtV3Available,
        "V3 ULE should trip the V3 JVM-lifetime capability latch")
    }
  }

  test("V3 corrupt frame falls back without disabling V3 capability") {
    ColumnarCachedBatchSerializer.withStatsExtV3AvailabilityForBenchmark(true) {
      val corruptV3Frame: Array[Byte] = Array[Byte](
        0xfe.toByte,
        0xca.toByte,
        0x53.toByte,
        0x03.toByte,
        0,
        0,
        0,
        0,
        1,
        0,
        0,
        0,
        1,
        0,
        0,
        0,
        4,
        0,
        0,
        0,
        0x11.toByte
      )
      val jni = mock(classOf[ColumnarBatchSerializerJniWrapper])
      when(jni.serializeV3(anyLong())).thenReturn(corruptV3Frame)
      val (fallback, wasCalled) = newFallbackProbe()

      val cb = ColumnarCachedBatchSerializer.serializeOneBatchV3(
        jni,
        0L,
        1,
        structSchema,
        includeStats = false,
        fallback)

      assert(wasCalled(), "fallback closure should be invoked on corrupt V3 frame")
      assert(cb.asInstanceOf[CachedColumnarBatch].stats == null)
      assert(
        ColumnarCachedBatchSerializer.statsExtV3Available,
        "corrupt data should not disable the V3 JNI capability latch")
    }
  }
}

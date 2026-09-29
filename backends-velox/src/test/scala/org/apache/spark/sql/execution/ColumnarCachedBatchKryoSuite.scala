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

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

/**
 * Tests for CachedColumnarBatch Kryo (de)serialization. Pure JVM, no native lib required.
 *
 * Wire-format invariant: Kryo `Output.writeInt(int)` writes a fixed 4-byte BIG-ENDIAN int; the
 * (int, boolean) overload silently forwards to writeVarInt (1-5 bytes) and would corrupt the
 * length-prefixed read path.
 */
class ColumnarCachedBatchKryoSuite extends AnyFunSuite {

  // Use serializer.write / serializer.read directly; writeObject/readObject would prepend a
  // varint refId envelope. Production goes through Spark SerializerInstance internals where
  // reference tracking is off for cached batches.
  private def roundTrip(batch: CachedColumnarBatch): CachedColumnarBatch = {
    val ser = new CachedColumnarBatchKryoSerializer
    val kryo = new Kryo()
    val baos = new ByteArrayOutputStream()
    val out = new Output(baos)
    ser.write(kryo, out, batch)
    out.close()
    val in = new Input(new ByteArrayInputStream(baos.toByteArray))
    val read = ser.read(kryo, in, classOf[CachedColumnarBatch])
    in.close()
    read
  }

  test("stats field round-trip") {
    val stats: InternalRow = new GenericInternalRow(
      Array[Any](42L, 100L, 0, 10, 64L))
    val batch = CachedColumnarBatch(
      numRows = 10,
      sizeInBytes = 64L,
      bytes = Array[Byte](1, 2, 3, 4),
      stats = stats)

    val read = roundTrip(batch)

    assert(read.numRows === 10)
    assert(read.sizeInBytes === 64L)
    assert(read.bytes === Array[Byte](1, 2, 3, 4))
    assert(read.stats !== null, "stats field must round-trip")
    assert(read.stats.numFields === 5, "vanilla PartitionStatistics = 5 slots / col")
    assert(read.stats.getLong(0) === 42L, "lowerBound at slot 0")
    assert(read.stats.getLong(1) === 100L, "upperBound at slot 1")
    assert(read.stats.getInt(2) === 0, "nullCount at slot 2")
    assert(read.stats.getInt(3) === 10, "count at slot 3")
    assert(read.stats.getLong(4) === 64L, "sizeInBytes at slot 4")
  }

  test("stats=null round-trip") {
    val batch = CachedColumnarBatch(
      numRows = 7,
      sizeInBytes = 123L,
      bytes = Array[Byte](9, 8, 7),
      stats = null)

    val read = roundTrip(batch)

    assert(read.numRows === 7)
    assert(read.sizeInBytes === 123L)
    assert(read.bytes === Array[Byte](9, 8, 7))
    assert(read.stats === null)
  }

  // Build a V1-format byte stream: numRows + sizeInBytes + length + bytes, with NO trailing
  // hasStats / hasSchema booleans. Mirrors a CachedColumnarBatch persisted by an older Gluten
  // jar and surviving a rolling upgrade (DISK_ONLY / MEMORY_AND_DISK storage).
  private def writeV1Stream(numRows: Int, sizeInBytes: Long, payload: Array[Byte]): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val out = new Output(baos)
    out.writeInt(numRows)
    out.writeLong(sizeInBytes)
    out.writeInt(payload.length + 1)
    out.writeBytes(payload)
    out.close()
    baos.toByteArray
  }

  test("V1 wire (no trailing hasStats/hasSchema booleans) reads as stats=null/schema=null") {
    val raw = writeV1Stream(numRows = 5, sizeInBytes = 99L, payload = Array[Byte](1, 2, 3))
    val ser = new CachedColumnarBatchKryoSerializer
    val kryo = new Kryo()
    val in = new Input(new ByteArrayInputStream(raw))
    val read = ser.read(kryo, in, classOf[CachedColumnarBatch])
    in.close()

    assert(read.numRows === 5)
    assert(read.sizeInBytes === 99L)
    assert(read.bytes === Array[Byte](1, 2, 3))
    assert(read.stats === null, "absent trailing hasStats must read as null, not throw")
    assert(read.schema === null, "absent trailing hasSchema must read as null, not throw")
  }

  // Construct a corrupt stream by hand-writing the length-prefix only (no payload follows). The
  // production read path must reject the bogus length BEFORE allocating the array, otherwise
  // either negative-size or multi-GB allocation would crash the executor.
  private def streamWithBogusLength(length: Int): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val out = new Output(baos)
    out.writeInt(1) // numRows
    out.writeLong(0L) // sizeInBytes
    out.writeInt(length) // bogus length
    out.close()
    baos.toByteArray
  }

  test("read rejects negative length without NegativeArraySizeException") {
    val raw = streamWithBogusLength(-100) // payloadLen = -101
    val ser = new CachedColumnarBatchKryoSerializer
    val in = new Input(new ByteArrayInputStream(raw))
    val ex = intercept[IllegalArgumentException] {
      ser.read(new Kryo(), in, classOf[CachedColumnarBatch])
    }
    assert(
      ex.getMessage.contains("out of bounds"),
      s"expected bounds-check failure, got: ${ex.getMessage}")
  }

  test("read rejects oversized length without OOM") {
    // length = Int.MaxValue would attempt a 2 GB allocation (well above 64 MiB ceiling).
    val raw = streamWithBogusLength(Int.MaxValue)
    val ser = new CachedColumnarBatchKryoSerializer
    val in = new Input(new ByteArrayInputStream(raw))
    val ex = intercept[IllegalArgumentException] {
      ser.read(new Kryo(), in, classOf[CachedColumnarBatch])
    }
    assert(
      ex.getMessage.contains("out of bounds"),
      s"expected bounds-check failure, got: ${ex.getMessage}")
  }

  test("read rejects oversized statsLen without OOM") {
    // Build a stream with valid payload, hasStats=true, then a bogus statsLen.
    val baos = new ByteArrayOutputStream()
    val out = new Output(baos)
    out.writeInt(1)
    out.writeLong(0L)
    out.writeInt(2) // payload length+1, payloadLen = 1
    out.writeBytes(Array[Byte](7))
    out.writeBoolean(true) // hasStats
    out.writeInt(Int.MaxValue) // bogus statsLen
    out.close()
    val ser = new CachedColumnarBatchKryoSerializer
    val in = new Input(new ByteArrayInputStream(baos.toByteArray))
    val ex = intercept[IllegalArgumentException] {
      ser.read(new Kryo(), in, classOf[CachedColumnarBatch])
    }
    assert(
      ex.getMessage.contains("stats length"),
      s"expected statsLen bounds-check failure, got: ${ex.getMessage}")
  }
}

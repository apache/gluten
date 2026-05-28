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
import org.apache.spark.sql.types.{LongType, StructField, StructType}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}

/**
 * Deterministic repro for the L154/L180 Input.available() boolean-probe bug.
 *
 * Trigger conditions (all required):
 *   (1) Multi-batch deserialize via kryo.readClassAndObject from one stream.
 *   (2) Kryo Input wraps an InputStream (not byte[]).
 *   (3) At a batch's trailing hasStats/hasSchema position, the underlying
 *       InputStream returns available()=0 AND the Kryo Input buffer is drained
 *       (limit==position). Both conditions must hit the SAME byte position.
 *
 * Real prod path observed in production:
 *   BufferedInputStream over shuffle-spill / network ManagedBuffer chunk
 *   boundary -> stream.available()=0 between chunks, Kryo Input.available()
 *   = (limit-pos) + 0 -> reads 0 when buffer drained.
 *
 * Fixture: 1-byte-per-read stream + lying available()=0 -> every byte boundary
 * satisfies (3); any trailing-boolean byte aligned with a Kryo refill triggers
 * the false-EOF.
 */
class ColumnarCachedBatchKryoBoundaryProbeBugSuite extends AnyFunSuite {

  final private class LyingOneByteStream(src: InputStream) extends InputStream {
    override def read(): Int = src.read()
    override def read(b: Array[Byte], off: Int, len: Int): Int = {
      if (len == 0) 0
      else {
        val c = src.read()
        if (c == -1) -1
        else {
          b(off) = c.toByte
          1
        }
      }
    }
    override def available(): Int = 0
  }

  private def mkBatch(i: Int): CachedColumnarBatch = {
    // PartitionStatistics per-column slots:
    //   [lower(typed) upper(typed) count(Int) nullCount(Int) sizeBytes(Long)]
    val stats: InternalRow =
      new GenericInternalRow(Array[Any](i.toLong, (i * 10).toLong, i, 0, 8L))
    val schema = StructType(Seq(StructField(s"col$i", LongType, nullable = true)))
    val bytes = Array.fill[Byte](128)(i.toByte)
    CachedColumnarBatch(
      numRows = i,
      sizeInBytes = bytes.length.toLong,
      bytes = bytes,
      stats = stats,
      schema = schema)
  }

  test("multi-batch deserialize survives boundary-aligned trailing-boolean probe") {
    val kryo = new Kryo()
    val ser = new CachedColumnarBatchKryoSerializer()
    kryo.register(classOf[CachedColumnarBatch], ser)

    val baos = new ByteArrayOutputStream()
    val out = new Output(baos)
    val originals = (1 to 10).map(mkBatch)
    originals.foreach(b => kryo.writeClassAndObject(out, b))
    out.close()

    val raw = baos.toByteArray
    val in = new Input(new LyingOneByteStream(new ByteArrayInputStream(raw)), 32)

    val read = (1 to 10).map(_ => kryo.readClassAndObject(in).asInstanceOf[CachedColumnarBatch])
    in.close()

    originals.zip(read).zipWithIndex.foreach {
      case ((o, r), i) =>
        info(s"batch $i: orig.stats=${o.stats != null} schema=${o.schema}")
        info(s"batch $i: read.stats=${r.stats != null} schema=${r.schema}")
        assert(r.numRows == o.numRows, s"batch $i numRows mismatch")
        assert(r.bytes.toSeq == o.bytes.toSeq, s"batch $i bytes mismatch")
        assert(r.stats != null, s"batch $i stats lost (BUG)")
        assert(r.schema == o.schema, s"batch $i schema mismatch (BUG)")
    }
  }

  // V1 wire backward-compat is locked by ColumnarCachedBatchKryoSuite#"V1 wire ..."
  // -- not duplicated here. This suite only covers the chunked-fill probe path.
}

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
package org.apache.gluten.connector.write

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{BaseFixedWidthVector, BigIntVector, BitVector, DecimalVector, IntVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class LanceNullSlotsSuite extends AnyFunSuite {
  private def field(name: String, arrowType: ArrowType, children: Field*): Field =
    new Field(name, FieldType.nullable(arrowType), children.asJava)

  private def withRoot(fields: Field*)(f: (VectorSchemaRoot, BufferAllocator) => Unit): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val root = VectorSchemaRoot.create(new Schema(fields.asJava), allocator)
      try f(root, allocator)
      finally root.close()
    } finally {
      allocator.close()
    }
  }

  /** Fills `vector` with `rows` non-zero values, then nulls `nulls`, which keep their values. */
  private def fill(vector: BaseFixedWidthVector, rows: Int, nulls: Set[Int]): Unit = {
    vector.allocateNew(rows)
    (0 until rows).foreach {
      i =>
        vector match {
          case v: IntVector => v.set(i, i + 1)
          case v: BigIntVector => v.set(i, (i + 1) * 1000000007L)
          case v: DecimalVector => v.set(i, (i + 1) * 1000000007L)
          case v: BitVector => v.set(i, 1)
        }
    }
    nulls.foreach(vector.setNull)
    vector.setValueCount(rows)
  }

  private def slotIsZero(vector: BaseFixedWidthVector, row: Int): Boolean = {
    val width = vector.getTypeWidth
    (0 until width).forall(i => vector.getDataBuffer.getByte(row.toLong * width + i) == 0)
  }

  test("zeroes the values under nulls across bitmap words and keeps every other value") {
    // Nulls at both ends of each 64-bit word, and runs that cross a word boundary.
    val rows = 131
    val nulls = Set(0, 1, 2, 63, 64, 65, 100, 127, 128, 129, 130)
    withRoot(field("i", new ArrowType.Int(32, true)), field("l", new ArrowType.Int(64, true))) {
      (root, allocator) =>
        fill(root.getVector(0).asInstanceOf[IntVector], rows, nulls)
        fill(root.getVector(1).asInstanceOf[BigIntVector], rows, nulls)
        root.setRowCount(rows)
        val before = (0 until rows).map(i => root.getFieldVectors.asScala.map(_.getObject(i)))

        LanceNullSlots.zero(root, allocator)

        assert((0 until rows).map(i => root.getFieldVectors.asScala.map(_.getObject(i))) == before)
        root.getFieldVectors.asScala.foreach {
          case v: BaseFixedWidthVector =>
            assert(v.getValueCount == rows)
            assert((0 until rows).filter(slotIsZero(v, _)) == nulls.toSeq.sorted)
        }
    }
  }

  test("copies a column before zeroing it, so the exported buffers stay as they were") {
    withRoot(field("dec", new ArrowType.Decimal(38, 0, 128))) {
      (root, allocator) =>
        val vector = root.getVector(0).asInstanceOf[DecimalVector]
        fill(vector, 10, Set(3, 4))
        root.setRowCount(10)
        // Another holder of the original buffer, as Velox may be.
        val original = vector.getDataBuffer
        original.getReferenceManager.retain()
        try {
          val leftover = original.getLong(3 * 16L)
          assert(leftover != 0)

          LanceNullSlots.zero(root, allocator)

          assert(vector.getDataBuffer.memoryAddress() != original.memoryAddress())
          assert(slotIsZero(vector, 3) && slotIsZero(vector, 4))
          assert(original.getLong(3 * 16L) == leftover)
        } finally {
          original.getReferenceManager.release()
        }
    }
  }

  test("leaves columns without nulls, with zero null slots, or of booleans as they are") {
    withRoot(
      field("full", new ArrowType.Int(32, true)),
      field("zeroed", new ArrowType.Int(32, true)),
      field("bools", ArrowType.Bool.INSTANCE)) {
      (root, allocator) =>
        fill(root.getVector(0).asInstanceOf[IntVector], 70, Set.empty)
        val zeroed = root.getVector(1).asInstanceOf[IntVector]
        fill(zeroed, 70, Set.empty)
        Seq(5, 69).foreach(i => zeroed.set(i, 0))
        Seq(5, 69).foreach(zeroed.setNull)
        fill(root.getVector(2).asInstanceOf[BitVector], 70, Set(5))
        root.setRowCount(70)
        val addresses = root.getFieldVectors.asScala.map(_.getDataBuffer.memoryAddress())

        LanceNullSlots.zero(root, allocator)

        assert(root.getFieldVectors.asScala.map(_.getDataBuffer.memoryAddress()) == addresses)
    }
  }

  test("zeroes the null slots of a struct's children") {
    withRoot(field("s", ArrowType.Struct.INSTANCE, field("i", new ArrowType.Int(32, true)))) {
      (root, allocator) =>
        val struct = root.getVector(0).asInstanceOf[StructVector]
        val child = struct.getChild("i", classOf[IntVector])
        fill(child, 8, Set(2, 7))
        (0 until 8).foreach(struct.setIndexDefined)
        struct.setValueCount(8)
        root.setRowCount(8)

        LanceNullSlots.zero(root, allocator)

        val zeroedChild = struct.getChild("i", classOf[IntVector])
        assert((0 until 8).filter(slotIsZero(zeroedChild, _)) == Seq(2, 7))
        assert((0 until 8).map(zeroedChild.getObject) == (0 until 8).map {
          i => if (i == 2 || i == 7) null else Integer.valueOf(i + 1)
        })
    }
  }

  test("zeroes the values of a struct's children in the rows where the struct is null") {
    // s (null at 1 and 70) holds i (null at 3) and n (null at 5), and n holds l (no nulls).
    val rows = 72
    withRoot(
      field(
        "s",
        ArrowType.Struct.INSTANCE,
        field("i", new ArrowType.Int(32, true)),
        field("n", ArrowType.Struct.INSTANCE, field("l", new ArrowType.Int(64, true))))) {
      (root, allocator) =>
        val s = root.getVector(0).asInstanceOf[StructVector]
        val n = s.getChild("n", classOf[StructVector])
        fill(s.getChild("i", classOf[IntVector]), rows, Set(3))
        fill(n.getChild("l", classOf[BigIntVector]), rows, Set.empty)
        (0 until rows).foreach {
          row =>
            if (row != 5) n.setIndexDefined(row)
            if (row != 1 && row != 70) s.setIndexDefined(row)
        }
        n.setValueCount(rows)
        s.setValueCount(rows)
        root.setRowCount(rows)
        val before = (0 until rows).map(s.getObject)

        LanceNullSlots.zero(root, allocator)

        val i = s.getChild("i", classOf[IntVector])
        val l = n.getChild("l", classOf[BigIntVector])
        assert((0 until rows).filter(slotIsZero(i, _)) == Seq(1, 3, 70))
        assert((0 until rows).filter(slotIsZero(l, _)) == Seq(1, 5, 70))
        // Only the values change: a child keeps its own validity under a null struct.
        assert(!i.isNull(1) && l.getNullCount == 0)
        assert((0 until rows).map(s.getObject) == before)
    }
  }
}

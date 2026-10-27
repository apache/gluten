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

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{Float4Vector, IntVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.{FixedSizeListVector, ListVector}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class LanceFixedSizeListsSuite extends AnyFunSuite {
  private val commitSchema: Schema = {
    val element =
      new Field(
        "item",
        FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
        null)
    val vector =
      new Field("v", FieldType.nullable(new ArrowType.FixedSizeList(2)), List(element).asJava)
    val id = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)
    new Schema(List(id, vector).asJava)
  }

  test("exported schema has plain lists in place of top-level fixed-size lists") {
    val exported = LanceFixedSizeLists.exportedSchema(commitSchema)
    assert(exported.getFields.get(0) == commitSchema.getFields.get(0))
    val v = exported.getFields.get(1)
    assert(v.getType == ArrowType.List.INSTANCE)
    assert(v.getChildren == commitSchema.getFields.get(1).getChildren)
  }

  /**
   * Moves rows whose lists are given as offsets into elements 0 to 9, which hold their own index
   * unless they are in `nullElements`, and returns the fixed-size lists, None for a null row.
   */
  private def move(
      offsets: Seq[Int],
      nullRows: Set[Int],
      nullElements: Set[Int] = Set.empty): Seq[Option[Seq[Any]]] = {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val from =
        VectorSchemaRoot.create(LanceFixedSizeLists.exportedSchema(commitSchema), allocator)
      val to = VectorSchemaRoot.create(commitSchema, allocator)
      try {
        val rows = offsets.size - 1
        val ids = from.getVector(0).asInstanceOf[IntVector]
        ids.allocateNew(rows)
        (0 until rows).foreach(i => ids.set(i, i))
        val list = from.getVector(1).asInstanceOf[ListVector]
        list.allocateNew()
        val elements = list.getDataVector.asInstanceOf[Float4Vector]
        (0 until 10).foreach {
          i => if (nullElements(i)) elements.setNull(i) else elements.setSafe(i, i.toFloat)
        }
        elements.setValueCount(10)
        while (list.getOffsetBuffer.capacity() < (rows + 1) * 4) list.reAlloc()
        offsets.zipWithIndex.foreach { case (o, i) => list.getOffsetBuffer.setInt(i * 4L, o) }
        (0 until rows).foreach(i => if (nullRows(i)) list.setNull(i) else list.setNotNull(i))
        list.setLastSet(rows - 1)
        from.setRowCount(rows)

        LanceFixedSizeLists.moveInto(from, to)

        // The exported lists are freed once copied.
        assert(list.getOffsetBuffer.capacity() == 0)
        assert(list.getDataVector.getDataBuffer.capacity() == 0)
        assert(to.getRowCount == rows)
        assert((0 until rows).map(to.getVector(0).getObject) == (0 until rows))
        val fixed = to.getVector(1).asInstanceOf[FixedSizeListVector]
        assert(fixed.getDataVector.getValueCount == rows * 2)
        // The elements of a null row are null, as lance-spark's writer leaves them.
        nullRows.foreach(r => assert((r * 2 until r * 2 + 2).forall(fixed.getDataVector.isNull)))
        (0 until rows).map {
          i =>
            if (fixed.isNull(i)) None
            else Some(fixed.getObject(i).asScala.toSeq)
        }
      } finally {
        from.close()
        to.close()
      }
    } finally {
      allocator.close()
    }
  }

  test("moves lists that do not start at the first element without a copy") {
    assert(move(
      Seq(3, 5, 7, 9),
      Set.empty) == Seq(Some(Seq(3f, 4f)), Some(Seq(5f, 6f)), Some(Seq(7f, 8f))))
  }

  test("copies lists around null rows, which may take no elements") {
    assert(move(Seq(1, 3, 3, 5), Set(1)) == Seq(Some(Seq(1f, 2f)), None, Some(Seq(3f, 4f))))
  }

  test("copies lists with null elements one element at a time") {
    assert(
      move(Seq(1, 3, 3, 5), Set(1), nullElements = Set(2)) ==
        Seq(Some(Seq(1f, null)), None, Some(Seq(3f, 4f))))
  }

  test("rejects a list of the wrong size") {
    val e = intercept[IllegalArgumentException](move(Seq(0, 2, 5), Set.empty))
    assert(e.getMessage.contains("holds lists of 2 elements, but row 1 has 3 elements"))
  }
}

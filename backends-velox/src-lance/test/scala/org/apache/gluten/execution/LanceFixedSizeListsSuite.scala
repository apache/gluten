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
package org.apache.gluten.execution

import org.apache.spark.sql.vectorized.ArrowColumnVector

import org.apache.arrow.memory.{BufferAllocator, OutOfMemoryException, RootAllocator}
import org.apache.arrow.vector.{FieldVector, Float4Vector, IntVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.{FixedSizeListVector, ListVector, StructVector}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

/**
 * Unit tests for how the Lance scan takes the columns of an imported batch
 * ([[LanceFragmentIterator.takeColumns]]) and rewrites its FixedSizeList vectors as Lists
 * ([[FixedSizeLists]]). Every test runs on its own [[RootAllocator]], whose `close()` fails if any
 * buffer leaked.
 */
class LanceFixedSizeListsSuite extends AnyFunSuite {

  private val floatType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)

  private def withAllocator(f: BufferAllocator => Unit): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    try f(allocator)
    finally allocator.close()
  }

  private def vectorField(name: String, dim: Int): Field =
    new Field(
      name,
      FieldType.nullable(new ArrowType.FixedSizeList(dim)),
      Seq(new Field("item", FieldType.notNullable(floatType), null)).asJava)

  /** Fills the FixedSizeList `vector` with `rows`; null slots get junk child values. */
  private def fill(vector: FixedSizeListVector, rows: Seq[Option[Seq[Float]]]): Unit = {
    val dim = vector.getListSize
    val values = vector.getDataVector.asInstanceOf[Float4Vector]
    rows.zipWithIndex.foreach {
      case (row, i) =>
        row match {
          case Some(_) => vector.setNotNull(i)
          case None => vector.setNull(i)
        }
        (0 until dim).foreach(j => values.setSafe(i * dim + j, row.map(_(j)).getOrElse(-1f)))
    }
  }

  private def floatVectors(
      allocator: BufferAllocator,
      dim: Int,
      rows: Seq[Option[Seq[Float]]]): FixedSizeListVector = {
    val vector = vectorField("vec", dim).createVector(allocator).asInstanceOf[FixedSizeListVector]
    vector.allocateNew()
    fill(vector, rows)
    vector.setValueCount(rows.size)
    vector
  }

  /** Reads the arrays of `column` through Spark's ArrowColumnVector. */
  private def read(column: ArrowColumnVector, rows: Int): Seq[Option[Seq[Float]]] =
    (0 until rows).map {
      i => if (column.isNullAt(i)) None else Some(column.getArray(i).toFloatArray.toSeq)
    }

  /**
   * Rewrites the `dim`-sized vectors in `source`, checks the result reads as `expected`, and
   * releases both vectors.
   */
  private def checkRewrite(
      source: FieldVector,
      allocator: BufferAllocator,
      dim: Int,
      expected: Seq[Option[Seq[Float]]]): Unit = {
    val converted = FixedSizeLists.toLists(source, allocator)
    source.close()
    val column = new ArrowColumnVector(converted)
    try {
      assert(converted.getField.getType == ArrowType.List.INSTANCE)
      assert(converted.getValueCount == expected.size)
      val offsets = converted.asInstanceOf[ListVector].getOffsetBuffer
      (0 to expected.size).foreach(i => assert(offsets.getInt(i * 4L) == i * dim))
      assert(read(column, expected.size) == expected)
    } finally {
      column.close()
    }
  }

  test("contains finds FixedSizeLists at any depth") {
    val plain = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)
    val list = new Field(
      "values",
      FieldType.nullable(ArrowType.List.INSTANCE),
      Seq(new Field("item", FieldType.nullable(floatType), null)).asJava)
    def struct(children: Field*): Field =
      new Field("s", FieldType.nullable(ArrowType.Struct.INSTANCE), children.asJava)
    def listOf(child: Field): Field =
      new Field("l", FieldType.nullable(ArrowType.List.INSTANCE), Seq(child).asJava)

    assert(!FixedSizeLists.contains(plain))
    assert(!FixedSizeLists.contains(list))
    assert(!FixedSizeLists.contains(struct(plain, list)))
    assert(FixedSizeLists.contains(vectorField("vec", 4)))
    assert(FixedSizeLists.contains(struct(plain, vectorField("vec", 4))))
    assert(FixedSizeLists.contains(listOf(vectorField("vec", 4))))
    assert(FixedSizeLists.contains(struct(listOf(struct(vectorField("vec", 4))))))
  }

  test("rewrites a FixedSizeList with null rows as a List with the same values") {
    withAllocator {
      allocator =>
        // The null row's child slots hold junk, which must not leak into the neighboring rows.
        val rows = Seq(
          Some(Seq(0f, 1f, 2f, 3f)),
          None,
          Some(Seq(8f, 9f, 10f, 11f)),
          None,
          Some(Seq(16f, 17f, 18f, 19f)))
        checkRewrite(floatVectors(allocator, 4, rows), allocator, 4, rows)
    }
  }

  test("rewrites FixedSizeLists with no nulls, only nulls, and no rows") {
    withAllocator {
      allocator =>
        // Uniform validity, and an empty batch whose offsets buffer holds a single 0.
        val noNulls = Seq(Some(Seq(1f, 2f)), Some(Seq(3f, 4f)), Some(Seq(5f, 6f)))
        checkRewrite(floatVectors(allocator, 2, noNulls), allocator, 2, noNulls)
        val onlyNulls = Seq(None, None, None)
        checkRewrite(floatVectors(allocator, 2, onlyNulls), allocator, 2, onlyNulls)
        checkRewrite(floatVectors(allocator, 2, Seq.empty), allocator, 2, Seq.empty)
    }
  }

  test("moves the vector values without copying them") {
    withAllocator {
      allocator =>
        val source = floatVectors(allocator, 3, Seq(Some(Seq(1f, 2f, 3f)), None))
        val values = source.getDataVector.asInstanceOf[Float4Vector].getDataBufferAddress
        val converted = FixedSizeLists.toLists(source, allocator)
        try {
          val list = converted.asInstanceOf[ListVector]
          assert(list.getDataVector.asInstanceOf[Float4Vector].getDataBufferAddress == values)
          assert(list.getValidityBufferAddress == source.getValidityBufferAddress)
        } finally {
          converted.close()
          source.close()
        }
    }
  }

  test("rewrites vectors nested in a struct and in a list") {
    withAllocator {
      allocator =>
        val docField = new Field(
          "doc",
          FieldType.nullable(ArrowType.Struct.INSTANCE),
          Seq(
            new Field("label", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
            vectorField("emb", 2)).asJava
        )
        val doc = docField.createVector(allocator).asInstanceOf[StructVector]
        doc.allocateNew()
        val label = doc.getChild("label", classOf[VarCharVector])
        val emb = doc.getChild("emb", classOf[FixedSizeListVector])
        // Rows: a full struct, a struct with a null vector, and a null struct.
        Seq(0, 1).foreach {
          i =>
            doc.setIndexDefined(i)
            label.setSafe(i, s"d$i".getBytes(StandardCharsets.UTF_8))
        }
        doc.setNull(2)
        fill(emb, Seq(Some(Seq(1f, 2f)), None, None))
        doc.setValueCount(3)

        val multiField =
          new Field(
            "multi",
            FieldType.nullable(ArrowType.List.INSTANCE),
            Seq(vectorField("e", 2)).asJava)
        val multi = multiField.createVector(allocator).asInstanceOf[ListVector]
        multi.allocateNew()
        val elements = multi.getDataVector.asInstanceOf[FixedSizeListVector]
        // Rows: two vectors, an empty list, and a null list.
        multi.startNewValue(0)
        multi.endValue(0, 2)
        multi.startNewValue(1)
        multi.endValue(1, 0)
        multi.setNull(2)
        fill(elements, Seq(Some(Seq(1f, 2f)), Some(Seq(3f, 4f))))
        multi.setValueCount(3)

        val convertedDoc = FixedSizeLists.toLists(doc, allocator)
        val convertedMulti = FixedSizeLists.toLists(multi, allocator)
        doc.close()
        multi.close()
        val docColumn = new ArrowColumnVector(convertedDoc)
        val multiColumn = new ArrowColumnVector(convertedMulti)
        try {
          assert(
            convertedDoc.getField.getChildren.asScala.map(_.getType) ==
              Seq(ArrowType.Utf8.INSTANCE, ArrowType.List.INSTANCE))
          assert((0 until 3).map(docColumn.isNullAt) == Seq(false, false, true))
          assert((0 until 2).map(docColumn.getChild(0).getUTF8String(_).toString) == Seq(
            "d0",
            "d1"))
          assert(read(docColumn.getChild(1), 2) == Seq(Some(Seq(1f, 2f)), None))

          assert(convertedMulti.getField.getChildren.get(0).getType == ArrowType.List.INSTANCE)
          assert((0 until 3).map(multiColumn.isNullAt) == Seq(false, false, true))
          val first = multiColumn.getArray(0)
          assert(
            (0 until first.numElements).map(first.getArray(_).toFloatArray.toSeq) ==
              Seq(Seq(1f, 2f), Seq(3f, 4f)))
          assert(multiColumn.getArray(1).numElements == 0)
        } finally {
          docColumn.close()
          multiColumn.close()
        }
    }
  }

  test("releases the partial result when the rewrite fails") {
    withAllocator {
      allocator =>
        val docField = new Field(
          "doc",
          FieldType.nullable(ArrowType.Struct.INSTANCE),
          Seq(
            new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
            vectorField("emb", 2)).asJava
        )
        val doc = docField.createVector(allocator).asInstanceOf[StructVector]
        doc.allocateNew()
        val id = doc.getChild("id", classOf[IntVector])
        doc.setIndexDefined(0)
        id.setSafe(0, 7)
        doc.setNull(1)
        fill(doc.getChild("emb", classOf[FixedSizeListVector]), Seq(Some(Seq(1f, 2f)), None))
        doc.setValueCount(2)

        // `id` moves into the rewrite before the offsets of `emb` fail to allocate, so the failed
        // rewrite has to release what it already took.
        val limited = allocator.newChildAllocator("limited", 0, 0)
        try {
          intercept[OutOfMemoryException](FixedSizeLists.toLists(doc, limited))
          assert(limited.getAllocatedMemory == 0)
        } finally {
          limited.close()
          doc.close()
        }
    }
  }

  /** A batch with an int column `id` and a vector column `vec`. */
  private def batch(allocator: BufferAllocator): VectorSchemaRoot = {
    val id = new IntVector("id", allocator)
    id.allocateNew(2)
    id.set(0, 7)
    id.set(1, 8)
    id.setValueCount(2)
    val vec = floatVectors(allocator, 2, Seq(Some(Seq(1f, 2f)), None))
    new VectorSchemaRoot(Seq[FieldVector](id, vec).asJava)
  }

  test("takes the columns of a batch, rewriting only the vector columns") {
    withAllocator {
      allocator =>
        val root = batch(allocator)
        try {
          val columns =
            LanceFragmentIterator.takeColumns(root, allocator).asScala.map(new ArrowColumnVector(_))
          try {
            assert(
              columns.map(_.getValueVector.getField.getType) ==
                Seq(new ArrowType.Int(32, true), ArrowType.List.INSTANCE))
            assert((0 until 2).map(columns(0).getInt) == Seq(7, 8))
            assert(read(columns(1), 2) == Seq(Some(Seq(1f, 2f)), None))
          } finally {
            columns.foreach(_.close())
          }
        } finally {
          root.close()
        }
    }
  }

  test("releases the columns already taken when a later column fails") {
    withAllocator {
      allocator =>
        val root = batch(allocator)
        // `id` moves into `limited` before the offsets of `vec` fail to allocate there.
        val limited = allocator.newChildAllocator("limited", 0, 0)
        try {
          intercept[OutOfMemoryException](LanceFragmentIterator.takeColumns(root, limited))
          assert(limited.getAllocatedMemory == 0)
        } finally {
          limited.close()
          root.close()
        }
    }
  }
}

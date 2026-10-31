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
package org.apache.gluten.execution.python

import org.apache.gluten.vectorized.RefCountedArrowColumnVector

import org.apache.spark.api.python.BoltArrowPythonInputWriter
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataOutputStream, IOException}

class BoltArrowPythonInputWriterSuite extends AnyFunSuite {
  private class FailingOutputStream(failure: IOException) extends ByteArrayOutputStream {
    var failWrites = false
    override def write(data: Array[Byte], offset: Int, length: Int): Unit = {
      if (failWrites) throw failure
      super.write(data, offset, length)
    }
  }

  private val schema = StructType(Seq(StructField("value", IntegerType)))
  private def values(allocator: RootAllocator, data: Seq[Int]): ColumnarBatch = {
    val vector = new IntVector("value", allocator)
    vector.allocateNew(data.size)
    data.zipWithIndex.foreach { case (v, i) => vector.setSafe(i, v) }
    vector.setValueCount(data.size)
    val wrapped = new RefCountedArrowColumnVector(vector)
    new ColumnarBatch(Array[ColumnVector](wrapped), data.size)
  }
  private def read(bytes: Array[Byte], allocator: RootAllocator): Seq[Int] = {
    val reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator)
    try {
      assert(reader.getVectorSchemaRoot.getSchema.getFields.get(0).getName == "value")
      val out = scala.collection.mutable.ArrayBuffer.empty[Int]
      while (reader.loadNextBatch()) {
        val root = reader.getVectorSchemaRoot
        val vector = root.getVector(0).asInstanceOf[IntVector]
        (0 until root.getRowCount).foreach(i => out += vector.get(i))
      }
      out.toSeq
    } finally { reader.close() }
  }
  test("legacy empty Python input emits schema and Arrow end marker") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val bytes = new ByteArrayOutputStream
      val writer = new BoltArrowPythonInputWriter(Iterator.empty, schema, "UTC", () => allocator)
      writer.writeToStream(new DataOutputStream(bytes))
      assert(read(bytes.toByteArray, allocator).isEmpty)
      assert(allocator.getAllocatedMemory == 0)
    } finally { allocator.close() }
  }
  test("legacy nonempty Python input retains every batch") {
    val allocator = new RootAllocator(Long.MaxValue)
    val batches = Seq(values(allocator, Seq(1, 2)), values(allocator, Seq(3)))
    try {
      val bytes = new ByteArrayOutputStream
      val writer = new BoltArrowPythonInputWriter(batches.iterator, schema, "UTC", () => allocator)
      writer.writeToStream(new DataOutputStream(bytes))
      assert(read(bytes.toByteArray, allocator) == Seq(1, 2, 3))
    } finally {
      batches.foreach(_.close())
      allocator.close()
    }
  }
  test("incremental empty Python input emits one schema stream then stays finished") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val bytes = new ByteArrayOutputStream
      val out = new DataOutputStream(bytes)
      val writer = new BoltArrowPythonInputWriter(Iterator.empty, schema, "UTC", () => allocator)
      assert(!writer.writeNext(out))
      val length = bytes.size()
      assert(!writer.writeNext(out))
      assert(bytes.size() == length)
      assert(read(bytes.toByteArray, allocator).isEmpty)
      assert(allocator.getAllocatedMemory == 0)
    } finally { allocator.close() }
  }
  test("incremental Python input consumes one batch per call and ends the same stream") {
    val allocator = new RootAllocator(Long.MaxValue)
    val batches = Seq(values(allocator, Seq(1, 2)), values(allocator, Seq(3)))
    try {
      var consumed = 0
      val inputs = batches.iterator.map {
        b =>
          consumed += 1
          b
      }
      val bytes = new ByteArrayOutputStream
      val out = new DataOutputStream(bytes)
      val writer = new BoltArrowPythonInputWriter(inputs, schema, "UTC", () => allocator)
      assert(writer.writeNext(out))
      assert(consumed == 1)
      assert(writer.writeNext(out))
      assert(consumed == 2)
      assert(!writer.writeNext(out))
      assert(read(bytes.toByteArray, allocator) == Seq(1, 2, 3))
    } finally {
      batches.foreach(_.close())
      allocator.close()
    }
  }
  test("failed Arrow end releases buffers and repeated close stays finished") {
    val allocator = new RootAllocator(Long.MaxValue)
    val batch = values(allocator, Seq(1, 2))
    val failure = new IOException("failed Arrow end")
    val bytes = new FailingOutputStream(failure)
    val writer = new BoltArrowPythonInputWriter(Iterator(batch), schema, "UTC", () => allocator)
    try {
      assert(writer.writeNext(new DataOutputStream(bytes)))
      batch.close()
      assert(allocator.getAllocatedMemory > 0)
      bytes.failWrites = true
      assert(intercept[IOException](writer.close()) eq failure)
      assert(allocator.getAllocatedMemory == 0)
      writer.close()
    } finally {
      writer.close()
      batch.close()
      allocator.close()
    }
  }

  test("legacy input failure remains primary when Arrow end also fails") {
    val allocator = new RootAllocator(Long.MaxValue)
    val batch = values(allocator, Seq(1, 2))
    val inputFailure = new IOException("failed Python input")
    val closeFailure = new IOException("failed Arrow end")
    val bytes = new FailingOutputStream(closeFailure)
    val input = new Iterator[ColumnarBatch] {
      private var consumed = false
      override def hasNext: Boolean = {
        if (consumed) {
          bytes.failWrites = true
          throw inputFailure
        }
        true
      }
      override def next(): ColumnarBatch = {
        consumed = true
        batch
      }
    }
    val writer = new BoltArrowPythonInputWriter(input, schema, "UTC", () => allocator)
    try {
      assert(intercept[IOException] {
        writer.writeToStream(new DataOutputStream(bytes))
      } eq inputFailure)
      assert(inputFailure.getSuppressed.contains(closeFailure))
      batch.close()
      assert(allocator.getAllocatedMemory == 0)
      writer.close()
    } finally {
      writer.close()
      batch.close()
      allocator.close()
    }
  }

}

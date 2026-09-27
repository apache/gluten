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
package org.apache.gluten.vectorized

import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.{SparkArrowUtil, SparkSchemaUtil}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{FieldVector, ValueVector, VectorSchemaRoot}
import org.apache.arrow.vector.util.VectorAppender

import scala.collection.JavaConverters._

/** Utilities to build Gluten's Java Arrow batches of [[RefCountedArrowColumnVector]]s. */
object ArrowColumnVectors {

  /** Allocates an empty [[VectorSchemaRoot]] for `schema`. */
  def allocateRoot(
      schema: StructType,
      allocator: BufferAllocator = ArrowBufferAllocators.contextInstance()): VectorSchemaRoot = {
    val arrowSchema = SparkArrowUtil.toArrowSchema(schema, SparkSchemaUtil.getLocalTimezoneID)
    VectorSchemaRoot.create(arrowSchema, allocator)
  }

  /** Wraps `vectors`, whose ownership moves to the returned column vectors. */
  def wrap(vectors: java.util.List[FieldVector]): Array[RefCountedArrowColumnVector] = {
    vectors.asScala.map(new RefCountedArrowColumnVector(_)).toArray
  }

  /** Wraps the vectors of `root` into a batch, which owns them. */
  def toBatch(root: VectorSchemaRoot): ColumnarBatch = {
    new ColumnarBatch(
      wrap(root.getFieldVectors).map(_.asInstanceOf[ColumnVector]),
      root.getRowCount)
  }

  /** Writes `rows` into a new batch with Spark's [[ArrowWriter]]. */
  def fromRows(
      schema: StructType,
      rows: Iterator[InternalRow],
      allocator: BufferAllocator = ArrowBufferAllocators.contextInstance()): ColumnarBatch = {
    val root = allocateRoot(schema, allocator)
    val writer = ArrowWriter.create(root)
    rows.foreach(writer.write)
    writer.finish()
    toBatch(root)
  }

  /**
   * Takes ownership of `vector` in `allocator`: its buffers are moved to a new vector without
   * copying when it shares the allocator's root, and copied otherwise (Arrow buffers can't move
   * across allocator roots, e.g. from vectors allocated by Spark).
   */
  def adopt(vector: ValueVector, allocator: BufferAllocator): FieldVector = {
    if (vector.getAllocator.getRoot == allocator.getRoot) {
      val pair = vector.getTransferPair(allocator)
      pair.transfer()
      pair.getTo.asInstanceOf[FieldVector]
    } else {
      val target = vector.getField.createVector(allocator)
      target.allocateNew()
      vector.accept(new VectorAppender(target), null)
      target
    }
  }
}

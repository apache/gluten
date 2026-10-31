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
package org.apache.spark.api.python

import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.vectorized.RefCountedArrowColumnVector

import org.apache.spark.{SparkConf, SparkEnv, TaskContext}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.task.TaskResources

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{IntVector, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.mockito.Mockito.{mock, when}
import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util.concurrent.atomic.AtomicBoolean

class ColumnarArrowPythonRunnerSuite extends AnyFunSuite {
  private def stream(): DataInputStream = {
    val allocator = new RootAllocator(Long.MaxValue)
    val vector = new IntVector("value", allocator)
    val root = VectorSchemaRoot.of(vector)
    val bytes = new ByteArrayOutputStream
    val out = new DataOutputStream(bytes)
    out.writeInt(SpecialLengths.START_ARROW_STREAM)
    val writer = new ArrowStreamWriter(root, null, out)
    try {
      writer.start()
      Seq(Seq(11, 12), Seq(21, 22, 23)).foreach {
        values =>
          vector.allocateNew(values.size)
          values.zipWithIndex.foreach { case (value, index) => vector.setSafe(index, value) }
          root.setRowCount(values.size)
          writer.writeBatch()
          vector.clear()
      }
      writer.end()
      out.writeInt(SpecialLengths.END_OF_DATA_SECTION)
      out.writeInt(0) // No accumulator updates.
      out.writeInt(SpecialLengths.END_OF_STREAM)
      new DataInputStream(new ByteArrayInputStream(bytes.toByteArray))
    } finally {
      writer.close()
      root.close()
      allocator.close()
    }
  }

  // Only the worker/writer boundary is stubbed; decoding and buffer ownership use real Arrow IPC.
  private def withReader(f: Iterator[ColumnarBatch] => Unit): Unit = {
    val previousEnv = SparkEnv.get
    val env = mock(classOf[SparkEnv])
    when(env.conf).thenReturn(new SparkConf(false).setMaster("local[1]").set(
      "spark.python.worker.reuse",
      "false"))
    SparkEnv.set(env)
    try {
      TaskResources.runUnsafe {
        val function = mock(classOf[PythonFunction])
        when(function.envVars).thenReturn(new java.util.HashMap[String, String]())
        val input = stream()
        class TestRunner extends ColumnarArrowPythonRunner(
            Seq((ChainedPythonFunctions(Seq(function)), 0L)),
            PythonEvalType.SQL_SCALAR_PANDAS_UDF,
            Array(Array((0, Option.empty[String]))),
            new StructType,
            "UTC",
            Map.empty,
            Map.empty
          ) {
          def output(): Iterator[ColumnarBatch] = {
            val writer = mock(classOf[Writer])
            when(writer.exception).thenReturn(None)
            newReaderIterator(
              input,
              writer,
              0L,
              env,
              null,
              None,
              new AtomicBoolean(true),
              TaskContext.get())
          }
        }
        try { f(new TestRunner().output()) }
        finally { input.close() }
      }
    } finally { SparkEnv.set(previousEnv) }
  }

  private def values(batch: ColumnarBatch): Seq[Int] =
    (0 until batch.numRows()).map(batch.column(0).getInt)

  test("closing the first Python output batch does not release the second batch") {
    withReader {
      batches =>
        val first = batches.next()
        assert(values(first) == Seq(11, 12))
        first.close()
        val second = batches.next()
        try {
          assert(second.column(0).asInstanceOf[RefCountedArrowColumnVector].refCnt() == 1)
          assert(values(second) == Seq(21, 22, 23))
          // End-of-stream closes the reader, while the returned batch must remain readable.
          assert(!batches.hasNext)
          assert(values(second) == Seq(21, 22, 23))
        } finally { second.close() }
        assert(ArrowBufferAllocators.contextInstance().getAllocatedMemory == 0)
    }
  }

  test("prefetching Python output preserves outstanding batches and independent close ownership") {
    withReader {
      batches =>
        val first = batches.next()
        var second: ColumnarBatch = null
        try {
          assert(batches.hasNext)
          assert(values(first) == Seq(11, 12))
          second = batches.next()
          assert(!batches.hasNext)
          assert(values(first) == Seq(11, 12))
          first.close()
          assert(values(second) == Seq(21, 22, 23))
        } finally {
          first.close()
          if (second != null) second.close()
        }
        assert(ArrowBufferAllocators.contextInstance().getAllocatedMemory == 0)
    }
  }

  test("task completion releases Python output prefetched without next") {
    var allocator: BufferAllocator = null
    withReader {
      batches =>
        allocator = ArrowBufferAllocators.contextInstance()
        assert(batches.hasNext)
        assert(allocator.getAllocatedMemory > 0)
    }
    assert(allocator.getAllocatedMemory == 0)
  }

  test("task failure releases a prefetched batch after an earlier batch was consumed") {
    var allocator: BufferAllocator = null
    val failure = new IllegalStateException("stop after prefetch")
    val thrown = intercept[IllegalStateException] {
      withReader {
        batches =>
          allocator = ArrowBufferAllocators.contextInstance()
          val first = batches.next()
          assert(values(first) == Seq(11, 12))
          first.close()
          assert(batches.hasNext)
          throw failure
      }
    }
    assert(thrown eq failure)
    assert(allocator.getAllocatedMemory == 0)
  }

}

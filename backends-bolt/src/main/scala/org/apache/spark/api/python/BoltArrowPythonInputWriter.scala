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

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.{SparkSchemaUtil, SparkVectorUtil}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.ArrowStreamWriter

import java.io.DataOutputStream

class BoltArrowPythonInputWriter(
    inputIterator: Iterator[ColumnarBatch],
    schema: StructType,
    timeZoneId: String,
    allocatorFactory: () => BufferAllocator) extends AutoCloseable {
  private var root: VectorSchemaRoot = _
  private var loader: VectorLoader = _
  private var writer: ArrowStreamWriter = _
  private var finished = false

  private def start(dataOut: DataOutputStream): Unit = {
    if (writer == null) {
      root = VectorSchemaRoot.create(
        SparkSchemaUtil.toArrowSchema(schema, timeZoneId),
        allocatorFactory())
      loader = new VectorLoader(root)
      writer = new ArrowStreamWriter(root, null, dataOut)
      writer.start()
    }
  }

  private def writeBatch(batch: ColumnarBatch): Unit = {
    val recordBatch = SparkVectorUtil.toArrowRecordBatch(batch)
    try {
      loader.load(recordBatch)
      writer.writeBatch()
    } finally {
      recordBatch.close()
    }
  }

  def writeToStream(dataOut: DataOutputStream): Unit = {
    Utils.tryWithSafeFinally {
      start(dataOut)
      while (inputIterator.hasNext) {
        writeBatch(inputIterator.next())
      }
    } {
      close()
    }
  }

  def writeNext(dataOut: DataOutputStream): Boolean = {
    if (finished) {
      return false
    }
    start(dataOut)
    if (!inputIterator.hasNext) {
      close()
      return false
    }
    writeBatch(inputIterator.next())
    true
  }

  override def close(): Unit = {
    if (!finished) {
      finished = true
      if (root != null) {
        try {
          if (writer != null) {
            writer.end()
          }
        } finally {
          root.close()
        }
      }
    }
  }
}

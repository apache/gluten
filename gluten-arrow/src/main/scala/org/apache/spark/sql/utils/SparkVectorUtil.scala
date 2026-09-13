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
package org.apache.spark.sql.utils

import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.vectorized.ArrowWritableColumnVector

import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.message.{ArrowFieldNode, ArrowRecordBatch}

import scala.collection.JavaConverters.{asScalaBufferConverter, seqAsJavaListConverter}

object SparkVectorUtil {

  def toArrowRecordBatch(batch: ColumnarBatch): ArrowRecordBatch = {
    ColumnarBatches.checkLoaded(batch)
    val numRowsInBatch = batch.numRows()
    val cols = (0 until batch.numCols).toList.map(
      i =>
        batch
          .column(i)
          .asInstanceOf[ArrowWritableColumnVector]
          .getValueVector)
    toArrowRecordBatch(numRowsInBatch, cols)
  }

  def toArrowRecordBatch(numRows: Int, cols: List[ValueVector]): ArrowRecordBatch = {
    val nodes = new java.util.ArrayList[ArrowFieldNode]()
    val buffers = new java.util.ArrayList[ArrowBuf]()
    val variadicBufferCounts = new java.util.ArrayList[java.lang.Long]()
    cols.foreach(
      vector => {
        appendNodes(
          vector.asInstanceOf[FieldVector],
          nodes,
          buffers,
          variadicBufferCounts = variadicBufferCounts)
      })
    if (variadicBufferCounts.isEmpty) {
      new ArrowRecordBatch(numRows, nodes, buffers)
    } else {
      // The variadic-buffer constructor was added after Arrow 15. Keep this reflective so the
      // default Arrow 15 build remains binary-compatible.
      val bodyCompressionClass =
        Utils.classForName("org.apache.arrow.vector.ipc.message.ArrowBodyCompression")
      val noCompressionClass =
        Utils.classForName("org.apache.arrow.vector.compression.NoCompressionCodec")
      val bodyCompression =
        noCompressionClass.getField("DEFAULT_BODY_COMPRESSION").get(null)
      classOf[ArrowRecordBatch]
        .getConstructor(
          java.lang.Integer.TYPE,
          classOf[java.util.List[_]],
          classOf[java.util.List[_]],
          bodyCompressionClass,
          classOf[java.util.List[_]],
          java.lang.Boolean.TYPE)
        .newInstance(
          Int.box(numRows),
          nodes,
          buffers,
          bodyCompression,
          variadicBufferCounts,
          Boolean.box(true))
        .asInstanceOf[ArrowRecordBatch]
    }
  }

  def getArrowBuffers(vector: FieldVector): Array[ArrowBuf] = {
    try {
      vector.getFieldBuffers.asScala.toArray
    } catch {
      case _: Throwable =>
        vector match {
          case fixed: BaseFixedWidthVector =>
            Array(fixed.getValidityBuffer, fixed.getDataBuffer)
          case variable: BaseVariableWidthVector =>
            Array(variable.getValidityBuffer, variable.getOffsetBuffer, variable.getDataBuffer)
          case _ =>
            throw new UnsupportedOperationException(
              s"Could not decompress vector of class ${vector.getClass}")
        }
    }
  }

  def appendNodes(
      vector: FieldVector,
      nodes: java.util.List[ArrowFieldNode],
      buffers: java.util.List[ArrowBuf],
      bits: java.util.List[Boolean] = null,
      variadicBufferCounts: java.util.List[java.lang.Long] = null): Unit = {
    if (nodes != null) {
      nodes.add(new ArrowFieldNode(vector.getValueCount, vector.getNullCount))
    }
    val fieldBuffers = getArrowBuffers(vector)
    val expectedBufferCount = TypeLayout.getTypeBufferCount(vector.getField.getType)
    val typeName = vector.getField.getType.getClass.getSimpleName
    val isViewType = typeName == "Utf8View" || typeName == "BinaryView"
    if (isViewType && variadicBufferCounts != null) {
      if (fieldBuffers.size < expectedBufferCount) {
        throw new IllegalArgumentException(
          s"Wrong number of buffers for field ${vector.getField} in vector " +
            s"${vector.getClass.getSimpleName}. found: $fieldBuffers")
      }
      variadicBufferCounts.add(Long.box(fieldBuffers.size - expectedBufferCount))
    } else if (fieldBuffers.size != expectedBufferCount) {
      throw new IllegalArgumentException(
        s"Wrong number of buffers for field ${vector.getField} in vector " +
          s"${vector.getClass.getSimpleName}. found: $fieldBuffers")
    }
    buffers.addAll(fieldBuffers.toSeq.asJava)
    if (bits != null) {
      val bits_tmp = Array.fill[Boolean](expectedBufferCount)(false)
      bits_tmp(0) = true
      bits.addAll(bits_tmp.toSeq.asJava)
      vector.getChildrenFromFields.asScala.foreach(
        child => appendNodes(child, nodes, buffers, bits, variadicBufferCounts))
    } else {
      vector.getChildrenFromFields.asScala.foreach(
        child => appendNodes(child, nodes, buffers, variadicBufferCounts = variadicBufferCounts))
    }
  }
}

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
package org.apache.gluten.utils

import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.vectorized.ArrowColumnVectors

import org.apache.spark.sql.utils.SparkVectorUtil
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.arrow.c.{ArrowArray, ArrowSchema, CDataDictionaryProvider, Data}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{BaseIntVector, FieldVector, VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.ipc.message.{ArrowFieldNode, ArrowRecordBatch}
import org.apache.arrow.vector.types.pojo.{Field, FieldType, Schema}

import scala.collection.JavaConverters._

object ArrowAbiUtil {

  def importIntoVectorSchemaRoot(
      allocator: BufferAllocator,
      array: ArrowArray,
      root: VectorSchemaRoot,
      provider: DictionaryProvider): Unit = {
    Data.importIntoVectorSchemaRoot(allocator, array, root, provider)
  }

  def importToArrowRecordBatch(
      allocator: BufferAllocator,
      cSchema: ArrowSchema,
      cArray: ArrowArray): ArrowRecordBatch = {
    val vsr = importToVectorSchemaRoot(allocator, cSchema, cArray)
    val unloader = new VectorUnloader(vsr)
    unloader.getRecordBatch
  }

  def importToSparkColumnarBatch(
      allocator: BufferAllocator,
      cSchema: ArrowSchema,
      cArray: ArrowArray): ColumnarBatch = {
    val provider = new CDataDictionaryProvider
    try {
      val imported = Data.importVectorSchemaRoot(allocator, cArray, cSchema, provider)
      val root = if (provider.getDictionaryIds.isEmpty) {
        imported
      } else {
        try {
          decodeDictionaries(imported, allocator, provider)
        } finally {
          imported.close()
        }
      }
      try {
        toSparkColumnarBatch(root)
      } catch {
        case t: Throwable =>
          root.close()
          throw t
      }
    } finally {
      provider.close()
    }
  }

  // Spark's Arrow column vectors read values directly, so expand C Data dictionary indices
  // while the imported dictionary buffers are still open.
  private def decodeDictionaries(
      root: VectorSchemaRoot,
      allocator: BufferAllocator,
      provider: DictionaryProvider): VectorSchemaRoot = {
    def dictionary(field: Field): FieldVector = {
      provider.lookup(field.getDictionary.getId).getVector
    }

    def decodedField(field: Field): Field = {
      val values = if (field.getDictionary == null) field else dictionary(field).getField
      val children = values.getChildren.asScala.map(decodedField).asJava
      new Field(
        field.getName,
        new FieldType(field.isNullable, values.getType, null, field.getMetadata),
        children)
    }

    def decode(input: FieldVector, output: FieldVector): Unit = {
      val rowCount = input.getValueCount
      if (input.getField.getDictionary == null) {
        output.loadFieldBuffers(
          new ArrowFieldNode(rowCount, input.getNullCount),
          input.getFieldBuffers)
        input.getChildrenFromFields.asScala.zip(output.getChildrenFromFields.asScala).foreach {
          case (childInput, childOutput) => decode(childInput, childOutput)
        }
      } else {
        val indices = input.asInstanceOf[BaseIntVector]
        val values = dictionary(input.getField)
        output.setInitialCapacity(rowCount)
        output.allocateNew()
        for (i <- 0 until rowCount) {
          if (input.isNull(i)) {
            output.setNull(i)
          } else {
            output.copyFromSafe(Math.toIntExact(indices.getValueAsLong(i)), i, values)
          }
        }
        output.setValueCount(rowCount)
      }
    }

    val fields = root.getSchema.getFields.asScala.map(decodedField).asJava
    val decoded =
      VectorSchemaRoot.create(new Schema(fields, root.getSchema.getCustomMetadata), allocator)
    try {
      root.getFieldVectors.asScala.zip(decoded.getFieldVectors.asScala).foreach {
        case (input, output) => decode(input, output)
      }
      decoded.setRowCount(root.getRowCount)
      decoded
    } catch {
      case t: Throwable =>
        decoded.close()
        throw t
    }
  }

  private def importToVectorSchemaRoot(
      allocator: BufferAllocator,
      cSchema: ArrowSchema,
      cArray: ArrowArray): VectorSchemaRoot = {
    val dictProvider = new CDataDictionaryProvider
    val vsr = Data.importVectorSchemaRoot(allocator, cArray, cSchema, dictProvider)
    try {
      vsr
    } finally {
      dictProvider.close()
      //        vsr.close() // remove this if encountering uaf
    }
  }

  def importToSparkColumnarBatch(
      allocator: BufferAllocator,
      schema: Schema,
      cArray: ArrowArray): ColumnarBatch = {
    val vsr = toVectorSchemaRoot(allocator, schema, cArray)
    toSparkColumnarBatch(vsr)
  }

  private def toSparkColumnarBatch(vsr: VectorSchemaRoot): ColumnarBatch = {
    ArrowColumnVectors.toBatch(vsr)
  }

  private def toVectorSchemaRoot(
      allocator: BufferAllocator,
      schema: Schema,
      array: ArrowArray): VectorSchemaRoot = {
    val provider = new CDataDictionaryProvider

    val vsr = VectorSchemaRoot.create(schema, allocator)
    try {
      if (array != null) {
        Data.importIntoVectorSchemaRoot(allocator, array, vsr, provider)
      }
      vsr
    } finally {
      provider.close()
    }
  }

  def importToSchema(allocator: BufferAllocator, cSchema: ArrowSchema): Schema = {
    val dictProvider = new CDataDictionaryProvider
    val schema = Data.importSchema(allocator, cSchema, dictProvider)
    try {
      schema
    } finally {
      dictProvider.close()
    }
  }

  def exportField(allocator: BufferAllocator, field: Field, out: ArrowSchema): Unit = {
    val dictProvider = new CDataDictionaryProvider
    try {
      Data.exportField(allocator, field, dictProvider, out)
    } finally {
      dictProvider.close()
    }
  }

  def exportSchema(allocator: BufferAllocator, schema: Schema, out: ArrowSchema): Unit = {
    val dictProvider = new CDataDictionaryProvider
    try {
      Data.exportSchema(allocator, schema, dictProvider, out)
    } finally {
      dictProvider.close()
    }
  }

  def exportFromSparkColumnarBatch(
      allocator: BufferAllocator,
      batch: ColumnarBatch,
      cSchema: ArrowSchema,
      cArray: ArrowArray): Unit = {
    ColumnarBatches.checkLoaded(batch)
    val schema = ArrowUtil.toSchema(batch)
    val rb = SparkVectorUtil.toArrowRecordBatch(batch)
    try {
      exportFromArrowRecordBatch(allocator, rb, schema, cSchema, cArray)
    } finally {
      if (rb != null) {
        rb.close()
      }
    }
  }

  def exportFromArrowRecordBatch(
      allocator: BufferAllocator,
      arrowBatch: ArrowRecordBatch,
      schema: Schema,
      cSchema: ArrowSchema,
      cArray: ArrowArray): Unit = {
    val vsr = toVectorSchemaRoot(allocator, schema, arrowBatch)
    try {
      Data.exportVectorSchemaRoot(allocator, vsr, new CDataDictionaryProvider(), cArray, cSchema)
    } catch {
      case e: Exception =>
        throw new GlutenException(
          String.format(
            "error exporting columnar batch with schema: %s, vectors: %s",
            vsr.getSchema,
            vsr.getFieldVectors),
          e)
    } finally {
      vsr.close()
    }
  }

  // will release input record batch
  private def toVectorSchemaRoot(
      allocator: BufferAllocator,
      schema: Schema,
      arrowBatch: ArrowRecordBatch): VectorSchemaRoot = {
    if (arrowBatch.getNodes.size() == 0) {
      return VectorSchemaRoot.of()
    }
    val root: VectorSchemaRoot =
      VectorSchemaRoot.create(schema, allocator)
    val loader: VectorLoader = new VectorLoader(root)
    loader.load(arrowBatch)
    root
  }
}

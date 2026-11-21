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

import org.apache.gluten.utils.{ArrowAbiUtil, ArrowUtil}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.{SparkArrowUtil, SparkSchemaUtil}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SparkVersionUtil

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.util.AutoCloseables
import org.apache.arrow.vector.{IntVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.dictionary.Dictionary
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding, FieldType}

class GlutenLargeArrowVarTypesSuite extends SparkFunSuite {
  test("nested Arrow schema uses large offsets only when enabled on Spark 3.5 or later") {
    val schema = StructType.fromDDL("s struct<text: string, data: array<binary>>")
    val conf = new SQLConf
    Seq(false, true).foreach {
      enabled =>
        conf.setConfString("spark.sql.execution.arrow.useLargeVarTypes", enabled.toString)
        val fields = SQLConf.withExistingConf(conf) {
          SparkSchemaUtil.toArrowSchema(schema, "UTC").getFields.get(0).getChildren
        }
        val (textType, binaryType) = if (enabled && SparkVersionUtil.gteSpark35) {
          (ArrowType.LargeUtf8.INSTANCE, ArrowType.LargeBinary.INSTANCE)
        } else {
          (ArrowType.Utf8.INSTANCE, ArrowType.Binary.INSTANCE)
        }
        withClue(s"useLargeVarTypes=$enabled ") {
          assert(fields.get(0).getType == textType)
          assert(fields.get(1).getChildren.get(0).getType == binaryType)
        }
    }
  }

  test("C Data import preserves dictionary-encoded UTF-8 and binary values and layout") {
    val allocator = new RootAllocator(Long.MaxValue)
    val indexType = new ArrowType.Int(32, true)
    val textEncoding = new DictionaryEncoding(1, false, indexType)
    val binaryEncoding = new DictionaryEncoding(2, false, indexType)
    val dictionarySchema = SparkArrowUtil.toArrowSchema(
      StructType.fromDDL("dict_text string, dict_data binary"),
      "UTC",
      largeVarTypes = SparkVersionUtil.gteSpark35)
    val dictionary = VectorSchemaRoot.create(dictionarySchema, allocator)
    val provider = new MapDictionaryProvider(
      new Dictionary(dictionary.getVector(0), textEncoding),
      new Dictionary(dictionary.getVector(1), binaryEncoding))
    val textIndex = new IntVector("text", new FieldType(true, indexType, textEncoding), allocator)
    val payload = StructVector.empty("payload", allocator)
    val binaryIndex = payload.addOrGet(
      "data",
      new FieldType(true, indexType, binaryEncoding),
      classOf[IntVector])
    val root = VectorSchemaRoot.of(textIndex, payload)
    val cSchema = ArrowSchema.allocateNew(allocator)
    val cArray = ArrowArray.allocateNew(allocator)
    try {
      val writer = ArrowWriter.create(dictionary)
      writer.write(InternalRow(UTF8String.fromString("first"), Array[Byte](1, 2)))
      writer.write(InternalRow(UTF8String.fromString("second"), Array[Byte](3, 4)))
      writer.finish()
      textIndex.allocateNew()
      payload.allocateNew()
      (0 until 3).foreach(payload.setIndexDefined)
      Seq(textIndex, binaryIndex).foreach {
        vector =>
          vector.setSafe(0, 1)
          vector.setNull(1)
          vector.setSafe(2, 0)
      }
      root.setRowCount(3)
      Data.exportVectorSchemaRoot(allocator, root, provider, cArray, cSchema)
      val batch = ArrowAbiUtil.importToSparkColumnarBatch(allocator, cSchema, cArray)
      try {
        assert(batch.column(0).getUTF8String(0).toString == "second")
        assert(batch.column(0).isNullAt(1))
        assert(batch.column(0).getUTF8String(2).toString == "first")
        val data = batch.column(1).getChild(0)
        assert(data.getBinary(0).sameElements(Array[Byte](3, 4)))
        assert(data.isNullAt(1))
        assert(data.getBinary(2).sameElements(Array[Byte](1, 2)))
        val fields = ArrowUtil.toSchema(batch).getFields
        assert(fields.get(0).getName == "text")
        assert(fields.get(0).getType == dictionarySchema.getFields.get(0).getType)
        val binaryField = fields.get(1).getChildren.get(0)
        assert(binaryField.getName == "data")
        assert(binaryField.getType == dictionarySchema.getFields.get(1).getType)
      } finally {
        batch.close()
      }
    } finally {
      AutoCloseables.close(cArray, cSchema, root, provider, allocator)
    }
  }
}

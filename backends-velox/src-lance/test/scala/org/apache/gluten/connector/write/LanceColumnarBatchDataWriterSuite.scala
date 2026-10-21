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

import org.apache.arrow.vector.types.{FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding, Field, FieldType, Schema}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class LanceColumnarBatchDataWriterSuite extends AnyFunSuite {
  private val int32 = new ArrowType.Int(32, true)
  private val int64 = new ArrowType.Int(64, true)
  private val float32 = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)

  private def field(name: String, arrowType: ArrowType, children: Field*): Field =
    new Field(name, FieldType.nullable(arrowType), children.asJava)

  private def schema(fields: Field*): Schema = new Schema(fields.asJava)

  private def mismatch(expected: Schema, exported: Schema): Option[String] =
    LanceColumnarBatchDataWriter.layoutMismatch(expected, exported)

  test("a batch with the layout of the commit schema fits") {
    val expected = schema(
      field("id", int32),
      field("s", ArrowType.Struct.INSTANCE, field("a", int64), field("b", ArrowType.Utf8.INSTANCE)),
      field("l", ArrowType.List.INSTANCE, field("item", float32))
    )
    assert(mismatch(expected, expected).isEmpty)
  }

  test("names, nullability and metadata are not compared") {
    val expected = schema(
      new Field("id", new FieldType(true, int32, null, Map("k" -> "v").asJava), null),
      field("l", ArrowType.List.INSTANCE, field("item", float32)))
    val exported = schema(
      new Field("n0_0", FieldType.notNullable(int32), null),
      field("n0_1", ArrowType.List.INSTANCE, field("element", float32)))
    assert(mismatch(expected, exported).isEmpty)
  }

  test("timestamps differ by unit, not by time zone") {
    val expected = schema(field("t", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")))
    val noZone = schema(field("t", new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)))
    val nanos = schema(field("t", new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC")))
    assert(mismatch(expected, noZone).isEmpty)
    assert(mismatch(expected, nanos).exists(_.startsWith("column t has Arrow type Timestamp")))
  }

  test("maps differ by their entries, not by whether their keys are sorted") {
    def map(sorted: Boolean, value: ArrowType): Schema = {
      val entries = new Field(
        "entries",
        FieldType.notNullable(ArrowType.Struct.INSTANCE),
        Seq(new Field("key", FieldType.notNullable(int32), null), field("value", value)).asJava)
      schema(field("m", new ArrowType.Map(sorted), entries))
    }
    assert(mismatch(map(sorted = false, int64), map(sorted = true, int64)).isEmpty)
    assert(
      mismatch(map(sorted = false, int64), map(sorted = false, int32))
        .contains(
          "column m.entries.value has Arrow type Int(32, true) where Lance expects " +
            "Int(64, true)"))
  }

  test("a column of another type is named") {
    val expected = schema(field("id", int32), field("v", int64))
    val exported = schema(field("id", int32), field("v", int32))
    assert(
      mismatch(expected, exported).contains(
        "column v has Arrow type Int(32, true) where Lance expects Int(64, true)"))
  }

  test("a nested column of another type is named by its path") {
    val expected = schema(field("s", ArrowType.Struct.INSTANCE, field("a", int32)))
    val exported = schema(field("s", ArrowType.Struct.INSTANCE, field("a", float32)))
    assert(mismatch(expected, exported).exists(_.startsWith("column s.a has Arrow type")))
  }

  test("a different number of columns or of children is reported") {
    val expected = schema(
      field("id", int32),
      field("s", ArrowType.Struct.INSTANCE, field("a", int32), field("b", int32)))
    assert(
      mismatch(expected, schema(field("id", int32)))
        .contains("the batch has 1 fields where Lance expects 2"))
    val fewer = schema(field("id", int32), field("s", ArrowType.Struct.INSTANCE, field("a", int32)))
    assert(mismatch(expected, fewer).contains("column s has 1 fields where Lance expects 2"))
  }

  test("a dictionary-encoded column does not fit") {
    val encoded = new Field(
      "s",
      new FieldType(true, ArrowType.Utf8.INSTANCE, new DictionaryEncoding(1L, false, null)),
      null)
    assert(
      mismatch(schema(field("s", ArrowType.Utf8.INSTANCE)), schema(encoded))
        .contains("column s is dictionary-encoded"))
  }
}

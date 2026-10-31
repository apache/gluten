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
package org.apache.gluten.expression

import org.apache.gluten.substrait.SubstraitContext

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, JsonToStructs}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import io.substrait.proto.Type
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

/**
 * Plan-level regression test for apache/gluten#13251. `from_json` matches JSON keys against schema
 * field names case-sensitively, and Bolt's native `from_json` does the same. So the struct type
 * sent to Bolt must carry the schema field names exactly as written, whatever the value of
 * `spark.sql.caseSensitive` is. This test inspects the generated Substrait output type only, so it
 * does not execute native code.
 */
class FromJsonSchemaCaseSuite extends AnyFunSuite {

  private val txt = AttributeReference("txt", StringType)()

  private def withConf(kvs: (String, String)*)(body: => Unit): Unit = {
    val conf = SQLConf.get
    val old = kvs.map { case (k, _) => k -> Option(conf.getConfString(k, null)) }
    kvs.foreach { case (k, v) => conf.setConfString(k, v) }
    try body
    finally old.foreach {
        case (k, Some(v)) => conf.setConfString(k, v)
        case (k, None) => conf.unsetConf(k)
      }
  }

  /** Converts `from_json(txt, schema)` to Substrait and returns the scalar function output type. */
  private def outputType(schema: DataType): Type = {
    val expr = JsonToStructs(schema, Map.empty[String, String], txt, Some("UTC"))
    ExpressionConverter
      .replaceWithExpressionTransformer(expr, Seq(txt))
      .doTransform(new SubstraitContext)
      .toProtobuf
      .getScalarFunction
      .getOutputType
  }

  private def names(t: Type): Seq[String] = t.getStruct.getNamesList.asScala.toSeq

  test("from_json keeps schema field name case when caseSensitive=false") {
    withConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val schema = new StructType().add("Id", StringType).add("Value", StringType)
      assert(names(outputType(schema)) === Seq("Id", "Value"))
    }
  }

  test("from_json keeps mixed-case and upper-case field names") {
    withConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val schema = new StructType().add("ID", IntegerType).add("vALUE", StringType)
      assert(names(outputType(schema)) === Seq("ID", "vALUE"))
    }
  }

  test("from_json keeps nested struct field names, including inside array and map") {
    withConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val deep = new StructType().add("DeepKey", IntegerType)
      val inner = new StructType()
        .add("InnerKey", StringType)
        .add("Arr", ArrayType(deep))
        .add("M", MapType(StringType, deep))
      val schema = new StructType().add("OuterKey", inner)

      val outer = outputType(schema)
      assert(names(outer) === Seq("OuterKey"))
      val innerType = outer.getStruct.getTypes(0)
      assert(names(innerType) === Seq("InnerKey", "Arr", "M"))
      val arrElem = innerType.getStruct.getTypes(1).getList.getType
      assert(names(arrElem) === Seq("DeepKey"))
      val mapValue = innerType.getStruct.getTypes(2).getMap.getValue
      assert(names(mapValue) === Seq("DeepKey"))
    }
  }
}

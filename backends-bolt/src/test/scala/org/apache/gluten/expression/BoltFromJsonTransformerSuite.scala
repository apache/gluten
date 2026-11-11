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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.backendsapi.bolt.BoltSparkPlanExecApi
import org.apache.gluten.substrait.SubstraitContext

import org.apache.spark.sql.catalyst.expressions.{JsonToStructs, Literal}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class BoltFromJsonTransformerSuite extends AnyFunSuite {
  private val sqlConf = new SQLConf
  sqlConf.setConfString("spark.sql.json.enablePartialResults", "true")
  private def generated(schema: DataType): io.substrait.proto.Type =
    SQLConf.withExistingConf(sqlConf) {
      val expression = JsonToStructs(schema, Map.empty[String, String], Literal("{}"), Some("UTC"))
      new BoltSparkPlanExecApi().genFromJsonTransformer(
        "from_json",
        Seq(LiteralTransformer(expression.child)),
        expression)
        .doTransform(new SubstraitContext).toProtobuf.getScalarFunction.getOutputType
    }

  test("from_json preserves exact capitalized schema keys") {
    assert(BackendsApiManager.getBackendName == "bolt")
    val typ =
      generated(StructType(Seq(StructField("Id", StringType), StructField("Value", StringType))))
    assert(typ.getStruct.getNamesList.asScala.toSeq == Seq("Id", "Value"))
  }

  test("from_json preserves schema names inside nested arrays maps and structs") {
    val nested = StructType(Seq(StructField("ChildKey", IntegerType, nullable = false)))
    val schema = StructType(Seq(
      StructField("Outer", nested),
      StructField("ArrayKey", ArrayType(nested, containsNull = false)),
      StructField("MapKey", MapType(StringType, nested, valueContainsNull = true))
    ))
    val typ = generated(schema).getStruct
    assert(typ.getNamesList.asScala.toSeq == Seq("Outer", "ArrayKey", "MapKey"))
    assert(typ.getTypes(0).getStruct.getNamesList.asScala.toSeq == Seq("ChildKey"))
    assert(typ.getTypes(1).getList.getType.getStruct.getNamesList.asScala.toSeq == Seq("ChildKey"))
    assert(typ.getTypes(2).getMap.getValue.getStruct.getNamesList.asScala.toSeq == Seq("ChildKey"))
    assert(typ.getTypes(0).getStruct.getTypes(0).getI32.getNullability ==
      io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE)
    assert(typ.getTypes(1).getList.getType.getStruct.getNullability ==
      io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE)
  }

  test("from_json preserves names when root schema is an array or map") {
    val schema = StructType(Seq(StructField("Nested", LongType)))
    assert(generated(ArrayType(schema)).getList.getType.getStruct.getNamesList.asScala.toSeq == Seq(
      "Nested"))
    assert(generated(MapType(
      StringType,
      schema)).getMap.getValue.getStruct.getNamesList.asScala.toSeq == Seq("Nested"))
  }

  test("generic schema serialization retains prior backend field normalization") {
    val schema = StructType(Seq(StructField("Id", StringType)))
    val generic = ConverterUtils.getTypeNode(schema, nullable = true).toProtobuf
    assert(generic.getStruct.getNamesList.asScala.toSeq == Seq("id"))
  }

  test("from_json primitive types and nullability match existing serialization") {
    val primitive = DecimalType(15, 3)
    val schema = StructType(Seq(StructField("Price", primitive, nullable = false)))
    val typ = generated(schema).getStruct.getTypes(0)
    assert(typ == ConverterUtils.getTypeNode(primitive, nullable = true).toProtobuf)
  }
}

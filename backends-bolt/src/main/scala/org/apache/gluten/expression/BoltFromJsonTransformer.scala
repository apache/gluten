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

import org.apache.gluten.substrait.`type`.{TypeBuilder, TypeNode}
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.sql.catalyst.expressions.JsonToStructs
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

import scala.collection.JavaConverters._

case class BoltFromJsonTransformer(
    substraitExprName: String,
    children: Seq[ExpressionTransformer],
    original: JsonToStructs)
  extends ExpressionTransformer {

  override def doTransform(context: SubstraitContext): ExpressionNode = {
    val funcName =
      ConverterUtils.makeFuncName(substraitExprName, original.children.map(_.dataType))
    val functionId = context.registerFunction(funcName)
    val childNodes = children.map(_.doTransform(context)).asJava
    val typeNode = BoltFromJsonTransformer.jsonTypeNode(dataType, nullable)
    ExpressionBuilder.makeScalarFunction(functionId, childNodes, typeNode)
  }
}

object BoltFromJsonTransformer {
  // JSON keys are matched to schema names exactly, independently of SQL identifier casing.
  // Preserve them recursively without changing normalization for other expressions or scans.
  private def jsonTypeNode(dataType: DataType, nullable: Boolean): TypeNode = dataType match {
    case struct: StructType =>
      val fields = struct.map(field => jsonTypeNode(field.dataType, field.nullable))
      TypeBuilder.makeStruct(nullable, fields.asJava, struct.fieldNames.toSeq.asJava)
    case array: ArrayType =>
      TypeBuilder.makeList(nullable, jsonTypeNode(array.elementType, array.containsNull))
    case map: MapType =>
      TypeBuilder.makeMap(
        nullable,
        jsonTypeNode(map.keyType, nullable = false),
        jsonTypeNode(map.valueType, map.valueContainsNull))
    case primitive =>
      ConverterUtils.getTypeNode(primitive, nullable)
  }
}

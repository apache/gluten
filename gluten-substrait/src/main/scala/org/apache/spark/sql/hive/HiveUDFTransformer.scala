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
package org.apache.spark.sql.hive

import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression.{ExpressionConverter, ExpressionTransformer, GenericExpressionTransformer, UDFMappings}

import org.apache.spark.sql.catalyst.expressions._

import java.util.Locale

object HiveUDFTransformer {
  def isHiveUDF(expr: Expression): Boolean = {
    expr match {
      case _: HiveSimpleUDF | _: HiveGenericUDF | _: HiveGenericUDTF => true
      case _ => false
    }
  }

  def getHiveUDFNameAndClassName(expr: Expression): (String, String) = expr match {
    case s: HiveSimpleUDF =>
      (s.name.stripPrefix("default."), s.funcWrapper.functionClassName)
    case g: HiveGenericUDF =>
      (g.name.stripPrefix("default."), g.funcWrapper.functionClassName)
    case t: HiveGenericUDTF =>
      (t.name.stripPrefix("default."), t.funcWrapper.functionClassName)
    case _ =>
      throw new GlutenNotSupportException(
        s"Expression $expr is not a HiveSimpleUDF or HiveGenericUDF")
  }

  def replaceWithExpressionTransformer(
      expr: Expression,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    val (udfName, _) = getHiveUDFNameAndClassName(expr)
    genTransformerFromUDFMappings(udfName, expr, attributeSeq)
  }

  def genTransformerFromUDFMappings(
      udfName: String,
      expr: Expression,
      attributeSeq: Seq[Attribute]): GenericExpressionTransformer = {
    UDFMappings.hiveUDFMap.get(udfName.toLowerCase(Locale.ROOT)) match {
      case Some(name) =>
        GenericExpressionTransformer(
          name,
          ExpressionConverter.replaceWithExpressionTransformer(expr.children, attributeSeq),
          expr)
      case _ =>
        throw new GlutenNotSupportException(
          s"Not supported hive udf:$expr"
            + s" name:$udfName hiveUDFMap:${UDFMappings.hiveUDFMap}")
    }
  }
}

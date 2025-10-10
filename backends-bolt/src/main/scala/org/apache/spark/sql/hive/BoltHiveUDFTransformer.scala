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

import org.apache.gluten.expression.{ExpressionConverter, ExpressionTransformer, UDFMappings}

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.expression.UDFResolver

import java.util.Locale

object BoltHiveUDFTransformer {
  def replaceWithExpressionTransformer(
      expr: Expression,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    val (udfName, udfClassName) = HiveUDFTransformer.getHiveUDFNameAndClassName(expr)

    if (UDFResolver.UDFNames.contains(udfClassName)) {
      val udfExpression = UDFResolver
        .getUdfExpression(udfClassName, udfName)(expr.children)
      udfExpression.getTransformer(
        ExpressionConverter.replaceWithExpressionTransformer(udfExpression.children, attributeSeq)
      )
    } else {
      HiveUDFTransformer.genTransformerFromUDFMappings(udfName, expr, attributeSeq)
    }
  }

  /**
   * Check whether the input hive udf expression is supported to transform. It maybe transformed by
   * [[BoltHiveUDFTransformer]] or [[HiveUDFTransformer]].
   */
  def isSupportedHiveUDF(expr: Expression): Boolean = {
    val (udfName, udfClassName) = HiveUDFTransformer.getHiveUDFNameAndClassName(expr)
    // Transformable by BoltHiveUDFTransformer
    UDFResolver.UDFNames.contains(udfClassName) ||
    // Transformable by HiveUDFTransformer
    UDFMappings.hiveUDFMap.contains(udfName.toLowerCase(Locale.ROOT))
  }
}

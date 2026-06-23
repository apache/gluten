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
package org.apache.gluten.expressions

import org.apache.gluten.expression._
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.GlutenQueryTest
import org.apache.spark.sql.catalyst.expressions.{CheckOverflow, Literal}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{Decimal, DecimalType}

class GlutenCheckOverflowTransformerSuite extends GlutenQueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.ui.enabled", "false")
  }

  testWithSpecifiedSparkVersion(
    "CheckOverflow transformer casts transformed child type",
    "3.3",
    "3.4",
    "3.5",
    "4.0",
    "4.1") {
    assume(BackendTestUtils.isVeloxBackendLoaded())

    val targetType = DecimalType(38, 17)
    val transformedChildType = DecimalType(38, 18)
    val original = CheckOverflow(
      Literal(Decimal(0, targetType.precision, targetType.scale), targetType),
      targetType,
      nullOnOverflow = true)
    val child = LiteralTransformer(
      Literal(
        Decimal(0, transformedChildType.precision, transformedChildType.scale),
        transformedChildType))
    assert(original.child.dataType != child.dataType)

    val transformedNode =
      CheckOverflowTransformer(ExpressionNames.CHECK_OVERFLOW, child, original)
        .doTransform(new SubstraitContext)
        .toProtobuf
    assert(transformedNode.hasCast)
    val castType = transformedNode.getCast.getType.getDecimal
    assert(castType.getPrecision == targetType.precision)
    assert(castType.getScale == targetType.scale)
  }
}

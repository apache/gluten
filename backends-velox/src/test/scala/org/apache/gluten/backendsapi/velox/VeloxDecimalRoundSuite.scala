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
package org.apache.gluten.backendsapi.velox

import org.apache.gluten.expression.{DecimalRoundTransformer, LiteralTransformer}
import org.apache.gluten.substrait.SubstraitContext

import org.apache.spark.sql.catalyst.expressions.{Add, BoundReference, Cast, Literal, Round}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.scalatest.funsuite.AnyFunSuite

class VeloxDecimalRoundSuite extends AnyFunSuite {
  test("decimal round preserves a typed NULL scale") {
    val value = Literal.create(Decimal(new java.math.BigDecimal("2.5")), DecimalType(3, 1))
    val scale = Literal.create(null, IntegerType)
    val original = Round(value, scale)
    val transformer = DecimalRoundTransformer("round", LiteralTransformer(value), original)
    val scalar = transformer.doTransform(new SubstraitContext).toProtobuf.getScalarFunction
    assert(scalar.getArgumentsCount == 2)
    assert(scalar.getArguments(1).getValue.getLiteral.hasNull)
    assert(transformer.dataType == DecimalType(3, 0))
  }

  test("decimal round eligibility defers to native validation") {
    val validator = new VeloxValidatorApi
    val value = BoundReference(0, DecimalType(38, 38), nullable = true)
    val scales = Seq(
      Literal(-401),
      Literal(401),
      BoundReference(1, IntegerType, nullable = false),
      Add(Literal(400), Literal(1)))
    scales.foreach {
      scale => assert(validator.doExprValidate("round", Round(value, scale)))
    }
    val conf = new SQLConf
    conf.setConfString(SQLConf.ANSI_ENABLED.key, "true")
    SQLConf.withExistingConf(conf) {
      val scale = Cast(Literal("invalid-scale"), IntegerType)
      intercept[Exception](scale.eval())
      assert(validator.doExprValidate("round", Round(value, scale)))
    }
  }
}

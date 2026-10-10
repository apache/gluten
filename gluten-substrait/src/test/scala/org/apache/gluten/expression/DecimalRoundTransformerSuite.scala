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

import org.apache.spark.sql.catalyst.expressions.{BRound, Literal, Round}
import org.apache.spark.sql.types.{DecimalType, IntegerType}

import org.scalatest.funsuite.AnyFunSuite

class DecimalRoundTransformerSuite extends AnyFunSuite {
  test("supports round and bround decimal expressions") {
    val value = Literal.create(new java.math.BigDecimal("2.55"), DecimalType(3, 2))
    val scale = Literal(1)

    Seq(("round", Round(value, scale)), ("bround", BRound(value, scale))).foreach {
      case (name, expression) =>
        val transformer =
          DecimalRoundTransformer(name, LiteralTransformer(value), expression)
        assert(transformer.dataType == DecimalType(3, 1))
        assert(transformer.right.original.asInstanceOf[Literal].value == 1)
    }
  }

  test("preserves null scale for decimal bround") {
    val value = Literal.create(new java.math.BigDecimal("2.55"), DecimalType(3, 2))
    val scale = Literal.create(null, IntegerType)
    val transformer =
      DecimalRoundTransformer("bround", LiteralTransformer(value), BRound(value, scale))

    assert(transformer.dataType == DecimalType(2, 0))
    assert(transformer.right.original.asInstanceOf[Literal].value == null)
  }

  test("handles extreme negative decimal bround scale") {
    val value = Literal.create(new java.math.BigDecimal("0.1"), DecimalType(2, 1))
    val transformer = DecimalRoundTransformer(
      "bround",
      LiteralTransformer(value),
      BRound(value, Literal(Int.MinValue)))

    assert(transformer.dataType == DecimalType(38, 0))
  }
}

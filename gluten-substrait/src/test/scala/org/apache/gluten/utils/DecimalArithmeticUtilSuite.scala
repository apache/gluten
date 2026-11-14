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
package org.apache.gluten.utils

import org.apache.gluten.exception.GlutenNotSupportException

import org.apache.spark.sql.catalyst.expressions.{Literal, Pmod, Remainder}
import org.apache.spark.sql.types.DecimalType

import org.scalatest.funsuite.AnyFunSuite

class DecimalArithmeticUtilSuite extends AnyFunSuite {

  private val decimalType = DecimalType(12, 2)
  private val left = Literal.create(BigDecimal("1.00"), decimalType)
  private val right = Literal.create(BigDecimal("2.00"), decimalType)

  // The shared result-type utility must keep rejecting these operators. Supporting one through a
  // backend override must not bypass validation or change behavior for other backends.
  test("remainder and pmod are admitted but have no result type") {
    Seq(Remainder(left, right), Pmod(left, right)).foreach {
      expr =>
        assert(DecimalArithmeticUtil.isDecimalArithmetic(expr))
        intercept[GlutenNotSupportException] {
          DecimalArithmeticUtil.getResultType(expr, decimalType, decimalType)
        }
    }
  }
}

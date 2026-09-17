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

import org.apache.spark.sql.catalyst.expressions.{BoundReference, BRound, Literal, Round}
import org.apache.spark.sql.types._

import org.scalatest.funsuite.AnyFunSuite

import scala.util.Properties

class VeloxValidatorApiSuite extends AnyFunSuite {
  private val validator = new VeloxValidatorApi

  test("floating bround with nonzero scale requires the qualified JVM conversion") {
    Seq(FloatType, DoubleType).foreach {
      dataType =>
        Seq(-3, 2).foreach {
          scale =>
            val expression =
              new BRound(BoundReference(0, dataType, nullable = true), Literal(scale))
            assert(validator.doExprValidate("bround", expression) == Properties.isJavaAtLeast("21"))
        }
    }
  }

  test("floating bround at scale zero is supported on every JVM") {
    Seq(FloatType, DoubleType).foreach {
      dataType =>
        val expression = new BRound(BoundReference(0, dataType, nullable = true), Literal(0))
        assert(validator.doExprValidate("bround", expression))
    }
  }

  test("floating bround with null scale is supported on every JVM") {
    Seq(FloatType, DoubleType).foreach {
      dataType =>
        val expression = new BRound(
          BoundReference(0, dataType, nullable = true),
          Literal.create(null, IntegerType))
        assert(validator.doExprValidate("bround", expression))
    }
  }

  test("integral and decimal bround are independent of floating conversion") {
    Seq(ByteType, ShortType, IntegerType, LongType, DecimalType(38, 38)).foreach {
      dataType =>
        val expression = new BRound(BoundReference(0, dataType, nullable = true), Literal(-3))
        assert(validator.doExprValidate("bround", expression))
    }
  }

  test("bround JVM validation does not change round validation") {
    val expression = new Round(BoundReference(0, DoubleType, nullable = true), Literal(2))
    assert(validator.doExprValidate("round", expression))
  }

  test("unsupported bround scales are rejected before deferred native initialization") {
    Seq(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType(38, 38))
      .foreach {
        dataType =>
          Seq(Int.MinValue, -401, 401, Int.MaxValue).foreach {
            scale =>
              val expression =
                new BRound(BoundReference(0, dataType, nullable = true), Literal(scale))
              assert(!validator.doExprValidate("bround", expression))
          }
      }
  }

  test("native bround scale bounds are inclusive") {
    Seq(IntegerType, DecimalType(38, 38)).foreach {
      dataType =>
        Seq(-400, 400).foreach {
          scale =>
            val expression =
              new BRound(BoundReference(0, dataType, nullable = true), Literal(scale))
            assert(validator.doExprValidate("bround", expression))
        }
    }
  }

  test("bround scale fields are not accepted as constants") {
    val expression = new BRound(
      BoundReference(0, DoubleType, nullable = true),
      BoundReference(1, IntegerType, nullable = false))
    assert(!validator.doExprValidate("bround", expression))
  }
}

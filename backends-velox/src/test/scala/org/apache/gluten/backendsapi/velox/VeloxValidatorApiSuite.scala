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

import org.apache.gluten.backendsapi.velox.VeloxValidatorApi._

import org.apache.spark.sql.catalyst.expressions.{BoundReference, BRound, Literal, Round}
import org.apache.spark.sql.types._

import org.scalatest.funsuite.AnyFunSuite

import scala.util.Properties

class VeloxValidatorApiSuite extends AnyFunSuite {
  private val validator = new VeloxValidatorApi

  test("bround compatibility constants match the qualified native contract") {
    assert(MIN_BROUND_SCALE == -400)
    assert(MAX_BROUND_SCALE == 400)
    assert(MIN_BROUND_FLOATING_JAVA_VERSION == "21")
  }

  test("floating bround with nonzero scale requires the qualified JVM conversion") {
    Seq(FloatType, DoubleType).foreach {
      dataType =>
        Seq(-3, 2).foreach {
          scale =>
            val expression =
              new BRound(BoundReference(0, dataType, nullable = true), Literal(scale))
            assert(
              validator.doExprValidate("bround", expression) ==
                Properties.isJavaAtLeast(MIN_BROUND_FLOATING_JAVA_VERSION))
        }
    }
  }

  test("floating bround JVM qualification is cached across validator instances") {
    val expected = Properties.isJavaAtLeast(MIN_BROUND_FLOATING_JAVA_VERSION)
    val expression = new BRound(BoundReference(0, DoubleType, nullable = true), Literal(2))
    assert(validator.doExprValidate("bround", expression) == expected)
    val property = "java.specification.version"
    val previous = System.getProperty(property)
    try {
      System.setProperty(property, if (expected) "1.8" else "99")
      assert(validator.doExprValidate("bround", expression) == expected)
      assert(new VeloxValidatorApi().doExprValidate("bround", expression) == expected)
    } finally {
      if (previous == null) {
        System.clearProperty(property)
      } else {
        System.setProperty(property, previous)
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

  test("bround compatibility checks do not change round validation") {
    Seq(MIN_BROUND_SCALE - 1, -3, 0, 2, MAX_BROUND_SCALE + 1).foreach {
      scale =>
        val child = BoundReference(0, DoubleType, nullable = true)
        val round = new Round(child, Literal(scale))
        val bround = new BRound(child, Literal(scale))
        assert(validator.doExprValidate("round", round))
        val expectNativeBround = scale >= MIN_BROUND_SCALE && scale <= MAX_BROUND_SCALE &&
          (scale == 0 || Properties.isJavaAtLeast(MIN_BROUND_FLOATING_JAVA_VERSION))
        assert(validator.doExprValidate("bround", bround) == expectNativeBround)
    }
  }

  test("unsupported bround scales are rejected before deferred native initialization") {
    Seq(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType(38, 38))
      .foreach {
        dataType =>
          Seq(Int.MinValue, MIN_BROUND_SCALE - 1, MAX_BROUND_SCALE + 1, Int.MaxValue).foreach {
            scale =>
              val expression =
                new BRound(BoundReference(0, dataType, nullable = true), Literal(scale))
              assert(!validator.doExprValidate("bround", expression))
          }
      }
  }

  test("native bround scale bounds are inclusive") {
    Seq(
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType,
      DecimalType(38, 38)).foreach {
      dataType =>
        Seq(MIN_BROUND_SCALE, MAX_BROUND_SCALE).foreach {
          scale =>
            val expression =
              new BRound(BoundReference(0, dataType, nullable = true), Literal(scale))
            val expectNative = (dataType != FloatType && dataType != DoubleType) ||
              Properties.isJavaAtLeast(MIN_BROUND_FLOATING_JAVA_VERSION)
            assert(validator.doExprValidate("bround", expression) == expectNative)
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

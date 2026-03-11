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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.GlutenTestsTrait
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types._

class GlutenMathExpressionsSuite extends MathExpressionsSuite with GlutenTestsTrait {
  testGluten("round/bround/floor/ceil") {
    val scales = -6 to 6
    val doublePi: Double = math.Pi
    val shortPi: Short = 31415
    val intPi: Int = 314159265
    val longPi: Long = 31415926535897932L
    val bdPi: BigDecimal = BigDecimal(31415927L, 7)
    val floatPi: Float = 3.1415f

    val doubleResults: Seq[Double] =
      Seq(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3.0, 3.1, 3.14, 3.142, 3.1416, 3.14159, 3.141593)

    val floatResults: Seq[Float] =
      Seq(0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 3.0f, 3.1f, 3.14f, 3.142f, 3.1415f, 3.1415f, 3.1415f)

    val bRoundFloatResults: Seq[Float] =
      Seq(0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 3.0f, 3.1f, 3.14f, 3.141f, 3.1415f, 3.1415f, 3.1415f)

    val shortResults: Seq[Short] = Seq[Short](0, 0, 30000, 31000, 31400, 31420) ++
      Seq.fill[Short](7)(31415)

    val intResults: Seq[Int] =
      Seq(314000000, 314200000, 314160000, 314159000, 314159300, 314159270) ++ Seq.fill(7)(
        314159265)

    val longResults: Seq[Long] = Seq(31415926536000000L, 31415926535900000L, 31415926535900000L,
      31415926535898000L, 31415926535897900L, 31415926535897930L) ++
      Seq.fill(7)(31415926535897932L)

    val intResultsB: Seq[Int] =
      Seq(314000000, 314200000, 314160000, 314159000, 314159300, 314159260) ++ Seq.fill(7)(
        314159265)

    def doubleResultsFloor(i: Int): Decimal = {
      val results = Seq(0, 0, 0, 0, 0, 0, 3, 3.1, 3.14, 3.141, 3.1415, 3.14159, 3.141592)
      Decimal(results(i))
    }

    def doubleResultsCeil(i: Int): Any = {
      val results =
        Seq(1000000, 100000, 10000, 1000, 100, 10, 4, 3.2, 3.15, 3.142, 3.1416, 3.1416, 3.141593)
      Decimal(results(i))
    }

    def floatResultsFloor(i: Int): Any = {
      val results = Seq(0, 0, 0, 0, 0, 0, 3, 3.1, 3.14, 3.141, 3.1415, 3.1415, 3.1415)
      Decimal(results(i))
    }

    def floatResultsCeil(i: Int): Any = {
      val results =
        Seq(1000000, 100000, 10000, 1000, 100, 10, 4, 3.2, 3.15, 3.142, 3.1415, 3.1415, 3.1415)
      Decimal(results(i))
    }

    def shortResultsFloor(i: Int): Decimal = {
      val results = Seq(0, 0, 30000, 31000, 31400, 31410) ++ Seq.fill(7)(31415)
      Decimal(results(i))
    }

    def shortResultsCeil(i: Int): Decimal = {
      val results = Seq(1000000, 100000, 40000, 32000, 31500, 31420) ++ Seq.fill(7)(31415)
      Decimal(results(i))
    }

    def longResultsFloor(i: Int): Decimal = {
      val results = Seq(31415926535000000L, 31415926535800000L, 31415926535890000L,
        31415926535897000L, 31415926535897900L, 31415926535897930L, 31415926535897932L) ++
        Seq.fill(6)(31415926535897932L)
      Decimal(results(i))
    }

    def longResultsCeil(i: Int): Decimal = {
      val results = Seq(31415926536000000L, 31415926535900000L, 31415926535900000L,
        31415926535898000L, 31415926535898000L, 31415926535897940L) ++
        Seq.fill(7)(31415926535897932L)
      Decimal(results(i))
    }

    def intResultsFloor(i: Int): Decimal = {
      val results =
        Seq(314000000, 314100000, 314150000, 314159000, 314159200, 314159260) ++ Seq.fill(7)(
          314159265)
      Decimal(results(i))
    }

    def intResultsCeil(i: Int): Decimal = {
      val results =
        Seq(315000000, 314200000, 314160000, 314160000, 314159300, 314159270) ++ Seq.fill(7)(
          314159265)
      Decimal(results(i))
    }

    scales.zipWithIndex.foreach {
      case (scale, i) =>
        checkEvaluation(Round(doublePi, scale), doubleResults(i), EmptyRow)
        checkEvaluation(Round(shortPi, scale), shortResults(i), EmptyRow)
        checkEvaluation(Round(intPi, scale), intResults(i), EmptyRow)
        checkEvaluation(Round(longPi, scale), longResults(i), EmptyRow)
        checkEvaluation(Round(floatPi, scale), floatResults(i), EmptyRow)
        checkEvaluation(BRound(doublePi, scale), doubleResults(i), EmptyRow)
        checkEvaluation(BRound(shortPi, scale), shortResults(i), EmptyRow)
        checkEvaluation(BRound(intPi, scale), intResultsB(i), EmptyRow)
        checkEvaluation(BRound(longPi, scale), longResults(i), EmptyRow)
        checkEvaluation(BRound(floatPi, scale), bRoundFloatResults(i), EmptyRow)
        checkEvaluation(
          checkDataTypeAndCast(RoundFloor(Literal(doublePi), Literal(scale))),
          doubleResultsFloor(i),
          EmptyRow)
        checkEvaluation(
          checkDataTypeAndCast(RoundFloor(Literal(shortPi), Literal(scale))),
          shortResultsFloor(i),
          EmptyRow)
        checkEvaluation(
          checkDataTypeAndCast(RoundFloor(Literal(intPi), Literal(scale))),
          intResultsFloor(i),
          EmptyRow)
        checkEvaluation(
          checkDataTypeAndCast(RoundFloor(Literal(longPi), Literal(scale))),
          longResultsFloor(i),
          EmptyRow)
        checkEvaluation(
          checkDataTypeAndCast(RoundFloor(Literal(floatPi), Literal(scale))),
          floatResultsFloor(i),
          EmptyRow)
        checkEvaluation(
          checkDataTypeAndCast(RoundCeil(Literal(doublePi), Literal(scale))),
          doubleResultsCeil(i),
          EmptyRow)
        checkEvaluation(
          checkDataTypeAndCast(RoundCeil(Literal(shortPi), Literal(scale))),
          shortResultsCeil(i),
          EmptyRow)
        checkEvaluation(
          checkDataTypeAndCast(RoundCeil(Literal(intPi), Literal(scale))),
          intResultsCeil(i),
          EmptyRow)
        checkEvaluation(
          checkDataTypeAndCast(RoundCeil(Literal(longPi), Literal(scale))),
          longResultsCeil(i),
          EmptyRow)
        checkEvaluation(
          checkDataTypeAndCast(RoundCeil(Literal(floatPi), Literal(scale))),
          floatResultsCeil(i),
          EmptyRow)
    }

    val bdResults: Seq[BigDecimal] = Seq(
      BigDecimal(3),
      BigDecimal("3.1"),
      BigDecimal("3.14"),
      BigDecimal("3.142"),
      BigDecimal("3.1416"),
      BigDecimal("3.14159"),
      BigDecimal("3.141593"),
      BigDecimal("3.1415927")
    )

    val bdResultsFloor: Seq[BigDecimal] =
      Seq(
        BigDecimal(3),
        BigDecimal("3.1"),
        BigDecimal("3.14"),
        BigDecimal("3.141"),
        BigDecimal("3.1415"),
        BigDecimal("3.14159"),
        BigDecimal("3.141592"),
        BigDecimal("3.1415927")
      )

    val bdResultsCeil: Seq[BigDecimal] = Seq(
      BigDecimal(4),
      BigDecimal("3.2"),
      BigDecimal("3.15"),
      BigDecimal("3.142"),
      BigDecimal("3.1416"),
      BigDecimal("3.14160"),
      BigDecimal("3.141593"),
      BigDecimal("3.1415927")
    )

    (0 to 7).foreach {
      i =>
        checkEvaluation(Round(bdPi, i), bdResults(i), EmptyRow)
        checkEvaluation(BRound(bdPi, i), bdResults(i), EmptyRow)
        checkEvaluation(RoundFloor(bdPi, i), bdResultsFloor(i), EmptyRow)
        checkEvaluation(RoundCeil(bdPi, i), bdResultsCeil(i), EmptyRow)
    }
    (8 to 10).foreach {
      scale =>
        checkEvaluation(Round(bdPi, scale), bdPi, EmptyRow)
        checkEvaluation(BRound(bdPi, scale), bdPi, EmptyRow)
        checkEvaluation(RoundFloor(bdPi, scale), bdPi, EmptyRow)
        checkEvaluation(RoundCeil(bdPi, scale), bdPi, EmptyRow)
    }

    DataTypeTestUtils.numericTypes.foreach {
      dataType =>
        checkEvaluation(Round(Literal.create(null, dataType), Literal(2)), null)
        checkEvaluation(
          Round(Literal.create(null, dataType), Literal.create(null, IntegerType)),
          null)
        checkEvaluation(BRound(Literal.create(null, dataType), Literal(2)), null)
        checkEvaluation(
          BRound(Literal.create(null, dataType), Literal.create(null, IntegerType)),
          null)
        checkEvaluation(
          checkDataTypeAndCast(RoundFloor(Literal.create(null, dataType), Literal(2))),
          null)
        checkEvaluation(
          checkDataTypeAndCast(RoundCeil(Literal.create(null, dataType), Literal(2))),
          null)
    }

    checkEvaluation(Round(2.5, 0), 3.0)
    checkEvaluation(Round(3.5, 0), 4.0)
    checkEvaluation(Round(-2.5, 0), -3.0)
    checkEvaluation(Round(-3.5, 0), -4.0)
    checkEvaluation(Round(-0.35, 1), -0.4)
    checkEvaluation(Round(-35, -1), -40)
    checkEvaluation(Round(BigDecimal("45.00"), -1), BigDecimal(50))
    checkEvaluation(Round(44, -1), 40)
    checkEvaluation(Round(78, 1), 78)
    checkEvaluation(BRound(2.5, 0), 2.0)
    checkEvaluation(BRound(3.5, 0), 4.0)
    checkEvaluation(BRound(-2.5, 0), -2.0)
    checkEvaluation(BRound(-3.5, 0), -4.0)
    checkEvaluation(BRound(-0.35, 1), -0.4)
    checkEvaluation(BRound(-35, -1), -40)
    checkEvaluation(Round(1.12345678901234567, 8), 1.12345679)
    checkEvaluation(Round(-0.98765432109876543, 5), -0.98765)
    checkEvaluation(Round(12345.67890123456789, 6), 12345.678901)
    // Enable the test after fixing https://github.com/apache/incubator-gluten/issues/6827
    // checkEvaluation(Round(0.5549999999999999, 2), 0.55)
    checkEvaluation(BRound(BigDecimal("45.00"), -1), BigDecimal(40))
    checkEvaluation(checkDataTypeAndCast(RoundFloor(Literal(2.5), Literal(0))), Decimal(2))
    checkEvaluation(checkDataTypeAndCast(RoundFloor(Literal(3.5), Literal(0))), Decimal(3))
    checkEvaluation(checkDataTypeAndCast(RoundFloor(Literal(-2.5), Literal(0))), Decimal(-3L))
    checkEvaluation(checkDataTypeAndCast(RoundFloor(Literal(-3.5), Literal(0))), Decimal(-4L))
    checkEvaluation(checkDataTypeAndCast(RoundFloor(Literal(-0.35), Literal(1))), Decimal(-0.4))
    checkEvaluation(checkDataTypeAndCast(RoundFloor(Literal(-35), Literal(-1))), Decimal(-40))
    checkEvaluation(checkDataTypeAndCast(RoundFloor(Literal(-0.1), Literal(0))), Decimal(-1))
    checkEvaluation(checkDataTypeAndCast(RoundFloor(Literal(5), Literal(0))), Decimal(5))
    checkEvaluation(checkDataTypeAndCast(RoundFloor(Literal(3.1411), Literal(-3))), Decimal(0))
    checkEvaluation(checkDataTypeAndCast(RoundFloor(Literal(135.135), Literal(-2))), Decimal(100))
    checkEvaluation(checkDataTypeAndCast(RoundCeil(Literal(2.5), Literal(0))), Decimal(3))
    checkEvaluation(checkDataTypeAndCast(RoundCeil(Literal(3.5), Literal(0))), Decimal(4L))
    checkEvaluation(checkDataTypeAndCast(RoundCeil(Literal(-2.5), Literal(0))), Decimal(-2L))
    checkEvaluation(checkDataTypeAndCast(RoundCeil(Literal(-3.5), Literal(0))), Decimal(-3L))
    checkEvaluation(checkDataTypeAndCast(RoundCeil(Literal(-0.35), Literal(1))), Decimal(-0.3))
    checkEvaluation(checkDataTypeAndCast(RoundCeil(Literal(-35), Literal(-1))), Decimal(-30))
    checkEvaluation(checkDataTypeAndCast(RoundCeil(Literal(-0.1), Literal(0))), Decimal(0))
    checkEvaluation(checkDataTypeAndCast(RoundCeil(Literal(5), Literal(0))), Decimal(5))
    checkEvaluation(checkDataTypeAndCast(RoundCeil(Literal(3.1411), Literal(-3))), Decimal(1000))
    checkEvaluation(checkDataTypeAndCast(RoundCeil(Literal(135.135), Literal(-2))), Decimal(200))
  }
}

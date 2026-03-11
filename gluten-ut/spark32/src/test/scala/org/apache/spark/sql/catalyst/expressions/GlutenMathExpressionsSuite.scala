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

import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.sql.GlutenTestsTrait
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types._

class GlutenMathExpressionsSuite extends MathExpressionsSuite with GlutenTestsTrait {

  override def testNameBlackList: Seq[String] = super.testNameBlackList ++ Seq(
    "bin"
  )

  testGluten("round/bround") {
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
        checkEvaluation(
          BRound(floatPi, scale),
          // the velox backend will fallback when executing bround,
          // so uses the same excepted results with the vanilla spark
          if (BackendTestUtils.isCHBackendLoaded()) floatResults(i) else bRoundFloatResults(i),
          EmptyRow
        )
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

    (0 to 7).foreach {
      i =>
        checkEvaluation(Round(bdPi, i), bdResults(i), EmptyRow)
        checkEvaluation(BRound(bdPi, i), bdResults(i), EmptyRow)
    }
    (8 to 10).foreach {
      scale =>
        checkEvaluation(Round(bdPi, scale), bdPi, EmptyRow)
        checkEvaluation(BRound(bdPi, scale), bdPi, EmptyRow)
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
    }

    checkEvaluation(Round(2.5, 0), 3.0)
    checkEvaluation(Round(3.5, 0), 4.0)
    checkEvaluation(Round(-2.5, 0), -3.0)
    checkEvaluation(Round(-3.5, 0), -4.0)
    checkEvaluation(Round(-0.35, 1), -0.4)
    checkEvaluation(Round(-35, -1), -40)
    checkEvaluation(Round(1.12345678901234567, 8), 1.12345679)
    checkEvaluation(Round(-0.98765432109876543, 5), -0.98765)
    checkEvaluation(Round(12345.67890123456789, 6), 12345.678901)
    checkEvaluation(Round(44, -1), 40)
    checkEvaluation(Round(78, 1), 78)
    // Enable the test after fixing https://github.com/apache/gluten/issues/6827
    // checkEvaluation(Round(0.5549999999999999, 2), 0.55)
    checkEvaluation(BRound(2.5, 0), 2.0)
    checkEvaluation(BRound(3.5, 0), 4.0)
    checkEvaluation(BRound(-2.5, 0), -2.0)
    checkEvaluation(BRound(-3.5, 0), -4.0)
    checkEvaluation(BRound(-0.35, 1), -0.4)
    checkEvaluation(BRound(-35, -1), -40)
  }
}

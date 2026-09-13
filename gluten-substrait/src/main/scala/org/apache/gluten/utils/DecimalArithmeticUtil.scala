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
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.catalyst.expressions.{Add, BinaryArithmetic, Divide, Multiply, Pmod, Remainder, Subtract}
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.utils.DecimalTypeUtil

object DecimalArithmeticUtil {

  val MIN_ADJUSTED_SCALE = 6
  val MAX_PRECISION = 38
  val MAX_SCALE = 38

  // Returns the result decimal type of a decimal arithmetic computing.
  def getResultType(expr: BinaryArithmetic, type1: DecimalType, type2: DecimalType): DecimalType = {

    val allowPrecisionLoss = SparkShimLoader.getSparkShims.decimalAllowPrecisionLoss(expr)
    var resultScale = 0
    var resultPrecision = 0
    expr match {
      case _: Add =>
        resultScale = Math.max(type1.scale, type2.scale)
        resultPrecision =
          resultScale + Math.max(type1.precision - type1.scale, type2.precision - type2.scale) + 1
      case _: Subtract =>
        resultScale = Math.max(type1.scale, type2.scale)
        resultPrecision =
          resultScale + Math.max(type1.precision - type1.scale, type2.precision - type2.scale) + 1
      case _: Multiply =>
        resultScale = type1.scale + type2.scale
        resultPrecision = type1.precision + type2.precision + 1
      case _: Divide =>
        if (allowPrecisionLoss) {
          resultScale = Math.max(MIN_ADJUSTED_SCALE, type1.scale + type2.precision + 1)
          resultPrecision = type1.precision - type1.scale + type2.scale + resultScale
        } else {
          var intDig = Math.min(MAX_SCALE, type1.precision - type1.scale + type2.scale)
          var decDig = Math.min(MAX_SCALE, Math.max(6, type1.scale + type2.precision + 1))
          val diff = (intDig + decDig) - MAX_SCALE
          if (diff > 0) {
            decDig -= diff / 2 + 1
            intDig = MAX_SCALE - decDig
          }
          resultPrecision = intDig + decDig
          resultScale = decDig
        }
      // Remainder and Pmod land here: isDecimalArithmetic admits them but no result type is
      // derived above. On the transformCheckOverflow path this throw is what makes decimal % and
      // pmod fall back.
      case other =>
        throw new GlutenNotSupportException(s"$other is not supported.")
    }

    if (allowPrecisionLoss) {
      DecimalTypeUtil.adjustPrecisionScale(resultPrecision, resultScale)
    } else {
      bounded(resultPrecision, resultScale)
    }

  }

  def bounded(precision: Int, scale: Int): DecimalType = {
    DecimalType(Math.min(precision, MAX_PRECISION), Math.min(scale, MAX_SCALE))
  }

  // Whether the expression is an arithmetic over two decimals. Remainder and Pmod are admitted on
  // purpose even though getResultType rejects them: dropping them here would send both through the
  // generic arm and offload them. See the comment on that rejection in getResultType.
  def isDecimalArithmetic(b: BinaryArithmetic): Boolean = {
    if (
      b.left.dataType.isInstanceOf[DecimalType] &&
      b.right.dataType.isInstanceOf[DecimalType]
    ) {
      b match {
        case _: Divide | _: Multiply | _: Add | _: Subtract | _: Remainder | _: Pmod => true
        case _ => false
      }
    } else false
  }
}

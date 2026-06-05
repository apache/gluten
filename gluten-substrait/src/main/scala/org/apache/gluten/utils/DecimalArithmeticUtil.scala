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

import org.apache.spark.sql.catalyst.expressions.{Add, BinaryArithmetic, Cast, Divide, Expression, Literal, Multiply, Pmod, PromotePrecision, Remainder, Subtract}
import org.apache.spark.sql.types.{ByteType, Decimal, DecimalType, IntegerType, LongType, ShortType}
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

  // If casting between DecimalType, unnecessary cast is skipped to avoid data loss,
  // because argument input type of "cast" is actually the res type of "+-*/".
  // Cast will use a wider input type, then calculates result type with less scale than expected.
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

  // For decimal * 10 case, dec will be Decimal(38, 18), then the result precision is wrong,
  // so here we will get the real precision and scale of the literal.
  private def getNewPrecisionScale(dec: Decimal): (Integer, Integer) = {
    val input = dec.abs.toJavaBigDecimal.toPlainString()
    val dotIndex = input.indexOf(".")
    if (dotIndex == -1) {
      return (input.length, 0)
    }
    if (dec.toBigDecimal.isValidLong) {
      return (dotIndex, 0)
    }
    (dec.precision, dec.scale)
  }

  // Change the precision and scale to the actual precision and scale of a literal,
  // otherwise the result precision loses.
  def rescaleLiteral(arithmeticExpr: BinaryArithmetic): BinaryArithmetic = {
    if (
      arithmeticExpr.left.isInstanceOf[PromotePrecision] &&
      arithmeticExpr.right.isInstanceOf[Literal]
    ) {
      val lit = arithmeticExpr.right.asInstanceOf[Literal]
      lit.value match {
        case decLit: Decimal =>
          val (precision, scale) = getNewPrecisionScale(decLit)
          if (precision != decLit.precision || scale != decLit.scale) {
            arithmeticExpr
              .withNewChildren(Seq(arithmeticExpr.left, Cast(lit, DecimalType(precision, scale))))
              .asInstanceOf[BinaryArithmetic]
          } else arithmeticExpr
        case _ => arithmeticExpr
      }
    } else if (
      arithmeticExpr.right.isInstanceOf[PromotePrecision]
      && arithmeticExpr.left.isInstanceOf[Literal]
    ) {
      val lit = arithmeticExpr.left.asInstanceOf[Literal]
      lit.value match {
        case decLit: Decimal =>
          val (precision, scale) = getNewPrecisionScale(decLit)
          if (precision != decLit.precision || scale != decLit.scale) {
            arithmeticExpr
              .withNewChildren(Seq(Cast(lit, DecimalType(precision, scale)), arithmeticExpr.right))
              .asInstanceOf[BinaryArithmetic]
          } else arithmeticExpr
        case _ => arithmeticExpr
      }
    } else {
      arithmeticExpr
    }
  }

  // Returns whether the input expression is a combination of PromotePrecision(Cast as DecimalType).
  private def isPromoteCast(expr: Expression): Boolean = expr match {
    case PromotePrecision(Cast(_, _: DecimalType, _, _)) => true
    case _ => false
  }

  def rescaleCastForDecimal(left: Expression, right: Expression): (Expression, Expression) = {

    def doScale(e1: Expression, e2: Expression): (Expression, Expression) = {
      val newE2 = rescaleCastForOneSide(e2)
      val isWiderType = checkIsWiderType(
        e1.dataType.asInstanceOf[DecimalType],
        newE2.dataType.asInstanceOf[DecimalType],
        e2.dataType.asInstanceOf[DecimalType])
      if (isWiderType) (e1, newE2) else (e1, e2)
    }

    if (!isPromoteCast(left) && isPromoteCastIntegral(right)) {
      // Have removed PromotePrecision(Cast(DecimalType)).
      // Decimal * cast int.
      doScale(left, right)
    } else if (!isPromoteCast(right) && isPromoteCastIntegral(left)) {
      // Cast int * decimal.
      val (r, l) = doScale(right, left)
      (l, r)
    } else {
      (left, right)
    }
  }

  /**
   * Remove the Cast when child is PromotePrecision and PromotePrecision is Cast(Decimal, Decimal)
   *
   * @param arithmeticExpr
   *   BinaryArithmetic left or right
   * @return
   *   expression removed child PromotePrecision->Cast
   */
  def removeCastForDecimal(arithmeticExpr: Expression): Expression = arithmeticExpr match {
    case PromotePrecision(_ @Cast(child, _: DecimalType, _, _))
        if child.dataType.isInstanceOf[DecimalType] =>
      child
    case _ => arithmeticExpr
  }

  private def isPromoteCastIntegral(expr: Expression): Boolean = expr match {
    case PromotePrecision(_ @Cast(child, _: DecimalType, _, _)) =>
      child.dataType match {
        case IntegerType | ByteType | ShortType | LongType => true
        case _ => false
      }
    case _ => false
  }

  private def rescaleCastForOneSide(expr: Expression): Expression = expr match {
    case precision @ PromotePrecision(_ @Cast(child, _: DecimalType, _, _)) =>
      child.dataType match {
        case IntegerType | ByteType | ShortType =>
          precision.withNewChildren(Seq(Cast(child, DecimalType(10, 0))))
        case LongType =>
          precision.withNewChildren(Seq(Cast(child, DecimalType(20, 0))))
        case _ => expr
      }
    case _ => expr
  }

  private def checkIsWiderType(
      left: DecimalType,
      right: DecimalType,
      wider: DecimalType): Boolean = {
    val widerType = SparkShimLoader.getSparkShims.widerDecimalType(left, right)
    widerType.equals(wider)
  }
}

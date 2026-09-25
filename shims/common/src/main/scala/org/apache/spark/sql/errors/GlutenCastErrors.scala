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
package org.apache.spark.sql.errors

import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object GlutenCastErrors {
  private val integralTypes: Map[String, DataType] = Map(
    "TINYINT" -> ByteType,
    "SMALLINT" -> ShortType,
    "INTEGER" -> IntegerType,
    "BIGINT" -> LongType)

  private val integralOverflow =
    ("""Cannot cast (TINYINT|SMALLINT|INTEGER|BIGINT) '(-?[0-9]{1,19})' to """ +
      """(TINYINT|SMALLINT|INTEGER|BIGINT)\. Overflow during arithmetic conversion: """).r
  private val floatingOverflow =
    ("""Cannot cast (REAL|DOUBLE) '(-?(?:[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?|inf|Infinity)|NaN)' to """ +
      """(TINYINT|SMALLINT|INTEGER|BIGINT)\. """ +
      """Cannot cast floating-point value to an integral value due to overflow\.""").r
  private val decimalOverflow =
    ("""Cannot cast (TINYINT|SMALLINT|INTEGER|BIGINT|DECIMAL) """ +
      """'(-?[0-9]{1,38}(?:\.[0-9]{1,38})?)' to DECIMAL\(([0-9]{1,2}), ([0-9]{1,2})\)""").r
  private val stringToIntegral =
    """(?s)Cannot cast VARCHAR '(.*)' to (TINYINT|SMALLINT|INTEGER|BIGINT)\. .+""".r

  private def integralValue(value: String, from: DataType): Option[Any] = {
    val number = BigInt(value)
    from match {
      case ByteType if number.isValidByte => Some(number.toByte)
      case ShortType if number.isValidShort => Some(number.toShort)
      case IntegerType if number.isValidInt => Some(number.toInt)
      case LongType if number.isValidLong => Some(number.toLong)
      case _ => None
    }
  }

  /**
   * Only accepts the reason of a native error already attributed to a cast. The native diagnostic
   * (including expression text and stack traces) must never be used for classification.
   */
  def fromNativeReason(reason: String, cause: Throwable): RuntimeException = {
    // Velox renders string values without escaping. An embedded delimiter is ambiguous.
    if (reason == null || reason.indexOf("' to ") != reason.lastIndexOf("' to ")) {
      return null
    }

    val translated: Option[RuntimeException] = reason match {
      case integralOverflow(from, value, to) =>
        integralValue(value, integralTypes(from)).map {
          v =>
            QueryExecutionErrors.castingCauseOverflowError(
              v,
              integralTypes(from),
              integralTypes(to))
        }

      case floatingOverflow(from, value, to) =>
        val normalized = value match {
          case "inf" => "Infinity"
          case "-inf" => "-Infinity"
          case _ => value
        }
        val (number, fromType): (Any, DataType) = if (from == "REAL") {
          (normalized.toFloat, FloatType)
        } else {
          (normalized.toDouble, DoubleType)
        }
        Some(QueryExecutionErrors.castingCauseOverflowError(number, fromType, integralTypes(to)))

      case decimalOverflow(from, value, precision, scale) =>
        val p = precision.toInt
        val s = scale.toInt
        val decimal = new java.math.BigDecimal(value)
        val validSource = from == "DECIMAL" ||
          (!value.contains(".") && integralValue(value, integralTypes(from)).isDefined)
        if (
          validSource && decimal.precision <= DecimalType.MAX_PRECISION &&
          p > 0 && p <= DecimalType.MAX_PRECISION && s <= p
        ) {
          Some(QueryExecutionErrors.cannotChangeDecimalPrecisionError(Decimal(decimal), p, s, null))
        } else {
          None
        }

      case stringToIntegral(value, to) =>
        Some(
          QueryExecutionErrors.invalidInputInCastToNumberError(
            integralTypes(to),
            UTF8String.fromString(value),
            null))

      case _ => None
    }
    translated.foreach(_.initCause(cause))
    translated.orNull
  }
}

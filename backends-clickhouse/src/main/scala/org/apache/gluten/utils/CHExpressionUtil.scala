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

import org.apache.gluten.expression.ExpressionNames._
import org.apache.gluten.utils.FunctionValidator._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

trait FunctionValidator {
  def doValidate(expr: Expression): Boolean
}

object FunctionValidator {
  def isDateTimeType(dataType: DataType): Boolean = dataType match {
    case DateType | TimestampType => true
    case _ => false
  }
}

case class DefaultValidator() extends FunctionValidator {
  override def doValidate(expr: Expression): Boolean = false
}

case class SequenceValidator() extends FunctionValidator {
  override def doValidate(expr: Expression): Boolean = {
    !expr.children.exists(x => isDateTimeType(x.dataType))
  }
}

case class UtcTimestampValidator() extends FunctionValidator {
  override def doValidate(expr: Expression): Boolean = expr match {
    // CH backend doest not support non-const timezone parameter
    case t: ToUTCTimestamp => t.children(1).isInstanceOf[Literal]
    case f: FromUTCTimestamp => f.children(1).isInstanceOf[Literal]
    case _ => false
  }
}

case class UnixTimeStampValidator() extends FunctionValidator {
  final val DATE_TYPE = "date"

  override def doValidate(expr: Expression): Boolean = expr match {
    // CH backend does not support non-const format
    case t: ToUnixTimestamp => t.format.isInstanceOf[Literal]
    case u: UnixTimestamp => u.format.isInstanceOf[Literal]
    case _ => true
  }
}

case class GetJsonObjectValidator() extends FunctionValidator {
  override def doValidate(expr: Expression): Boolean = {
    val path = expr.asInstanceOf[GetJsonObject].path
    if (!path.isInstanceOf[Literal]) {
      return false
    }
    val pathStr = path.asInstanceOf[Literal].toString()
    // Not supported: double dot and filter expression
    if (pathStr.contains("..") || pathStr.contains("?(")) {
      return false
    }
    true
  }
}

case class StringSplitValidator() extends FunctionValidator {
  override def doValidate(expr: Expression): Boolean = {
    val split = expr.asInstanceOf[StringSplit]
    if (!split.regex.isInstanceOf[Literal] || !split.limit.isInstanceOf[Literal]) {
      return false
    }

    // TODO: When limit is positive, CH result is wrong, fix it later
    val limitLiteral = split.limit.asInstanceOf[Literal]
    if (limitLiteral.value.asInstanceOf[Int] > 0) {
      return false
    }

    true
  }
}

case class SubstringIndexValidator() extends FunctionValidator {
  override def doValidate(expr: Expression): Boolean = {
    val substringIndex = expr.asInstanceOf[SubstringIndex]

    // TODO: CH substringIndexUTF8 function only support string literal as delimiter
    if (!substringIndex.delimExpr.isInstanceOf[Literal]) {
      return false
    }

    // TODO: CH substringIndexUTF8 function only support single character as delimiter
    val delim = substringIndex.delimExpr.asInstanceOf[Literal]
    if (delim.value.asInstanceOf[UTF8String].toString.length != 1) {
      return false
    }

    true
  }
}

case class StringLPadValidator() extends FunctionValidator {
  override def doValidate(expr: Expression): Boolean = {
    val lpad = expr.asInstanceOf[StringLPad]
    if (!lpad.pad.isInstanceOf[Literal]) {
      return false
    }

    true
  }
}

case class StringRPadValidator() extends FunctionValidator {
  override def doValidate(expr: Expression): Boolean = {
    val rpad = expr.asInstanceOf[StringRPad]
    if (!rpad.pad.isInstanceOf[Literal]) {
      return false
    }

    true
  }
}

case class DateFormatClassValidator() extends FunctionValidator {
  override def doValidate(expr: Expression): Boolean = {
    val dateFormatClass = expr.asInstanceOf[DateFormatClass]

    // TODO: CH formatDateTimeInJodaSyntax/fromUnixTimestampInJodaSyntax only support
    // string literal as format
    if (!dateFormatClass.right.isInstanceOf[Literal]) {
      return false
    }

    true
  }
}

case class EncodeDecodeValidator() extends FunctionValidator {
  override def doValidate(expr: Expression): Boolean = expr match {
    case d: StringDecode => d.charset.isInstanceOf[Literal]
    case e: Encode => e.charset.isInstanceOf[Literal]
    case _ => true
  }
}

case class FormatStringValidator() extends FunctionValidator {
  override def doValidate(expr: Expression): Boolean = {
    val formatString = expr.asInstanceOf[FormatString]
    formatString.children.head.isInstanceOf[Literal]
  }
}

object CHExpressionUtil {
  final val CH_AGGREGATE_FUNC_BLACKLIST: Map[String, FunctionValidator] = Map(
    MAX_BY -> DefaultValidator(),
    MIN_BY -> DefaultValidator()
  )

  final val CH_BLACKLIST_SCALAR_FUNCTION: Map[String, FunctionValidator] = Map(
    SPLIT_PART -> DefaultValidator(),
    TO_PRETTY_STRING -> DefaultValidator(),
    TO_UNIX_TIMESTAMP -> UnixTimeStampValidator(),
    UNIX_TIMESTAMP -> UnixTimeStampValidator(),
    SEQUENCE -> SequenceValidator(),
    GET_JSON_OBJECT -> GetJsonObjectValidator(),
    SPLIT -> StringSplitValidator(),
    SUBSTRING_INDEX -> SubstringIndexValidator(),
    LPAD -> StringLPadValidator(),
    RPAD -> StringRPadValidator(),
    DATE_FORMAT -> DateFormatClassValidator(),
    DECODE -> EncodeDecodeValidator(),
    ENCODE -> EncodeDecodeValidator(),
    DATE_FROM_UNIX_DATE -> DefaultValidator(),
    SPARK_PARTITION_ID -> DefaultValidator(),
    AT_LEAST_N_NON_NULLS -> DefaultValidator(),
    URL_DECODE -> DefaultValidator(),
    URL_ENCODE -> DefaultValidator(),
    FORMAT_STRING -> FormatStringValidator(),
    SKEWNESS -> DefaultValidator(),
    MAKE_YM_INTERVAL -> DefaultValidator(),
    MAP_ZIP_WITH -> DefaultValidator(),
    KURTOSIS -> DefaultValidator(),
    REGR_R2 -> DefaultValidator(),
    REGR_SLOPE -> DefaultValidator(),
    REGR_INTERCEPT -> DefaultValidator(),
    REGR_SXY -> DefaultValidator(),
    TO_UTC_TIMESTAMP -> UtcTimestampValidator(),
    FROM_UTC_TIMESTAMP -> UtcTimestampValidator(),
    STACK -> DefaultValidator(),
    RAISE_ERROR -> DefaultValidator(),
    WIDTH_BUCKET -> DefaultValidator(),
    MAKE_DATE -> DefaultValidator(),
    ARRAY_APPEND -> DefaultValidator(),
    JSON_OBJECT_KEYS -> DefaultValidator(),
    LUHN_CHECK -> DefaultValidator(),
    VARCHAR_TYPE_WRITE_SIDE_CHECK -> DefaultValidator(),
    CHAR_TYPE_WRITE_SIDE_CHECK -> DefaultValidator(),
    READ_SIDE_PADDING -> DefaultValidator(),
    DIV -> DefaultValidator()
  )
}

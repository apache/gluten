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

import org.apache.spark.sql.internal.SQLConf

trait ExpressionRestrictions {
  val functionName: String
  val restrictionMessages: Array[String]
}

object StrToMapRestrictions extends ExpressionRestrictions {
  val ONLY_SUPPORT_MAP_KEY_DEDUP_POLICY: String =
    s"Only ${SQLConf.MAP_KEY_DEDUP_POLICY.key} = " +
      s"${SQLConf.MapKeyDedupPolicy.EXCEPTION.toString} is supported for Velox backend"

  override val functionName: String = ExpressionNames.STR_TO_MAP

  override val restrictionMessages: Array[String] = Array(
    ONLY_SUPPORT_MAP_KEY_DEDUP_POLICY
  )
}

object FromJsonRestrictions extends ExpressionRestrictions {
  val MUST_ENABLE_PARTIAL_RESULTS: String =
    s"${ExpressionNames.FROM_JSON} with 'spark.sql.json.enablePartialResults = false' " +
      s"is not supported in Velox"
  val NOT_SUPPORT_WITH_OPTIONS: String =
    s"${ExpressionNames.FROM_JSON} with options is not supported in Velox"
  val NOT_SUPPORT_CASE_SENSITIVE: String =
    s"${ExpressionNames.FROM_JSON} with " +
      s"'${SQLConf.CASE_SENSITIVE.key} = true' is not supported in Velox"
  val NOT_SUPPORT_DUPLICATE_KEYS: String =
    s"${ExpressionNames.FROM_JSON} with duplicate keys is not supported in Velox"
  val NOT_SUPPORT_COLUMN_CORRUPT_RECORD: String =
    s"${ExpressionNames.FROM_JSON} with column corrupt record is not supported in Velox"

  override val functionName: String = ExpressionNames.FROM_JSON

  override val restrictionMessages: Array[String] = Array(
    MUST_ENABLE_PARTIAL_RESULTS,
    NOT_SUPPORT_WITH_OPTIONS,
    NOT_SUPPORT_CASE_SENSITIVE,
    NOT_SUPPORT_DUPLICATE_KEYS,
    NOT_SUPPORT_COLUMN_CORRUPT_RECORD
  )
}

object ToJsonRestrictions extends ExpressionRestrictions {
  val NOT_SUPPORT_WITH_OPTIONS: String =
    s"${ExpressionNames.TO_JSON} with options is not supported in Velox"

  val NOT_SUPPORT_UPPERCASE_STRUCT: String =
    s"When 'spark.sql.caseSensitive = false', ${ExpressionNames.TO_JSON} produces unexpected" +
      s" result for struct field with uppercase name"

  override val functionName: String = ExpressionNames.TO_JSON

  override val restrictionMessages: Array[String] =
    Array(NOT_SUPPORT_WITH_OPTIONS, NOT_SUPPORT_UPPERCASE_STRUCT)
}

object Unbase64Restrictions extends ExpressionRestrictions {
  val NOT_SUPPORT_FAIL_ON_ERROR: String =
    s"${ExpressionNames.UNBASE64} with failOnError is not supported"

  override val functionName: String = ExpressionNames.UNBASE64

  override val restrictionMessages: Array[String] = Array(NOT_SUPPORT_FAIL_ON_ERROR)
}

object Base64Restrictions extends ExpressionRestrictions {
  val NOT_SUPPORT_DISABLE_CHUNK_BASE64_STRING: String =
    s"${ExpressionNames.BASE64} with chunkBase64String disabled is not supported"

  override val functionName: String = ExpressionNames.BASE64

  override val restrictionMessages: Array[String] = Array(NOT_SUPPORT_DISABLE_CHUNK_BASE64_STRING)
}

object RaiseErrorRestrictions extends ExpressionRestrictions {
  val ONLY_SUPPORT_ERROR_MESSAGE: String =
    s"Only 'errorMessage' is supported as the second argument in ${ExpressionNames.RAISE_ERROR}"

  override val functionName: String = ExpressionNames.RAISE_ERROR

  override val restrictionMessages: Array[String] = Array(ONLY_SUPPORT_ERROR_MESSAGE)
}

object FormatNumberRestrictions extends ExpressionRestrictions {
  val NOT_SUPPORT_DECIMAL_INPUT: String =
    s"${ExpressionNames.FORMAT_NUMBER} only supports tinyint, smallint, integer, bigint, " +
      s"float and double input; DecimalType input is not supported in Velox"

  val NOT_SUPPORT_STRING_FORMAT: String =
    s"${ExpressionNames.FORMAT_NUMBER} with a string format argument (e.g. '#,###.##') is not " +
      s"supported in Velox; only an integer number of decimal places is supported"

  override val functionName: String = ExpressionNames.FORMAT_NUMBER

  override val restrictionMessages: Array[String] =
    Array(NOT_SUPPORT_DECIMAL_INPUT, NOT_SUPPORT_STRING_FORMAT)
}

object ExpressionRestrictions {
  // Called by gen-function-support-docs.py to get all restrictions.
  def listAllRestrictions(): Array[ExpressionRestrictions] = {
    Array(
      StrToMapRestrictions,
      FromJsonRestrictions,
      ToJsonRestrictions,
      Unbase64Restrictions,
      Base64Restrictions,
      FormatNumberRestrictions
    )
  }
}

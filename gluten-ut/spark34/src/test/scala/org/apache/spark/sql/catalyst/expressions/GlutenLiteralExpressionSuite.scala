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
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

import java.nio.charset.StandardCharsets
import java.time.{Instant, LocalDate}

class GlutenLiteralExpressionSuite extends LiteralExpressionSuite with GlutenTestsTrait {
  testGluten("default") {
    checkEvaluation(Literal.default(BooleanType), false)
    checkEvaluation(Literal.default(ByteType), 0.toByte)
    checkEvaluation(Literal.default(ShortType), 0.toShort)
    checkEvaluation(Literal.default(IntegerType), 0)
    checkEvaluation(Literal.default(LongType), 0L)
    checkEvaluation(Literal.default(FloatType), 0.0f)
    checkEvaluation(Literal.default(DoubleType), 0.0)
    checkEvaluation(Literal.default(StringType), "")
    checkEvaluation(Literal.default(BinaryType), "".getBytes(StandardCharsets.UTF_8))
    checkEvaluation(Literal.default(DecimalType.USER_DEFAULT), Decimal(0))
    checkEvaluation(Literal.default(DecimalType.SYSTEM_DEFAULT), Decimal(0))
    withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "false") {
      checkEvaluation(Literal.default(DateType), DateTimeUtils.toJavaDate(0))
      checkEvaluation(Literal.default(TimestampType), DateTimeUtils.toJavaTimestamp(0L))
    }
    withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
      checkEvaluation(Literal.default(DateType), LocalDate.ofEpochDay(0))
      checkEvaluation(Literal.default(TimestampType), Instant.ofEpochSecond(0))
    }
    checkEvaluation(Literal.default(CalendarIntervalType), new CalendarInterval(0, 0, 0L))
    checkEvaluation(Literal.default(YearMonthIntervalType()), 0)
    checkEvaluation(Literal.default(DayTimeIntervalType()), 0L)
    checkEvaluation(Literal.default(ArrayType(StringType)), Array())
    checkEvaluation(Literal.default(MapType(IntegerType, StringType)), Map())
    checkEvaluation(Literal.default(StructType(StructField("a", StringType) :: Nil)), Row(""))
  }
}

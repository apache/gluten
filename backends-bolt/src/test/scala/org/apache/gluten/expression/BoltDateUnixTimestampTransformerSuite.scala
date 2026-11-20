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

import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.substrait.SubstraitContext

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import org.scalatest.funsuite.AnyFunSuite

class BoltDateUnixTimestampTransformerSuite extends AnyFunSuite {
  private val date = Literal.create(java.sql.Date.valueOf("2015-07-24"), DateType)
  private val nullFormat = Literal.create(null, StringType)

  private def generated(
      time: Literal,
      format: Literal,
      unix: Boolean = false,
      timezone: Option[String] = Some("-08:00")): io.substrait.proto.Expression.ScalarFunction = {
    val original = if (unix) UnixTimestamp(time, format, timezone)
    else ToUnixTimestamp(time, format, timezone)
    ToUnixTimestampTransformer(
      if (unix) "unix_timestamp" else "to_unix_timestamp",
      LiteralTransformer(time),
      LiteralTransformer(format),
      original)
      .doTransform(new SubstraitContext).toProtobuf.getScalarFunction
  }

  test("DATE to_unix_timestamp passes null format and exact original timezone to native overload") {
    val function = generated(date, nullFormat)
    assert(function.getArgumentsCount == 3)
    assert(function.getArguments(0).getValue.getLiteral.getDate == 16640)
    assert(function.getArguments(1).getValue.getLiteral.hasNull)
    assert(function.getArguments(2).getValue.getLiteral.getString == "-08:00")
  }

  test("DATE unix timestamp ignores non-null formats and preserves the original timezone") {
    for (format <- Seq("yyyy-MM-dd", "YYYY"); unix <- Seq(false, true)) {
      val function = generated(date, Literal(format), unix = unix)
      assert(function.getArgumentsCount == 3)
      assert(function.getArguments(1).getValue.getLiteral.hasNull)
      assert(function.getArguments(1).getValue.getLiteral.getNull.hasString)
      assert(function.getArguments(2).getValue.getLiteral.getString == "-08:00")
    }
  }

  test("DATE unix_timestamp also preserves null format") {
    val function = generated(date, nullFormat, unix = true)
    assert(function.getArgumentsCount == 3)
    assert(function.getArguments(1).getValue.getLiteral.hasNull)
    assert(function.getArguments(2).getValue.getLiteral.getString == "-08:00")
  }

  test("DATE unix timestamp rejects unresolved timezone instead of substituting session default") {
    intercept[GlutenNotSupportException](generated(date, nullFormat, timezone = None))
  }

  test("TIMESTAMP unix timestamp retains original unary overload") {
    val timestamp = Literal.create(java.sql.Timestamp.valueOf("2015-07-24 00:00:00"), TimestampType)
    assert(generated(timestamp, nullFormat).getArgumentsCount == 1)
    assert(generated(timestamp, nullFormat, unix = true).getArgumentsCount == 1)
  }

  test("STRING unix timestamp retains original binary overload including null format") {
    val function = generated(Literal("2015-07-24"), nullFormat)
    assert(function.getArgumentsCount == 2)
    assert(function.getArguments(0).getValue.getLiteral.getString == "2015-07-24")
    assert(function.getArguments(1).getValue.getLiteral.hasNull)
  }
}

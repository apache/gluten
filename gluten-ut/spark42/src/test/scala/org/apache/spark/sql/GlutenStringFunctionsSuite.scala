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
package org.apache.spark.sql

import org.apache.gluten.test.FallbackUtil

import org.apache.spark.sql.catalyst.expressions.ExpressionEvalHelper
import org.apache.spark.sql.functions._

import org.junit.Assert

class GlutenStringFunctionsSuite
  extends StringFunctionsSuite
  with GlutenSQLTestsTrait
  with ExpressionEvalHelper {

  import testImplicits._

  testGluten("string split function with no limit and regex pattern") {
    val df1 = Seq(("aaAbbAcc4")).toDF("a").select(split($"a", "A"))
    checkAnswer(df1, Row(Seq("aa", "bb", "cc4")))
    Assert.assertFalse(FallbackUtil.hasFallback(df1.queryExecution.executedPlan))

    // scalastyle:off nonascii
    val df2 = Seq(("test_gluten单测_")).toDF("a").select(split($"a", "_"))
    checkAnswer(df2, Row(Seq("test", "gluten单测", "")))
    // scalastyle:on nonascii
    Assert.assertFalse(FallbackUtil.hasFallback(df2.queryExecution.executedPlan))
  }

  testGluten("string split function with limit explicitly set to 0") {
    val df1 = Seq(("aaAbbAcc4")).toDF("a").select(split($"a", "A", 0))
    checkAnswer(df1, Row(Seq("aa", "bb", "cc4")))
    Assert.assertFalse(FallbackUtil.hasFallback(df1.queryExecution.executedPlan))

    // scalastyle:off nonascii
    val df2 = Seq(("test_gluten单测_")).toDF("a").select(split($"a", "_", 0))
    checkAnswer(df2, Row(Seq("test", "gluten单测", "")))
    // scalastyle:on nonascii
    Assert.assertFalse(FallbackUtil.hasFallback(df2.queryExecution.executedPlan))
  }

  testGluten("string split function with negative limit") {
    val df1 = Seq(("aaAbbAcc4")).toDF("a").select(split($"a", "A", -1))
    checkAnswer(df1, Row(Seq("aa", "bb", "cc4")))
    Assert.assertFalse(FallbackUtil.hasFallback(df1.queryExecution.executedPlan))

    // scalastyle:off nonascii
    val df2 = Seq(("test_gluten单测_")).toDF("a").select(split($"a", "_", -2))
    checkAnswer(df2, Row(Seq("test", "gluten单测", "")))
    // scalastyle:on nonascii
    Assert.assertFalse(FallbackUtil.hasFallback(df2.queryExecution.executedPlan))
  }
}

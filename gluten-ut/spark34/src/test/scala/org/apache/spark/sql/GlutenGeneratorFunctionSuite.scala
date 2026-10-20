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

import org.apache.gluten.execution.GenerateExecTransformerBase

class GlutenGeneratorFunctionSuite extends GeneratorFunctionSuite with GlutenSQLTestsTrait {
  testGluten("stack is offloaded") {
    val df = spark.range(2).selectExpr("stack(2, id, id + 1, id + 2)")
    checkAnswer(df, Seq(Row(0L, 1L), Row(2L, null), Row(1L, 2L), Row(3L, null)))
    assert(
      df.queryExecution.executedPlan
        .find(_.isInstanceOf[GenerateExecTransformerBase])
        .isDefined)
  }

  testGluten("stack without null padding is offloaded") {
    val df = spark.range(2).selectExpr("stack(2, id, id + 1, id + 2, id + 3)")
    checkAnswer(df, Seq(Row(0L, 1L), Row(2L, 3L), Row(1L, 2L), Row(3L, 4L)))
    assert(
      df.queryExecution.executedPlan
        .find(_.isInstanceOf[GenerateExecTransformerBase])
        .isDefined)
  }

  testGluten("single-column stack with null padding is offloaded") {
    val df = spark.range(2).selectExpr("stack(3, id, id + 1)")
    checkAnswer(df, Seq(Row(0L), Row(1L), Row(null), Row(1L), Row(2L), Row(null)))
    assert(
      df.queryExecution.executedPlan
        .find(_.isInstanceOf[GenerateExecTransformerBase])
        .isDefined)
  }
}

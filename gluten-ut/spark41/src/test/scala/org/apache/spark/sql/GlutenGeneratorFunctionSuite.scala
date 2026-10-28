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

import org.apache.spark.sql.internal.SQLConf

class GlutenGeneratorFunctionSuite extends GeneratorFunctionSuite with GlutenSQLTestsTrait {
  testGluten("outer generators preserve padding and null elements across output batches") {
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "false",
      "spark.gluten.sql.columnar.maxBatchSize" -> "2",
      "spark.gluten.sql.columnar.backend.velox.unnest_split_output" -> "true"
    ) {
      val input = spark.range(4).selectExpr(
        "id",
        "CASE id WHEN 0 THEN CAST(NULL AS ARRAY<BIGINT>) WHEN 1 THEN array() " +
          "WHEN 2 THEN array(CAST(NULL AS BIGINT)) ELSE array(7L, NULL, 9L) END AS a",
        "CASE id WHEN 0 THEN CAST(NULL AS MAP<STRING,BIGINT>) WHEN 1 THEN map() " +
          "WHEN 2 THEN map('a', CAST(NULL AS BIGINT)) " +
          "ELSE map('a', 7L, 'b', NULL, 'c', 9L) END AS m",
        "CASE id WHEN 0 THEN CAST(NULL AS ARRAY<STRUCT<v:BIGINT>>) WHEN 1 THEN array() " +
          "WHEN 2 THEN array(CAST(NULL AS STRUCT<v:BIGINT>)) " +
          "ELSE array(named_struct('v', 7L), named_struct('v', CAST(NULL AS BIGINT)), " +
          "named_struct('v', 9L)) END AS s",
        "CASE id WHEN 0 THEN CAST(NULL AS ARRAY<STRING>) WHEN 1 THEN array() " +
          "WHEN 2 THEN array(CAST(NULL AS STRING)) " +
          "ELSE array(repeat('a', 64), NULL, repeat('b', 64)) END AS t"
      )
      val values = Seq(
        Row(0L, null),
        Row(1L, null),
        Row(2L, null),
        Row(3L, 7L),
        Row(3L, null),
        Row(3L, 9L))
      val cases = Seq(
        "explode_outer(a)" -> values,
        "inline_outer(s)" -> values,
        "explode_outer(t)" -> Seq(
          Row(0L, null),
          Row(1L, null),
          Row(2L, null),
          Row(3L, "a" * 64),
          Row(3L, null),
          Row(3L, "b" * 64)),
        "posexplode_outer(a)" -> Seq(
          Row(0L, null, null),
          Row(1L, null, null),
          Row(2L, 0, null),
          Row(3L, 0, 7L),
          Row(3L, 1, null),
          Row(3L, 2, 9L)),
        "explode_outer(m)" -> Seq(
          Row(0L, null, null),
          Row(1L, null, null),
          Row(2L, "a", null),
          Row(3L, "a", 7L),
          Row(3L, "b", null),
          Row(3L, "c", 9L)),
        "posexplode_outer(m)" -> Seq(
          Row(0L, null, null, null),
          Row(1L, null, null, null),
          Row(2L, 0, "a", null),
          Row(3L, 0, "a", 7L),
          Row(3L, 1, "b", null),
          Row(3L, 2, "c", 9L))
      )
      cases.foreach {
        case (expression, expected) =>
          val result = input.selectExpr("id", expression)
          checkAnswer(result, expected)
          assert(
            result.queryExecution.executedPlan
              .find(_.isInstanceOf[GenerateExecTransformerBase])
              .isDefined)
          if (expression.startsWith("posexplode_outer")) {
            val positions = result.select("id", "pos")
            checkAnswer(positions, expected.map(row => Row(row.get(0), row.get(1))))
            assert(
              getExecutedPlan(positions).exists(_.isInstanceOf[GenerateExecTransformerBase]))
          }
      }
    }
  }

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

  testGluten("SPARK-45171: Handle evaluated nondeterministic expression") {
    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
      val df = sql("select explode(array(rand(0)))")
      checkAnswer(df, Row(0.7604953758285915))
    }
  }
}

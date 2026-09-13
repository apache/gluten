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

import org.apache.gluten.execution.HashAggregateExecBaseTransformer

import org.apache.spark.sql.catalyst.expressions.BitmapOrAgg
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

class GlutenBitmapExpressionsQuerySuite
  extends BitmapExpressionsQuerySuite
  with GlutenSQLTestsTrait
  with AdaptiveSparkPlanHelper {

  test("bitmap_construct_agg routes to native") {
    val df = spark.sql(
      "SELECT bitmap_construct_agg(bitmap_bit_position(col)) " +
        "FROM values (1L), (2L), (3L) AS t(col)")
    df.collect()
    assert(
      collectWithSubqueries(df.queryExecution.executedPlan) {
        case h: HashAggregateExecBaseTransformer => h
      }.nonEmpty,
      "Expected native HashAggregateExecBaseTransformer in plan"
    )
  }

  test("bitmap_or_agg routes to native") {
    val df = spark.sql(
      "SELECT bitmap_or_agg(bm) FROM (" +
        "SELECT bitmap_construct_agg(bitmap_bit_position(col)) AS bm " +
        "FROM values (1L), (2L), (3L) AS t(col)" +
        ") sub")
    df.collect()
    val nativeBitmapOrAggs = collectWithSubqueries(df.queryExecution.executedPlan) {
      case h: HashAggregateExecBaseTransformer
          if h.aggregateExpressions.exists(
            _.aggregateFunction.isInstanceOf[BitmapOrAgg]) =>
        h
    }
    assert(
      nativeBitmapOrAggs.nonEmpty,
      "Expected native HashAggregateExecBaseTransformer with bitmap_or_agg in plan"
    )
  }
}

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
package org.apache.spark.sql.execution

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{HashAggregateExecBaseTransformer, SortAggregateExecTransformer}

import org.apache.spark.SparkConf
import org.apache.spark.sql.GlutenSQLTestsTrait
import org.apache.spark.sql.catalyst.expressions.aggregate.Complete
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.internal.SQLConf

class GlutenCombineAdjacentAggregationSuite
  extends CombineAdjacentAggregationSuite
  with GlutenSQLTestsTrait {

  // Gluten's MergeTwoPhasesHashBaseAggregate also merges adjacent partial and final aggregates.
  // Disable it so the aggregate counts reflect Spark's CombineAdjacentAggregation alone.
  override def sparkConf: SparkConf =
    super.sparkConf.set(GlutenConfig.MERGE_TWO_PHASES_ENABLED.key, "false")

  testGluten("Combine adjacent sort aggregate") {
    withTempView("t") {
      spark
        .range(20)
        .selectExpr("id % 3 as k", "cast(id % 7 as string) as v")
        .createOrReplaceTempView("t")
      val query = "SELECT k, max(v) FROM (SELECT /*+ repartition(k) */ * FROM t) GROUP BY k"

      val expected = withSQLConf(SQLConf.COMBINE_ADJACENT_AGGREGATION_ENABLED.key -> "false") {
        val df = sql(query)
        val result = df.collect()
        val aggs = collect(df.queryExecution.executedPlan) { case agg: BaseAggregateExec => agg }
        assert(aggs.size == 2)
        assert(aggs.forall(_.isInstanceOf[HashAggregateExecBaseTransformer]))
        result
      }

      withSQLConf(SQLConf.COMBINE_ADJACENT_AGGREGATION_ENABLED.key -> "true") {
        val df = sql(query)
        checkAnswer(df, expected)
        val aggs = collect(df.queryExecution.executedPlan) { case agg: BaseAggregateExec => agg }
        // The combined complete mode sort aggregate is offloaded as a native aggregate.
        assert(aggs.size == 1)
        assert(aggs.head.isInstanceOf[SortAggregateExecTransformer])
        assert(aggs.head.aggregateExpressions.forall(_.mode == Complete))
      }
    }
  }
}

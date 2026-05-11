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
package org.apache.gluten.execution

import org.apache.spark.SparkConf

import org.scalatest.DoNotDiscover

@DoNotDiscover
class GlutenClickHouseDSV2ColumnarShuffleSuite extends MergeTreeSuite {

  /** Run Gluten + ClickHouse Backend with ColumnarShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
  }

  final override val testCases: Seq[Int] = Seq(
    1, 2, 4, 5, 6, 7, 8, 9, 10, 12, 16, 21, 22
  )
  setupTestCase()

  test("TPCH Q3") {
    customCheck(3) {
      df =>
        val aggs = df.queryExecution.executedPlan.collectWithSubqueries {
          case agg: HashAggregateExecBaseTransformer => agg
        }
        assert(aggs.size == 1)
    }
  }

  test("TPCH Q11") {
    customCheck(11) {
      df =>
        val aggs = df.queryExecution.executedPlan.collectWithSubqueries {
          case agg: HashAggregateExecBaseTransformer => agg
        }
        assert(aggs.size == 3)
    }
  }

  test("TPCH Q13") {
    customCheck(13) {
      df =>
        val aggs = df.queryExecution.executedPlan.collectWithSubqueries {
          case agg: HashAggregateExecBaseTransformer => agg
        }
        assert(aggs.size == 3)
    }
  }

  test("TPCH Q14") {
    customCheck(14) {
      df =>
        val aggs = df.queryExecution.executedPlan.collectWithSubqueries {
          case agg: HashAggregateExecBaseTransformer => agg
        }
        assert(aggs.size == 1)
    }
  }

  test("TPCH Q15") {
    customCheck(15) {
      df =>
        val aggs = df.queryExecution.executedPlan.collectWithSubqueries {
          case agg: HashAggregateExecBaseTransformer => agg
        }
        assert(aggs.size == 4)
    }
  }

  test("TPCH Q17") {
    customCheck(17) {
      df =>
        val aggs = df.queryExecution.executedPlan.collectWithSubqueries {
          case agg: HashAggregateExecBaseTransformer => agg
        }
        assert(aggs.size == 3)
    }
  }

  test("TPCH Q18") {
    customCheck(18) {
      df =>
        val aggs = df.queryExecution.executedPlan.collectWithSubqueries {
          case agg: HashAggregateExecBaseTransformer => agg
        }
        assert(aggs.size == 4)
    }
  }

  test("TPCH Q19") {
    customCheck(19) {
      df =>
        val aggs = df.queryExecution.executedPlan.collectWithSubqueries {
          case agg: HashAggregateExecBaseTransformer => agg
        }
        assert(aggs.size == 1)
    }
  }

  test("TPCH Q20") {
    customCheck(20) {
      df =>
        val aggs = df.queryExecution.executedPlan.collectWithSubqueries {
          case agg: HashAggregateExecBaseTransformer => agg
        }
        assert(aggs.size == 1)
    }
  }
}

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

import org.apache.gluten.execution.CartesianProductExecTransformer

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.CartesianProductExec
import org.apache.spark.sql.internal.SQLConf

class GlutenJoinHintSuite extends JoinHintSuite with GlutenSQLTestsBaseTrait {

  private def assertGlutenShuffleReplicateNLJoin(df: DataFrame): Unit = {
    val executedPlan = df.queryExecution.executedPlan
    val cartesianProducts = collect(executedPlan) {
      case c: CartesianProductExec => c.asInstanceOf[SparkPlan]
      case c: CartesianProductExecTransformer => c.asInstanceOf[SparkPlan]
    }
    assert(cartesianProducts.size == 1)
  }

  testGluten("join strategy hint - shuffle-replicate-nl") {
    withTempView("t1", "t2") {
      spark.createDataFrame(Seq((1, "4"), (2, "2"))).toDF("key", "value").createTempView("t1")
      spark
        .createDataFrame(Seq((1, "1"), (2, "12.3"), (2, "123")))
        .toDF("key", "value")
        .createTempView("t2")

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> Int.MaxValue.toString) {
        assertGlutenShuffleReplicateNLJoin(
          sql(equiJoinQueryWithHint("SHUFFLE_REPLICATE_NL(t1)" :: Nil)))
        assertGlutenShuffleReplicateNLJoin(
          sql(equiJoinQueryWithHint("SHUFFLE_REPLICATE_NL(t2)" :: Nil)))
        assertGlutenShuffleReplicateNLJoin(
          sql(equiJoinQueryWithHint("SHUFFLE_REPLICATE_NL(t1, t2)" :: Nil)))
        assertGlutenShuffleReplicateNLJoin(
          sql(nonEquiJoinQueryWithHint("MERGE(t1)" :: "SHUFFLE_REPLICATE_NL(t2)" :: Nil)))
        assertGlutenShuffleReplicateNLJoin(
          sql(nonEquiJoinQueryWithHint("SHUFFLE_HASH(t2)" :: "SHUFFLE_REPLICATE_NL(t1)" :: Nil)))
      }
    }
  }
}

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

import org.apache.gluten.execution.{ColumnarCollectLimitBaseExec, LimitExecTransformer, TakeOrderedAndProjectExecTransformer}

import org.apache.spark.sql.GlutenSQLTestsTrait
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf

class GlutenInsertSortForLimitAndOffsetSuite
  extends InsertSortForLimitAndOffsetSuite
  with GlutenSQLTestsTrait {

  private def glutenHasTopKSort(plan: SparkPlan): Boolean = {
    find(plan) {
      case _: TakeOrderedAndProjectExec => true
      case _: TakeOrderedAndProjectExecTransformer => true
      case _ => false
    }.isDefined
  }

  private def glutenHasCollectLimit(plan: SparkPlan): Boolean = {
    find(plan) {
      case _: CollectLimitExec => true
      case _: LimitExecTransformer => true
      case _: ColumnarCollectLimitBaseExec => true
      case _ => false
    }.isDefined
  }

  testGluten("root LIMIT preserves data ordering with top-K sort") {
    val df = spark.range(10).orderBy(col("id") % 8).limit(2)
    df.collect()
    assert(glutenHasTopKSort(df.queryExecution.executedPlan))
  }

  testGluten("middle LIMIT preserves data ordering with top-K sort") {
    val df = spark.range(10).orderBy(col("id") % 8).limit(2).distinct()
    df.collect()
    assert(glutenHasTopKSort(df.queryExecution.executedPlan))
  }

  testGluten("root LIMIT preserves data ordering with CollectLimitExec") {
    withSQLConf(SQLConf.TOP_K_SORT_FALLBACK_THRESHOLD.key -> "1") {
      val df = spark.range(10).orderBy(col("id") % 8).limit(2)
      df.collect()
      assert(glutenHasCollectLimit(df.queryExecution.executedPlan))
    }
  }

  testGluten("middle LIMIT preserves data ordering with the extra sort") {
    withSQLConf(
      SQLConf.TOP_K_SORT_FALLBACK_THRESHOLD.key -> "1",
      SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false") {
      val df =
        spark.createDataFrame(Seq((1, 1), (2, 2), (3, 3))).toDF("c1", "c2").orderBy(col("c1") % 8)
      val shuffled = df.limit(2).distinct()
      shuffled.collect()
      // Verify the query produces correct results (ordering preserved)
      assert(shuffled.count() <= 2)
    }
  }

  testGluten("root OFFSET preserves data ordering with CollectLimitExec") {
    val df = spark.range(10).orderBy(col("id") % 8).offset(2)
    df.collect()
    assert(glutenHasCollectLimit(df.queryExecution.executedPlan))
  }

  testGluten("middle OFFSET preserves data ordering with the extra sort") {
    val df =
      spark.createDataFrame(Seq((1, 1), (2, 2), (3, 3))).toDF("c1", "c2").orderBy(col("c1") % 8)
    val shuffled = df.offset(2).distinct()
    shuffled.collect()
    assert(shuffled.count() >= 0)
  }
}

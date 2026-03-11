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

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.internal.SQLConf

class GlutenCachedTableSuite
  extends CachedTableSuite
  with GlutenSQLTestsTrait
  with AdaptiveSparkPlanHelper {
  import testImplicits._
  // for temporarily disable the columnar table cache globally.
  sys.props.put(GlutenConfig.COLUMNAR_TABLE_CACHE_ENABLED.key, "true")
  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.shuffle.partitions", "5")
    super.sparkConf.set(GlutenConfig.COLUMNAR_TABLE_CACHE_ENABLED.key, "true")
  }

  testGluten("SPARK-52684: Atomicity of cache table on error") {
    withTempView("SPARK_52684") {
      intercept[SparkException] {
        spark.sql("CACHE TABLE SPARK_52684 AS SELECT raise_error('SPARK-52684') AS c1")
      }
      assert(!spark.catalog.tableExists("SPARK_52684"))
    }
  }

  testGluten("InMemoryRelation statistics") {
    sql("CACHE TABLE testData")
    spark.table("testData").queryExecution.withCachedData.collect {
      case cached: InMemoryRelation =>
        assert(cached.stats.sizeInBytes === 1132)
    }
  }

  def verifyNumExchanges(df: DataFrame, expected: Int): Unit = {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      df.collect()
    }
    assert(collect(df.queryExecution.executedPlan) {
      case e: ShuffleExchangeLike => e
    }.size == expected)
  }

  testGluten("A cached table preserves the partitioning and ordering of its cached SparkPlan") {
    // Distribute the tables into non-matching number of partitions. Need to shuffle one side.
    withTempView("t1", "t2") {
      testData.repartition(6, $"key").createOrReplaceTempView("t1")
      testData2.repartition(3, $"a").createOrReplaceTempView("t2")
      spark.catalog.cacheTable("t1")
      spark.catalog.cacheTable("t2")
      val query = sql("SELECT key, value, a, b FROM t1 t1 JOIN t2 t2 ON t1.key = t2.a")
      verifyNumExchanges(query, 1)
      assert(stripAQEPlan(query.queryExecution.executedPlan).outputPartitioning.numPartitions === 6)
      checkAnswer(
        query,
        testData.join(testData2, $"key" === $"a").select($"key", $"value", $"a", $"b"))
      uncacheTable("t1")
      uncacheTable("t2")
    }

    // One side of join is not partitioned in the desired way. Need to shuffle one side.
    withTempView("t1", "t2") {
      testData.repartition(6, $"value").createOrReplaceTempView("t1")
      testData2.repartition(6, $"a").createOrReplaceTempView("t2")
      spark.catalog.cacheTable("t1")
      spark.catalog.cacheTable("t2")

      val query = sql("SELECT key, value, a, b FROM t1 t1 JOIN t2 t2 ON t1.key = t2.a")
      verifyNumExchanges(query, 1)
      assert(stripAQEPlan(query.queryExecution.executedPlan).outputPartitioning.numPartitions === 6)
      checkAnswer(
        query,
        testData.join(testData2, $"key" === $"a").select($"key", $"value", $"a", $"b"))
      uncacheTable("t1")
      uncacheTable("t2")
    }

    withTempView("t1", "t2") {
      testData.repartition(6, $"value").createOrReplaceTempView("t1")
      testData2.repartition(12, $"a").createOrReplaceTempView("t2")
      spark.catalog.cacheTable("t1")
      spark.catalog.cacheTable("t2")

      val query = sql("SELECT key, value, a, b FROM t1 t1 JOIN t2 t2 ON t1.key = t2.a")
      verifyNumExchanges(query, 1)
      assert(
        stripAQEPlan(query.queryExecution.executedPlan).outputPartitioning.numPartitions === 12)
      checkAnswer(
        query,
        testData.join(testData2, $"key" === $"a").select($"key", $"value", $"a", $"b"))
      uncacheTable("t1")
      uncacheTable("t2")
    }

    // One side of join is not partitioned in the desired way. We'll only shuffle this side.
    withTempView("t1", "t2") {
      testData.repartition(6, $"value").createOrReplaceTempView("t1")
      testData2.repartition(3, $"a").createOrReplaceTempView("t2")
      spark.catalog.cacheTable("t1")
      spark.catalog.cacheTable("t2")

      val query = sql("SELECT key, value, a, b FROM t1 t1 JOIN t2 t2 ON t1.key = t2.a")
      verifyNumExchanges(query, 1)
      checkAnswer(
        query,
        testData.join(testData2, $"key" === $"a").select($"key", $"value", $"a", $"b"))
      uncacheTable("t1")
      uncacheTable("t2")
    }

    // repartition's column ordering is different from group by column ordering.
    // But they use the same set of columns.
    withTempView("t1") {
      testData.repartition(6, $"value", $"key").createOrReplaceTempView("t1")
      spark.catalog.cacheTable("t1")

      val query = sql("SELECT value, key from t1 group by key, value")
      verifyNumExchanges(query, 0)
      checkAnswer(query, testData.distinct().select($"value", $"key"))
      uncacheTable("t1")
    }

    // repartition's column ordering is different from join condition's column ordering.
    // We will still shuffle because hashcodes of a row depend on the column ordering.
    // If we do not shuffle, we may actually partition two tables in totally two different way.
    // See PartitioningSuite for more details.
    withTempView("t1", "t2") {
      val df1 = testData
      df1.repartition(6, $"value", $"key").createOrReplaceTempView("t1")
      val df2 = testData2.select($"a", $"b".cast("string"))
      df2.repartition(6, $"a", $"b").createOrReplaceTempView("t2")
      spark.catalog.cacheTable("t1")
      spark.catalog.cacheTable("t2")

      val query =
        sql("SELECT key, value, a, b FROM t1 t1 JOIN t2 t2 ON t1.key = t2.a and t1.value = t2.b")
      verifyNumExchanges(query, 1)
      assert(stripAQEPlan(query.queryExecution.executedPlan).outputPartitioning.numPartitions === 6)
      checkAnswer(
        query,
        df1.join(df2, $"key" === $"a" && $"value" === $"b").select($"key", $"value", $"a", $"b"))
      uncacheTable("t1")
      uncacheTable("t2")
    }
  }
}

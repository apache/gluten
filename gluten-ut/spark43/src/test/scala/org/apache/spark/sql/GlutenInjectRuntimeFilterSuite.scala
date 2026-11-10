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
import org.apache.gluten.execution.{FilterExecTransformerBase, HashAggregateExecBaseTransformer, HashJoinLikeExecTransformer}
import org.apache.gluten.expression.VeloxBloomFilterMightContain
import org.apache.gluten.expression.aggregate.VeloxBloomFilterAggregate

import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.storage.StorageLevel

class GlutenInjectRuntimeFilterSuite extends InjectRuntimeFilterSuite with GlutenSQLTestsBaseTrait {

  testGluten("GLUTEN-9849: bloom filter applied to partition filter") {
    withSQLConf(
      SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000"
    ) {
      assertRewroteWithBloomFilter(
        "select * from bf5part join bf2 on " +
          "bf5part.f5 = bf2.c2 where bf2.a2 = 67")
    }
  }

  testGluten("SPARK-58272: native join with a bloom filter from a cache built without Gluten") {
    val cacheName = "vanilla_cached_bloom_filter_keys"
    val query = s"SELECT * FROM bf1 JOIN $cacheName ON bf1.c1 = $cacheName.c2"
    withSQLConf(
      SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "0",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "0",
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false"
    ) {
      withTempView(cacheName) {
        withCache(cacheName) {
          withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
            spark
              .range(0, 100, 1, numPartitions = 4)
              .where("id = 74")
              .selectExpr("CAST(id AS INT) AS c2")
              .persist(StorageLevel.MEMORY_AND_DISK)
              .createOrReplaceTempView(cacheName)
            assert(spark.table(cacheName).count() == 1)
          }
          val relation = spark.table(cacheName).queryExecution.withCachedData.collectFirst {
            case cached: InMemoryRelation => cached
          }.get
          assert(relation.statsAvailable)

          var expected: Array[Row] = null
          withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false") {
            expected = sql(query).collect()
          }
          assert(expected.nonEmpty)

          val actual = sql(query)
          assert(getNumBloomFilters(actual.queryExecution.optimizedPlan) == 1)
          checkAnswer(actual, expected)

          val executedPlan = actual.queryExecution.executedPlan
          assert(executedPlan.collect { case j: HashJoinLikeExecTransformer => j }.size == 1)
          assert(executedPlan.collect {
            case f: FilterExecTransformerBase
                if f.cond.exists(_.isInstanceOf[VeloxBloomFilterMightContain]) =>
              f
          }.size == 1)
          val creationSide = executedPlan.collectWithSubqueries {
            case a: HashAggregateExecBaseTransformer
                if a.aggregateExpressions.exists(
                  _.aggregateFunction.isInstanceOf[VeloxBloomFilterAggregate]) =>
              a
          }
          assert(creationSide.nonEmpty)
          assert(creationSide.head.collect { case s: InMemoryTableScanExec => s }.nonEmpty)
        }
      }
    }
  }
}

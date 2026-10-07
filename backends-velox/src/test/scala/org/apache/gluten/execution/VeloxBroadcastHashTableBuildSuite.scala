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

import org.apache.gluten.config.{GlutenConfig, VeloxConfig}

import org.apache.spark.SparkConf

/**
 * Broadcast hash joins. On executors the hash table is built by running Velox HashBuild in a
 * dedicated task; the driver-side build uses HashTableBuilder. The build side spans many small
 * batches and several build threads, so the parallel build is exercised as well.
 */
class VeloxBroadcastHashTableBuildSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.unsafe.exceptionOnMemoryLeak", "true")
    .set(VeloxConfig.COLUMNAR_VELOX_BROADCAST_HASH_TABLE_BUILD_TARGET_BYTES.key, "4096")
    .set(GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key, "32")
    .set(VeloxConfig.VELOX_MIN_TABLE_ROWS_FOR_PARALLEL_JOIN_BUILD.key, "0")
    .set("spark.sql.adaptive.enabled", "false")
    .set("spark.sql.autoBroadcastJoinThreshold", "10MB")

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    VeloxBroadcastBuildSideCache.cleanAll()
  }

  override protected def afterEach(): Unit = {
    try {
      VeloxBroadcastBuildSideCache.cleanAll()
    } finally {
      super.afterEach()
    }
  }

  private def withJoinTables(buildNullKeys: Boolean)(f: => Unit): Unit = {
    withTempView("probe_t", "build_u") {
      spark
        .range(0, 2000)
        .selectExpr("IF(id % 97 = 0, NULL, id % 300) AS k", "id % 103 AS v")
        .createOrReplaceTempView("probe_t")
      val nullKey = if (buildNullKeys) "IF(id = 17, NULL, (id * 7) % 250)" else "(id * 7) % 250"
      spark
        .range(0, 1000)
        .selectExpr(s"$nullKey AS k", "IF(id % 37 = 0, NULL, id % 101) AS v")
        .createOrReplaceTempView("build_u")
      f
    }
  }

  private def checkBroadcastJoin(sql: String): Unit = {
    runQueryAndCompare(sql) {
      df =>
        val joins = df.queryExecution.executedPlan.collect {
          case bhj: BroadcastHashJoinExecTransformer => bhj
        }
        assert(joins.nonEmpty, df.queryExecution.executedPlan)
    }
  }

  test("inner join") {
    withJoinTables(buildNullKeys = true) {
      checkBroadcastJoin(
        "SELECT /*+ BROADCAST(u) */ t.k, t.v, u.v FROM probe_t t JOIN build_u u ON t.k = u.k")
    }
  }

  test("inner join with extra condition") {
    withJoinTables(buildNullKeys = true) {
      checkBroadcastJoin(
        "SELECT /*+ BROADCAST(u) */ t.k, t.v, u.v FROM probe_t t JOIN build_u u " +
          "ON t.k = u.k AND t.v > u.v")
    }
  }

  test("left outer join") {
    withJoinTables(buildNullKeys = true) {
      checkBroadcastJoin(
        "SELECT /*+ BROADCAST(u) */ t.k, t.v, u.v FROM probe_t t LEFT JOIN build_u u " +
          "ON t.k = u.k AND t.v > u.v")
    }
  }

  test("left semi join") {
    withJoinTables(buildNullKeys = true) {
      checkBroadcastJoin(
        "SELECT /*+ BROADCAST(u) */ t.k, t.v FROM probe_t t LEFT SEMI JOIN build_u u " +
          "ON t.k = u.k")
      checkBroadcastJoin(
        "SELECT /*+ BROADCAST(u) */ t.k, t.v FROM probe_t t LEFT SEMI JOIN build_u u " +
          "ON t.k = u.k AND t.v > u.v")
    }
  }

  test("left anti join") {
    withJoinTables(buildNullKeys = true) {
      checkBroadcastJoin(
        "SELECT /*+ BROADCAST(u) */ t.k, t.v FROM probe_t t LEFT ANTI JOIN build_u u " +
          "ON t.k = u.k")
      // The filter drops build rows with a null 'v'.
      checkBroadcastJoin(
        "SELECT /*+ BROADCAST(u) */ t.k, t.v FROM probe_t t LEFT ANTI JOIN build_u u " +
          "ON t.k = u.k AND t.v > u.v")
    }
  }

  test("existence join") {
    withJoinTables(buildNullKeys = true) {
      checkBroadcastJoin(
        "SELECT t.k, t.v FROM probe_t t " +
          "WHERE EXISTS (SELECT 1 FROM build_u u WHERE u.k = t.k AND u.v < t.v) OR t.v > 90")
    }
  }

  test("null aware anti join without null build keys") {
    withJoinTables(buildNullKeys = false) {
      checkBroadcastJoin("SELECT t.k, t.v FROM probe_t t WHERE t.k NOT IN (SELECT k FROM build_u)")
    }
  }

  test("null aware anti join with a null build key") {
    withJoinTables(buildNullKeys = true) {
      checkBroadcastJoin("SELECT t.k, t.v FROM probe_t t WHERE t.k NOT IN (SELECT k FROM build_u)")
    }
  }

  test("driver-side build") {
    // The driver builds the table with HashTableBuilder and serializes it, and executors
    // deserialize it.
    withSQLConf(VeloxConfig.VELOX_DRIVER_SIDE_BROADCAST_HASH_TABLE_BUILD.key -> "true") {
      withJoinTables(buildNullKeys = true) {
        checkBroadcastJoin(
          "SELECT /*+ BROADCAST(u) */ t.k, t.v, u.v FROM probe_t t JOIN build_u u ON t.k = u.k")
        checkBroadcastJoin(
          "SELECT /*+ BROADCAST(u) */ t.k, t.v FROM probe_t t LEFT SEMI JOIN build_u u " +
            "ON t.k = u.k")
        checkBroadcastJoin(
          "SELECT /*+ BROADCAST(u) */ t.k, t.v FROM probe_t t LEFT ANTI JOIN build_u u " +
            "ON t.k = u.k AND t.v > u.v")
        checkBroadcastJoin(
          "SELECT t.k, t.v FROM probe_t t WHERE t.k NOT IN (SELECT k FROM build_u)")
      }
    }
  }
}

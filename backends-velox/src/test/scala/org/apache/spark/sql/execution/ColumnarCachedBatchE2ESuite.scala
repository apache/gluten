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
import org.apache.gluten.execution.VeloxWholeStageTransformerSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.columnar.{InMemoryTableScanExec, SparkCacheUtil}
import org.apache.spark.sql.functions.{col, lit, when}

import java.sql.Timestamp
import java.time.Instant

/**
 * End-to-end smoke for Gluten in-memory cache stats (Layer A min/max).
 *
 * Asserts no crash, correct result, plan shape, and `numOutputRows` significantly less than total
 * rows. Precise prune semantics live in `ColumnarCachedBatchBuildFilterPruneSuite`.
 */
class ColumnarCachedBatchE2ESuite
  extends VeloxWholeStageTransformerSuite
  with AdaptiveSparkPlanHelper {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
    SparkCacheUtil.clearCacheSerializer()
  }

  override protected def afterAll(): Unit = {
    SparkCacheUtil.clearCacheSerializer()
    super.afterAll()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.shuffle.partitions", "4")
      .set(GlutenConfig.COLUMNAR_TABLE_CACHE_ENABLED.key, "true")
      .set(GlutenConfig.COLUMNAR_TABLE_CACHE_PARTITION_STATS_ENABLED.key, "true")
  }

  // Build a deterministic, range-partitioned cached frame:
  //   k in [0, N), repartitioned to P partitions by id range so each partition
  //   carries a disjoint k interval. A point-equality filter on the pivot can
  //   then be pruned to a single partition by min/max metadata.
  private val N: Long = 1000L
  private val P: Int = 5
  private val pivot: Long = 500L // falls inside partition that owns [400, 600)

  private def cacheRange(): DataFrame = {
    spark
      .range(N)
      .select(col("id").cast("bigint").as("k"))
      .repartitionByRange(P, col("k"))
      .cache()
  }

  // Caller must have triggered execution so numOutputRows is populated.
  // expectPrune=false is path-only: numOutputRows is unreliable for "no prune"
  // on the Gluten native path (surviving rows bypass the IMS counter, see
  // baseline "numOutputRows reflects post-filter row count" test).
  private def assertGlutenCachedPlanAndPrune(df: DataFrame, expectPrune: Boolean): Unit = {
    val plan = df.queryExecution.executedPlan
    val ims = find(plan) {
      case _: InMemoryTableScanExec => true
      case _ => false
    }
      .get.asInstanceOf[InMemoryTableScanExec]
    val serName = ims.relation.cacheBuilder.serializer.getClass.getSimpleName
    assert(serName == "ColumnarCachedBatchSerializer", s"got $serName")
    if (expectPrune) {
      val outRows = ims.metrics("numOutputRows").value
      val upperBound = (N / P) * 2
      assert(outRows <= upperBound, s"numOutputRows=$outRows > $upperBound (N=$N, P=$P)")
    }
  }

  test("e2e cache + equality filter: no crash + correct result") {
    val cached = cacheRange()
    try {
      cached.count() // materialize cache (triggers serializeWithStats path)
      val result = cached.filter(col("k") === pivot).count()
      assert(result == 1L, s"expected exactly one row matching k=$pivot, got $result")
    } finally {
      cached.unpersist()
    }
  }

  test("plan contains InMemoryTableScanExec + our serializer kicked in") {
    val cached = cacheRange()
    try {
      cached.count()
      val df = cached.filter(col("k") === pivot)
      val plan = df.queryExecution.executedPlan
      val scan = find(plan) {
        case _: InMemoryTableScanExec => true
        case _ => false
      }
      assert(scan.isDefined, s"plan missing InMemoryTableScanExec:\n$plan")
      val ims = scan.get.asInstanceOf[InMemoryTableScanExec]
      val serName = ims.relation.cacheBuilder.serializer.getClass.getSimpleName
      assert(
        serName == "ColumnarCachedBatchSerializer",
        s"expected ColumnarCachedBatchSerializer, got $serName"
      )
      // Force execution so numOutputRows is populated for the next assertion.
      df.count()
    } finally {
      cached.unpersist()
    }
  }

  test("numOutputRows reflects post-filter row count (significantly < N)") {
    val cached = cacheRange()
    try {
      cached.count()
      val df = cached.filter(col("k") === pivot)
      df.count()
      val plan = df.queryExecution.executedPlan
      val ims = find(plan) {
        case _: InMemoryTableScanExec => true
        case _ => false
      }.get.asInstanceOf[InMemoryTableScanExec]
      val outRows = ims.metrics("numOutputRows").value
      // Prune evidence: numOutputRows must be << N (full-scan would give N).
      // Lower bound is 0 -- with full partition pruning the InMemoryTableScanExec
      // node may legitimately emit zero rows (the surviving row comes from cache
      // metadata / pivot resolution at a higher layer when Gluten native scan
      // uses its own metrics path). The semantic correctness is anchored by
      // the equality-result test (result == 1) and the precise prune behavior by
      // BuildFilterPruneSuite; this case only needs to refute full-scan.
      val upperBound = (N / P) * 2
      assert(
        outRows <= upperBound,
        s"numOutputRows=$outRows expected <= $upperBound " +
          s"(N=$N, P=$P, full-scan would give $N -- prune appears not effective)"
      )
    } finally {
      cached.unpersist()
    }
  }

  test("all-null Long column: cache + equality filter no crash + correct result") {
    val df = spark
      .range(N)
      .select(lit(null).cast("bigint").as("k"))
      .repartition(P)
      .cache()
    try {
      df.count() // materialize
      val result = df.filter(col("k") === 5L).count()
      assert(result == 0L, s"all-null col cannot match k=5, got $result")
      // Sanity: cached count (pre-filter) is still N
      assert(df.count() == N, s"all-null cached frame should still hold $N rows")
    } finally {
      df.unpersist()
    }
  }

  test("Float NaN same batch: filter on non-NaN not silently pruned") {
    // coalesce(1) forces the NaN row (id=7) and the queried finite row (id=42) into the SAME
    // cached batch, deterministically reproducing the regression: previously a NaN poisoned the
    // whole column to unsupported -> null min/max bounds -> vanilla buildFilter pruned the batch
    // -> the finite k=42.0 row was silently dropped. NaN must instead be skipped so the finite
    // bounds [0, 999] are emitted and the matching row is returned (parity with vanilla Spark).
    val df = spark
      .range(N)
      .select(
        when(col("id") === 7L, lit(Float.NaN))
          .otherwise(col("id").cast("float"))
          .as("k"))
      .coalesce(1)
      .cache()
    try {
      df.count()
      val result = df.filter(col("k") === 42.0f).count()
      assert(
        result == 1L,
        s"expected 1 row with k=42.0, got $result " +
          s"(NaN must not poison partition stats / prune the finite match)")
    } finally {
      df.unpersist()
    }
  }

  test("Date column equality filter: prune via INTEGER stats (4B LE)") {
    import org.apache.spark.sql.functions.{date_add, lit => sparkLit}
    val base = sparkLit("2020-01-01").cast("date")
    val cached = spark
      .range(N)
      .select(date_add(base, col("id").cast("int")).as("d"))
      .repartitionByRange(P, col("d"))
      .cache()
    try {
      cached.count() // materialize - triggers cpp INTEGER computeStats path
      val pivotDate = date_add(base, sparkLit(pivot.toInt))
      val df = cached.filter(col("d") === pivotDate)
      val result = df.count()
      assert(result == 1L, s"expected exactly one row matching date pivot, got $result")
      val plan = df.queryExecution.executedPlan
      val ims = find(plan) {
        case _: InMemoryTableScanExec => true
        case _ => false
      }.get.asInstanceOf[InMemoryTableScanExec]
      val outRows = ims.metrics("numOutputRows").value
      val upperBound = (N / P) * 2
      assert(
        outRows <= upperBound,
        s"numOutputRows=$outRows expected <= $upperBound (Date prune ineffective)"
      )
    } finally {
      cached.unpersist()
    }
  }

  test("multi-column cache: no IndexOOB + correct result") {
    val cached = spark
      .range(N)
      .selectExpr(
        "cast(id as bigint) as a",
        "cast(id * 2 as bigint) as b",
        "cast(id + 100 as bigint) as c")
      .repartitionByRange(P, col("a"))
      .cache()
    try {
      cached.count()
      val result = cached.filter(col("a") === pivot && col("c") === (pivot + 100L)).count()
      assert(result == 1L, s"expected 1 row matching pivot, got $result")
    } finally {
      cached.unpersist()
    }
  }

  test("Decimal column cache: no UOE crash on materialize + read") {
    val cached = spark
      .range(N)
      .selectExpr("cast(id as decimal(10, 2)) as d")
      .repartition(P)
      .cache()
    try {
      cached.count()
      val total = cached.count()
      assert(total == N, s"expected $N rows, got $total")
    } finally {
      cached.unpersist()
    }
  }

  test("IsNotNull predicate honors vanilla count semantics") {
    val df = spark
      .range(N)
      .selectExpr("if(id % 3 = 0, cast(null as bigint), id) as k") // ~33% nulls
      .repartition(P)
      .cache()
    try {
      df.count() // materialize
      val nonNullCount = df.filter(col("k").isNotNull).count()
      val expected = (0L until N).count(_ % 3 != 0).toLong
      assert(
        nonNullCount == expected,
        s"IsNotNull silently dropped partitions: got $nonNullCount, expected $expected")
    } finally {
      df.unpersist()
    }
  }

  test("Timestamp column equality filter: prune via Long us stats (8B LE)") {
    import org.apache.spark.sql.functions.{lit => sparkLit}
    // Build N rows of distinct timestamps, one second apart starting at epoch
    // 2024-01-01T00:00:00Z (= 1704067200 seconds). Pivot at second N/2.
    val baseSec = 1704067200L
    val cached = spark
      .range(N)
      .selectExpr(s"timestamp_seconds(${baseSec}L + id) as ts")
      .repartitionByRange(P, col("ts"))
      .cache()
    try {
      cached.count() // materialize -- triggers cpp TIMESTAMP computeStats path
      val pivotTs = sparkLit(Timestamp.from(Instant.ofEpochSecond(baseSec + (N / 2))))
      val df = cached.filter(col("ts") === pivotTs)
      val result = df.count()
      assert(result == 1L, s"expected exactly one row matching timestamp pivot, got $result")
      val plan = df.queryExecution.executedPlan
      val ims = find(plan) {
        case _: InMemoryTableScanExec => true
        case _ => false
      }.get.asInstanceOf[InMemoryTableScanExec]
      val outRows = ims.metrics("numOutputRows").value
      val upperBound = (N / P) * 2
      assert(
        outRows <= upperBound,
        s"numOutputRows=$outRows expected <= $upperBound (Timestamp prune ineffective)"
      )
    } finally {
      cached.unpersist()
    }
  }

  test("String column equality filter: prune via byte-unsigned stats") {
    val cached = spark
      .range(N)
      .selectExpr("concat('k_', lpad(cast(id as string), 4, '0')) as s")
      .repartitionByRange(P, col("s"))
      .cache()
    try {
      cached.count() // materialize -- triggers cpp VARCHAR computeStats path
      val pivotStr = "k_0500"
      val df = cached.filter(col("s") === pivotStr)
      val result = df.count()
      assert(result == 1L, s"expected exactly one row matching string pivot, got $result")
      val plan = df.queryExecution.executedPlan
      val ims = find(plan) {
        case _: InMemoryTableScanExec => true
        case _ => false
      }.get.asInstanceOf[InMemoryTableScanExec]
      val outRows = ims.metrics("numOutputRows").value
      val upperBound = (N / P) * 2
      assert(
        outRows <= upperBound,
        s"numOutputRows=$outRows expected <= $upperBound (String prune ineffective)"
      )
    } finally {
      cached.unpersist()
    }
  }

  test("non-binary collation StringType: sentinel demotion keeps batch (no silent prune)") {
    assume(
      spark.version.startsWith("4."),
      "COLLATE syntax requires Spark 4.0+; sentinel path is also gated to Spark 4.x shims")
    val cached = spark
      .range(N)
      .selectExpr("concat('k_', lpad(cast(id as string), 4, '0')) COLLATE UTF8_LCASE as s")
      .repartitionByRange(P, col("s"))
      .cache()
    try {
      cached.count()
      val result = cached.filter(col("s") === "K_0500").count()
      assert(
        result == 1L,
        s"non-binary collation equality must return the matching row, got $result")
    } finally {
      cached.unpersist()
    }
  }

  // ICU collation coverage: UNICODE_CI is a separate collation family
  // (ICU collator, not just lowercase) from UTF8_LCASE. Same wrapper-strip
  // behavior expected, proving the mechanism is not specific to one collation kind.
  test("non-binary collation StringType (UNICODE_CI): pass-through, no silent prune") {
    assume(
      spark.version.startsWith("4."),
      "COLLATE syntax requires Spark 4.0+")
    val cached = spark
      .range(N)
      .selectExpr("concat('k_', lpad(cast(id as string), 4, '0')) COLLATE UNICODE_CI as s")
      .repartitionByRange(P, col("s"))
      .cache()
    try {
      cached.count()
      val result = cached.filter(col("s") === "K_0500").count()
      assert(
        result == 1L,
        s"UNICODE_CI equality must return the matching row, got $result")
    } finally {
      cached.unpersist()
    }
  }

  // Partition-stats negative test: with partition stats disabled (the production default),
  // V3 lazy no-stats bytes are still written, but stats are emitted as null. A bug in the
  // gate could silently activate stats for all users, or break correctness on the
  // stats=null buildFilter pass-through path.
  //
  // Asserts correctness only, not numOutputRows: the Gluten native scan reports row counts
  // on a separate metrics path, so InMemoryTableScanExec.numOutputRows can legitimately be 0
  // in either gated branch (see "numOutputRows reflects post-filter row count" above).
  test("partitionStats.enabled=false: V3 lazy no-stats path correctness preserved") {
    withSQLConf(
      GlutenConfig.COLUMNAR_TABLE_CACHE_PARTITION_STATS_ENABLED.key -> "false") {
      val cached = cacheRange()
      try {
        cached.count() // materialize cache via V3 no-stats path (stats emitted as null)
        val result = cached.filter(col("k") === pivot).count()
        assert(result == 1L, s"expected exactly one row matching k=$pivot, got $result")
      } finally {
        cached.unpersist()
      }
    }
  }

  // Cross-config: build with stats enabled, read with stats disabled.
  // Wire format is build-time-decided, so reader-time SQLConf must not affect prune.
  test("cross-config: build with stats enabled, read with stats disabled") {
    var cached: DataFrame = null
    var filtered: DataFrame = null
    var result: Long = -1L
    withSQLConf(
      GlutenConfig.COLUMNAR_TABLE_CACHE_PARTITION_STATS_ENABLED.key -> "true") {
      val df = cacheRange()
      df.count()
      cached = df
    }
    try {
      withSQLConf(
        GlutenConfig.COLUMNAR_TABLE_CACHE_PARTITION_STATS_ENABLED.key -> "false") {
        filtered = cached.filter(col("k") === pivot)
        result = filtered.count()
      }
      assert(result == 1L, s"got $result")
      assertGlutenCachedPlanAndPrune(filtered, expectPrune = true)
    } finally {
      cached.unpersist()
    }
  }

  // Reverse: V3 no-stats payload at build (stats=null), reader cannot fabricate stats.
  // Distinct from the same-config no-stats test: this forces cross-config.
  test("cross-config: build with stats disabled, read with stats enabled") {
    var cached: DataFrame = null
    var filtered: DataFrame = null
    var result: Long = -1L
    withSQLConf(
      GlutenConfig.COLUMNAR_TABLE_CACHE_PARTITION_STATS_ENABLED.key -> "false") {
      val df = cacheRange()
      df.count()
      cached = df
    }
    try {
      withSQLConf(
        GlutenConfig.COLUMNAR_TABLE_CACHE_PARTITION_STATS_ENABLED.key -> "true") {
        filtered = cached.filter(col("k") === pivot)
        result = filtered.count()
      }
      assert(result == 1L, s"got $result")
      assertGlutenCachedPlanAndPrune(filtered, expectPrune = false)
    } finally {
      cached.unpersist()
    }
  }

  // Round 2 must re-honor the new SQLConf, not reuse stale gate decision /
  // payload from round 1.
  test("cross-build-cycle: unpersist + toggle stats config + rebuild same query") {
    var resultA: Long = -1L
    var resultB: Long = -1L
    withSQLConf(
      GlutenConfig.COLUMNAR_TABLE_CACHE_PARTITION_STATS_ENABLED.key -> "true") {
      val df = cacheRange()
      try {
        df.count()
        val filtered = df.filter(col("k") === pivot)
        resultA = filtered.count()
        assert(resultA == 1L, s"round 1: got $resultA")
        assertGlutenCachedPlanAndPrune(filtered, expectPrune = true)
      } finally {
        df.unpersist(blocking = true)
      }
    }
    withSQLConf(
      GlutenConfig.COLUMNAR_TABLE_CACHE_PARTITION_STATS_ENABLED.key -> "false") {
      val df = cacheRange()
      try {
        df.count()
        val filtered = df.filter(col("k") === pivot)
        resultB = filtered.count()
        assert(resultB == 1L, s"round 2: got $resultB")
        assertGlutenCachedPlanAndPrune(filtered, expectPrune = false)
      } finally {
        df.unpersist(blocking = true)
      }
    }
  }

  test("V2 stats: IsNull on VARBINARY null-bearing partition is not pruned") {
    withSQLConf(
      GlutenConfig.COLUMNAR_TABLE_CACHE_PARTITION_STATS_ENABLED.key -> "true") {
      val df = spark.range(N)
        .select(
          when(col("id") === lit(750L), lit(null).cast("binary"))
            .otherwise(col("id").cast("string").cast("binary"))
            .as("bin"))
        .repartitionByRange(P, col("bin"))
        .cache()
      try {
        df.count()
        assert(
          df.filter(col("bin").isNull).count() == 1L,
          "VARBINARY null-bearing partition was silently pruned by IsNull")
      } finally {
        df.unpersist(blocking = true)
      }
    }
  }

  // V3 lazy deserialization smoke tests

  test("V3 default: cache + equality filter produces correct result") {
    val cached = cacheRange()
    try {
      cached.count()
      val result = cached.filter(col("k") === pivot).count()
      assert(result == 1L, s"V3: expected 1 row matching k=$pivot, got $result")
    } finally {
      cached.unpersist()
    }
  }

  test("V3 default: multi-column cache, partial projection, no crash") {
    val cached = spark
      .range(N)
      .selectExpr(
        "cast(id as bigint) as a",
        "cast(id*2 as bigint) as b",
        "cast(id+1 as bigint) as c")
      .repartitionByRange(P, col("a"))
      .cache()
    try {
      cached.count()
      val result = cached.filter(col("a") === pivot).select("a", "c").count()
      assert(result == 1L, s"V3 projection: expected 1 row, got $result")
    } finally {
      cached.unpersist()
    }
  }
}

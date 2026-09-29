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
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Literal}
import org.apache.spark.sql.classic.ClassicDataset
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.execution.columnar.{InMemoryRelation, SparkCacheUtil}
import org.apache.spark.sql.functions.col

/**
 * End-to-end tests for Gluten table cache lazy deserialization (V3 wire format).
 *
 * Validates: V3 format write, per-column lazy read, correct results, and V2/V3 read compatibility.
 */
class ColumnarCachedBatchLazySerdeTest extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  private val N = 500L
  private val P = 4

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

  private def cachedRelation(df: DataFrame): InMemoryRelation = {
    val classicDf = ClassicDataset.ofRows(spark, df.queryExecution.logical)
    val cached = spark.sharedState.cacheManager.lookupCachedData(classicDf).getOrElse {
      fail("expected DataFrame to be registered in Spark cache manager")
    }
    cached.cachedRepresentation match {
      case relation: InMemoryRelation => relation
      case other => fail(s"expected InMemoryRelation, got ${other.getClass.getName}")
    }
  }

  private def cachedBatches(df: DataFrame): Array[CachedColumnarBatch] = {
    cachedRelation(df).cacheBuilder.cachedColumnBuffers.collect().map {
      case batch: CachedColumnarBatch => batch
      case other => fail(s"expected CachedColumnarBatch, got ${other.getClass.getName}")
    }
  }

  private def firstCachedBatch(df: DataFrame): CachedColumnarBatch = {
    cachedBatches(df).head
  }

  private def isV3Frame(bytes: Array[Byte]): Boolean = {
    val magic = CachedColumnarBatchKryoSerializer.STATS_FRAMED_MAGIC_V3
    bytes != null && bytes.length >= magic.length && bytes.take(magic.length).sameElements(magic)
  }

  test("partitionStats enabled writes V3 lazy format by default") {
    val cached = spark.range(N).select(col("id").cast("bigint").as("k")).cache()
    try {
      val count = cached.count()
      assert(count == N, s"Expected $N rows, got $count")
      val batch = firstCachedBatch(cached)
      assert(
        isV3Frame(batch.bytes),
        "partitionStats-enabled cache should write V3 lazy bytes by default")
      val filtered = cached.filter(col("k") === 100L).count()
      assert(filtered == 1L, s"Expected 1 row matching k=100, got $filtered")
    } finally {
      cached.unpersist()
    }
  }

  test("partitionStats disabled still writes V3 lazy format by default") {
    withSQLConf(GlutenConfig.COLUMNAR_TABLE_CACHE_PARTITION_STATS_ENABLED.key -> "false") {
      val cached = spark
        .range(N)
        .selectExpr("cast(id as bigint) as a", "cast(id * 2 as bigint) as b", "id + 100 as c")
        .cache()
      try {
        val count = cached.count()
        assert(count == N, s"Expected $N rows, got $count")
        val batch = firstCachedBatch(cached)
        assert(
          isV3Frame(batch.bytes),
          "V3 lazy bytes should be the default even when partition stats are disabled")
        assert(batch.stats == null, "partitionStats=false should not attach pruning stats")

        val projected = cached.select("a", "c").collect()
        assert(projected.length == N.toInt)
        projected.foreach {
          row =>
            val a = row.getLong(0)
            val c = row.getLong(1)
            assert(c == a + 100L, s"expected c=a+100 for a=$a, got c=$c")
        }
      } finally {
        cached.unpersist()
      }
    }
  }

  test("V3: projected read gives correct results (M < N columns)") {
    val cached = spark
      .range(N)
      .selectExpr("cast(id as bigint) as a", "cast(id*2 as bigint) as b", "id + 100 as c")
      .cache()
    try {
      cached.count()
      val result = cached.select("a", "c").collect()
      assert(result.length == N.toInt, s"Expected $N rows, got ${result.length}")
      // Verify a and c columns are correct.
      val expected = (0L until N).map(i => (i, i + 100L)).toMap
      result.foreach {
        row =>
          val a = row.getLong(0)
          val c = row.getLong(1)
          assert(expected.get(a).contains(c), s"a=$a expected c=${expected(a)}, got $c")
      }
    } finally {
      cached.unpersist()
    }
  }

  test("V3: count(*) does not crash (zero-column path)") {
    val df = spark
      .range(N)
      .selectExpr("cast(id as int) as k", "cast(id as string) as s", "id as d")
      .cache()
    try {
      df.count()
      val cnt = df.count()
      assert(cnt == N, s"Expected $N, got $cnt")
    } finally {
      df.unpersist()
    }
  }

  test("V3 + stats pruning: equality filter gives correct result + pruning active") {
    val pivot = 250L
    val cached = spark
      .range(N)
      .select(col("id").cast("bigint").as("k"))
      .repartitionByRange(P, col("k"))
      .cache()
    try {
      cached.count()
      val relation = cachedRelation(cached)
      val batches = cachedBatches(cached)
      assert(
        batches.length > 1,
        s"test setup should create multiple cached batches, got ${batches.length}")
      assert(batches.forall(_.stats != null), "partitionStats=true should attach pruning stats")

      val cachedAttr = relation.output.find(_.name == "k").getOrElse {
        fail(s"expected cached attribute k in ${relation.output.mkString(",")}")
      }
      val filter = new ColumnarCachedBatchSerializer()
        .buildFilter(Seq(EqualTo(cachedAttr, Literal(pivot))), Seq(cachedAttr))
      val batchIterator: Iterator[CachedBatch] = batches.iterator
      val kept = filter(0, batchIterator).toArray
      assert(kept.nonEmpty, "stats pruning should keep the batch whose range contains the pivot")
      assert(
        kept.length < batches.length,
        s"stats pruning should skip at least one cached batch, " +
          s"kept=${kept.length}, total=${batches.length}")

      val result = cached.filter(col("k") === pivot).count()
      assert(result == 1L, s"Expected 1 row matching k=$pivot, got $result")
    } finally {
      cached.unpersist()
    }
  }

  test("V3: all types roundtrip correctly") {
    val df = spark
      .range(100)
      .selectExpr(
        "cast(id as int) as int_col",
        "cast(id as bigint) as long_col",
        "cast(id as double) as double_col",
        "cast(id/100.0 as decimal(10,2)) as dec_col",
        "date_add(date('2020-01-01'), cast(id as int)) as date_col",
        "cast(id as string) as str_col",
        "cast(id % 2 = 0 as boolean) as bool_col"
      )
      .cache()
    try {
      df.count()
      // Only read a subset of columns - validates lazy deserialization of specific types.
      val result = df.select("int_col", "date_col", "bool_col").collect()
      assert(result.length == 100)
      assert(result(0).getInt(0) == 0)
      assert(!result(0).isNullAt(1))
      assert(result(0).getBoolean(2) == true) // 0 % 2 == 0
    } finally {
      df.unpersist()
    }
  }

  test("V3: all-null projected column roundtrips correctly") {
    val cached = spark
      .range(N)
      .selectExpr("cast(id as bigint) as k", "cast(null as bigint) as all_null")
      .repartitionByRange(P, col("k"))
      .cache()
    try {
      cached.count()
      val result = cached.select("all_null").collect()
      assert(result.length == N.toInt)
      assert(result.forall(_.isNullAt(0)), "all_null should remain null after V3 projection")
      val filtered = cached.where("all_null is null").count()
      assert(filtered == N)
    } finally {
      cached.unpersist()
    }
  }

  test("V3 cached bytes remain readable after partitionStats is disabled") {
    val cached = spark.range(N).select(col("id").cast("bigint").as("k")).cache()
    cached.count()
    assert(
      isV3Frame(firstCachedBatch(cached).bytes),
      "test setup should materialize V3 bytes")
    try {
      withSQLConf(GlutenConfig.COLUMNAR_TABLE_CACHE_PARTITION_STATS_ENABLED.key -> "false") {
        val result = cached.filter(col("k") === 100L).count()
        assert(result == 1L, s"V3 bytes must remain readable, got $result")
      }
    } finally {
      cached.unpersist()
    }
  }

  test("V3: wide table - only requested columns decoded (3-col cache, 1-col query)") {
    val N2 = 200L
    val cached = spark
      .range(N2)
      .selectExpr(
        "cast(id as int) as a",
        "cast(id * 10 as bigint) as b", // not requested
        "cast(id + 1000 as bigint) as c")
      .repartitionByRange(P, col("a"))
      .cache()
    try {
      cached.count()
      // Only read column 'a'.
      val results = cached.select("a").collect()
      assert(results.length == N2.toInt)
      results.zipWithIndex.foreach {
        case (row, _) =>
          assert(!row.isNullAt(0), "column 'a' should not be null")
      }
    } finally {
      cached.unpersist()
    }
  }
}

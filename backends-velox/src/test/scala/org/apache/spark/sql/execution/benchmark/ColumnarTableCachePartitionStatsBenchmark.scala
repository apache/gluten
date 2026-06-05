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
package org.apache.spark.sql.execution.benchmark

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.SchemaJsonInternCache
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import java.nio.charset.StandardCharsets

/**
 * Benchmark to measure write/read overhead and pruning benefit of partition stats in columnar table
 * cache, plus microbench coverage for the schema-codec intern cache used by
 * `ColumnarCachedBatchSerializer`. To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
 * }}}
 */
object ColumnarTableCachePartitionStatsBenchmark extends SqlBasedBenchmark {
  private val numRows = 100L * 1000 * 1000
  private val numParts = 32
  private val confKey = GlutenConfig.COLUMNAR_TABLE_CACHE_PARTITION_STATS_ENABLED.key

  private def buildCache(statsOn: Boolean): DataFrame = {
    import org.apache.spark.sql.functions.col
    val prev = spark.conf.getOption(confKey)
    spark.conf.set(confKey, statsOn.toString)
    try {
      val cached = spark
        .range(numRows)
        .selectExpr(
          "cast(id as int) as c0",
          "id as c2",
          "cast(id as string) as c3",
          "uuid() as c4")
        .repartitionByRange(numParts, col("c2"))
        .persist(StorageLevel.MEMORY_ONLY)
      cached.count() // materialize cache (stats are emitted on the write path)
      cached
    } finally {
      prev match {
        case Some(v) => spark.conf.set(confKey, v)
        case None => spark.conf.unset(confKey)
      }
    }
  }

  // ============================================================================
  // Schema-codec intern microbench (SchemaJsonInternCache).
  //
  // ColumnarCachedBatchSerializer hot paths call StructType.json on every batch
  // write and DataType.fromJson on every batch read. The intern cache memoizes
  // the round-trip without changing the wire format. Sections below compare two
  // distinct method calls in the same JVM as cache off (raw codec) vs cache on
  // (intern memoized round-trip), with no toggle on the cache class itself.
  // ============================================================================

  private val INTERN_CAP = SchemaJsonInternCache.CAP.toInt

  private def schemaFixture(numCols: Int, nameLen: Int): StructType = {
    val name = "c" + ("x" * math.max(0, nameLen - 1))
    StructType(
      (0 until numCols).map(i => StructField(s"$name$i", LongType, nullable = true)))
  }

  // TPC-DS store_sales-derived 23-col mixed-type fixture; realistic name shape.
  private def realisticSchema: StructType = StructType(
    Seq(
      StructField("ss_sold_date_sk", IntegerType),
      StructField("ss_sold_time_sk", IntegerType),
      StructField("ss_item_sk", IntegerType),
      StructField("ss_customer_sk", IntegerType),
      StructField("ss_cdemo_sk", IntegerType),
      StructField("ss_hdemo_sk", IntegerType),
      StructField("ss_addr_sk", IntegerType),
      StructField("ss_store_sk", IntegerType),
      StructField("ss_promo_sk", IntegerType),
      StructField("ss_ticket_number", LongType),
      StructField("ss_quantity", IntegerType),
      StructField("ss_wholesale_cost", DecimalType(7, 2)),
      StructField("ss_list_price", DecimalType(7, 2)),
      StructField("ss_sales_price", DecimalType(7, 2)),
      StructField("ss_ext_discount_amt", DecimalType(7, 2)),
      StructField("ss_ext_sales_price", DecimalType(7, 2)),
      StructField("ss_ext_wholesale_cost", DecimalType(7, 2)),
      StructField("ss_ext_list_price", DecimalType(7, 2)),
      StructField("ss_ext_tax", DecimalType(7, 2)),
      StructField("ss_coupon_amt", DecimalType(7, 2)),
      StructField("ss_net_paid", DecimalType(7, 2)),
      StructField("ss_net_paid_inc_tax", DecimalType(7, 2)),
      StructField("ss_net_profit", DecimalType(7, 2))
    ))

  private val internSchemas: Seq[(String, StructType)] =
    (for {
      width <- Seq(10, 100, 1000)
      nameLen <- Seq(1, 32)
    } yield (s"w=$width n=$nameLen", schemaFixture(width, nameLen))) :+
      ("tpcds-store_sales-23col" -> realisticSchema)

  private def runInternEncode(label: String, schema: StructType): Unit = {
    val N = 1L * 1000 * 1000
    val intern = new SchemaJsonInternCache
    val bench = new Benchmark(label, N, output = output)
    bench.addCase("off (raw schema.json.getBytes per call)", 5) {
      _ =>
        var i = 0L
        var checksum = 0L
        while (i < N) {
          val bytes = schema.json.getBytes(StandardCharsets.UTF_8)
          checksum ^= bytes.length.toLong
          i += 1
        }
        assert(checksum != Long.MinValue, s"checksum=$checksum")
    }
    bench.addCase("on  (intern.encodeBytes: cached canonical bytes)", 5) {
      _ =>
        var i = 0L
        var checksum = 0L
        while (i < N) {
          val bytes = intern.encodeBytes(schema)
          checksum ^= bytes.length.toLong
          i += 1
        }
        assert(checksum != Long.MinValue, s"checksum=$checksum")
    }
    bench.run()
  }

  private def runInternDecode(label: String, schema: StructType): Unit = {
    val N = 1L * 100 * 1000
    val intern = new SchemaJsonInternCache
    val jsonBytes = schema.json.getBytes(StandardCharsets.UTF_8)
    val bench = new Benchmark(label, N, output = output)
    bench.addCase("off (raw DataType.fromJson per call)", 5) {
      _ =>
        var i = 0L
        var checksum = 0L
        while (i < N) {
          val s = DataType
            .fromJson(new String(jsonBytes, StandardCharsets.UTF_8))
            .asInstanceOf[StructType]
          checksum ^= s.length.toLong
          i += 1
        }
        assert(checksum != Long.MinValue, s"checksum=$checksum")
    }
    bench.addCase("on  (intern.decodeStructType: cached canonical StructType)", 5) {
      _ =>
        var i = 0L
        var checksum = 0L
        while (i < N) {
          val s = intern.decodeStructType(jsonBytes)
          checksum ^= s.length.toLong
          i += 1
        }
        assert(checksum != Long.MinValue, s"checksum=$checksum")
    }
    bench.run()
  }

  // Working-set sweep across three regimes around cap = 256:
  //   C1 == cap     -> 100% hit steady state
  //   C2 == 2 x cap -> eviction pressure, partial hit
  //   C3 == 4 x cap -> worst-case round-robin, ~all miss
  // Gates (read at results-read time):
  //   C1 on must be >= off; C2 on within 1.5x of off; C3 documented as known regression.
  private def runInternWorkingSetSweep(): Unit = {
    val passes = 100
    Seq(
      ("C1 hit (256 schemas == cap)", INTERN_CAP),
      ("C2 partial (512 schemas == 2x cap)", INTERN_CAP * 2),
      ("C3 churn (1024 schemas == 4x cap)", INTERN_CAP * 4)
    ).foreach {
      case (label, distinctCount) =>
        val many = (0 until distinctCount).map(i => schemaFixture(10, 8 + (i % 16)))
        val N = many.length.toLong * passes
        val intern = new SchemaJsonInternCache
        val bench = new Benchmark(label, N, output = output)
        bench.addCase("off", 5) {
          _ =>
            var p = 0
            var checksum = 0L
            while (p < passes) {
              many.foreach(s => checksum ^= s.json.getBytes(StandardCharsets.UTF_8).length.toLong)
              p += 1
            }
            assert(checksum != Long.MinValue, s"checksum=$checksum")
        }
        bench.addCase("on", 5) {
          _ =>
            var p = 0
            var checksum = 0L
            while (p < passes) {
              many.foreach(s => checksum ^= intern.encodeBytes(s).length.toLong)
              p += 1
            }
            assert(checksum != Long.MinValue, s"checksum=$checksum")
        }
        bench.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    // === Benchmark 1: write-path overhead (cache build) ===
    val buildBench = new Benchmark("table cache build", numRows, output = output)
    Seq(false, true).foreach {
      on =>
        buildBench.addCase(s"partitionStats ${if (on) "on " else "off"}", 3) {
          _ =>
            spark.catalog.clearCache()
            buildCache(statsOn = on).unpersist()
        }
    }
    buildBench.run()
    spark.catalog.clearCache()

    // Build two cached relations once for the read-path benchmarks
    val cachedOff = buildCache(statsOn = false)
    val cachedOn = buildCache(statsOn = true)

    // Heavier follow-up operator: groupBy + sum on c2 + count over c3.
    // Pruned partitions also skip the agg work, amplifying the prune speedup.
    import org.apache.spark.sql.functions._
    def heavyAgg(df: DataFrame, predicate: String): Unit = {
      df.where(predicate)
        .groupBy((col("c2") % 1000).as("g"))
        .agg(sum("c2"), count("c3"), avg("c0"))
        .noop()
    }

    // === Benchmark 2: read prune, high selectivity (~0.001%) ===
    val readHighBench =
      new Benchmark(
        "table cache filter+agg (high selectivity, ~0.001%)",
        numRows,
        output = output)
    readHighBench.addCase("partitionStats off", 3)(_ => heavyAgg(cachedOff, "c2 < 1000"))
    readHighBench.addCase("partitionStats on ", 3)(_ => heavyAgg(cachedOn, "c2 < 1000"))
    readHighBench.run()

    // === Benchmark 3: read prune, low selectivity (~50%) ===
    val readLowBench =
      new Benchmark(
        "table cache filter+agg (low selectivity, ~50%)",
        numRows,
        output = output)
    readLowBench.addCase("partitionStats off", 3)(_ => heavyAgg(cachedOff, "c2 < 50000000"))
    readLowBench.addCase("partitionStats on ", 3)(_ => heavyAgg(cachedOn, "c2 < 50000000"))
    readLowBench.run()

    // === Benchmark 4: read prune, point lookup (~1 row) ===
    val readPointBench =
      new Benchmark(
        "table cache filter+agg (point lookup, 1 row)",
        numRows,
        output = output)
    readPointBench.addCase("partitionStats off", 3)(_ => heavyAgg(cachedOff, "c2 = 50000000"))
    readPointBench.addCase("partitionStats on ", 3)(_ => heavyAgg(cachedOn, "c2 = 50000000"))
    readPointBench.run()

    spark.catalog.clearCache()

    // === Benchmark 5: schema-codec intern microbench - encode (Section A) ===
    runBenchmark("StructType JSON codec - encode (Section A)") {
      internSchemas.foreach { case (label, sch) => runInternEncode(s"encode $label", sch) }
    }

    // === Benchmark 6: schema-codec intern microbench - decode (Section B) ===
    runBenchmark("StructType JSON codec - decode (Section B)") {
      internSchemas.foreach { case (label, sch) => runInternDecode(s"decode $label", sch) }
    }

    // === Benchmark 7: schema-codec intern working-set sweep (Section C) ===
    runBenchmark("StructType JSON codec - working-set sweep (Section C)") {
      runInternWorkingSetSweep()
    }
  }
}

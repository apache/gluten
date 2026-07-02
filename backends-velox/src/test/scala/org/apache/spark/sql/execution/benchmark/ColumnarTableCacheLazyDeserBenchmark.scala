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
import org.apache.spark.sql.execution.ColumnarCachedBatchSerializer
import org.apache.spark.storage.StorageLevel

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Locale

/**
 * Benchmark to measure write/read overhead, cache footprint and column-skip benefit of the columnar
 * table cache across the two serialization generations and the optional partition-stats payload.
 *
 * Compares the FOUR cache environments (eager "V2" vs lazy "V3", each with and without stats):
 *   - V2 without stats (eager): the legacy raw Presto payload. There is no V2 stats-less frame --
 *     stats-off + V3-off writes the legacy bytes -- so this is the eager/no-pruning baseline.
 *   - V2 with stats (eager): `framedSerializeWithStats`. Eager full-batch decode, partition-stats
 *     pruning, but NO per-column skip.
 *   - V3 without stats (lazy): default per-column payload with lazy projected deserialization.
 *   - V3 with stats (lazy): same lazy per-column payload plus partition stats for pruning.
 *
 * The two axes are driven by two independent toggles:
 *   - lazy V3 vs eager V2: `ColumnarCachedBatchSerializer.withStatsExtV3AvailabilityForBenchmark`
 *     (when V3 is unavailable the write path falls back to V2-with-stats / legacy, exactly the
 *     paths an older native library would take).
 *   - stats on/off: `spark.gluten.sql.columnar.tableCache.partitionStats.enabled`.
 *
 * Set `spark.gluten.benchmark.includeLegacyBaseline=false` to drop the two eager V2 modes and
 * benchmark only the V3 lazy modes (halves the cache memory needed for the read phases).
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
 * }}}
 */
object ColumnarTableCacheLazyDeserBenchmark extends SqlBasedBenchmark {
  private val requestedRows =
    spark.sparkContext.conf.getLong("spark.gluten.benchmark.rows", 100L * 1000 * 1000)
  private val numParts =
    spark.sparkContext.conf.getInt("spark.gluten.benchmark.partitions", 32)
  private val benchmarkIters =
    spark.sparkContext.conf.getInt("spark.gluten.benchmark.iterations", 3)
  private val benchmarkPhases = spark.sparkContext.conf
    .get("spark.gluten.benchmark.phases", "build,read1,read4,readAll,filter")
    .split(",")
    .map(_.trim.toLowerCase(Locale.ROOT))
    .filter(_.nonEmpty)
    .toSet
  // Backward-compatible knob (the original 3-mode benchmark called the eager path the "legacy
  // baseline"). When true, the two eager V2 modes (no-stats + with-stats) are included alongside
  // the two V3 lazy modes for the full 4-way comparison; when false only the V3 modes run.
  private val includeV2Modes =
    spark.sparkContext.conf.getBoolean("spark.gluten.benchmark.includeLegacyBaseline", true)
  // Wide schema: 16 columns; queries project only a small subset to demonstrate skip benefit.
  private val numCols = 16
  private val cacheMarkerCol = "__cache_benchmark_mode"
  private val statsConfKey = GlutenConfig.COLUMNAR_TABLE_CACHE_PARTITION_STATS_ENABLED.key

  /**
   * One of the four cache environments. `marker` is an arbitrary per-mode literal embedded in the
   * cached frame so each mode produces a distinct logical plan and therefore a separate Spark cache
   * entry (no accidental reuse between modes).
   */
  private case class Mode(label: String, statsOn: Boolean, v3Available: Boolean, marker: Int)

  private val modes: Seq[Mode] = {
    val v2 = Seq(
      Mode("V2 without stats (eager)", statsOn = false, v3Available = false, marker = -2),
      Mode("V2 with stats (eager)", statsOn = true, v3Available = false, marker = -1)
    )
    val v3 = Seq(
      Mode("V3 without stats (lazy)", statsOn = false, v3Available = true, marker = 0),
      Mode("V3 with stats (lazy)", statsOn = true, v3Available = true, marker = 1)
    )
    (if (includeV2Modes) v2 else Seq.empty) ++ v3
  }

  // Both the build phase and the read phases keep only ONE mode's cache materialized at a time
  // (build -> measure -> unpersist), so peak heap is ~a single cache (the cached payloads are JVM
  // byte[] blocks; ~110 B/row for this 16-col schema, measured ~104 B at 5M). Auto-scale the row
  // count down to a fraction of the actual JVM heap ceiling (Runtime.maxMemory, which reflects
  // -Xmx / -XX:MaxRAMPercentage and any container cgroup limit) so the single cache never evicts
  // or OOMs; on a large heap the full request runs unchanged. This is what lets a 100M-row run
  // complete on a modest runner where 4 simultaneous caches could not. The effective row count is
  // logged in the benchmark header so results are honest about the scale actually exercised.
  private val bytesPerRowPerMode =
    spark.sparkContext.conf.getLong("spark.gluten.benchmark.bytesPerRowPerMode", 110L)
  // Fraction of the JVM heap budgeted for the single live cache; stays inside Spark's ~0.6 unified
  // storage region with margin for query execution.
  private val memBudgetFraction =
    spark.sparkContext.conf.getDouble("spark.gluten.benchmark.memBudgetFraction", 0.5)

  private val heapMaxBytes = Runtime.getRuntime.maxMemory
  private val numRows: Long = {
    val perRowOneCache = math.max(1L, bytesPerRowPerMode)
    val budget = (heapMaxBytes * memBudgetFraction).toLong
    val maxRows = math.max(1L, budget / perRowOneCache)
    math.min(requestedRows, maxRows)
  }
  private val rowsAutoScaled = numRows < requestedRows

  private val filterValue =
    spark.sparkContext.conf.getLong("spark.gluten.benchmark.filterValue", Math.max(0L, numRows / 2))

  private case class CacheFootprint(
      storageMemoryBytes: Long,
      storageDiskBytes: Long,
      numPartitions: Int,
      numCachedPartitions: Int,
      rddCount: Int)

  private case class CachedTable(
      mode: Mode,
      df: DataFrame,
      cachedDf: DataFrame,
      footprint: CacheFootprint)

  private def buildCache(mode: Mode): DataFrame = {
    import org.apache.spark.sql.functions.col
    val prevStats = spark.conf.getOption(statsConfKey)
    spark.conf.set(statsConfKey, mode.statsOn.toString)
    ColumnarCachedBatchSerializer.withStatsExtV3AvailabilityForBenchmark(mode.v3Available) {
      try {
        // Wide table: c0 (key), c1-c15 (payload columns)
        val exprs = Seq(
          "cast(id as int) as c0",
          "id as c1",
          "cast(id as string) as c2",
          "concat('payload-a-', cast(id % 100000 as string)) as c3",
          "cast(id % 100 as int) as c4",
          "cast(id * 2 as long) as c5",
          "cast(id as double) as c6",
          "cast(id % 10 as int) as c7",
          "concat('payload-b-', cast(id % 100000 as string)) as c8",
          "cast(id + 1 as long) as c9",
          "cast(id as string) as c10",
          "cast(id % 50 as int) as c11",
          "cast(id * 3 as long) as c12",
          "concat('payload-c-', cast(id % 100000 as string)) as c13",
          "cast(id % 200 as int) as c14",
          "cast(id as double) as c15"
        )
        val cached = spark
          .range(numRows)
          .selectExpr((exprs :+ s"${mode.marker} as $cacheMarkerCol"): _*)
          .repartitionByRange(numParts, col("c0"))
          .persist(StorageLevel.MEMORY_ONLY)
        cached.count() // materialize
        cached
      } finally {
        prevStats match {
          case Some(v) => spark.conf.set(statsConfKey, v)
          case None => spark.conf.unset(statsConfKey)
        }
      }
    }
  }

  private def buildCacheWithFootprint(mode: Mode): CachedTable = {
    val beforeIds = spark.sparkContext.getRDDStorageInfo.map(_.id).toSet
    val cached = buildCache(mode)
    val rdds = spark.sparkContext.getRDDStorageInfo
      .filterNot(info => beforeIds.contains(info.id))
    val footprint = CacheFootprint(
      rdds.map(_.memSize).sum,
      rdds.map(_.diskSize).sum,
      rdds.map(_.numPartitions).sum,
      rdds.map(_.numCachedPartitions).sum,
      rdds.length)
    CachedTable(mode, cached.drop(cacheMarkerCol), cached, footprint)
  }

  private def printCacheFootprint(rows: Seq[(String, CacheFootprint)]): Unit = {
    outputLine()
    outputLine("Cache footprint after materialization")
    outputLine("-------------------------------------")
    outputLine(
      f"${"mode"}%-32s ${"rdds"}%6s ${"cachedPartitions"}%18s ${"partitions"}%12s " +
        f"${"storageMemoryBytes"}%20s ${"storageMemoryMiB"}%18s ${"storageDiskBytes"}%18s")
    rows.foreach {
      case (mode, footprint) =>
        outputLine(
          f"$mode%-32s ${footprint.rddCount}%6d ${footprint.numCachedPartitions}%18d " +
            f"${footprint.numPartitions}%12d ${footprint.storageMemoryBytes}%20d " +
            f"${footprint.storageMemoryBytes.toDouble / 1024.0 / 1024.0}%18.2f " +
            f"${footprint.storageDiskBytes}%18d")
    }
    outputLine()
  }

  private def phaseEnabled(name: String): Boolean =
    benchmarkPhases.contains(name.toLowerCase(Locale.ROOT))

  private def outputLine(line: String = ""): Unit = {
    val out = output.getOrElse(System.out)
    out.write((line + System.lineSeparator()).getBytes(UTF_8))
    out.flush()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val heapMiB = heapMaxBytes / (1024 * 1024)
    val scaleNote =
      if (rowsAutoScaled) {
        s" [AUTO-SCALED DOWN from requested $requestedRows to fit jvmHeapMax=${heapMiB}MiB x " +
          s"budgetFraction=$memBudgetFraction for a single live cache @ " +
          s"~${bytesPerRowPerMode}B/row]"
      } else {
        ""
      }
    outputLine(
      s"Benchmark config: rows=$numRows (requested=$requestedRows)$scaleNote, " +
        s"partitions=$numParts, iterations=$benchmarkIters, " +
        s"phases=${benchmarkPhases.toSeq.sorted.mkString(",")}, " +
        s"includeV2Modes=$includeV2Modes, modes=${modes.map(_.label).mkString(" | ")}")

    // === Benchmark 1: write-path overhead (cache build) ===
    // Measures the eager-vs-lazy serialize cost and the optional partition-stats overhead.
    if (phaseEnabled("build")) {
      val buildBench =
        new Benchmark(
          s"table cache build ($numCols cols, $numParts parts)",
          numRows,
          output = output)
      modes.foreach {
        mode =>
          buildBench.addCase(mode.label, benchmarkIters) {
            _ =>
              spark.catalog.clearCache()
              buildCache(mode).unpersist()
          }
      }
      buildBench.run()
      spark.catalog.clearCache()
    }

    // Read benchmarks: build ONE mode's cache at a time, measure all read phases against it, then
    // unpersist before moving to the next mode. Holding a single cache (instead of all `modes` at
    // once) caps peak heap to ~one cache, so large datasets (e.g. 100M rows) fit where 4
    // simultaneous caches would not. Results are collated into a combined per-phase table after.
    import org.apache.spark.sql.functions._
    def aggAll(df: DataFrame): Unit =
      df.agg(
        sum("c0"),
        sum("c1"),
        sum(length(col("c2"))),
        sum(length(col("c3"))),
        sum("c4"),
        sum("c5"),
        sum("c6"),
        sum("c7"),
        sum(length(col("c8"))),
        sum("c9"),
        sum(length(col("c10"))),
        sum("c11"),
        sum("c12"),
        sum(length(col("c13"))),
        sum("c14"),
        sum("c15")
      ).noop()
    val readPhases: Seq[(String, String, DataFrame => Unit)] = Seq(
      (
        "read1",
        s"table cache read: 1/$numCols columns, sum(c0)",
        (df: DataFrame) => df.agg(sum("c0")).noop()),
      (
        "read4",
        s"table cache read: 4/$numCols columns, group+agg",
        (df: DataFrame) =>
          df.groupBy((col("c0") % 1000).as("g")).agg(
            sum("c1"),
            sum(length(col("c2"))),
            avg("c4")).noop()),
      (
        "readall",
        s"table cache read: all $numCols columns (no skip benefit)",
        (df: DataFrame) => aggAll(df)),
      (
        "filter",
        s"table cache read: filter + 2/$numCols columns (batch skip + lazy)",
        (df: DataFrame) => df.where(s"c0 = $filterValue").select("c0", "c1").noop())
    ).filter { case (key, _, _) => phaseEnabled(key) }

    if (readPhases.nonEmpty) {
      val footprints = scala.collection.mutable.ArrayBuffer.empty[(String, CacheFootprint)]
      val results = scala.collection.mutable.LinkedHashMap
        .empty[String, scala.collection.mutable.ArrayBuffer[(String, Double, Double)]]
      readPhases.foreach {
        case (_, title, _) => results(title) = scala.collection.mutable.ArrayBuffer.empty
      }
      modes.foreach {
        mode =>
          val ct = buildCacheWithFootprint(mode)
          footprints += mode.label -> ct.footprint
          try {
            readPhases.foreach {
              case (_, title, op) =>
                val (best, avg) = timePhase(ct.df, op)
                results(title) += ((mode.label, best, avg))
            }
          } finally {
            ct.cachedDf.unpersist(blocking = true)
          }
      }
      printCacheFootprint(footprints.toSeq)
      readPhases.foreach { case (_, title, _) => printResultTable(title, results(title).toSeq) }
    }

    spark.catalog.clearCache()
  }

  // Manual timing (one cache alive at a time, so the Spark Benchmark group harness is not usable).
  // First two untimed runs warm up JIT and trigger the lazy cache load; then `benchmarkIters` timed
  // runs report best + average wall time in ms.
  private def timePhase(df: DataFrame, op: DataFrame => Unit): (Double, Double) = {
    op(df)
    op(df)
    var best = Double.MaxValue
    var total = 0.0
    var i = 0
    while (i < benchmarkIters) {
      val t0 = System.nanoTime()
      op(df)
      val ms = (System.nanoTime() - t0) / 1e6
      if (ms < best) best = ms
      total += ms
      i += 1
    }
    (best, total / benchmarkIters)
  }

  // Relative is baseline(first mode) avg / this avg, so a faster mode shows > 1.0X (matches the
  // convention of org.apache.spark.benchmark.Benchmark).
  private def printResultTable(title: String, rows: Seq[(String, Double, Double)]): Unit = {
    outputLine()
    outputLine(title)
    outputLine("-" * math.min(title.length, 80))
    outputLine(f"${"mode"}%-28s ${"best(ms)"}%12s ${"avg(ms)"}%12s ${"relative"}%10s")
    val baselineAvg = rows.headOption.map(_._3).getOrElse(1.0)
    rows.foreach {
      case (label, best, avg) =>
        val rel = if (avg > 0) baselineAvg / avg else 0.0
        outputLine(f"$label%-28s $best%12.1f $avg%12.1f $rel%9.2fX")
    }
    outputLine()
  }
}

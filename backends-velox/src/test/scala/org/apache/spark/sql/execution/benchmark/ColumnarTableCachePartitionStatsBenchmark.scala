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
import org.apache.spark.storage.StorageLevel

/**
 * Benchmark to measure write/read overhead and pruning benefit of partition stats in columnar table
 * cache. To run this benchmark:
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
  }
}

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
import org.apache.spark.sql.functions.{col, repeat}
import org.apache.spark.storage.StorageLevel

import java.io.File

/**
 * Benchmark to measure performance for columnar table cache across source formats and schema
 * shapes. To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
 * }}}
 *
 * Matrix:
 *   - sources : parquet (velox-native columnar), csv, json (row-based fallback per GLUTEN-3456)
 *   - shapes : numeric (5 cols mixed), wide-string (16 x ~200 char) -- the GLUTEN-3488 hazard
 *   - cases : count / column pruning / filter for numeric; count only for wide-string
 */
object ColumnarTableCacheBenchmark extends SqlBasedBenchmark {
  private val numRows = 10L * 1000 * 1000
  private val wideStringRows = 2L * 1000 * 1000
  private val wideStringCols = 16
  private val wideStringLen = 200

  private val sources = Seq("parquet", "csv", "json")

  private def numericDf(): DataFrame = {
    spark
      .range(numRows)
      .selectExpr(
        "cast(id as int) as c0",
        "cast(id as double) as c1",
        "id as c2",
        "cast(id as string) as c3",
        "uuid() as c4")
  }

  private def wideStringDf(): DataFrame = {
    val cols = (0 until wideStringCols).map(
      i =>
        repeat(col("id").cast("string"), wideStringLen / 8 + 1).as(s"c$i"))
    spark.range(wideStringRows).select(cols: _*)
  }

  private def writeSource(df: DataFrame, source: String, dir: File): String = {
    val path = new File(dir, source).getCanonicalPath
    val writer = df.write.format(source)
    val w = if (source == "csv") writer.option("header", "true") else writer
    w.save(path)
    path
  }

  private def readSource(source: String, path: String): DataFrame = {
    val reader = spark.read.format(source)
    val r = if (source == "csv") {
      reader.option("header", "true").option("inferSchema", "true")
    } else reader
    r.load(path)
  }

  private def doBenchmark(name: String, cardinality: Long)(f: => Unit): Unit = {
    val benchmark = new Benchmark(name, cardinality, output = output)
    val flag =
      if (
        spark.sessionState.conf
          .getConfString(GlutenConfig.COLUMNAR_TABLE_CACHE_ENABLED.key)
          .toBoolean
      ) {
        "enable"
      } else {
        "disable"
      }
    benchmark.addCase(s"$flag columnar table cache", 3)(_ => f)
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    withTempPath {
      f =>
        // --- numeric workload: write once per source, run 3 cases ---
        val numericPaths =
          sources.map(src => src -> writeSource(numericDf(), src, new File(f, "numeric"))).toMap

        sources.foreach {
          src =>
            val path = numericPaths(src)
            doBenchmark(s"numeric/$src table cache count", numRows) {
              readSource(src, path).persist(StorageLevel.MEMORY_ONLY).count()
              spark.catalog.clearCache()
            }
            doBenchmark(s"numeric/$src table cache column pruning", numRows) {
              val cached = readSource(src, path).persist(StorageLevel.MEMORY_ONLY)
              cached.select("c1", "c2").noop()
              cached.select("c0", "c3").noop()
              spark.catalog.clearCache()
            }
            doBenchmark(s"numeric/$src table cache filter", numRows) {
              val cached = readSource(src, path).persist(StorageLevel.MEMORY_ONLY)
              cached.where("c1 % 100 > 10").noop()
              cached.where("c1 % 100 > 20").noop()
              spark.catalog.clearCache()
            }
        }

        // --- wide-string workload: GLUTEN-3488 hazard reproducer, count only ---
        val wsPaths = sources.map {
          src => src -> writeSource(wideStringDf(), src, new File(f, "widestring"))
        }.toMap

        sources.foreach {
          src =>
            val path = wsPaths(src)
            doBenchmark(s"wide-string/$src table cache count", wideStringRows) {
              readSource(src, path).persist(StorageLevel.MEMORY_ONLY).count()
              spark.catalog.clearCache()
            }
        }
    }
  }
}

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
import org.apache.spark.storage.StorageLevel

/**
 * Benchmark to measure performance for columnar table cache. To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
 * }}}
 */
object ColumnarTableCacheBenchmark extends SqlBasedBenchmark {
  private val numRows = 20L * 1000 * 1000

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
        spark
          .range(numRows)
          .selectExpr(
            "cast(id as int) as c0",
            "cast(id as double) as c1",
            "id as c2",
            "cast(id as string) as c3",
            "uuid() as c4")
          .write
          .parquet(f.getCanonicalPath)

        doBenchmark("table cache count", numRows) {
          spark.read.parquet(f.getCanonicalPath).persist(StorageLevel.MEMORY_ONLY).count()
          spark.catalog.clearCache()
        }

        doBenchmark("table cache column pruning", numRows) {
          val cached = spark.read
            .parquet(f.getCanonicalPath)
            .persist(StorageLevel.MEMORY_ONLY)
          cached.select("c1", "c2").noop()
          cached.select("c0", "c3").noop()
          spark.catalog.clearCache()
        }

        doBenchmark("table cache filter", numRows) {
          val cached = spark.read
            .parquet(f.getCanonicalPath)
            .persist(StorageLevel.MEMORY_ONLY)
          cached.where("c1 % 100 > 10").noop()
          cached.where("c1 % 100 > 20").noop()
          spark.catalog.clearCache()
        }
    }
  }
}

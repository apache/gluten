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

import org.apache.gluten.config.VeloxConfig

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.internal.SQLConf

/** Benchmark to measure performance for BHJ build once per executor. */
object VeloxBroadcastBuildOnceBenchmark extends SqlBasedBenchmark {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val numRows = 5 * 1000 * 1000
    val broadcastRows = 1000 * 1000

    withTempPath {
      f =>
        val path = f.getCanonicalPath
        val probePath = s"$path/probe"
        val buildPath = s"$path/build"

        // Generate probe table with many partitions to simulate many tasks
        spark
          .range(numRows)
          .repartition(100)
          .selectExpr("id as k1", "id as v1")
          .write
          .parquet(probePath)

        // Generate build table
        spark
          .range(broadcastRows)
          .selectExpr("id as k2", "id as v2")
          .write
          .parquet(buildPath)

        spark.read.parquet(probePath).createOrReplaceTempView("probe")
        spark.read.parquet(buildPath).createOrReplaceTempView("build")

        val query = "SELECT /*+ BROADCAST(build) */ count(*) FROM probe JOIN build ON k1 = k2"

        val benchmark = new Benchmark("BHJ Build Once Benchmark", numRows, output = output)

        // Warm up
        spark.sql(query).collect()

        benchmark.addCase("Build once per executor enabled=false", 3) {
          _ =>
            withSQLConf(
              VeloxConfig.VELOX_BROADCAST_BUILD_HASHTABLE_ONCE_PER_EXECUTOR.key -> "false",
              SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "200MB"
            ) {
              spark.sql(query).collect()
            }
        }

        benchmark.addCase("Build once per executor enabled=true", 3) {
          _ =>
            withSQLConf(
              VeloxConfig.VELOX_BROADCAST_BUILD_HASHTABLE_ONCE_PER_EXECUTOR.key -> "true",
              SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "200MB"
            ) {
              spark.sql(query).collect()
            }
        }

        benchmark.run()
    }
  }
}

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
import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmark to measure performance for streaming aggregate. To run this benchmark:
 * {{{
 *    bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
 * }}}
 */
object StreamingAggregateBenchmark extends SqlBasedBenchmark {
  private val numRows = {
    spark.sparkContext.conf.getLong("spark.gluten.benchmark.rows", 8 * 1000 * 1000)
  }

  private val mode = {
    spark.sparkContext.conf.getLong("spark.gluten.benchmark.remainder", 4 * 1000 * 1000)
  }

  private def doBenchmark(): Unit = {
    val benchmark = new Benchmark("streaming aggregate", numRows, output = output)

    val query =
      """
        |SELECT c1, count(*), sum(c2) FROM (
        |SELECT t1.c1, t2.c2 FROM t t1 JOIN t t2 ON t1.c1 = t2.c1
        |)
        |GROUP BY c1
        |""".stripMargin
    benchmark.addCase(s"Enable streaming aggregate", 3) {
      _ =>
        withSQLConf(
          GlutenConfig.COLUMNAR_PREFER_STREAMING_AGGREGATE.key -> "true",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false"
        ) {
          spark.sql(query).noop()
        }
    }

    benchmark.addCase(s"Disable streaming aggregate", 3) {
      _ =>
        withSQLConf(
          GlutenConfig.COLUMNAR_PREFER_STREAMING_AGGREGATE.key -> "false",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false"
        ) {
          spark.sql(query).noop()
        }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    spark
      .range(numRows)
      .selectExpr(s"id % $mode as c1", "id as c2")
      .write
      .saveAsTable("t")

    try {
      doBenchmark()
    } finally {
      spark.sql("DROP TABLE t")
    }
  }
}

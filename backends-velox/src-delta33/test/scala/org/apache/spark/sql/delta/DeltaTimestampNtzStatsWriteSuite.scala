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
package org.apache.spark.sql.delta

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Velox cannot offload an aggregation over TIMESTAMP_NTZ, so collecting Delta write statistics on a
 * TIMESTAMP_NTZ column used to fail with `ClassCastException: ProjectExec cannot be cast to
 * WholeStageTransformer` inside GlutenDeltaJobStatsTracker, which assumed the local stats
 * aggregation is always offloaded to Velox (see apache/gluten#12538).
 *
 * GlutenDeltaJobStatsTracker now checks -- on the executor, where the plan is actually built --
 * whether the whole statistics plan was offloaded, and falls back to row-based statistics
 * collection when it was not. The native Delta write path (enabled by DeltaSQLCommandTest) still
 * runs, so without the fallback these writes reach GlutenDeltaJobStatsTracker and fail; with it the
 * write succeeds AND Delta min/max statistics are still produced.
 */
class DeltaTimestampNtzStatsWriteSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  import testImplicits._

  private def collectedStats(path: String): Seq[String] =
    DeltaLog.forTable(spark, path).update().allFiles.collect().flatMap(f => Option(f.stats)).toSeq

  test("collect stats on a top-level TIMESTAMP_NTZ column falls back instead of failing") {
    withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "true") {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          spark
            .range(3)
            .selectExpr("id", "make_timestamp_ntz(2024, 1, cast(id + 1 AS int), 10, 0, 0) AS ts")
            .write
            .format("delta")
            .mode("overwrite")
            .save(path)

          // Without the fallback the write above throws ClassCastException; reading the data back
          // verifies the native write succeeded and the values are intact.
          checkAnswer(
            spark.read.format("delta").load(path).selectExpr("id", "extract(DAY FROM ts)"),
            Seq(Row(0, 1), Row(1, 2), Row(2, 3)))

          // The row-based fallback must still produce Delta statistics -- including min/max on the
          // TIMESTAMP_NTZ column -- rather than silently skipping them.
          val stats = collectedStats(path)
          assert(stats.nonEmpty, "Expected the Delta write to produce file statistics")
          assert(
            stats.forall(_.contains("numRecords")),
            s"Every written file should carry a numRecords stat, got: $stats")
          val parsed = spark.read.json(stats.toDS())
          val Row(minTs: String, maxTs: String) =
            parsed.selectExpr("min(minValues.ts) AS minTs", "max(maxValues.ts) AS maxTs").head()
          assert(minTs.startsWith("2024-01-01"), s"unexpected min stat for ts: $minTs")
          assert(maxTs.startsWith("2024-01-03"), s"unexpected max stat for ts: $maxTs")
      }
    }
  }

  test("collect stats on a TIMESTAMP_NTZ column nested in a struct falls back instead of failing") {
    withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "true") {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          spark
            .range(2)
            .selectExpr(
              "id",
              "named_struct('ts', make_timestamp_ntz(2024, 1, cast(id + 1 AS int), 10, 0, 0)) AS s")
            .write
            .format("delta")
            .mode("overwrite")
            .save(path)

          checkAnswer(
            spark.read.format("delta").load(path).selectExpr("id", "extract(DAY FROM s.ts)"),
            Seq(Row(0, 1), Row(1, 2)))

          // Delta collects statistics on nested struct fields too, so min/max on the nested
          // TIMESTAMP_NTZ must be produced by the fallback.
          val stats = collectedStats(path)
          assert(stats.nonEmpty, "Expected the Delta write to produce file statistics")
          val parsed = spark.read.json(stats.toDS())
          val Row(minTs: String, maxTs: String) =
            parsed
              .selectExpr("min(minValues.s.ts) AS minTs", "max(maxValues.s.ts) AS maxTs")
              .head()
          assert(minTs.startsWith("2024-01-01"), s"unexpected min stat for s.ts: $minTs")
          assert(maxTs.startsWith("2024-01-02"), s"unexpected max stat for s.ts: $maxTs")
      }
    }
  }
}

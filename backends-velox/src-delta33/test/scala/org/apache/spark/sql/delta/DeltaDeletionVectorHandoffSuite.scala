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

import org.apache.gluten.config.VeloxDeltaConfig
import org.apache.gluten.execution.DeltaScanTransformer

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.tags.ExtendedSQLTest
import org.apache.spark.util.SparkVersionUtil

import org.apache.hadoop.fs.Path

import java.io.File

@ExtendedSQLTest
class DeltaDeletionVectorHandoffSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLTestUtils
  with DeltaSQLCommandTest
  with AdaptiveSparkPlanHelper {

  import testImplicits._

  private def containsNativeDeltaScan(plan: SparkPlan): Boolean = {
    collectWithSubqueries(plan) { case scan: DeltaScanTransformer => scan }.nonEmpty
  }

  private def captureDeletePlans(path: String, predicate: String): Seq[SparkPlan] = {
    DeltaTestUtils.withAllPlansCaptured(spark) {
      spark.sql(s"DELETE FROM delta.`$path` WHERE $predicate").collect()
    }.map(_.executedPlan)
  }

  private def activeDvCardinality(path: String): Long = {
    val log = DeltaLog.forTable(spark, new Path(path))
    log.update().allFiles.collect().flatMap(
      file => Option(file.deletionVector).map(_.cardinality)).sum
  }

  private def writeDvTable(path: String, rows: Seq[(Int, String)]): Unit = {
    rows
      .toDF("id", "value")
      .coalesce(1)
      .write
      .format("delta")
      .save(path)
    spark.sql(
      s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)")
  }

  test("Spark 3.5 Delta DV scan handoff should filter deleted rows") {
    withTempDir {
      tempDir =>
        val path = tempDir.getCanonicalPath
        writeDvTable(path, Seq((1, "a"), (2, "b"), (3, "c"), (4, "d")))
        spark.sql(s"DELETE FROM delta.`$path` WHERE id IN (3, 4)")

        val log = DeltaLog.forTable(spark, new Path(path))
        val addFileWithDv = log.update().allFiles.collect().find(_.deletionVector != null)
        assert(addFileWithDv.nonEmpty)

        val dataFile = addFileWithDv.get
        assert(dataFile.deletionVector.cardinality == 2L)

        val df = spark.read.format("delta").load(path)
        val executedPlan = df.queryExecution.executedPlan
        val nativeScans =
          collectWithSubqueries(executedPlan) { case scan: DeltaScanTransformer => scan }
        assert(nativeScans.nonEmpty)
        val planText = executedPlan.toString()
        assert(!planText.contains("__delta_internal_is_row_deleted"))
        assert(!planText.contains("__delta_internal_row_index"))
        checkAnswer(df, Seq((1, "a"), (2, "b")).toDF())

        val metrics = nativeScans.head.metrics
        assert(metrics("dvDescriptorCount").value == 1L)
        assert(metrics("dvPayloadReadAttempts").value == 1L)
        assert(metrics("dvPayloadReadBytes").value > 0L)
        assert(metrics("dvPayloadReadTime").value > 0L)
    }
  }

  test("Delta metadata row-index predicate should not be stripped from a native scan") {
    assume(SparkVersionUtil.gteSpark35, "metadata row index is available in Spark 3.5+")
    withTempDir {
      tempDir =>
        val path = tempDir.getCanonicalPath
        writeDvTable(path, Seq((1, "a"), (2, "b"), (3, "c"), (4, "d")))

        val df = spark.sql(
          s"SELECT id, _metadata.row_index AS row_index FROM delta.`$path` " +
            "WHERE _metadata.row_index = 2")
        val rows = df.collect()
        val executedPlan = df.queryExecution.executedPlan
        val planText = executedPlan.treeString
        assert(containsNativeDeltaScan(executedPlan), planText)
        assert(rows.length === 1, planText)
        assert(rows.head.getLong(1) === 2L, planText)
    }
  }

  Seq(true, false).foreach {
    useMetadataRowIndex =>
      test(
        "Delta DV DELETE should write correct deletion vectors, " +
          s"metadata row index=$useMetadataRowIndex") {
        assume(SparkVersionUtil.gteSpark35, "DV DML coverage targets Spark 3.5+")
        withTempDir {
          tempDir =>
            val path = tempDir.getCanonicalPath
            writeDvTable(path, Seq((1, "a"), (2, "b"), (3, "c"), (4, "d")))

            withSQLConf(
              DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key ->
                useMetadataRowIndex.toString) {
              val executedPlans = captureDeletePlans(path, "id IN (3, 4)")
              val planText = executedPlans.map(_.treeString).mkString("\n\n")
              // With the metadata row index, the DML target scan offloads like any other DV
              // scan; without it, Delta relies on Spark's injected row-index filter column and
              // the scan stays on Spark.
              assert(
                executedPlans.exists(containsNativeDeltaScan) === useMetadataRowIndex,
                planText)

              assert(activeDvCardinality(path) === 2L)
              checkAnswer(spark.read.format("delta").load(path), Seq((1, "a"), (2, "b")).toDF())
            }
        }
      }
  }

  test("Delta DV repeated DELETE over an existing DV should accumulate deleted rows") {
    assume(SparkVersionUtil.gteSpark35, "DV DML coverage targets Spark 3.5+")
    withTempDir {
      tempDir =>
        val path = new File(tempDir, "delta table with spaces").getCanonicalPath
        writeDvTable(path, Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (6, "f")))

        withSQLConf(DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key -> "true") {
          // Delete the LEADING rows first: after DV {0, 1} masks them, every surviving row's
          // absolute row index differs from its post-mask position. A scan that renumbered row
          // indexes after applying the existing DV would emit {0, 1} for the second DELETE
          // instead of {2, 3}, failing both the cardinality and the result checks below.
          val firstDeletePlans = captureDeletePlans(path, "id IN (1, 2)")
          assert(
            firstDeletePlans.exists(containsNativeDeltaScan),
            firstDeletePlans.map(_.treeString).mkString("\n\n"))
          assert(activeDvCardinality(path) === 2L)

          // The second DELETE scans files that already carry a DV and must merge into it.
          val secondDeletePlans = captureDeletePlans(path, "id IN (3, 4)")
          assert(
            secondDeletePlans.exists(containsNativeDeltaScan),
            secondDeletePlans.map(_.treeString).mkString("\n\n"))
          assert(activeDvCardinality(path) === 4L)

          checkAnswer(spark.read.format("delta").load(path), Seq((5, "e"), (6, "f")).toDF())
        }
    }
  }

  // MERGE puts joins between the target scan and Delta's BitmapAggregator. With a shuffle join
  // the scan lands in its own AQE query stage, so this covers target-scan offload both with and
  // without the rest of the DML plan visible in the same stage.
  Seq(true, false).foreach {
    broadcastJoin =>
      test(s"Delta DV MERGE should write correct deletion vectors, broadcast join=$broadcastJoin") {
        assume(SparkVersionUtil.gteSpark35, "DV DML coverage targets Spark 3.5+")
        withTempDir {
          tempDir =>
            val path = tempDir.getCanonicalPath
            writeDvTable(path, Seq((1, "a"), (2, "b"), (3, "c"), (4, "d")))

            withTempView("merge_source") {
              Seq((3, "c2"), (4, "d2")).toDF("id", "value").createOrReplaceTempView("merge_source")

              withSQLConf(
                SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key ->
                  (if (broadcastJoin) "10485760" else "-1"),
                DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key -> "true"
              ) {
                val executedPlans = DeltaTestUtils.withAllPlansCaptured(spark) {
                  spark
                    .sql(s"""MERGE INTO delta.`$path` AS t
                            |USING merge_source AS s
                            |ON t.id = s.id
                            |WHEN MATCHED THEN DELETE""".stripMargin)
                    .collect()
                }.map(_.executedPlan)
                assert(
                  executedPlans.exists(containsNativeDeltaScan),
                  executedPlans.map(_.treeString).mkString("\n\n"))
              }

              val log = DeltaLog.forTable(spark, new Path(path))
              assert(log.update().allFiles.collect().exists(_.deletionVector != null))
              checkAnswer(
                spark.read.format("delta").load(path),
                Seq((1, "a"), (2, "b")).toDF())
            }
        }
      }
  }

  test("Delta DV DML row-index scan should stay on Spark when disabled") {
    assume(SparkVersionUtil.gteSpark35, "DV DML coverage targets Spark 3.5+")
    withTempDir {
      tempDir =>
        val path = tempDir.getCanonicalPath
        writeDvTable(path, Seq((1, "a"), (2, "b"), (3, "c"), (4, "d")))

        withSQLConf(
          DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key -> "true",
          VeloxDeltaConfig.ENABLE_NATIVE_DML_ROW_INDEX_SCAN.key -> "false") {
          val executedPlans = captureDeletePlans(path, "id IN (3, 4)")
          assert(
            !executedPlans.exists(containsNativeDeltaScan),
            executedPlans.map(_.treeString).mkString("\n\n"))
          assert(activeDvCardinality(path) === 2L)

          // The fallback is scoped to the DML target scan: a plain read of the same table keeps
          // offloading even while the config is off.
          val df = spark.read.format("delta").load(path)
          assert(containsNativeDeltaScan(df.queryExecution.executedPlan))
          checkAnswer(df, Seq((1, "a"), (2, "b")).toDF())
        }

        // The config is read per query, so re-enabling in the same session restores DML offload.
        withSQLConf(DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key -> "true") {
          val executedPlans = captureDeletePlans(path, "id = 2")
          assert(
            executedPlans.exists(containsNativeDeltaScan),
            executedPlans.map(_.treeString).mkString("\n\n"))
          assert(activeDvCardinality(path) === 3L)
          checkAnswer(spark.read.format("delta").load(path), Seq((1, "a")).toDF())
        }
    }
  }

  test("Delta non-DV DML should offload with a user column named row_index when disabled") {
    assume(SparkVersionUtil.gteSpark35, "DV DML coverage targets Spark 3.5+")
    withTempDir {
      tempDir =>
        val path = tempDir.getCanonicalPath
        // Deletion vectors are deliberately left off: this DELETE rewrites whole files and never
        // reads a generated row index. The scoped fallback must not claim it merely because the
        // table has a user column called row_index -- that name is only Delta's row index when it
        // appears inside the file metadata struct.
        Seq((1, "a", 10L), (2, "b", 20L), (3, "c", 30L))
          .toDF("id", "value", "row_index")
          .coalesce(1)
          .write
          .format("delta")
          .save(path)

        withSQLConf(VeloxDeltaConfig.ENABLE_NATIVE_DML_ROW_INDEX_SCAN.key -> "false") {
          val executedPlans = captureDeletePlans(path, "id = 3")
          assert(
            executedPlans.exists(containsNativeDeltaScan),
            executedPlans.map(_.treeString).mkString("\n\n"))
        }

        checkAnswer(
          spark.read.format("delta").load(path),
          Seq((1, "a", 10L), (2, "b", 20L)).toDF())
    }
  }
}

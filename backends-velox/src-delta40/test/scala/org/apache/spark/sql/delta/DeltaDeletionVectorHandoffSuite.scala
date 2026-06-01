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

import org.apache.gluten.execution.{DeltaScanTransformer, FilterExecTransformerBase, ProjectExecTransformerBase}
import org.apache.gluten.extension.DeltaDeletionVectorDmlUtils
import org.apache.gluten.extension.columnar.FallbackTags

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, ProjectExec, SparkPlan}
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
  with DeltaSQLCommandTest {

  import testImplicits._

  private val DmlFallbackReason = "fallback Delta DV DML row-index scan"

  private def containsDmlFallbackScan(plan: SparkPlan): Boolean = {
    plan.exists {
      case scan: FileSourceScanExec =>
        DeltaDeletionVectorDmlUtils.isDeletionVectorDmlRowIndexScan(scan) &&
        FallbackTags.getOption(scan).exists(_.reason().contains(DmlFallbackReason))
      case _ => false
    }
  }

  private def hasSparkParentOverDmlFallbackScan(plan: SparkPlan): Boolean = {
    plan.exists {
      case ProjectExec(_, child) if containsDmlFallbackScan(child) => true
      case FilterExec(_, child) if containsDmlFallbackScan(child) => true
      case _ => false
    }
  }

  private def hasNativeParentOverDmlFallbackScan(plan: SparkPlan): Boolean = {
    plan.exists {
      case project: ProjectExecTransformerBase if containsDmlFallbackScan(project.child) => true
      case filter: FilterExecTransformerBase if containsDmlFallbackScan(filter.child) => true
      case _ => false
    }
  }

  private def captureDeletePlans(
      path: String,
      predicate: String,
      useMetadataRowIndex: Boolean): Seq[SparkPlan] = {
    var executedPlans: Seq[SparkPlan] = Seq.empty
    withSQLConf(
      DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key ->
        useMetadataRowIndex.toString,
      "spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite" -> "false",
      "spark.gluten.sql.delta.enableNativeDmlRowIndexScan" -> "false"
    ) {
      executedPlans = DeltaTestUtils.withAllPlansCaptured(spark) {
        spark.sql(s"DELETE FROM delta.`$path` WHERE $predicate").collect()
      }.map(_.executedPlan)
    }
    executedPlans
  }

  private def assertSparkDmlFallback(executedPlans: Seq[SparkPlan]): Unit = {
    val planText = executedPlans.map(_.treeString).mkString("\n\n")
    assert(executedPlans.exists(containsDmlFallbackScan), planText)
    assert(executedPlans.exists(hasSparkParentOverDmlFallbackScan), planText)
    assert(!executedPlans.exists(hasNativeParentOverDmlFallbackScan), planText)
  }

  private def activeDvCardinality(path: String): Long = {
    val log = DeltaLog.forTable(spark, new Path(path))
    log.update().allFiles.collect().flatMap(
      file => Option(file.deletionVector).map(_.cardinality)).sum
  }

  test("Spark 4 Delta DV scan should fall back when metadata row index is disabled") {
    withTempDir {
      tempDir =>
        val path = tempDir.getCanonicalPath
        Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"))
          .toDF("id", "value")
          .coalesce(1)
          .write
          .format("delta")
          .save(path)

        spark.sql(
          s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)")
        spark.sql(s"DELETE FROM delta.`$path` WHERE id IN (3, 4)")

        val log = DeltaLog.forTable(spark, new Path(path))
        assert(log.update().allFiles.collect().exists(_.deletionVector != null))

        // This covers scan behavior over an existing DV. Keep the no-metadata-row-index
        // path on Spark until the native path can prove the same contract for DML DVs.
        withSQLConf(DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key -> "false") {
          val df = spark.read.format("delta").load(path)
          val executedPlan = df.queryExecution.executedPlan
          assert(executedPlan.collect { case _: DeltaScanTransformer => true }.isEmpty)
          checkAnswer(df, Seq((1, "a"), (2, "b")).toDF())
        }
    }
  }

  Seq(true, false).foreach {
    useMetadataRowIndex =>
      test(
        "Delta DV DML row-index scan should fall back with Spark project/filter, " +
          s"metadata row index=$useMetadataRowIndex") {
        assume(SparkVersionUtil.gteSpark35, "DML row-index scan fallback is Spark 3.5+ coverage")
        withTempDir {
          tempDir =>
            val path = tempDir.getCanonicalPath
            Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"))
              .toDF("id", "value")
              .coalesce(1)
              .write
              .format("delta")
              .save(path)

            spark.sql(
              s"ALTER TABLE delta.`$path` SET TBLPROPERTIES " +
                "('delta.enableDeletionVectors' = true)")

            var executedPlans: Seq[SparkPlan] = Seq.empty
            withSQLConf(
              DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key ->
                useMetadataRowIndex.toString,
              "spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite" -> "false",
              "spark.gluten.sql.delta.enableNativeDmlRowIndexScan" -> "false"
            ) {
              executedPlans = DeltaTestUtils.withAllPlansCaptured(spark) {
                spark.sql(s"DELETE FROM delta.`$path` WHERE id IN (3, 4)").collect()
              }.map(_.executedPlan)
            }
            val planText = executedPlans.map(_.treeString).mkString("\n\n")
            assert(executedPlans.exists(containsDmlFallbackScan), planText)
            assert(executedPlans.exists(hasSparkParentOverDmlFallbackScan), planText)
            assert(!executedPlans.exists(hasNativeParentOverDmlFallbackScan), planText)

            val log = DeltaLog.forTable(spark, new Path(path))
            assert(log.update().allFiles.collect().exists(_.deletionVector != null))
            checkAnswer(spark.read.format("delta").load(path), Seq((1, "a"), (2, "b")).toDF())
        }
      }
  }

  test("Spark 4 Delta DV scan handoff should filter deleted rows") {
    withTempDir {
      tempDir =>
        val path = tempDir.getCanonicalPath
        Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"))
          .toDF("id", "value")
          .coalesce(1)
          .write
          .format("delta")
          .save(path)

        spark.sql(
          s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)")
        spark.sql(s"DELETE FROM delta.`$path` WHERE id IN (3, 4)")

        val log = DeltaLog.forTable(spark, new Path(path))
        val addFileWithDv = log.update().allFiles.collect().find(_.deletionVector != null)
        assert(addFileWithDv.nonEmpty)

        val dataFile = addFileWithDv.get
        assert(dataFile.deletionVector.cardinality == 2L)

        val df = spark.read.format("delta").load(path)
        val executedPlan = df.queryExecution.executedPlan
        assert(executedPlan.collect { case _: DeltaScanTransformer => true }.nonEmpty)
        val planText = executedPlan.toString()
        assert(!planText.contains("__delta_internal_is_row_deleted"))
        assert(!planText.contains("__delta_internal_row_index"))
        checkAnswer(df, Seq((1, "a"), (2, "b")).toDF())
    }
  }

  test("Delta DV DML row-index scan should fall back when updating an existing DV") {
    assume(SparkVersionUtil.gteSpark35, "DML row-index scan fallback is Spark 3.5+ coverage")
    withTempDir {
      tempDir =>
        val path = new File(tempDir, "delta table with spaces").getCanonicalPath
        Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (6, "f"))
          .toDF("id", "value")
          .coalesce(1)
          .write
          .format("delta")
          .save(path)

        spark.sql(
          s"ALTER TABLE delta.`$path` SET TBLPROPERTIES " +
            "('delta.enableDeletionVectors' = true)")

        assertSparkDmlFallback(captureDeletePlans(path, "id IN (5, 6)", useMetadataRowIndex = true))
        assert(activeDvCardinality(path) === 2L)

        assertSparkDmlFallback(captureDeletePlans(path, "id IN (3, 4)", useMetadataRowIndex = true))
        assert(activeDvCardinality(path) === 4L)

        checkAnswer(spark.read.format("delta").load(path), Seq((1, "a"), (2, "b")).toDF())
    }
  }
}

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
import org.apache.gluten.execution.{DeltaScanTransformer, FilterExecTransformerBase, ProjectExecTransformerBase}
import org.apache.gluten.extension.DeltaDeletionVectorDmlUtils
import org.apache.gluten.extension.columnar.FallbackTags

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
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

  private val DmlFallbackReason = "fallback Delta DV DML row-index scan"

  private def containsDmlFallbackScan(plan: SparkPlan): Boolean = {
    collectWithSubqueries(plan) {
      case scan: FileSourceScanExec
          if DeltaDeletionVectorDmlUtils.isDeletionVectorDmlRowIndexScan(scan) &&
            FallbackTags
              .getOption(scan)
              .exists(_.reason().contains(DmlFallbackReason)) =>
        scan
    }.nonEmpty
  }

  private def hasSparkParentOverDmlFallbackScan(plan: SparkPlan): Boolean = {
    collectWithSubqueries(plan) {
      case project @ ProjectExec(_, child) if isDmlFallbackSubtree(child) => project
      case filter @ FilterExec(_, child) if isDmlFallbackSubtree(child) => filter
    }.nonEmpty
  }

  private def hasNativeParentOverDmlFallbackScan(plan: SparkPlan): Boolean = {
    collectWithSubqueries(plan) {
      case project: ProjectExecTransformerBase if isDmlFallbackSubtree(project.child) => project
      case filter: FilterExecTransformerBase if isDmlFallbackSubtree(filter.child) => filter
    }.nonEmpty
  }

  private def containsNativeDeltaScan(plan: SparkPlan): Boolean = {
    collectWithSubqueries(plan) { case scan: DeltaScanTransformer => scan }.nonEmpty
  }

  private def isDmlFallbackSubtree(plan: SparkPlan): Boolean = plan match {
    case scan: FileSourceScanExec => containsDmlFallbackScan(scan)
    case ProjectExec(_, child) => isDmlFallbackSubtree(child)
    case FilterExec(_, child) => isDmlFallbackSubtree(child)
    case project: ProjectExecTransformerBase => isDmlFallbackSubtree(project.child)
    case filter: FilterExecTransformerBase => isDmlFallbackSubtree(filter.child)
    case _ => false
  }

  private def captureDeletePlans(
      path: String,
      predicate: String,
      useMetadataRowIndex: Boolean): Seq[SparkPlan] = {
    var executedPlans: Seq[SparkPlan] = Seq.empty
    withSQLConf(
      DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key ->
        useMetadataRowIndex.toString,
      VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> "false",
      VeloxDeltaConfig.ENABLE_NATIVE_DML_ROW_INDEX_SCAN.key -> "false"
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

  private def assertReadPlanAfterDmlFallback(path: String, useMetadataRowIndex: Boolean): Unit = {
    withSQLConf(
      DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key -> useMetadataRowIndex.toString) {
      val df = spark.read.format("delta").load(path)
      val executedPlan = df.queryExecution.executedPlan
      val planText = executedPlan.treeString
      if (useMetadataRowIndex) {
        assert(containsNativeDeltaScan(executedPlan), planText)
        assert(!planText.contains(DmlFallbackReason), planText)
      } else {
        assert(!containsNativeDeltaScan(executedPlan), planText)
      }
      checkAnswer(df, Seq((1, "a"), (2, "b")).toDF())
    }
  }

  private def activeDvCardinality(path: String): Long = {
    val log = DeltaLog.forTable(spark, new Path(path))
    log.update().allFiles.collect().flatMap(
      file => Option(file.deletionVector).map(_.cardinality)).sum
  }

  test("Spark 3.5 Delta DV scan handoff should filter deleted rows") {
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
        assert(containsNativeDeltaScan(executedPlan))
        val planText = executedPlan.toString()
        assert(!planText.contains("__delta_internal_is_row_deleted"))
        assert(!planText.contains("__delta_internal_row_index"))
        checkAnswer(df, Seq((1, "a"), (2, "b")).toDF())
    }
  }

  test("Delta metadata row-index predicate should not be stripped from a native scan") {
    assume(SparkVersionUtil.gteSpark35, "metadata row index is available in Spark 3.5+")
    withTempDir {
      tempDir =>
        val path = tempDir.getCanonicalPath
        Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"))
          .toDF("id", "value")
          .coalesce(1)
          .write
          .format("delta")
          .save(path)

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
              VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> "false",
              VeloxDeltaConfig.ENABLE_NATIVE_DML_ROW_INDEX_SCAN.key -> "false"
            ) {
              executedPlans = DeltaTestUtils.withAllPlansCaptured(spark) {
                spark.sql(s"DELETE FROM delta.`$path` WHERE id IN (3, 4)").collect()
              }.map(_.executedPlan)
            }
            assertSparkDmlFallback(executedPlans)

            val log = DeltaLog.forTable(spark, new Path(path))
            assert(log.update().allFiles.collect().exists(_.deletionVector != null))
            assertReadPlanAfterDmlFallback(path, useMetadataRowIndex)
        }
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

        assertReadPlanAfterDmlFallback(path, useMetadataRowIndex = true)
    }
  }
}

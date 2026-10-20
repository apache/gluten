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
package org.apache.gluten.execution

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog, DeltaOperations, MaterializedRowCommitVersion, MaterializedRowId, RowCommitVersion, RowId}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.col

class VeloxDeltaRowTrackingSuite
  extends QueryTest
  with DeltaSQLCommandTest
  with AdaptiveSparkPlanHelper {

  test("filters on constant metadata fields run above the native scan") {
    withSQLConf(DeltaConfigs.ROW_TRACKING_ENABLED.defaultTablePropertyKey -> "true") {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          // Three files with base row IDs 0, 10 and 20.
          Seq(0, 10, 20).foreach {
            start =>
              spark
                .range(start, start + 10)
                .toDF("id")
                .repartition(1)
                .write
                .format("delta")
                .mode("append")
                .save(path)
          }
          val table = spark.read.format("delta").load(path)

          val byBaseRowId = table.where(col("_metadata.base_row_id") === 10)
          checkAnswer(byBaseRowId, (10 until 20).map(Row(_)))
          assertNativeScanWithoutMetadataFilters(byBaseRowId)

          val firstFileName =
            table.where(col("id") === 0).select("_metadata.file_name").head().getString(0)
          val byFileName = table.where(col("_metadata.file_name") === firstFileName)
          checkAnswer(byFileName, (0 until 10).map(Row(_)))
          assertNativeScanWithoutMetadataFilters(byFileName)

          val inSubquery = sql(s"""
                                  |SELECT * FROM delta.`$path`
                                  |WHERE id IN (
                                  |  SELECT id FROM delta.`$path` WHERE _metadata.base_row_id = 20)
                                  |""".stripMargin)
          checkAnswer(inSubquery, (20 until 30).map(Row(_)))
          assertNativeScanWithoutMetadataFilters(inSubquery)
      }
    }
  }

  test("reading row tracking metadata that a file lacks fails as in Delta") {
    withTable("t") {
      spark.range(0, 10).repartition(1).write.format("delta").saveAsTable("t")
      // Turn row tracking on without backfilling base row IDs, as Delta Lake's RowIdSuite does.
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("t"))
      val metadata = deltaLog.update().metadata
      val rowTrackingMetadata = metadata.copy(configuration = metadata.configuration ++ Map(
        DeltaConfigs.ROW_TRACKING_ENABLED.key -> "true",
        MaterializedRowId.MATERIALIZED_COLUMN_NAME_PROP -> "x",
        MaterializedRowCommitVersion.MATERIALIZED_COLUMN_NAME_PROP -> "y"
      ))
      deltaLog.startTransaction().commit(Seq(rowTrackingMetadata), DeltaOperations.ManualUpdate)
      spark.range(10, 20).repartition(1).write.format("delta").mode("append").saveAsTable("t")

      val table = spark.read.table("t")
      Seq(RowId.QUALIFIED_COLUMN_NAME, RowCommitVersion.QUALIFIED_COLUMN_NAME).foreach {
        column =>
          val df = table.select(col("id"), col(column))
          intercept[SparkException](df.collect())
          assert(nativeScans(df).isEmpty, df.queryExecution.executedPlan)
      }

      // Data skipping leaves only the file written with row tracking on, which reads natively.
      val skipped = table.where(col("id") >= 10).select(col("id"), col(RowId.QUALIFIED_COLUMN_NAME))
      checkAnswer(skipped, (0 until 10).map(i => Row(10L + i, i.toLong)))
      assert(nativeScans(skipped).nonEmpty, skipped.queryExecution.executedPlan)
    }
  }

  private def nativeScans(df: DataFrame): Seq[DeltaScanTransformer] =
    collect(df.queryExecution.executedPlan) { case scan: DeltaScanTransformer => scan }

  private def assertNativeScanWithoutMetadataFilters(df: DataFrame): Unit = {
    val scans = nativeScans(df)
    assert(scans.nonEmpty, df.queryExecution.executedPlan)
    scans.foreach {
      scan =>
        val metadataFilters = scan.filterExprs().filter(_.references.exists {
          attr => attr.name == RowId.BASE_ROW_ID || attr.name == "file_name"
        })
        assert(metadataFilters.isEmpty, df.queryExecution.executedPlan)
    }
  }
}

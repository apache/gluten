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
package org.apache.gluten.delta

import org.apache.gluten.execution.DeltaScanTransformer

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog, DeltaOperations, MaterializedRowCommitVersion, MaterializedRowId, RowCommitVersion, RowId}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession

import io.delta.sql.DeltaSparkSessionExtension

/**
 * Plans Delta row tracking reads on vanilla Spark and checks the decisions the native Delta scan
 * makes from those plans. Native execution is covered by `VeloxDeltaRowTrackingSuite`.
 */
class DeltaRowTrackingScanInfoSuite extends QueryTest with SharedSparkSession
  with DeltaSQLTestUtils {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key, classOf[DeltaSparkSessionExtension].getName)
      .set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
  }

  test("filters on constant metadata fields are not pushed into the native scan") {
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
                .coalesce(1)
                .write
                .format("delta")
                .mode("append")
                .save(path)
          }
          val df = spark.read
            .format("delta")
            .load(path)
            .where(col("_metadata.base_row_id") === 10 && col(
              "_metadata.file_name").endsWith(".parquet"))
            .where(col("id") > 12)
          checkAnswer(df, (13 until 20).map(Row(_)))

          val scan = fileSourceScan(df)
          val (metadataFilters, otherFilters) = scan.dataFilters.partition(readsMetadataField)
          assert(metadataFilters.size == 2, scan.dataFilters)
          assert(otherFilters.nonEmpty, scan.dataFilters)

          val transformer = DeltaScanTransformer(scan)
          assert(transformer.scanFilters == otherFilters)
          assert(
            transformer.withNewPushdownFilters(scan.dataFilters).pushDownFilters ==
              Some(otherFilters))
      }
    }
  }

  test("detects reads of row tracking metadata that a scanned file lacks") {
    withTable("t") {
      createTableWithMissingRowTrackingMetadata("t")
      val table = spark.read.table("t")
      Seq(RowId.QUALIFIED_COLUMN_NAME, RowCommitVersion.QUALIFIED_COLUMN_NAME).foreach {
        column =>
          val df = table.select(col("id"), col(column))
          assert(readsMissingRowTrackingMetadata(df), column)
          intercept[SparkException](df.collect())
      }
      assert(!readsMissingRowTrackingMetadata(table.select("id")))
      // Data skipping leaves only the file written with row tracking on.
      val skipped = table.where(col("id") >= 10).select(col("id"), col(RowId.QUALIFIED_COLUMN_NAME))
      assert(!readsMissingRowTrackingMetadata(skipped))
      checkAnswer(skipped, (0 until 10).map(i => Row(10L + i, i.toLong)))
    }
  }

  /**
   * Turns row tracking on without backfilling base row IDs, as Delta Lake's RowIdSuite does, so the
   * first file has no base row ID or default row commit version and the second has both.
   */
  private def createTableWithMissingRowTrackingMetadata(table: String): Unit = {
    spark.range(0, 10).coalesce(1).write.format("delta").saveAsTable(table)
    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(table))
    val metadata = deltaLog.update().metadata
    val rowTrackingMetadata = metadata.copy(configuration = metadata.configuration ++ Map(
      DeltaConfigs.ROW_TRACKING_ENABLED.key -> "true",
      MaterializedRowId.MATERIALIZED_COLUMN_NAME_PROP -> "x",
      MaterializedRowCommitVersion.MATERIALIZED_COLUMN_NAME_PROP -> "y"
    ))
    deltaLog.startTransaction().commit(Seq(rowTrackingMetadata), DeltaOperations.ManualUpdate)
    spark.range(10, 20).coalesce(1).write.format("delta").mode("append").saveAsTable(table)
  }

  private def readsMissingRowTrackingMetadata(df: DataFrame): Boolean = {
    val scan = fileSourceScan(df)
    DeltaRowTrackingScanInfo.missingRequiredRowTrackingMetadata(
      scan.relation,
      scan.output,
      scan.partitionFilters)
  }

  private def readsMetadataField(filter: Expression): Boolean =
    filter.references.exists(a => a.name == RowId.BASE_ROW_ID || a.name == "file_name")

  private def fileSourceScan(df: DataFrame): FileSourceScanExec = {
    val scans = df.queryExecution.sparkPlan.collect { case scan: FileSourceScanExec => scan }
    assert(scans.size == 1, df.queryExecution.sparkPlan)
    scans.head
  }
}

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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

import org.apache.commons.io.FileUtils
import org.apache.iceberg.{FileContent, FileFormat, SnapshotSummary}
import org.apache.iceberg.data.{GenericAppenderFactory, GenericRecord}
import org.apache.iceberg.io.OutputFileFactory
import org.apache.iceberg.spark.source.SparkTable

import java.nio.file.Files

import scala.collection.JavaConverters._

class GlutenIcebergEqualityDeleteFallbackSuite extends WholeStageTransformerSuite {
  override protected val resourcePath: String = "/"
  override protected val fileFormat: String = "parquet"

  private lazy val icebergWarehouse =
    Files.createTempDirectory("gluten-iceberg-equality-delete-fallback").toFile

  override protected def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      FileUtils.deleteDirectory(icebergWarehouse)
    }
  }

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
    .set(
      "spark.sql.extensions",
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .set("spark.sql.catalog.spark_catalog.type", "hadoop")
    .set("spark.sql.catalog.spark_catalog.warehouse", icebergWarehouse.toURI.toString)
    .set("spark.sql.shuffle.partitions", "1")
    .set("spark.sql.adaptive.enabled", "false")
    .set("spark.sql.iceberg.aggregate-push-down.enabled", "false")

  for (version <- Seq(2, 3)) {
    test(s"iceberg v$version time travel falls back for equality deletes after rewriting") {
      assume(!BackendsApiManager.getSettings.supportIcebergEqualityDeleteRead())
      if (version == 3) {
        assume(BackendsApiManager.getSettings.supportIcebergDeletionVectorRead())
      }
      val name = "iceberg_equality_delete_time_travel"
      withTable(name) {
        var snapshotId = 0L
        withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
          spark.sql(s"""CREATE TABLE $name (id BIGINT, payload STRING) USING iceberg
                       |TBLPROPERTIES ('format-version' = '2')""".stripMargin)
          spark.sql(s"INSERT INTO $name VALUES (1, 'deleted'), (2, 'kept')")
          val table = spark.sessionState.catalogManager
            .catalog("spark_catalog")
            .asInstanceOf[TableCatalog]
            .loadTable(Identifier.of(Array("default"), name))
            .asInstanceOf[SparkTable]
            .table()
          val deleteSchema = table.schema().select("id")
          val factory = new GenericAppenderFactory(
            table.schema(),
            table.spec(),
            Array(table.schema().findField("id").fieldId()),
            deleteSchema,
            null)
          val output = OutputFileFactory.builderFor(table, 0, 0)
            .format(FileFormat.PARQUET).build().newOutputFile()
          val writer = factory.newEqDeleteWriter(output, FileFormat.PARQUET, null)
          try {
            val record = GenericRecord.create(deleteSchema)
            record.setField("id", Long.box(1L))
            writer.write(record)
          } finally {
            writer.close()
          }
          table.newRowDelta().addDeletes(writer.toDeleteFile()).commit()
          snapshotId = table.currentSnapshot().snapshotId()
          spark.catalog.refreshTable(name)
          checkAnswer(spark.sql(s"SELECT * FROM $name"), Seq(Row(2L, "kept")))
          if (version == 3) {
            spark.sql(s"ALTER TABLE $name SET TBLPROPERTIES ('format-version' = '3')")
          }
          spark.sql(
            s"""CALL spark_catalog.system.rewrite_data_files(
               |table => 'default.$name', options => map('rewrite-all', 'true'))""".stripMargin)
            .collect()
          table.refresh()
          // The rewritten data no longer needs the equality delete retained in the old snapshot.
          table.newRewrite().deleteFile(writer.toDeleteFile()).commit()
          assert(table.currentSnapshot().snapshotId() != snapshotId)
          assert(
            table.currentSnapshot().summary().get(SnapshotSummary.TOTAL_EQ_DELETES_PROP) == "0")
          val tasks = table.newScan().useSnapshot(snapshotId).planFiles()
          try {
            assert(tasks.asScala.exists(
              _.deletes().asScala.exists(_.content() == FileContent.EQUALITY_DELETES)))
          } finally {
            tasks.close()
          }
          spark.catalog.refreshTable(name)
        }

        runQueryAndCompare(s"SELECT * FROM $name") {
          df =>
            checkAnswer(df, Seq(Row(2L, "kept")))
            checkGlutenPlan[IcebergScanTransformer](df)
        }
        for (projection <- Seq("id, payload", "payload")) {
          runQueryAndCompare(
            s"SELECT $projection FROM $name VERSION AS OF $snapshotId",
            noFallBack = false) {
            df =>
              val expected = if (projection == "payload") Row("kept") else Row(2L, "kept")
              checkAnswer(df, Seq(expected))
              checkSparkPlan[BatchScanExec](df)
              checkGlutenPlanCount[IcebergScanTransformer](df, 0)
          }
        }
      }
    }
  }
}

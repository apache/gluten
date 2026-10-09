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

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}

import org.apache.commons.io.FileUtils
import org.apache.iceberg.{FileFormat, Table => IcebergTable}
import org.apache.iceberg.deletes.BaseDVFileWriter
import org.apache.iceberg.io.OutputFileFactory
import org.apache.iceberg.spark.source.SparkTable

import java.nio.file.Files

import scala.collection.JavaConverters._

class VeloxIcebergDeletionVectorSuite extends WholeStageTransformerSuite {
  override protected val resourcePath: String = "/"
  override protected val fileFormat: String = "parquet"

  private lazy val icebergWarehouse =
    Files.createTempDirectory("gluten-iceberg-deletion-vectors").toFile

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
    .set("spark.gluten.sql.columnar.maxBatchSize", "128")

  private def icebergTable(name: String): IcebergTable = spark.sessionState.catalogManager
    .catalog("spark_catalog")
    .asInstanceOf[TableCatalog]
    .loadTable(Identifier.of(Array("default"), name))
    .asInstanceOf[SparkTable]
    .table()

  for (format <- Seq("parquet", "orc")) {
    test(s"iceberg v3 deletion vectors in a shared Puffin file over $format") {
      val name = "iceberg_shared_deletion_vectors"
      withTable(name) {
        var expected = Seq.empty[Row]
        withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
          spark.sql(s"""CREATE TABLE $name (id BIGINT, payload STRING) USING iceberg
                       |TBLPROPERTIES ('format-version' = '3',
                       |'write.format.default' = '$format',
                       |'write.parquet.row-group-size-bytes' = '4096',
                       |'write.parquet.compression-codec' = 'uncompressed',
                       |'read.split.target-size' = '4096',
                       |'read.split.open-file-cost' = '0',
                       |'read.split.adaptive-size.enabled' = 'false')""".stripMargin)
          (0 until 3).foreach {
            file =>
              spark.sql(s"""INSERT INTO $name SELECT id, concat('value-', id)
                           |FROM range(${file * 1100}, ${(file + 1) * 1100}, 1, 1)""".stripMargin)
          }
          val table = icebergTable(name)
          val tasks = table.newScan().planFiles()
          val files =
            try {
              tasks.asScala.map(_.file()).toList.sortBy(_.path().toString)
            } finally {
              tasks.close()
            }
          assert(files.size == 3)
          if (format == "parquet") {
            val splitTasks = table.newScan().planTasks()
            try {
              assert(splitTasks.asScala.flatMap(_.files().asScala).size > files.size)
            } finally {
              splitTasks.close()
            }
          }
          val deleted = Map(
            files(0).path().toString -> Set(0L, 127L, 128L, 1023L, 1099L),
            files(1).path().toString -> Set(1L, 129L, 1024L, 1098L))
          val rows = spark.sql(s"SELECT _file, _pos, id, payload FROM $name").collect()
          val remaining = rows.filterNot {
            row => deleted.getOrElse(row.getString(0), Set.empty[Long]).contains(row.getLong(1))
          }.map(row => Row(row.getLong(2), row.getString(3))).toSeq
          val writer = new BaseDVFileWriter(
            OutputFileFactory.builderFor(table, 0, 0).format(FileFormat.PUFFIN).build(),
            _ => null)
          try {
            files.take(2).foreach {
              file =>
                deleted(file.path().toString).foreach {
                  pos => writer.delete(file.path().toString, pos, table.spec(), file.partition())
                }
            }
          } finally {
            writer.close()
          }
          val deletes = writer.result().deleteFiles().asScala
          assert(deletes.size == 2)
          assert(deletes.map(_.path().toString).distinct.size == 1)
          assert(deletes.map(_.contentOffset()).distinct.size == 2)
          val delta = table.newRowDelta()
          deletes.foreach(delta.addDeletes)
          delta.commit()
          spark.catalog.refreshTable(name)
          expected = remaining
        }
        runQueryAndCompare(
          s"SELECT id, payload FROM $name") {
          df =>
            checkAnswer(df, expected)
            checkGlutenPlan[IcebergScanTransformer](df)
        }
        runQueryAndCompare(
          s"SELECT id, payload FROM $name WHERE id >= 128 AND id < 2500") {
          df =>
            checkAnswer(df, expected.filter(row => row.getLong(0) >= 128 && row.getLong(0) < 2500))
            checkGlutenPlan[IcebergScanTransformer](df)
        }
        runQueryAndCompare(
          s"SELECT count(*) FROM $name") {
          df =>
            checkAnswer(df, Seq(Row(expected.size.toLong)))
            checkGlutenPlan[IcebergScanTransformer](df)
        }
      }
    }
  }

  test("iceberg v3 reads v2 position deletes and replaced deletion vectors") {
    val name = "iceberg_upgraded_deletion_vectors"
    withTable(name) {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        spark.sql(s"""CREATE TABLE $name (id BIGINT) USING iceberg
                     |TBLPROPERTIES ('format-version' = '2',
                     |'write.delete.mode' = 'merge-on-read')""".stripMargin)
        spark.sql(s"INSERT INTO $name SELECT id FROM range(0, 100, 1, 1)")
        spark.sql(s"DELETE FROM $name WHERE id = 0")
        spark.sql(s"ALTER TABLE $name SET TBLPROPERTIES ('format-version' = '3')")
      }
      runQueryAndCompare(s"SELECT id FROM $name") {
        df =>
          checkAnswer(df, (1L until 100L).map(Row(_)))
          checkGlutenPlan[IcebergScanTransformer](df)
      }
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        spark.sql(s"INSERT INTO $name SELECT id FROM range(100, 200, 1, 1)")
        spark.sql(s"DELETE FROM $name WHERE id = 101")
        spark.sql(s"DELETE FROM $name WHERE id = 150")
        val formats = spark.sql(s"SELECT file_format FROM spark_catalog.default.$name.delete_files")
          .collect().map(_.getString(0)).toSet
        assert(formats == Set("PARQUET", "PUFFIN"))
      }
      runQueryAndCompare(
        s"SELECT id FROM $name") {
        df =>
          checkAnswer(df, (1L until 200L).filterNot(Set(101L, 150L)).map(Row(_)))
          checkGlutenPlan[IcebergScanTransformer](df)
      }
    }
  }
}

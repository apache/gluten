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
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}

import org.apache.commons.io.FileUtils
import org.apache.iceberg.{DataFiles, FileFormat, MetadataColumns, Table => IcebergTable}
import org.apache.iceberg.data.{GenericAppenderFactory, GenericRecord}
import org.apache.iceberg.spark.source.SparkTable

import java.nio.file.Files

import scala.collection.JavaConverters._

class VeloxIcebergRowLineageSuite extends WholeStageTransformerSuite {
  override protected val resourcePath: String = "/"
  override protected val fileFormat: String = "parquet"

  private lazy val warehouse = Files.createTempDirectory("gluten-iceberg-lineage").toFile

  override protected def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      FileUtils.deleteDirectory(warehouse)
    }
  }

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
    .set(
      "spark.sql.extensions",
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .set("spark.sql.catalog.spark_catalog.type", "hadoop")
    .set("spark.sql.catalog.spark_catalog.warehouse", warehouse.toURI.toString)
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

  private def checkScan(df: DataFrame): Unit = {
    checkGlutenPlan[IcebergScanTransformer](df)
    val scans = df.queryExecution.executedPlan.collect { case s: IcebergScanTransformer => s }
    assert(scans.nonEmpty)
    scans.foreach {
      scan =>
        assert(!scan.filterExprs().exists(_.references.exists {
          attr => Set("_row_id", "_last_updated_sequence_number").contains(attr.name.toLowerCase)
        }))
    }
  }

  private def checkQuery(
      query: String,
      expected: Seq[Row],
      compareWithVanilla: Boolean = true): Unit = {
    runQueryAndCompare(query, compareResult = compareWithVanilla) {
      df =>
        checkAnswer(df, expected)
        checkScan(df)
    }
  }

  for (format <- Seq("parquet", "orc")) {
    def checkFormatQuery(query: String, expected: Seq[Row]): Unit = {
      // Iceberg 1.10's Spark ORC reader returns constants for lineage columns,
      // including physically stored values. Assert the expected values directly.
      checkQuery(query, expected, compareWithVanilla = format == "parquet")
    }

    test(s"iceberg v3 inherited lineage and predicates over $format") {
      val name = "iceberg_inherited_lineage"
      withTable(name) {
        withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
          spark.sql(s"""CREATE TABLE $name (id BIGINT, payload STRING) USING iceberg
                       |TBLPROPERTIES ('format-version' = '3',
                       |'write.format.default' = '$format',
                       |'write.parquet.row-group-size-bytes' = '4096',
                       |'write.parquet.compression-codec' = 'uncompressed',
                       |'read.split.target-size' = '4096',
                       |'read.split.open-file-cost' = '0',
                       |'read.split.adaptive-size.enabled' = 'false')""".stripMargin)
          (0 until 2).foreach {
            file =>
              spark.sql(s"""INSERT INTO $name SELECT id, concat('value-', id)
                           |FROM range(${file * 1100}, ${(file + 1) * 1100}, 1, 1)""".stripMargin)
          }
          // Metadata columns must also work after ordinary schema evolution.
          spark.sql(s"ALTER TABLE $name ADD COLUMNS (added STRING)")
          if (format == "parquet") {
            val tasks = icebergTable(name).newScan().planTasks()
            try {
              assert(tasks.asScala.flatMap(_.files().asScala).size > 2)
            } finally {
              tasks.close()
            }
          }
        }
        val expected = (0L until 2200L).map(id => Row(id, id, if (id < 1100) 1L else 2L))
        val projection = s"SELECT id, _row_id, _last_updated_sequence_number FROM $name"
        checkFormatQuery(projection, expected)
        checkFormatQuery(s"$projection WHERE id >= 127 AND id < 130", expected.slice(127, 130))
        checkFormatQuery(
          s"$projection WHERE _row_id >= 1098 AND _row_id < 1102",
          expected.slice(1098, 1102))
        checkFormatQuery(
          s"$projection WHERE _last_updated_sequence_number = 2",
          expected.drop(1100))
        checkFormatQuery(s"$projection WHERE id >= 128 AND _row_id < 130", expected.slice(128, 130))
        checkFormatQuery(s"$projection WHERE _row_id IS NULL", Seq.empty)
        checkFormatQuery(s"$projection WHERE _row_id IS NOT NULL", expected)
        checkFormatQuery(s"SELECT id FROM $name WHERE _row_id = 128", Seq(Row(128L)))
        checkFormatQuery(
          s"SELECT count(*) FROM $name WHERE _last_updated_sequence_number = 2",
          Seq(Row(1100L)))
        checkFormatQuery(
          s"SELECT _row_id FROM $name WHERE _row_id IN (0, 127, 128, 2199)",
          Seq(0L, 127L, 128L, 2199L).map(Row(_)))
        checkFormatQuery(
          s"$projection WHERE _row_id + 1 = 130 OR id = 0",
          Seq(expected(0), expected(129)))
        checkFormatQuery(s"$projection WHERE _last_updated_sequence_number > 2", Seq.empty)
        checkFormatQuery(
          s"""SELECT /*+ BROADCAST(keys) */ t.id FROM $name t
             |JOIN (SELECT id AS wanted FROM $name WHERE id >= 127 AND id < 130) keys
             |ON t._row_id = keys.wanted""".stripMargin,
          Seq(127L, 128L, 129L).map(Row(_))
        )
      }
    }

    test(s"iceberg v3 lineage alongside nested data over $format") {
      val name = "iceberg_nested_lineage"
      withTable(name) {
        withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
          spark.sql(s"""CREATE TABLE $name
                       |(s STRUCT<x: BIGINT, y: STRING>, a ARRAY<BIGINT>, m MAP<STRING, BIGINT>)
                       |USING iceberg TBLPROPERTIES ('format-version' = '3',
                       |'write.format.default' = '$format')""".stripMargin)
          spark.sql(s"""INSERT INTO $name VALUES
                       |(named_struct('x', 7L, 'y', 'a'), array(1L, 2L), map('x', 3L)),
                       |(named_struct('x', 8L, 'y', 'b'), array(4L), map('x', 5L))""".stripMargin)
        }
        checkFormatQuery(
          s"SELECT s, a, m, _row_id, _last_updated_sequence_number FROM $name",
          Seq(
            Row(Row(7L, "a"), Seq(1L, 2L), Map("x" -> 3L), 0L, 1L),
            Row(Row(8L, "b"), Seq(4L), Map("x" -> 5L), 1L, 1L))
        )
        checkFormatQuery(
          s"SELECT s.x, a[0], m['x'], _row_id FROM $name WHERE s.x >= 8",
          Seq(Row(8L, 4L, 5L, 1L)))
      }
    }

    test(s"iceberg v3 stored and null lineage values over $format") {
      val name = "iceberg_stored_lineage"
      withTable(name) {
        val largeRowId = 1L << 33
        withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
          spark.sql(s"""CREATE TABLE $name (id BIGINT) USING iceberg
                       |TBLPROPERTIES ('format-version' = '3',
                       |'write.format.default' = '$format')""".stripMargin)
          val table = icebergTable(name)
          val schema = MetadataColumns.schemaWithRowLineage(table.schema())
          val path = s"${table.location()}/data/lineage.$format"
          val appender = new GenericAppenderFactory(schema, table.spec())
            .newAppender(table.io().newOutputFile(path), FileFormat.fromString(format))
          try {
            (0 until 4).foreach {
              id =>
                val record = GenericRecord.create(schema)
                record.setField("id", Long.box(id.toLong))
                record.setField("_row_id", if (id % 2 == 0) Long.box(largeRowId + id) else null)
                record.setField(
                  "_last_updated_sequence_number",
                  if (id == 0) Long.box(0L) else null)
                appender.add(record)
            }
          } finally {
            appender.close()
          }
          val file = DataFiles.builder(table.spec())
            .withInputFile(table.io().newInputFile(path))
            .withFormat(format)
            .withMetrics(appender.metrics())
            .withSplitOffsets(appender.splitOffsets())
            .build()
          table.newAppend().appendFile(file).commit()
          spark.catalog.refreshTable(name)
        }
        val expected = Seq(
          Row(0L, largeRowId, 0L),
          Row(1L, 1L, 1L),
          Row(2L, largeRowId + 2, 1L),
          Row(3L, 3L, 1L))
        val projection = s"SELECT id, _row_id, _last_updated_sequence_number FROM $name"
        checkFormatQuery(projection, expected)
        checkFormatQuery(s"$projection WHERE _row_id < 4", Seq(expected(1), expected(3)))
        checkFormatQuery(s"$projection WHERE _last_updated_sequence_number = 1", expected.tail)
        checkFormatQuery(s"$projection WHERE _row_id = $largeRowId", Seq(expected.head))
        checkFormatQuery(s"$projection WHERE id >= 1 AND _row_id IS NOT NULL", expected.tail)
      }
    }
  }

  test("iceberg lineage before and after the first commit following a v2 upgrade") {
    val name = "iceberg_upgraded_lineage"
    withTable(name) {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        spark.sql(
          s"CREATE TABLE $name (id BIGINT) USING iceberg TBLPROPERTIES ('format-version' = '2')")
        spark.sql(s"INSERT INTO $name SELECT id FROM range(0, 3, 1, 1)")
        spark.sql(s"ALTER TABLE $name SET TBLPROPERTIES ('format-version' = '3')")
      }
      val projection = s"SELECT id, _row_id, _last_updated_sequence_number FROM $name"
      val oldRows = (0L until 3L).map(Row(_, null, null))
      checkQuery(projection, oldRows)
      checkQuery(s"$projection WHERE _row_id IS NULL", oldRows)
      checkQuery(s"$projection WHERE _last_updated_sequence_number IS NOT NULL", Seq.empty)
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        spark.sql(s"INSERT INTO $name SELECT id FROM range(3, 6, 1, 1)")
      }
      runQueryAndCompare(projection)(checkScan)
      runQueryAndCompare(s"$projection WHERE _row_id IS NOT NULL")(checkScan)
      runQueryAndCompare(s"SELECT id FROM $name WHERE _last_updated_sequence_number = 2")(checkScan)
    }
  }
}

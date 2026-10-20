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

import org.apache.gluten.config.{GlutenConfig, VeloxDeltaConfig}

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.tags.ExtendedSQLTest

import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.{LogicalTypeAnnotation, Type}

import java.util.concurrent.CopyOnWriteArrayList

import scala.collection.JavaConverters._

/**
 * Native writes to tables with Iceberg compatibility (UniForm). Delta's writer gives their files
 * Parquet field IDs, which the Velox writer must write too: Iceberg, and Spark's reader under id
 * column mapping, match columns by them.
 */
@ExtendedSQLTest
class DeltaIcebergCompatNativeWriteSuite extends DeltaSQLCommandTest {

  // IcebergCompatV1 does not allow arrays and maps.
  private val flatColumns = "id BIGINT, s STRUCT<x: INT, y: STRING>, ts TIMESTAMP"
  private val nestedColumns = flatColumns +
    ", a ARRAY<INT>, m MAP<STRING, ARRAY<INT>>, items ARRAY<STRUCT<sku: STRING, qty: INT>>"

  private def rows(columns: String): String = {
    val flat = "id, named_struct('x', CAST(id AS INT), 'y', CAST(id AS STRING)) AS s, " +
      "timestamp_seconds(1767225600 + id) AS ts"
    val nested = ", array(CAST(id AS INT)) AS a, " +
      "map(CAST(id AS STRING), array(CAST(id AS INT), 1)) AS m, " +
      "array(named_struct('sku', CAST(id AS STRING), 'qty', CAST(id AS INT))) AS items"
    s"SELECT $flat${if (columns == nestedColumns) nested else ""} FROM range(10)"
  }

  private def createTable(
      path: String,
      version: Int,
      columns: String,
      partitioned: Boolean): Unit =
    sql(
      s"CREATE TABLE delta.`$path` ($columns${if (partitioned) ", part INT" else ""}) USING delta " +
        (if (partitioned) "PARTITIONED BY (part) " else "") +
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'id', " +
        s"'delta.enableIcebergCompatV$version' = 'true')")

  /** Appends with the DataFrame API, which Gluten offloads, unlike INSERT INTO. */
  private def append(path: String, query: String): Seq[SparkPlan] = {
    val plans = new CopyOnWriteArrayList[SparkPlan]()
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
        plans.add(qe.executedPlan)
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
    }
    spark.listenerManager.register(listener)
    try {
      sql(query).coalesce(1).write.format("delta").mode("append").save(path)
      spark.sparkContext.listenerBus.waitUntilEmpty()
    } finally {
      spark.listenerManager.unregister(listener)
    }
    plans.asScala.toSeq
  }

  private def isGlutenDeltaWrite(plan: SparkPlan): Boolean =
    plan.treeString.contains("GlutenDelta ")

  private def footers(path: String): Seq[(String, ParquetMetadata)] = {
    val deltaLog = DeltaLog.forTable(spark, path)
    val hadoopConf = deltaLog.newDeltaHadoopConf()
    deltaLog.update().allFiles.collect().toSeq.map {
      file =>
        val reader = ParquetFileReader.open(
          HadoopInputFile.fromPath(DeltaFileOperations.absolutePath(path, file.path), hadoopConf))
        try {
          file.path -> reader.getFooter
        } finally {
          reader.close()
        }
    }
  }

  // Vanilla Spark writes with parquet-mr; the Velox writer does not.
  private def writtenByVelox(footer: ParquetMetadata): Boolean =
    !footer.getFileMetaData.getCreatedBy.startsWith("parquet-mr")

  /**
   * The field IDs of a file's columns, along the Spark type tree: an array has one child, its
   * element, and a map two, its key and value. -1 is a node without an ID.
   */
  private def fieldIds(footer: ParquetMetadata): String =
    footer.getFileMetaData.getSchema.getFields.asScala.map(fieldIds).mkString(",")

  private def fieldIds(node: Type): String = {
    val children: Seq[Type] = if (node.isPrimitive) {
      Seq.empty
    } else {
      val group = node.asGroupType()
      group.getLogicalTypeAnnotation match {
        case _: LogicalTypeAnnotation.ListLogicalTypeAnnotation =>
          Seq(group.getType(0).asGroupType().getType(0))
        case _: LogicalTypeAnnotation.MapLogicalTypeAnnotation =>
          group.getType(0).asGroupType().getFields.asScala.toSeq
        case _ => group.getFields.asScala.toSeq
      }
    }
    val id = Option(node.getId).map(_.intValue()).getOrElse(-1)
    if (children.isEmpty) id.toString else s"$id(${children.map(fieldIds).mkString(",")})"
  }

  for ((version, columns) <- Seq(1 -> flatColumns, 2 -> nestedColumns)) {
    test(s"IcebergCompatV$version: the Velox writer writes the field IDs Delta's writer writes") {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          createTable(path, version, columns, partitioned = false)
          val plans = append(path, rows(columns))
          assert(plans.exists(isGlutenDeltaWrite), plans.map(_.treeString).mkString("\n---\n"))
          withSQLConf(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> "false") {
            append(path, rows(columns))
          }

          val (velox, vanilla) = footers(path).partition { case (_, f) => writtenByVelox(f) }
          assert(velox.nonEmpty, s"No file written by Velox: ${footers(path)}")
          assert(vanilla.nonEmpty, s"No file written by parquet-mr: ${footers(path)}")
          val expected = fieldIds(vanilla.head._2)
          // Every node has an ID: IcebergCompatV2 gives array elements and map keys and values IDs,
          // and IcebergCompatV1 tables have no arrays or maps.
          assert(!expected.contains("-1"), expected)
          velox.foreach {
            case (file, footer) =>
              assert(fieldIds(footer) == expected, s"$file: ${footer.getFileMetaData.getSchema}")
          }

          // Under id column mapping, Spark's reader matches the columns of the files by field ID.
          withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
            val expectedRows = sql(rows(columns)).collect().map(_.toString).sorted
            val actualRows = spark.read.format("delta").load(path).collect().map(_.toString).sorted
            assert(actualRows.toSeq == (expectedRows ++ expectedRows).sorted.toSeq)
          }
      }
    }
  }

  test("partitioned tables with Iceberg compatibility are written by Delta's writer") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        createTable(path, 2, flatColumns, partitioned = true)
        append(path, s"SELECT *, CAST(id % 2 AS INT) AS part FROM (${rows(flatColumns)})")

        val written = footers(path)
        assert(written.nonEmpty)
        written.foreach {
          case (file, footer) =>
            assert(!writtenByVelox(footer), s"$file: ${footer.getFileMetaData}")
            // Iceberg compatibility keeps the partition columns in the data files.
            assert(footer.getFileMetaData.getSchema.getFieldCount == 4, s"$file")
            assert(!fieldIds(footer).contains("-1"), s"$file: ${fieldIds(footer)}")
        }
    }
  }
}

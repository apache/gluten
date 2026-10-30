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

import org.apache.gluten.config.{GlutenConfig, VeloxDeltaConfig}

import org.apache.spark.sql.Row
import org.apache.spark.sql.delta.{DeltaLog, MaterializePartitionColumnsTableFeature}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.internal.SQLConf

import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.{GroupType, LogicalTypeAnnotation}
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.json4s.{JInt, JNothing}
import org.json4s.jackson.JsonMethods.parse

import scala.jdk.CollectionConverters._

/**
 * The native Delta write either produces the files vanilla Delta writes or falls back to it. Each
 * test checks the written files: which writer produced them, their schema and their statistics.
 */
class VeloxDelta4WriteSuite extends DeltaSQLCommandTest {

  private def footers(path: String): Seq[(AddFile, ParquetMetadata)] = {
    val deltaLog = DeltaLog.forTable(spark, path)
    val hadoopConf = deltaLog.newDeltaHadoopConf()
    deltaLog.update().allFiles.collect().toSeq.map {
      file =>
        val reader = ParquetFileReader.open(
          HadoopInputFile.fromPath(DeltaFileOperations.absolutePath(path, file.path), hadoopConf))
        try {
          file -> reader.getFooter
        } finally {
          reader.close()
        }
    }
  }

  private def fieldNames(footer: ParquetMetadata): Seq[String] =
    footer.getFileMetaData.getSchema.getFields.asScala.map(_.getName).toSeq

  // Vanilla Spark writes with parquet-mr; the Velox writer does not.
  private def writtenByVelox(footer: ParquetMetadata): Boolean =
    !footer.getFileMetaData.getCreatedBy.startsWith("parquet-mr")

  private def createTable(
      path: String,
      partitioned: Boolean,
      properties: (String, String)*): Unit = {
    val partitionedBy = if (partitioned) " PARTITIONED BY (part)" else ""
    val tblProperties = if (properties.isEmpty) {
      ""
    } else {
      properties.map { case (k, v) => s"'$k' = '$v'" }.mkString(" TBLPROPERTIES (", ", ", ")")
    }
    sql(
      s"CREATE TABLE delta.`$path` (id BIGINT, value STRING, part INT) USING delta" +
        partitionedBy + tblProperties)
  }

  private def writeRows(path: String): Unit = {
    spark
      .range(10)
      .selectExpr("id", "CAST(id AS STRING) AS value", "CAST(id % 2 AS INT) AS part")
      .coalesce(1)
      .write
      .format("delta")
      .mode("append")
      .save(path)
  }

  private def assertRows(path: String): Unit = {
    assert(
      spark.read.format("delta").load(path).collect().toSet ==
        (0 until 10).map(i => Row(i.toLong, i.toString, i % 2)).toSet)
  }

  private def assertWrittenBy(path: String, velox: Boolean, columns: Seq[String]): Unit = {
    val files = footers(path)
    assert(files.nonEmpty)
    files.foreach {
      case (file, footer) =>
        assert(writtenByVelox(footer) == velox, s"${file.path}: ${footer.getFileMetaData}")
        assert(fieldNames(footer) == columns, file.path)
    }
  }

  test("an unpartitioned table is written natively, with Delta's statistics") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        createTable(path, partitioned = false)
        writeRows(path)

        assertWrittenBy(path, velox = true, Seq("id", "value", "part"))
        val (file, _) = footers(path).head
        val stats = parse(file.stats)
        assert(stats \ "numRecords" == JInt(10), file.stats)
        assert(stats \ "minValues" \ "id" == JInt(0), file.stats)
        assert(stats \ "maxValues" \ "id" == JInt(9), file.stats)
        assertRows(path)
    }
  }

  for (feature <- Seq(false, true); property <- Seq(None, Some(false), Some(true))) {
    test(s"native partition materialization: feature=$feature, property=$property") {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          val properties = property.map(v => "delta.writePartitionColumnsToParquet" -> v.toString)
            .toSeq ++ (if (feature) {
                         Seq("delta.feature.materializePartitionColumns" -> "supported")
                       } else {
                         Seq.empty
                       })
          createTable(path, partitioned = true, properties: _*)
          writeRows(path)

          val snapshot = DeltaLog.forTable(spark, path).update()
          assert(snapshot.protocol.isFeatureSupported(MaterializePartitionColumnsTableFeature) ==
            feature)
          assert(snapshot.metadata.configuration.get("delta.writePartitionColumnsToParquet") ==
            property.map(_.toString))
          val materialize = feature || property.getOrElse(true)
          val columns = if (materialize) Seq("id", "value", "part") else Seq("id", "value")
          assertWrittenBy(path, velox = true, columns)
          withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
            footers(path).foreach {
              case (file, _) =>
                val stats = parse(file.stats)
                for (field <- Seq("minValues", "maxValues", "nullCount")) {
                  assert(stats \ field \ "part" == JNothing, file.stats)
                }
                if (materialize) {
                  val filePath = DeltaFileOperations.absolutePath(path, file.path).toString
                  val rows = spark.read.option("recursiveFileLookup", "true")
                    .parquet(filePath).select("part").collect()
                  assert(rows.nonEmpty)
                  assert(rows.forall(_.getInt(0).toString == file.partitionValues("part")))
                }
            }
          }
          assertRows(path)
      }
    }
  }

  test("enabling materialization preserves reads of older files without partition columns") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        createTable(path, partitioned = true, "delta.writePartitionColumnsToParquet" -> "false")
        writeRows(path)
        val oldFiles = footers(path).map(_._1.path).toSet

        sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES " +
          "('delta.feature.materializePartitionColumns' = 'supported')")
        writeRows(path)

        val files = footers(path)
        assert(files.exists { case (file, _) => !oldFiles.contains(file.path) })
        files.foreach {
          case (file, footer) =>
            assert(writtenByVelox(footer), file.path)
            val columns = if (oldFiles.contains(file.path)) {
              Seq("id", "value")
            } else {
              Seq("id", "value", "part")
            }
            assert(fieldNames(footer) == columns, file.path)
        }
        val expected = (0 until 10).flatMap(i => Seq.fill(2)(Row(i.toLong, i.toString, i % 2)))
        for (gluten <- Seq("false", "true")) {
          withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> gluten) {
            assert(spark.read.format("delta").load(path).orderBy("id").collect().toSeq == expected)
          }
        }
    }
  }

  test("native updates and deletes materialize partition columns like vanilla Delta") {
    withTempDir {
      nativeDir =>
        withTempDir {
          vanillaDir =>
            val nativePath = nativeDir.getCanonicalPath
            val vanillaPath = vanillaDir.getCanonicalPath
            val paths = Seq(nativePath -> "true", vanillaPath -> "false")
            paths.foreach {
              case (path, native) =>
                withSQLConf(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> native) {
                  createTable(
                    path,
                    partitioned = true,
                    "delta.feature.materializePartitionColumns" -> "supported",
                    "delta.writePartitionColumnsToParquet" -> "false",
                    "delta.enableDeletionVectors" -> "false"
                  )
                  writeRows(path)
                }
            }
            val commands = Seq(
              "UPDATE delta.`%s` SET part = part + 2, value = 'updated' WHERE id = 0",
              "DELETE FROM delta.`%s` WHERE id = 1")
            commands.foreach {
              command =>
                val oldFiles = footers(nativePath).map(_._1.path).toSet
                paths.foreach {
                  case (path, native) =>
                    withSQLConf(VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> native) {
                      sql(command.format(path)).collect()
                    }
                }
                val newFiles = footers(nativePath).filterNot {
                  case (file, _) => oldFiles.contains(file.path)
                }
                assert(newFiles.nonEmpty)
                newFiles.foreach {
                  case (file, footer) =>
                    assert(writtenByVelox(footer), file.path)
                    assert(fieldNames(footer) == Seq("id", "value", "part"), file.path)
                    val stats = parse(file.stats)
                    for (field <- Seq("minValues", "maxValues", "nullCount")) {
                      assert(stats \ field \ "part" == JNothing, file.stats)
                    }
                }
                withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
                  assert(
                    spark.read.format("delta").load(nativePath).orderBy("id").collect().toSeq ==
                      spark.read.format("delta").load(vanillaPath).orderBy("id").collect().toSeq)
                }
            }
        }
    }
  }

  test("Parquet format version 2.12.0 falls back") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        createTable(path, partitioned = false, "delta.parquet.format.version" -> "2.12.0")
        writeRows(path)

        footers(path).foreach { case (file, footer) => assert(!writtenByVelox(footer), file.path) }
        assertRows(path)
    }
  }

  private def timestampTypeName(path: String, column: String): Seq[PrimitiveTypeName] =
    footers(path).map {
      case (_, footer) =>
        footer.getFileMetaData.getSchema.getFields.asScala
          .find(_.getName == column)
          .get
          .asPrimitiveType
          .getPrimitiveTypeName
    }

  test("timestamps are written natively only when Spark writes them as microseconds") {
    def writeTimestamps(path: String, tblProperties: String = ""): Unit = {
      sql(s"CREATE TABLE delta.`$path` (id BIGINT, ts TIMESTAMP) USING delta $tblProperties")
      spark
        .range(10)
        .selectExpr("id", "timestamp_seconds(1704067200 + id) AS ts")
        .coalesce(1)
        .write
        .format("delta")
        .mode("append")
        .save(path)
    }
    withTempDir {
      dir =>
        // Spark writes INT96 by default.
        val path = dir.getCanonicalPath
        writeTimestamps(path)
        footers(path).foreach { case (file, footer) => assert(!writtenByVelox(footer), file.path) }
        assert(timestampTypeName(path, "ts").forall(_ == PrimitiveTypeName.INT96))
    }
    withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "TIMESTAMP_MICROS") {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          writeTimestamps(path)
          footers(path).foreach { case (file, footer) => assert(writtenByVelox(footer), file.path) }
          assert(timestampTypeName(path, "ts").forall(_ == PrimitiveTypeName.INT64))
      }
      withTempDir {
        dir =>
          // Table properties reach vanilla's writer through the job conf; Spark 4.2 honors this one.
          val path = dir.getCanonicalPath
          writeTimestamps(
            path,
            s"TBLPROPERTIES ('${SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key}' = 'INT96')")
          footers(path).foreach {
            case (file, footer) => assert(!writtenByVelox(footer), file.path)
          }
      }
    }
  }

  test("TIMESTAMP_NTZ falls back, at any depth") {
    for (column <- Seq("ts", "named_struct('ts', ts)")) {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          val data = spark
            .range(10)
            .selectExpr("id", "CAST(timestamp_seconds(1704067200 + id) AS TIMESTAMP_NTZ) AS ts")
            .selectExpr("id", s"$column AS ts")
          // A DataFrame write, which Gluten offloads; plain INSERT INTO is never offloaded.
          data.coalesce(1).write.format("delta").save(path)

          footers(path).foreach {
            case (file, footer) =>
              assert(!writtenByVelox(footer), s"$column: ${file.path}")
              // The Delta protocol stores TIMESTAMP_NTZ with isAdjustedToUTC=false.
              def child(group: GroupType) = group.getFields.asScala.find(_.getName == "ts").get
              val field = child(footer.getFileMetaData.getSchema)
              val leaf = if (field.isPrimitive) field else child(field.asGroupType)
              assert(
                leaf.getLogicalTypeAnnotation ==
                  LogicalTypeAnnotation.timestampType(false, TimeUnit.MICROS),
                s"$column: $leaf")
          }
          assert(
            spark.read.format("delta").load(path).collect().toSet == data.collect().toSet,
            column)
      }
    }
  }

  test("maxRecordsPerFile falls back and splits files as vanilla Delta does") {
    for (useOption <- Seq(true, false)) {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          createTable(path, partitioned = false)
          val writer = spark
            .range(25)
            .selectExpr("id", "CAST(id AS STRING) AS value", "CAST(id % 2 AS INT) AS part")
            .coalesce(1)
            .write
            .format("delta")
            .mode("append")
          if (useOption) {
            writer.option("maxRecordsPerFile", "10").save(path)
          } else {
            withSQLConf(SQLConf.MAX_RECORDS_PER_FILE.key -> "10") {
              writer.save(path)
            }
          }
          val files = footers(path)
          files.foreach { case (file, footer) => assert(!writtenByVelox(footer), file.path) }
          assert(files.map(_._1.numLogicalRecords.get).sorted == Seq(5L, 10L, 10L), useOption)
      }
    }
  }

  for (mode <- Seq("name", "id"); partitioned <- Seq(false, true)) {
    test(s"$mode column mapping falls back and writes field IDs: partitioned=$partitioned") {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          createTable(
            path,
            partitioned,
            "delta.columnMapping.mode" -> mode,
            "delta.feature.materializePartitionColumns" -> "supported")
          writeRows(path)

          footers(path).foreach {
            case (file, footer) =>
              assert(!writtenByVelox(footer), file.path)
              assert(
                footer.getFileMetaData.getSchema.getFields.asScala.forall(_.getId != null),
                file.path)
          }
          assertRows(path)
      }
    }
  }
}

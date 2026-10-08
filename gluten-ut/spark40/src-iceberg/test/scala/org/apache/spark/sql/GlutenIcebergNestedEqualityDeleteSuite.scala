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
package org.apache.spark.sql

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.WholeStageTransformerSuite

import org.apache.spark.SparkConf

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.util.HadoopInputFile

import java.nio.file.Files

class GlutenIcebergNestedEqualityDeleteSuite extends WholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"
  private lazy val icebergWarehouse = Files.createTempDirectory("gluten-nested-delete").toFile

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.shuffle.partitions", "1")
    .set(
      "spark.sql.extensions",
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .set("spark.sql.catalog.spark_catalog.type", "hadoop")
    .set("spark.sql.catalog.spark_catalog.warehouse", icebergWarehouse.toURI.toString)
    .set("spark.gluten.sql.columnar.iceberg.enableNativeWrite", "true")
    .set("spark.gluten.sql.columnar.iceberg.enableNativeEqualityDelete", "true")

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally FileUtils.deleteDirectory(icebergWarehouse)
  }

  private def create(columns: String, values: String): Unit = {
    assume(BackendsApiManager.getBackendName == "velox")
    spark.sql(s"""CREATE TABLE nested_deletes ($columns) USING iceberg
                 |TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read',
                 |'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')
                 |""".stripMargin)
    withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
      spark.sql(s"INSERT INTO nested_deletes VALUES $values")
    }
  }

  private def assertNoEqualityDeletes(): Unit = {
    checkAnswer(
      spark.sql("""SELECT count(*) FROM spark_catalog.default.nested_deletes.delete_files
                  |WHERE content = 2""".stripMargin),
      Seq(Row(0L))
    )
  }

  test("nested DELETE UPDATE and MERGE predicates leave unrelated projections readable") {
    val commands = Seq(
      "DELETE FROM nested_deletes WHERE nested.key IS NULL" -> Seq(3),
      "UPDATE nested_deletes SET id = id + 10 WHERE nested.key IS NULL" -> Seq(11, 12, 3),
      """MERGE INTO nested_deletes t USING (SELECT 3 AS key) s ON t.nested.key = s.key
        |WHEN MATCHED THEN DELETE""".stripMargin -> Seq(1, 2)
    )
    commands.foreach {
      case (command, expectedIds) =>
        withTable("nested_deletes") {
          create(
            "id INT, nested STRUCT<key:INT>",
            "(1, NULL), (2, named_struct('key', NULL)), (3, named_struct('key', 3))")
          spark.sql(command)
          checkAnswer(spark.sql("SELECT id FROM nested_deletes"), expectedIds.map(Row(_)))
          checkAnswer(
            spark.sql("SELECT count(*) FROM nested_deletes"),
            Seq(Row(expectedIds.size.toLong)))
          assertNoEqualityDeletes()
        }
    }
  }

  test("an unconditional UPDATE skips nested fields when choosing an equality key") {
    withTable("nested_deletes") {
      create(
        "nested STRUCT<key:INT>, id INT",
        "(named_struct('key', 1), 1), (named_struct('key', 2), 2)")
      spark.sql("UPDATE nested_deletes SET id = id + 10")
      checkAnswer(spark.sql("SELECT id FROM nested_deletes"), Seq(Row(11), Row(12)))
      checkAnswer(spark.sql("SELECT count(*) FROM nested_deletes"), Seq(Row(2L)))
      checkAnswer(
        spark.sql("""SELECT DISTINCT equality_ids
                    |FROM spark_catalog.default.nested_deletes.delete_files WHERE content = 2
                    |""".stripMargin),
        Seq(Row(Seq(2)))
      )
    }
  }

  test("an unconditional UPDATE falls back when no top-level equality key is supported") {
    withTable("nested_deletes") {
      create(
        "nested STRUCT<key:INT>, amount DOUBLE",
        "(named_struct('key', 1), 1.0), (named_struct('key', 2), 2.0)")
      spark.sql("UPDATE nested_deletes SET amount = amount + 1")
      checkAnswer(spark.sql("SELECT amount FROM nested_deletes"), Seq(Row(2.0), Row(3.0)))
      checkAnswer(spark.sql("SELECT count(*) FROM nested_deletes"), Seq(Row(2L)))
      assertNoEqualityDeletes()
    }
  }

  test("native equality deletes preserve Parquet field names and Iceberg metrics policies") {
    Seq(
      "DELETE FROM nested_deletes WHERE `key-with-dash` IS NULL AND value = 'abcdef'",
      "UPDATE nested_deletes SET id = 10 WHERE `key-with-dash` IS NULL AND value = 'abcdef'"
    ).foreach {
      command =>
        withTable("nested_deletes") {
          create(
            "id INT, `key-with-dash` INT, value STRING",
            "(1, NULL, 'abcdef'), (2, NULL, 'keep'), (3, 3, 'abcdef')")
          spark.sql("""ALTER TABLE nested_deletes SET TBLPROPERTIES
                      |('write.metadata.metrics.column.key-with-dash'='none',
                      |'write.metadata.metrics.column.value'='truncate(2)')""".stripMargin)
          spark.sql(command)
          val files =
            spark.sql("""SELECT file_path, equality_ids, value_counts, lower_bounds, upper_bounds
                        |FROM spark_catalog.default.nested_deletes.delete_files
                        |WHERE content = 2""".stripMargin).collect()
          assert(files.nonEmpty)
          files.foreach {
            file =>
              assert(file.getSeq[Int](1).toSet == Set(2, 3))
              assert(!file.getMap[Int, Long](2).contains(2))
              assert(new String(
                file.getMap[Int, Array[Byte]](3)(3),
                java.nio.charset.StandardCharsets.UTF_8) == "ab")
              assert(new String(
                file.getMap[Int, Array[Byte]](4)(3),
                java.nio.charset.StandardCharsets.UTF_8) == "ac")
              val reader = ParquetFileReader.open(HadoopInputFile.fromPath(
                new Path(file.getString(0)),
                spark.sessionState.newHadoopConf()))
              try {
                val metadata = reader.getFooter.getFileMetaData
                assert(metadata.getCreatedBy.startsWith("parquet-cpp-velox"))
                assert(
                  metadata.getSchema.asGroupType().getType("key-with-dash").getId.intValue() == 2)
              } finally reader.close()
          }
          val expected =
            if (command.startsWith("UPDATE")) Seq(Row(10), Row(2), Row(3)) else Seq(Row(2), Row(3))
          checkAnswer(spark.sql("SELECT id FROM nested_deletes"), expected)
        }
    }
  }

}

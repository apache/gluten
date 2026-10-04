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

import org.apache.gluten.config.{GlutenConfig, GlutenIcebergConfig}

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.{CommandExecutionMode, CommandResultExec, SparkPlan}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.iceberg.{Table => IcebergTable}
import org.apache.iceberg.exceptions.ValidationException
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.iceberg.spark.source.SparkTable

import java.nio.file.Files

import scala.collection.JavaConverters._

class VeloxIcebergEqualityDeleteSuite extends WholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"
  private lazy val warehouse = Files.createTempDirectory("gluten-equality-delete").toFile

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.shuffle.partitions", "1")
    .set("spark.sql.ansi.enabled", "false")
    .set("spark.sql.session.timeZone", "UTC")
    .set(
      "spark.sql.extensions",
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .set("spark.sql.catalog.spark_catalog.type", "hadoop")
    .set("spark.sql.catalog.spark_catalog.warehouse", warehouse.toURI.toString)
    .set(GlutenIcebergConfig.ENABLE_NATIVE_EQUALITY_DELETE.key, "true")

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally FileUtils.deleteDirectory(warehouse)
  }

  private def table(name: String): IcebergTable = spark.sessionState.catalogManager
    .catalog("spark_catalog").asInstanceOf[TableCatalog]
    .loadTable(Identifier.of(Array("default"), name)).asInstanceOf[SparkTable].table()

  private def create(name: String, columns: String, partition: String = ""): Unit = {
    spark.sql(s"""CREATE TABLE $name ($columns) USING iceberg $partition
                 |TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')
                 |""".stripMargin)
  }

  private def insert(name: String, values: String): Unit = {
    withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
      spark.sql(s"INSERT INTO $name VALUES $values")
    }
  }

  private def delete(query: String): Unit = {
    val df = spark.sql(query)
    val plan = df.queryExecution.executedPlan.asInstanceOf[CommandResultExec].commandPhysicalPlan
    assert(isEqualityWrite(plan), plan)
  }

  private def isEqualityWrite(plan: SparkPlan): Boolean = plan.exists {
    case _: VeloxIcebergEqualityDeleteExec | _: VeloxIcebergRowDeltaExec => true
    case _ => false
  }

  private def planned(query: String): SparkPlan = spark.sessionState
    .executePlan(spark.sessionState.sqlParser.parsePlan(query), CommandExecutionMode.SKIP)
    .executedPlan

  private def equalityIds(name: String): Seq[Seq[Int]] = {
    val files = spark.sql(
      s"SELECT content, equality_ids, file_path FROM spark_catalog.default.$name.delete_files").collect()
    assert(files.nonEmpty)
    files.map {
      row =>
        assert(row.getInt(0) == 2, row)
        val input = HadoopInputFile.fromPath(
          new Path(row.getString(2)),
          spark.sessionState.newHadoopConf())
        val reader = ParquetFileReader.open(input)
        try {
          assert(
            reader.getFooter.getFileMetaData.getCreatedBy.toLowerCase(java.util.Locale.ROOT)
              .contains("velox"),
            reader.getFooter.getFileMetaData)
        } finally reader.close()
        row.getSeq[Int](1).toSeq
    }.toSeq
  }

  test("SQL DELETE writes native equality files and reads back the surviving rows") {
    Seq(false, true).foreach {
      adaptive =>
        withSQLConf("spark.sql.adaptive.enabled" -> adaptive.toString) {
          withTable("eq_basic") {
            create("eq_basic", "id BIGINT, value STRING")
            insert("eq_basic", "(1, 'a'), (2, 'b'), (3, 'c')")
            delete("DELETE FROM eq_basic WHERE id = 2")
            assert(equalityIds("eq_basic").forall(_ == Seq(1)))
            checkAnswer(spark.table("eq_basic"), Seq(Row(1L, "a"), Row(3L, "c")))
            insert("eq_basic", "(2, 'new')")
            checkAnswer(spark.table("eq_basic"), Seq(Row(1L, "a"), Row(3L, "c"), Row(2L, "new")))
            delete("DELETE FROM eq_basic WHERE id = 3")
            checkAnswer(spark.table("eq_basic"), Seq(Row(1L, "a"), Row(2L, "new")))
          }
        }
    }
  }

  test("all predicate columns are keys, including null values and evolved field IDs") {
    withTable("eq_keys") {
      create("eq_keys", "unused INT, id INT, flag STRING")
      spark.sql("ALTER TABLE eq_keys DROP COLUMN unused")
      insert("eq_keys", "(1, 'delete'), (1, 'keep'), (NULL, 'delete'), (NULL, 'keep'), (2, 'keep')")
      delete("DELETE FROM eq_keys WHERE (id = 1 OR id IS NULL) AND flag = 'delete'")
      assert(equalityIds("eq_keys").forall(_ == Seq(2, 3)))
      checkAnswer(spark.table("eq_keys"), Seq(Row(1, "keep"), Row(null, "keep"), Row(2, "keep")))
    }
  }

  test("partition columns are written without becoming extra equality keys") {
    Seq("PARTITIONED BY (part)", "PARTITIONED BY (bucket(2, part))").foreach {
      partition =>
        withTable("eq_partitioned") {
          create("eq_partitioned", "id INT, part INT, value STRING", partition)
          insert("eq_partitioned", "(1, 10, 'a'), (2, 10, 'b'), (1, 20, 'c'), (3, 20, 'd')")
          delete("DELETE FROM eq_partitioned WHERE id = 1")
          assert(equalityIds("eq_partitioned").forall(_ == Seq(1)))
          checkAnswer(spark.table("eq_partitioned"), Seq(Row(2, 10, "b"), Row(3, 20, "d")))
        }
    }
  }

  test("an empty delete does not commit a snapshot or file") {
    withTable("eq_empty") {
      create("eq_empty", "id INT")
      insert("eq_empty", "(1), (2)")
      val snapshot = table("eq_empty").currentSnapshot().snapshotId()
      delete("DELETE FROM eq_empty WHERE id = 99")
      assert(table("eq_empty").currentSnapshot().snapshotId() == snapshot)
      assert(spark.table("spark_catalog.default.eq_empty.delete_files").count() == 0)
      checkAnswer(spark.table("eq_empty"), Seq(Row(1), Row(2)))
    }
  }

  test("native delete refreshes cached table results") {
    withTable("eq_cached") {
      create("eq_cached", "id INT")
      insert("eq_cached", "(1), (2), (3)")
      spark.catalog.cacheTable("eq_cached")
      try {
        assert(spark.table("eq_cached").count() == 3)
        delete("DELETE FROM eq_cached WHERE id = 2")
        checkAnswer(spark.table("eq_cached"), Seq(Row(1), Row(3)))
      } finally spark.catalog.uncacheTable("eq_cached")
    }
  }

  test("DELETE reads its snapshot even when Spark has cached an older table snapshot") {
    withTable("eq_stale_cache", "eq_source") {
      create("eq_stale_cache", "id INT")
      create("eq_source", "id INT")
      insert("eq_stale_cache", "(1)")
      insert("eq_source", "(2)")
      spark.catalog.cacheTable("eq_stale_cache")
      try {
        assert(spark.table("eq_stale_cache").count() == 1)
        val tasks = table("eq_source").newScan().planFiles()
        try table("eq_stale_cache").newAppend().appendFile(tasks.iterator().next().file()).commit()
        finally tasks.close()
        assert(spark.table("eq_stale_cache").count() == 1)
        delete("DELETE FROM eq_stale_cache WHERE id = 2")
        assert(equalityIds("eq_stale_cache").forall(_ == Seq(1)))
        checkAnswer(spark.table("eq_stale_cache"), Seq(Row(1)))
      } finally spark.catalog.uncacheTable("eq_stale_cache")
    }
  }

  test("unsupported predicates and disabled native writes retain Iceberg's delete plan") {
    withTable("eq_fallback") {
      create("eq_fallback", "id INT, amount DOUBLE, nested STRUCT<id:INT>")
      insert("eq_fallback", "(1, 1.0, named_struct('id', 1)), (2, 2.0, named_struct('id', 2))")
      Seq("amount = 1.0", "nested IS NULL").foreach {
        predicate =>
          val plan = planned(s"DELETE FROM eq_fallback WHERE $predicate")
          assert(!isEqualityWrite(plan), plan)
      }
      Seq(
        GlutenConfig.GLUTEN_ENABLED.key,
        GlutenIcebergConfig.ENABLE_NATIVE_WRITE.key,
        GlutenIcebergConfig.ENABLE_NATIVE_EQUALITY_DELETE.key).foreach {
        key =>
          withSQLConf(key -> "false") {
            val plan = planned("DELETE FROM eq_fallback WHERE id = 1")
            assert(!isEqualityWrite(plan), plan)
          }
      }
      spark.sql("DELETE FROM eq_fallback WHERE amount = 1.0")
      checkAnswer(spark.table("eq_fallback"), Seq(Row(2, 2.0, Row(2))))
    }
  }

  test("format v3 equality deletes preserve supported primitive key types") {
    withTable("eq_v3") {
      create("eq_v3", "id INT, b BOOLEAN, d DATE, ts TIMESTAMP, amount DECIMAL(12,2), data BINARY")
      spark.sql("ALTER TABLE eq_v3 SET TBLPROPERTIES ('format-version'='3')")
      insert(
        "eq_v3",
        """(1, TRUE, DATE '2026-01-01', TIMESTAMP '2026-01-01 01:02:03.123456', 12.34, X'61'),
          |(2, FALSE, DATE '2026-01-02', TIMESTAMP '2026-01-02 00:00:00', 56.78, X'62')
          |""".stripMargin
      )
      delete("""DELETE FROM eq_v3 WHERE id = 1 AND b = TRUE AND d = DATE '2026-01-01'
               |AND ts = TIMESTAMP '2026-01-01 01:02:03.123456' AND amount = 12.34 AND data = X'61'
               |""".stripMargin)
      assert(equalityIds("eq_v3").forall(_ == Seq(1, 2, 3, 4, 5, 6)))
      checkAnswer(spark.sql("SELECT id FROM eq_v3"), Seq(Row(2)))
    }
  }

  test("native mixed timestamp keys retain per-field annotations, nulls and microseconds") {
    Seq("UTC", "America/Los_Angeles").foreach {
      zone =>
        withSQLConf("spark.sql.session.timeZone" -> zone) {
          Seq(
            "DELETE FROM eq_timestamps" -> "",
            "UPDATE eq_timestamps SET id = id + 10" -> "",
            "DELETE FROM eq_timestamps" -> "PARTITIONED BY (days(ts))").foreach {
            case (command, partition) =>
              withTable("eq_timestamps") {
                create("eq_timestamps", "id INT, ts TIMESTAMP_NTZ, zoned TIMESTAMP", partition)
                spark.sql("ALTER TABLE eq_timestamps SET TBLPROPERTIES ('write.update.mode'='merge-on-read')")
                insert(
                  "eq_timestamps",
                  """(1, TIMESTAMP_NTZ '1969-12-31 23:59:59.999999', TIMESTAMP '2026-01-01 01:02:03.123456'),
                    |(2, TIMESTAMP_NTZ '2026-01-02 00:00:00', TIMESTAMP '2026-01-02 00:00:00'),
                    |(3, NULL, NULL), (4, NULL, TIMESTAMP '2026-01-02 00:00:00')""".stripMargin
                )
                delete(s"""$command WHERE (ts = TIMESTAMP_NTZ '1969-12-31 23:59:59.999999'
                          |AND zoned = TIMESTAMP '2026-01-01 01:02:03.123456')
                          |OR (ts IS NULL AND zoned IS NULL)""".stripMargin)
                assert(equalityIds("eq_timestamps").forall(_.toSet == Set(2, 3)))
                spark.sql(
                  "SELECT file_path FROM spark_catalog.default.eq_timestamps.delete_files").collect().foreach {
                  row =>
                    val reader = ParquetFileReader.open(HadoopInputFile.fromPath(
                      new Path(row.getString(0)),
                      spark.sessionState.newHadoopConf()))
                    try {
                      val schema = reader.getFooter.getFileMetaData.getSchema.asGroupType()
                      def timestamp(name: String) = schema.getType(name).getLogicalTypeAnnotation
                        .asInstanceOf[
                          org.apache.iceberg.shaded.org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation]
                      assert(!timestamp("ts").isAdjustedToUTC)
                      assert(timestamp("zoned").isAdjustedToUTC)
                      assert(timestamp("ts").getUnit.toString == "MICROS")
                      assert(timestamp("zoned").getUnit.toString == "MICROS")
                    } finally reader.close()
                }
                val ids = if (command.startsWith("UPDATE")) Seq(11, 2, 13, 4) else Seq(2, 4)
                checkAnswer(spark.sql("SELECT id FROM eq_timestamps"), ids.map(Row(_)))
              }
          }
        }
    }
  }

  test("delete compression uses Iceberg session overrides ahead of table properties") {
    withTable("eq_compression") {
      create("eq_compression", "id INT")
      insert("eq_compression", "(1), (2), (3)")
      spark.sql("""ALTER TABLE eq_compression SET TBLPROPERTIES
                  |('write.parquet.compression-codec'='snappy',
                  | 'write.delete.parquet.compression-codec'='gzip')""".stripMargin)
      withSQLConf(
        "spark.sql.iceberg.compression-codec" -> "zstd",
        "spark.sql.iceberg.compression-level" -> "1") {
        delete("DELETE FROM eq_compression WHERE id = 1")
      }
      val paths = spark.sql(
        "SELECT file_path FROM spark_catalog.default.eq_compression.delete_files").collect()
      assert(paths.nonEmpty)
      paths.foreach {
        row =>
          val input =
            HadoopInputFile.fromPath(new Path(row.getString(0)), spark.sessionState.newHadoopConf())
          val reader = ParquetFileReader.open(input)
          try {
            assert(reader.getFooter.getBlocks.asScala.flatMap(_.getColumns.asScala)
              .forall(_.getCodec.name() == "ZSTD"))
          } finally reader.close()
      }
      withSQLConf("spark.sql.iceberg.compression-codec" -> "brotli") {
        val plan = planned("DELETE FROM eq_compression WHERE id = 2")
        assert(!isEqualityWrite(plan), plan)
      }
    }
  }

  test("native DELETE and UPDATE preserve the distinction between LZ4 and LZ4_RAW") {
    Seq("lz4", "lz4_raw").foreach {
      codec =>
        Seq("DELETE FROM eq_lz4 WHERE id = 1", "UPDATE eq_lz4 SET id = 3 WHERE id = 1").foreach {
          command =>
            withTable("eq_lz4") {
              create("eq_lz4", "id INT")
              insert("eq_lz4", "(1), (2)")
              spark.sql(s"""ALTER TABLE eq_lz4 SET TBLPROPERTIES
                           |('write.update.mode'='merge-on-read',
                           |'write.delete.parquet.compression-codec'='$codec')""".stripMargin)
              delete(command)
              assert(equalityIds("eq_lz4").forall(_ == Seq(1)))
              spark.sql(
                "SELECT file_path FROM spark_catalog.default.eq_lz4.delete_files").collect().foreach {
                row =>
                  val reader = ParquetFileReader.open(HadoopInputFile.fromPath(
                    new Path(row.getString(0)),
                    spark.sessionState.newHadoopConf()))
                  try {
                    assert(reader.getFooter.getBlocks.asScala.flatMap(_.getColumns.asScala)
                      .forall(_.getCodec.name() == codec.toUpperCase(java.util.Locale.ROOT)))
                  } finally reader.close()
              }
              checkAnswer(
                spark.table("eq_lz4"),
                if (command.startsWith("UPDATE")) Seq(Row(3), Row(2)) else Seq(Row(2)))
            }
        }
    }
  }

  test("a concurrent matching insert fails SQL DELETE and cleans its task output") {
    withTable("eq_concurrent") {
      create("eq_concurrent", "id INT")
      insert("eq_concurrent", "(1), (2)")
      val logical = spark.sessionState.executePlan(
        spark.sessionState.sqlParser.parsePlan("DELETE FROM eq_concurrent WHERE id = 1"),
        CommandExecutionMode.SKIP).analyzed
      insert("eq_concurrent", "(1)")
      val icebergTable = table("eq_concurrent")
      val snapshot = icebergTable.currentSnapshot().snapshotId()
      val directory = new java.io.File(new java.net.URI(icebergTable.location()))
      def parquetFiles: Set[String] = FileUtils.listFiles(directory, Array("parquet"), true)
        .asScala.map(_.getAbsolutePath).toSet
      val before = parquetFiles
      intercept[ValidationException] {
        spark.sessionState.executePlan(logical).executedPlan.executeCollect()
      }
      assert(table("eq_concurrent").currentSnapshot().snapshotId() == snapshot)
      assert(parquetFiles == before)
      checkAnswer(spark.table("eq_concurrent"), Seq(Row(1), Row(2), Row(1)))
    }
  }

  test("copy-on-write retains Iceberg's configured write mode") {
    withTable("eq_mode") {
      create("eq_mode", "id INT")
      insert("eq_mode", "(1), (2)")
      spark.sql("ALTER TABLE eq_mode SET TBLPROPERTIES ('write.delete.mode'='copy-on-write')")
      assert(!isEqualityWrite(planned("DELETE FROM eq_mode WHERE id = 1")))
    }
  }

  test("nested nullable predicates fall back and leave unrelated projections readable") {
    withTable("eq_nested") {
      create("eq_nested", "id INT, nested STRUCT<key:INT, other:ARRAY<INT>>")
      insert(
        "eq_nested",
        "(1, NULL), (2, named_struct('key', NULL, 'other', array(2))), " +
          "(3, named_struct('key', 3, 'other', array(3)))")
      val query = "DELETE FROM eq_nested WHERE nested.key IS NULL"
      assert(!isEqualityWrite(planned(query)))
      spark.sql(query)
      checkAnswer(spark.table("eq_nested"), Seq(Row(3, Row(3, Seq(3)))))
      checkAnswer(spark.sql("SELECT id FROM eq_nested"), Seq(Row(3)))
      checkAnswer(spark.sql("SELECT count(*) FROM eq_nested"), Seq(Row(1L)))
    }
  }

  test("subqueries preserve all correlated target keys") {
    withTable("eq_subquery", "eq_source") {
      create("eq_subquery", "id INT, flag STRING")
      create("eq_source", "id INT, flag STRING")
      insert("eq_subquery", "(1, 'remove'), (1, 'keep'), (2, 'keep')")
      insert("eq_source", "(1, 'remove')")
      delete("""DELETE FROM eq_subquery t WHERE EXISTS
               |(SELECT 1 FROM eq_source s WHERE s.id = t.id AND s.flag = t.flag)""".stripMargin)
      assert(equalityIds("eq_subquery").forall(_.toSet == Set(1, 2)))
      checkAnswer(spark.table("eq_subquery"), Seq(Row(1, "keep"), Row(2, "keep")))
    }
  }

  test("unconditional deletes and initially empty tables") {
    withTable("eq_all") {
      create("eq_all", "id INT, amount DOUBLE")
      delete("DELETE FROM eq_all")
      assert(table("eq_all").currentSnapshot() == null)
      insert("eq_all", "(NULL, 1.0), (1, 2.0), (1, 3.0)")
      delete("DELETE FROM eq_all")
      checkAnswer(spark.table("eq_all"), Nil)
      assert(equalityIds("eq_all").forall(_ == Seq(1)))
    }
  }

  test("partition evolution routes deletes to each original spec and partition") {
    withTable("eq_evolved") {
      create("eq_evolved", "id INT, part INT", "PARTITIONED BY (part)")
      insert("eq_evolved", "(1, 10), (2, 20)")
      spark.sql("ALTER TABLE eq_evolved ADD PARTITION FIELD bucket(2, id)")
      insert("eq_evolved", "(1, 30), (3, 40)")
      delete("DELETE FROM eq_evolved WHERE id = 1")
      checkAnswer(spark.table("eq_evolved"), Seq(Row(2, 20), Row(3, 40)))
      checkAnswer(
        spark.sql("SELECT DISTINCT spec_id FROM spark_catalog.default.eq_evolved.delete_files"),
        Seq(Row(0), Row(1)))
    }
  }

  test("dropping a partition field preserves deletes for both old and void partition specs") {
    withTable("eq_void") {
      // V1 retains removed partition fields as void transforms. Upgrade before deleting.
      spark.sql(
        """CREATE TABLE eq_void (id INT, part INT) USING iceberg PARTITIONED BY (part)
          |TBLPROPERTIES ('format-version'='1', 'write.delete.mode'='merge-on-read')""".stripMargin)
      insert("eq_void", "(1, 10), (2, 10)")
      spark.sql("ALTER TABLE eq_void DROP PARTITION FIELD part")
      assert(table("eq_void").spec().fields().asScala.exists(_.transform().isVoid))
      spark.sql("ALTER TABLE eq_void SET TBLPROPERTIES ('format-version'='2')")
      insert("eq_void", "(1, 20), (3, 30)")
      delete("DELETE FROM eq_void WHERE id = 1")
      assert(equalityIds("eq_void").forall(_ == Seq(1)))
      checkAnswer(spark.table("eq_void"), Seq(Row(2, 10), Row(3, 30)))
      checkAnswer(
        spark.sql("SELECT DISTINCT spec_id FROM spark_catalog.default.eq_void.delete_files"),
        Seq(Row(0), Row(1)))
    }
  }

  test("snapshot isolation preserves concurrent inserts with the same equality key") {
    withTable("eq_snapshot") {
      create("eq_snapshot", "id INT, value STRING")
      insert("eq_snapshot", "(1, 'old'), (2, 'keep')")
      spark.sql(
        "ALTER TABLE eq_snapshot SET TBLPROPERTIES ('write.delete.isolation-level'='snapshot')")
      val logical = spark.sessionState.executePlan(
        spark.sessionState.sqlParser.parsePlan("DELETE FROM eq_snapshot WHERE id = 1"),
        CommandExecutionMode.SKIP).analyzed
      val sequence = table("eq_snapshot").currentSnapshot().sequenceNumber()
      insert("eq_snapshot", "(1, 'concurrent')")
      val plan = spark.sessionState.executePlan(logical).executedPlan
      plan.executeCollect()
      checkAnswer(spark.table("eq_snapshot"), Seq(Row(2, "keep"), Row(1, "concurrent")))
      checkAnswer(
        spark.sql("SELECT sequence_number FROM spark_catalog.default.eq_snapshot.entries WHERE data_file.content = 2"),
        Seq(Row(sequence + 1)))
    }
  }

  test(
    "UPDATE commits old keys and replacement rows atomically including key and partition changes") {
    Seq(2, 3).foreach {
      version =>
        withTable("eq_update") {
          create("eq_update", "id INT, part INT, value STRING", "PARTITIONED BY (part)")
          spark.sql(s"ALTER TABLE eq_update SET TBLPROPERTIES ('write.update.mode'='merge-on-read', 'format-version'='$version')")
          insert("eq_update", "(1, 10, 'a'), (2, 20, 'b'), (3, 30, 'keep')")
          val count = table("eq_update").snapshots().asScala.size
          val oldRowIds = if (version == 3) {
            spark.sql("SELECT id, _row_id FROM eq_update").collect().map(
              r => r.getInt(0) -> r.getLong(1)).toMap
          } else Map.empty[Int, Long]
          delete("UPDATE eq_update SET id = 3 - id, part = part + 100, value = concat(value, '!') WHERE id IN (1, 2)")
          assert(table("eq_update").snapshots().asScala.size == count + 1)
          checkAnswer(
            spark.table("eq_update"),
            Seq(Row(2, 110, "a!"), Row(1, 120, "b!"), Row(3, 30, "keep")))
          assert(equalityIds("eq_update").forall(_ == Seq(1)))
          if (version == 3) {
            checkAnswer(
              spark.sql("SELECT id, _row_id FROM eq_update"),
              Seq(Row(2, oldRowIds(1)), Row(1, oldRowIds(2)), Row(3, oldRowIds(3))))
          }
        }
    }
  }

  test("MERGE handles conditional updates deletes inserts and not matched by source") {
    withTable("eq_merge", "eq_source") {
      create("eq_merge", "id INT, flag STRING, part INT", "PARTITIONED BY (part)")
      create("eq_source", "id INT, action STRING")
      spark.sql("ALTER TABLE eq_merge SET TBLPROPERTIES ('write.merge.mode'='merge-on-read')")
      insert("eq_merge", "(1, 'change', 10), (1, 'keep', 10), (2, 'remove', 20), (3, 'absent', 30)")
      insert("eq_source", "(1, 'update'), (2, 'delete'), (4, 'insert')")
      delete("""MERGE INTO eq_merge t USING eq_source s ON t.id = s.id
               |WHEN MATCHED AND s.action = 'delete' THEN DELETE
               |WHEN MATCHED AND t.flag = 'change' THEN UPDATE SET id = t.id + 10, part = 100
               |WHEN NOT MATCHED THEN INSERT (id, flag, part) VALUES (s.id, 'new', 40)
               |WHEN NOT MATCHED BY SOURCE THEN DELETE""".stripMargin)
      checkAnswer(
        spark.table("eq_merge"),
        Seq(Row(11, "change", 100), Row(1, "keep", 10), Row(4, "new", 40)))
      assert(equalityIds("eq_merge").forall(_.toSet == Set(1, 2)))
    }
  }

  test("MERGE rejects multiple source matches without committing partial output") {
    withTable("eq_cardinality", "eq_source") {
      create("eq_cardinality", "id INT, value STRING")
      create("eq_source", "id INT, value STRING")
      spark.sql("ALTER TABLE eq_cardinality SET TBLPROPERTIES ('write.merge.mode'='merge-on-read')")
      insert("eq_cardinality", "(1, 'old')")
      insert("eq_source", "(1, 'a'), (1, 'b')")
      val snapshot = table("eq_cardinality").currentSnapshot().snapshotId()
      val directory = new java.io.File(new java.net.URI(table("eq_cardinality").location()))
      def files =
        FileUtils.listFiles(directory, Array("parquet"), true).asScala.map(_.getAbsolutePath).toSet
      val before = files
      val error = intercept[Exception] {
        spark.sql("""MERGE INTO eq_cardinality t USING eq_source s ON t.id = s.id
                    |WHEN MATCHED THEN UPDATE SET value = s.value""".stripMargin)
      }
      assert(error.toString.contains("MERGE") || Option(
        error.getCause).exists(_.toString.contains("MERGE")))
      assert(table("eq_cardinality").currentSnapshot().snapshotId() == snapshot)
      assert(files == before)
      checkAnswer(spark.table("eq_cardinality"), Seq(Row(1, "old")))
    }
  }

  test("branches and staged WAP snapshots keep main unchanged until publication") {
    withTable("eq_branch") {
      create("eq_branch", "id INT")
      insert("eq_branch", "(1), (2), (3)")
      spark.sql("ALTER TABLE eq_branch CREATE BRANCH audit")
      delete("DELETE FROM spark_catalog.default.eq_branch.branch_audit WHERE id = 1")
      checkAnswer(spark.table("eq_branch"), Seq(Row(1), Row(2), Row(3)))
      checkAnswer(spark.table("spark_catalog.default.eq_branch.branch_audit"), Seq(Row(2), Row(3)))
      spark.sql("ALTER TABLE eq_branch SET TBLPROPERTIES ('write.wap.enabled'='true')")
      withSQLConf("spark.wap.branch" -> "audit") {
        delete("DELETE FROM eq_branch WHERE id = 2")
      }
      checkAnswer(spark.table("spark_catalog.default.eq_branch.branch_audit"), Seq(Row(3)))
      withSQLConf("spark.wap.id" -> "gluten-delete") {
        delete("DELETE FROM eq_branch WHERE id = 3")
      }
      checkAnswer(spark.table("eq_branch"), Seq(Row(1), Row(2), Row(3)))
      val staged = table("eq_branch").snapshots().asScala.find {
        _.summary().get("wap.id") == "gluten-delete"
      }.get
      table("eq_branch").manageSnapshots().cherrypick(staged.snapshotId()).commit()
      spark.catalog.refreshTable("eq_branch")
      checkAnswer(spark.table("eq_branch"), Seq(Row(1), Row(2)))
    }
  }

  test("Parquet supports non Avro-compatible field names and unsupported formats retain Iceberg planning") {
    Seq("orc", "avro", "parquet").foreach {
      format =>
        withTable("eq_formats") {
          create("eq_formats", "`key-with-dash` INT, value STRING")
          insert("eq_formats", "(1, 'delete'), (2, 'keep')")
          spark.sql(
            s"ALTER TABLE eq_formats SET TBLPROPERTIES ('write.delete.format.default'='$format')")
          val query = "DELETE FROM eq_formats WHERE `key-with-dash` = 1"
          if (format == "parquet") {
            delete(query)
            assert(equalityIds("eq_formats").forall(_ == Seq(1)))
          } else {
            assert(!isEqualityWrite(planned(query)))
            spark.sql(query)
          }
          checkAnswer(spark.table("eq_formats"), Seq(Row(2, "keep")))
          checkAnswer(
            spark.sql(
              "SELECT DISTINCT file_format FROM spark_catalog.default.eq_formats.delete_files"),
            Seq(Row(format.toUpperCase(java.util.Locale.ROOT))))
        }
    }
  }

  test("UUID and fixed keys retain Iceberg planning until native physical encodings are supported") {
    withTable("eq_encodings") {
      create("eq_encodings", "id INT")
      table("eq_encodings").updateSchema()
        .addColumn("uuid", org.apache.iceberg.types.Types.UUIDType.get())
        .addColumn("fixed", org.apache.iceberg.types.Types.FixedType.ofLength(3)).commit()
      spark.catalog.refreshTable("eq_encodings")
      insert(
        "eq_encodings",
        "(1, '00000000-0000-0000-0000-000000000001', X'010203'), " +
          "(2, '00000000-0000-0000-0000-000000000002', X'040506')")
      // Avoid Iceberg 1.10's broken Parquet UUID predicate pushdown.
      val query =
        "DELETE FROM eq_encodings WHERE lower(uuid) = '00000000-0000-0000-0000-000000000001' AND fixed = X'010203'"
      assert(!isEqualityWrite(planned(query)))
      spark.sql(query)
      checkAnswer(spark.sql("SELECT id FROM eq_encodings"), Seq(Row(2)))
    }
  }
  test("unconditional native changes skip UUID and fixed fields when choosing an equality key") {
    Seq("DELETE FROM eq_native_key", "UPDATE eq_native_key SET id = id + 10").foreach {
      command =>
        withTable("eq_native_key") {
          create("eq_native_key", "id INT")
          table("eq_native_key").updateSchema()
            .addColumn("uuid", org.apache.iceberg.types.Types.UUIDType.get())
            .addColumn("fixed", org.apache.iceberg.types.Types.FixedType.ofLength(3))
            .moveFirst("uuid").moveAfter("fixed", "uuid").commit()
          spark.catalog.refreshTable("eq_native_key")
          spark.sql(
            "ALTER TABLE eq_native_key SET TBLPROPERTIES ('write.update.mode'='merge-on-read')")
          insert(
            "eq_native_key",
            "('00000000-0000-0000-0000-000000000001', X'010203', 1), " +
              "('00000000-0000-0000-0000-000000000002', X'040506', 2)")
          delete(command)
          assert(equalityIds("eq_native_key").forall(_ == Seq(1)))
          checkAnswer(
            spark.sql("SELECT id FROM eq_native_key"),
            if (command.startsWith("UPDATE")) Seq(Row(11), Row(12)) else Nil)
        }
    }
  }

  test("schema-only evolution retains newly added columns in the snapshot-pinned scan") {
    withTable("eq_added") {
      create("eq_added", "id INT")
      insert("eq_added", "(1), (2)")
      spark.sql("ALTER TABLE eq_added ADD COLUMN added STRING")
      delete("DELETE FROM eq_added WHERE added IS NULL")
      assert(equalityIds("eq_added").forall(_ == Seq(2)))
      checkAnswer(spark.table("eq_added"), Nil)
    }
  }

  test("evolved partition fanout closes and reopens native writers without losing files") {
    withTable("eq_fanout") {
      create("eq_fanout", "id INT, part INT", "PARTITIONED BY (part)")
      insert("eq_fanout", "(0, 0)")
      spark.sql("ALTER TABLE eq_fanout ADD PARTITION FIELD bucket(2, id)")
      insert("eq_fanout", (1 to 40).map(i => s"($i, $i)").mkString(", "))
      delete("DELETE FROM eq_fanout WHERE id >= 0")
      val paths =
        spark.sql("SELECT file_path FROM spark_catalog.default.eq_fanout.delete_files").collect()
      assert(paths.length == 41)
      assert(paths.forall(_.getString(0).contains("-equality-delete-")))
      checkAnswer(spark.table("eq_fanout"), Nil)
    }
  }

  test("a new WAP branch starts from main's snapshot") {
    withTable("eq_new_branch") {
      create("eq_new_branch", "id INT")
      insert("eq_new_branch", "(1), (2)")
      spark.sql("ALTER TABLE eq_new_branch SET TBLPROPERTIES ('write.wap.enabled'='true')")
      withSQLConf("spark.wap.branch" -> "new_audit") {
        delete("DELETE FROM eq_new_branch WHERE id = 1")
      }
      checkAnswer(spark.table("eq_new_branch"), Seq(Row(1), Row(2)))
      checkAnswer(spark.table("spark_catalog.default.eq_new_branch.branch_new_audit"), Seq(Row(2)))
    }
  }

  test("MERGE into an initially empty table commits inserts without delete files") {
    withTable("eq_empty_merge", "eq_source") {
      create("eq_empty_merge", "id INT, value STRING")
      create("eq_source", "id INT, value STRING")
      spark.sql("ALTER TABLE eq_empty_merge SET TBLPROPERTIES ('write.merge.mode'='merge-on-read')")
      insert("eq_source", "(1, 'a'), (2, 'b')")
      delete("""MERGE INTO eq_empty_merge t USING eq_source s ON t.id = s.id
               |WHEN MATCHED THEN UPDATE SET value = s.value
               |WHEN NOT MATCHED THEN INSERT *""".stripMargin)
      checkAnswer(spark.table("eq_empty_merge"), Seq(Row(1, "a"), Row(2, "b")))
      assert(spark.table("spark_catalog.default.eq_empty_merge.delete_files").count() == 0)
    }
  }

  test("snapshot UPDATE preserves concurrent rows and evaluates a subquery") {
    withTable("eq_snapshot_update", "eq_source") {
      create("eq_snapshot_update", "id INT, value STRING")
      create("eq_source", "id INT")
      spark.sql(
        """ALTER TABLE eq_snapshot_update SET TBLPROPERTIES
          |('write.update.mode'='merge-on-read', 'write.update.isolation-level'='snapshot')""".stripMargin)
      insert("eq_snapshot_update", "(1, 'old')")
      insert("eq_source", "(1)")
      val logical = spark.sessionState.executePlan(
        spark.sessionState.sqlParser.parsePlan(
          "UPDATE eq_snapshot_update SET value = concat(value, '!') WHERE id IN (SELECT id FROM eq_source)"),
        CommandExecutionMode.SKIP
      ).analyzed
      insert("eq_snapshot_update", "(1, 'concurrent')")
      spark.sessionState.executePlan(logical).executedPlan.executeCollect()
      checkAnswer(spark.table("eq_snapshot_update"), Seq(Row(1, "old!"), Row(1, "concurrent")))
      assert(equalityIds("eq_snapshot_update").forall(_ == Seq(1)))
    }
  }

}

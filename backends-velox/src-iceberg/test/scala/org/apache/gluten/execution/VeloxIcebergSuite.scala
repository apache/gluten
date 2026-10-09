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

import org.apache.gluten.config.{GlutenConfig, VeloxConfig}

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}

import org.apache.iceberg.{FileFormat, StructLike, UpdateSchema}
import org.apache.iceberg.data.{GenericAppenderFactory, GenericRecord}
import org.apache.iceberg.expressions.Literal
import org.apache.iceberg.io.OutputFileFactory
import org.apache.iceberg.spark.source.SparkTable
import org.apache.iceberg.types.{Type, Types}

import scala.collection.JavaConverters._

class VeloxIcebergSuite extends IcebergSuite {
  override protected def vanillaSparkConfs(): Seq[(String, String)] =
    super.vanillaSparkConfs() :+ ("spark.sql.iceberg.executor-cache.delete-files.enabled" -> "false")

  test("iceberg parquet split uses name mapping for projected columns") {
    withTable("iceberg_parquet_name_mapping") {
      withSQLConf(VeloxConfig.PARQUET_USE_COLUMN_NAMES.key -> "false") {
        spark.sql("""
                    |CREATE TABLE iceberg_parquet_name_mapping (
                    |  id BIGINT,
                    |  amount DECIMAL(12, 2),
                    |  note STRING
                    |)
                    |USING iceberg
                    |TBLPROPERTIES ('write.format.default' = 'parquet')
                    |""".stripMargin)
        spark.sql("""
                    |INSERT INTO iceberg_parquet_name_mapping
                    |VALUES (CAST(1 AS BIGINT), CAST(10.50 AS DECIMAL(12, 2)), 'a')
                    |""".stripMargin)

        runQueryAndCompare("SELECT amount FROM iceberg_parquet_name_mapping") {
          df =>
            checkAnswer(df, Seq(Row(BigDecimal("10.50"))))
            checkGlutenPlan[IcebergScanTransformer](df)
        }
      }
    }
  }

  test("iceberg v3 initial default for an added column") {
    withTable("iceberg_v3_initial_default") {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        spark.sql("""
                    |CREATE TABLE iceberg_v3_initial_default (id INT)
                    |USING iceberg
                    |TBLPROPERTIES ('format-version' = '3')
                    |""".stripMargin)
        spark.sql("INSERT INTO iceberg_v3_initial_default VALUES (1), (2)")

        val catalog = spark.sessionState.catalogManager
          .catalog("spark_catalog")
          .asInstanceOf[TableCatalog]
        val updateSchema = catalog
          .loadTable(Identifier.of(Array("default"), "iceberg_v3_initial_default"))
          .asInstanceOf[SparkTable]
          .table()
          .updateSchema()
        classOf[UpdateSchema]
          .getMethod(
            "addColumn",
            classOf[String],
            classOf[Type],
            classOf[Literal[_]])
          .invoke(updateSchema, "country", Types.StringType.get(), Literal.of("IN"))
        updateSchema.commit()
        spark.catalog.refreshTable("iceberg_v3_initial_default")
      }

      runQueryAndCompare(
        "SELECT id, country FROM iceberg_v3_initial_default ORDER BY id") {
        df =>
          checkAnswer(df, Seq(Row(1, "IN"), Row(2, "IN")))
          checkGlutenPlan[IcebergScanTransformer](df)
      }
    }
  }
  private def loadIcebergTable(name: String): org.apache.iceberg.Table = {
    spark.sessionState.catalogManager.catalog("spark_catalog").asInstanceOf[TableCatalog]
      .loadTable(Identifier.of(Array("default"), name)).asInstanceOf[SparkTable].table()
  }

  private def addEqualityDeletes(
      table: org.apache.iceberg.Table,
      format: FileFormat,
      keys: Seq[String],
      rows: Seq[Seq[AnyRef]],
      partition: StructLike = null): Unit = {
    val schema = table.schema().select(keys.asJava)
    val factory = new GenericAppenderFactory(
      table.schema(),
      table.spec(),
      keys.map(table.schema().findField(_).fieldId()).toArray,
      schema,
      null)
    val files = OutputFileFactory.builderFor(table, 1, 1).format(format).build()
    val file = if (partition == null) files.newOutputFile() else files.newOutputFile(partition)
    val writer = factory.newEqDeleteWriter(file, format, partition)
    try {
      rows.foreach {
        values =>
          val record = GenericRecord.create(schema)
          keys.zip(values).foreach { case (name, value) => record.setField(name, value) }
          writer.write(record)
      }
    } finally {
      writer.close()
    }
    table.newRowDelta().addDeletes(writer.toDeleteFile()).commit()
  }

  for (
    dataFormat <- Seq("parquet", "orc"); deleteFormat <- Seq(FileFormat.PARQUET, FileFormat.ORC)
  ) {
    test(s"iceberg equality deletes: $dataFormat data and $deleteFormat deletes") {
      val name = "iceberg_equality_read"
      withTable(name) {
        withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
          spark.sql(s"""CREATE TABLE $name (
                       |  unused INT, k INT, s STRING, payload STRUCT<a: INT, b: STRING>)
                       |USING iceberg TBLPROPERTIES (
                       |  'format-version' = '2', 'write.format.default' = '$dataFormat')
                       |""".stripMargin)
          spark.sql(s"ALTER TABLE $name DROP COLUMN unused")
          spark.sql(s"""INSERT INTO $name VALUES
                       |(1, 'a', named_struct('a', 1, 'b', 'deleted')),
                       |(1, 'a', named_struct('a', 2, 'b', 'duplicate')),
                       |(2, 'b', named_struct('a', 3, 'b', 'keep')),
                       |(3, 'c', named_struct('a', 4, 'b', 'other')),
                       |(NULL, 'a', named_struct('a', 5, 'b', 'null-key')),
                       |(NULL, 'b', named_struct('a', 6, 'b', 'keep-null')),
                       |(NULL, NULL, named_struct('a', 7, 'b', 'both-null'))
                       |""".stripMargin)
          val table = loadIcebergTable(name)
          addEqualityDeletes(
            table,
            deleteFormat,
            Seq("s", "k"),
            Seq(
              Seq("a", Int.box(1)),
              Seq("a", Int.box(1)),
              Seq("a", null),
              Seq(null, null)))
          addEqualityDeletes(table, deleteFormat, Seq("k"), Seq(Seq(Int.box(99))))
          spark.catalog.refreshTable(name)
        }
        def checkRead(query: String, expected: Seq[Row]): Unit = {
          def checkResult(df: DataFrame): Unit = {
            checkAnswer(df, expected)
            checkGlutenPlan[IcebergScanTransformer](df)
          }
          if (deleteFormat == FileFormat.ORC) {
            checkResult(spark.sql(query))
          } else {
            runQueryAndCompare(query)(checkResult)
          }
        }
        checkRead(
          s"SELECT k, s, payload FROM $name",
          Seq(
            Row(2, "b", Row(3, "keep")),
            Row(3, "c", Row(4, "other")),
            Row(null, "b", Row(6, "keep-null"))))
        checkRead(
          s"SELECT payload.b FROM $name",
          Seq(Row("keep"), Row("other"), Row("keep-null")))
        checkRead(s"SELECT payload.b FROM $name WHERE k = 2", Seq(Row("keep")))
        checkRead(s"SELECT count(*) FROM $name", Seq(Row(3L)))
        withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
          spark.sql(s"INSERT INTO $name VALUES (1, 'a', named_struct('a', 8, 'b', 'new'))")
        }
        checkRead(
          s"SELECT payload.b FROM $name",
          Seq(Row("keep"), Row("other"), Row("keep-null"), Row("new")))
      }
    }
  }

  for (partitioning <- Seq("k", "bucket(2, k)", "d")) {
    test(s"iceberg equality deletes with partition transform $partitioning") {
      val name = "iceberg_equality_partition"
      withTable(name) {
        withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
          spark.sql(s"""CREATE TABLE $name (k INT, d DATE, payload STRING)
                       |USING iceberg PARTITIONED BY ($partitioning)
                       |TBLPROPERTIES ('format-version' = '2')""".stripMargin)
          spark.sql(s"""INSERT INTO $name VALUES
                       |(1, DATE '2024-01-01', 'remove'),
                       |(2, DATE '2024-01-02', 'keep'),
                       |(NULL, NULL, 'remove-null')""".stripMargin)
          val table = loadIcebergTable(name)
          val tasks = table.newScan().planFiles()
          val partitions =
            try {
              tasks.asScala.map(_.file().partition()).toVector
            } finally {
              tasks.close()
            }
          val key = if (partitioning == "d") "d" else "k"
          val value: AnyRef = if (key == "d") java.time.LocalDate.of(2024, 1, 1) else Int.box(1)
          partitions.foreach {
            partition =>
              addEqualityDeletes(
                table,
                FileFormat.PARQUET,
                Seq(key),
                Seq(Seq(value), Seq(null)),
                partition)
          }
          spark.catalog.refreshTable(name)
        }
        runQueryAndCompare(s"SELECT payload FROM $name") {
          df =>
            checkGlutenPlan[IcebergScanTransformer](df)
            checkAnswer(df, Seq(Row("keep")))
        }
        runQueryAndCompare(s"SELECT * FROM $name")(checkGlutenPlan[IcebergScanTransformer])
      }
    }
  }

  for (
    (keyType, sqlValue, value) <- Seq(
      (
        "DECIMAL(30, 2)",
        "CAST(12345678901234567890.12 AS DECIMAL(30, 2))",
        new java.math.BigDecimal("12345678901234567890.12")),
      (
        "TIMESTAMP",
        "TIMESTAMP '1500-01-01 00:00:00Z'",
        java.time.OffsetDateTime.parse("1500-01-01T00:00:00Z"))
    )
  ) {
    test(s"iceberg equality deletes fall back for $keyType keys") {
      val name = "iceberg_equality_fallback"
      withTable(name) {
        withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
          spark.sql(s"CREATE TABLE $name (k $keyType, payload STRING) USING iceberg")
          spark.sql(s"INSERT INTO $name VALUES ($sqlValue, 'remove'), (NULL, 'keep')")
          addEqualityDeletes(loadIcebergTable(name), FileFormat.PARQUET, Seq("k"), Seq(Seq(value)))
          spark.catalog.refreshTable(name)
        }
        runQueryAndCompare(s"SELECT payload FROM $name", noFallBack = false) {
          df =>
            checkGlutenPlanCount[IcebergScanTransformer](df, 0)
            checkAnswer(df, Seq(Row("keep")))
        }
      }
    }
  }

  test("iceberg equality deletes on renamed unprojected keys fall back") {
    val name = "iceberg_equality_renamed"
    withTable(name) {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        spark.sql(s"CREATE TABLE $name (k INT, payload STRING) USING iceberg")
        spark.sql(s"INSERT INTO $name VALUES (1, 'remove'), (2, 'keep')")
        addEqualityDeletes(
          loadIcebergTable(name),
          FileFormat.PARQUET,
          Seq("k"),
          Seq(Seq(Int.box(1))))
        spark.sql(s"ALTER TABLE $name RENAME COLUMN k TO renamed")
      }
      runQueryAndCompare(s"SELECT payload FROM $name", noFallBack = false) {
        df =>
          checkGlutenPlanCount[IcebergScanTransformer](df, 0)
          checkAnswer(df, Seq(Row("keep")))
      }
    }
  }
  for (
    (keyType, sqlValue, value) <- Seq[(String, String, AnyRef)](
      ("BOOLEAN", "true", Boolean.box(true)),
      ("BIGINT", "9223372036854775806", Long.box(9223372036854775806L)),
      (
        "DECIMAL(18, 2)",
        "CAST(1234567890123456.78 AS DECIMAL(18, 2))",
        new java.math.BigDecimal("1234567890123456.78")),
      ("BINARY", "X'00FF80'", java.nio.ByteBuffer.wrap(Array[Byte](0, -1, -128)))
    )
  ) {
    test(s"iceberg equality deletes with a top-level $keyType key") {
      val name = "iceberg_equality_type"
      withTable(name) {
        withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
          spark.sql(s"CREATE TABLE $name (k $keyType, `my/data` STRING) USING iceberg")
          spark.sql(s"INSERT INTO $name VALUES ($sqlValue, 'remove'), (NULL, 'keep')")
          addEqualityDeletes(loadIcebergTable(name), FileFormat.PARQUET, Seq("k"), Seq(Seq(value)))
          spark.catalog.refreshTable(name)
        }
        runQueryAndCompare(s"SELECT `my/data` FROM $name") {
          df =>
            checkGlutenPlan[IcebergScanTransformer](df)
            checkAnswer(df, Seq(Row("keep")))
        }
      }
    }
  }

  test("iceberg equality deletes preserve binary identity-partition values") {
    val name = "iceberg_equality_binary_partition"
    withTable(name) {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        spark.sql(s"CREATE TABLE $name (k BINARY, payload STRING) USING iceberg PARTITIONED BY (k)")
        spark.sql(s"INSERT INTO $name VALUES (X'00FF80', 'remove'), (X'00FF81', 'keep')")
        val table = loadIcebergTable(name)
        val tasks = table.newScan().planFiles()
        val partitions =
          try { tasks.asScala.map(_.file().partition()).toVector }
          finally { tasks.close() }
        partitions.foreach {
          partition =>
            addEqualityDeletes(
              table,
              FileFormat.PARQUET,
              Seq("k"),
              Seq(Seq(java.nio.ByteBuffer.wrap(Array[Byte](0, -1, -128)))),
              partition)
        }
        spark.catalog.refreshTable(name)
      }
      for (query <- Seq(s"SELECT payload FROM $name", s"SELECT * FROM $name")) {
        runQueryAndCompare(query)(checkGlutenPlan[IcebergScanTransformer])
      }
      checkAnswer(spark.sql(s"SELECT payload FROM $name"), Seq(Row("keep")))
    }
  }
  test("iceberg equality deletes fall back for evolved ORC structs") {
    val name = "iceberg_equality_evolved_orc"
    withTable(name) {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        spark.sql(s"""CREATE TABLE $name (k INT, payload STRUCT<a: INT, b: STRING>)
                     |USING iceberg TBLPROPERTIES ('write.format.default' = 'orc')""".stripMargin)
        spark.sql(s"""INSERT INTO $name VALUES
                     |(1, named_struct('a', 1, 'b', 'remove')),
                     |(2, named_struct('a', 2, 'b', 'keep'))""".stripMargin)
        addEqualityDeletes(
          loadIcebergTable(name),
          FileFormat.PARQUET,
          Seq("k"),
          Seq(Seq(Int.box(1))))
        spark.sql(s"ALTER TABLE $name ADD COLUMN payload.added INT FIRST")
      }
      runQueryAndCompare(s"SELECT payload FROM $name", noFallBack = false) {
        df =>
          checkGlutenPlanCount[IcebergScanTransformer](df, 0)
          checkAnswer(df, Seq(Row(Row(null, 2, "keep"))))
      }
    }
  }
  for (format <- Seq("parquet", "orc")) {
    test(s"iceberg equality deletes match null keys added after $format data was written") {
      val name = "iceberg_equality_added_key"
      withTable(name) {
        withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
          spark.sql(s"""CREATE TABLE $name (payload STRING) USING iceberg
                       |TBLPROPERTIES ('write.format.default' = '$format')""".stripMargin)
          spark.sql(s"INSERT INTO $name VALUES ('remove')")
          spark.sql(s"ALTER TABLE $name ADD COLUMN k INT")
          addEqualityDeletes(loadIcebergTable(name), FileFormat.PARQUET, Seq("k"), Seq(Seq(null)))
          spark.sql(s"INSERT INTO $name VALUES ('keep', 7)")
          spark.catalog.refreshTable(name)
        }
        runQueryAndCompare(s"SELECT payload FROM $name") {
          df =>
            checkGlutenPlan[IcebergScanTransformer](df)
            checkAnswer(df, Seq(Row("keep")))
        }
      }
    }
  }
}

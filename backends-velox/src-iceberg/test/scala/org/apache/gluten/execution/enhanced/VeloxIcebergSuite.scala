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
package org.apache.gluten.execution.enhanced

import org.apache.gluten.config.VeloxConfig.MAX_TARGET_FILE_SIZE_SESSION
import org.apache.gluten.execution._
import org.apache.gluten.tags.EnhancedFeaturesTest

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.CommandResultExec
import org.apache.spark.sql.execution.GlutenImplicits._
import org.apache.spark.sql.execution.datasources.v2.AppendDataExec
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.gluten.TestUtils

import org.apache.hadoop.fs.Path
import org.apache.iceberg.shaded.org.apache.parquet.ParquetReadOptions
import org.apache.iceberg.shaded.org.apache.parquet.column.Encoding
import org.apache.iceberg.shaded.org.apache.parquet.column.page.{DataPage, DataPageV1, DataPageV2}
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.util.HadoopInputFile

import scala.jdk.CollectionConverters._

@EnhancedFeaturesTest
class VeloxIcebergSuite extends IcebergSuite {

  import testImplicits._

  test("iceberg insert") {
    withTable("iceberg_tb2") {
      spark.sql("""
                  |create table if not exists iceberg_tb2(a int) using iceberg
                  |""".stripMargin)
      val df = spark.sql("""
                           |insert into table iceberg_tb2 values(1098)
                           |""".stripMargin)
      assert(
        df.queryExecution.executedPlan
          .asInstanceOf[CommandResultExec]
          .commandPhysicalPlan
          .isInstanceOf[VeloxIcebergAppendDataExec])
      val selectDf = spark.sql("""
                                 |select * from iceberg_tb2;
                                 |""".stripMargin)
      val result = selectDf.collect()
      assert(result.length == 1)
      assert(result(0).get(0) == 1098)
    }
  }

  test("iceberg insert partition table identity transform") {
    withTable("iceberg_tb2") {
      spark.sql("""
                  |create table if not exists iceberg_tb2(a int, b int)
                  |using iceberg
                  |partitioned by (a);
                  |""".stripMargin)
      val df = spark.sql("""
                           |insert into table iceberg_tb2 values(1098, 189)
                           |""".stripMargin)
      assert(
        df.queryExecution.executedPlan
          .asInstanceOf[CommandResultExec]
          .commandPhysicalPlan
          .isInstanceOf[VeloxIcebergAppendDataExec])
      val selectDf = spark.sql("""
                                 |select * from iceberg_tb2;
                                 |""".stripMargin)
      val result = selectDf.collect()
      assert(result.length == 1)
      assert(result(0).get(0) == 1098)
      assert(result(0).get(1) == 189)
    }
  }

  test("iceberg insert partition table with uppercase partition name") {
    withTable("iceberg_tb2") {
      spark.sql("""
                  |create table if not exists iceberg_tb2(A int, b int)
                  |using iceberg
                  |partitioned by (A);
                  |""".stripMargin)
      val df = spark.sql("""
                           |insert into table iceberg_tb2 values(1, 1)
                           |""".stripMargin)
      assert(
        df.queryExecution.executedPlan
          .asInstanceOf[CommandResultExec]
          .commandPhysicalPlan
          .isInstanceOf[VeloxIcebergAppendDataExec])
      checkAnswer(spark.sql("select * from iceberg_tb2"), Seq(Row(1, 1)))

      val filePath = spark
        .sql("select * from default.iceberg_tb2.files")
        .select("file_path")
        .collect()
        .apply(0)
        .getString(0)
      val partitionPath = filePath.split('/').init.last
      assert(partitionPath == "A=1")
    }
  }

  test("iceberg read cow table - delete") {
    withTable("iceberg_cow_tb") {
      spark.sql("""
                  |create table iceberg_cow_tb (
                  |  id int,
                  |  name string,
                  |  p string
                  |) using iceberg
                  |tblproperties (
                  |  'format-version' = '2',
                  |  'write.delete.mode' = 'copy-on-write',
                  |  'write.update.mode' = 'copy-on-write',
                  |  'write.merge.mode' = 'copy-on-write'
                  |);
                  |""".stripMargin)

      // Insert some test rows.
      spark.sql("""
                  |insert into table iceberg_cow_tb
                  |values (1, 'a1', 'p1'), (2, 'a2', 'p1'), (3, 'a3', 'p2'),
                  |       (4, 'a4', 'p1'), (5, 'a5', 'p2'), (6, 'a6', 'p1');
                  |""".stripMargin)

      // Delete row.
      val df = spark.sql(
        """
          |delete from iceberg_cow_tb where name = 'a1';
          |""".stripMargin
      )
      assert(
        df.queryExecution.executedPlan
          .asInstanceOf[CommandResultExec]
          .commandPhysicalPlan
          .isInstanceOf[VeloxIcebergReplaceDataExec])
      val selectDf = spark.sql("""
                                 |select * from iceberg_cow_tb;
                                 |""".stripMargin)
      val result = selectDf.collect()
      assert(result.length == 5)

    }
  }

  test("iceberg insert partition table bucket transform") {
    withTable("iceberg_tb2") {
      spark.sql("""
                  |create table if not exists iceberg_tb2(a int, b int)
                  |using iceberg
                  |partitioned by (bucket(16, a));
                  |""".stripMargin)
      val df = spark.sql("""
                           |insert into table iceberg_tb2 values(1098, 189)
                           |""".stripMargin)
      assert(
        df.queryExecution.executedPlan
          .asInstanceOf[CommandResultExec]
          .commandPhysicalPlan
          .isInstanceOf[VeloxIcebergAppendDataExec])
      val selectDf = spark.sql("""
                                 |select * from iceberg_tb2;
                                 |""".stripMargin)
      val result = selectDf.collect()
      assert(result.length == 1)
      assert(result(0).get(0) == 1098)
      assert(result(0).get(1) == 189)
    }
  }

  test("iceberg insert partition table truncate transform") {
    withTable("iceberg_tb2") {
      spark.sql("""
                  |create table if not exists iceberg_tb2(a int, b int)
                  |using iceberg
                  |partitioned by (truncate(16, a));
                  |""".stripMargin)
      val df = spark.sql("""
                           |insert into table iceberg_tb2 values(1098, 189)
                           |""".stripMargin)
      assert(
        df.queryExecution.executedPlan
          .asInstanceOf[CommandResultExec]
          .commandPhysicalPlan
          .isInstanceOf[VeloxIcebergAppendDataExec])
      val selectDf = spark.sql("""
                                 |select * from iceberg_tb2;
                                 |""".stripMargin)
      val result = selectDf.collect()
      assert(result.length == 1)
      assert(result(0).get(0) == 1098)
      assert(result(0).get(1) == 189)
    }
  }

  test("iceberg insert overwrite") {
    withTable("iceberg_tb2") {
      spark.sql("""
                  |create table if not exists iceberg_tb2(a int) using iceberg
                  |""".stripMargin)

      spark.sql("insert into table iceberg_tb2 values (1)")

      // Overwrite table
      val df = spark.sql("""
                           |insert overwrite table iceberg_tb2 values (2)
                           |""".stripMargin)
      assert(
        df.queryExecution.executedPlan
          .asInstanceOf[CommandResultExec]
          .commandPhysicalPlan
          .isInstanceOf[VeloxIcebergOverwriteByExpressionExec])

      val selectDf = spark.sql("""
                                 |select * from iceberg_tb2;
                                 |""".stripMargin)
      val result = selectDf.collect()
      assert(result.length == 1)
      assert(result(0).get(0) == 2)
    }
  }

  test("iceberg create table as select") {
    withTable("iceberg_tb1", "iceberg_tb2") {
      spark.sql("""
                  |create table iceberg_tb1 (a int, pt int) using iceberg
                  |partitioned by (pt)
                  |""".stripMargin)

      spark.sql("insert into table iceberg_tb1 values (1, 1), (2, 2)")

      // CTAS
      val sqlStr = """
                     |create table iceberg_tb2 using iceberg
                     |partitioned by (pt)
                     |as select * from iceberg_tb1
                     |""".stripMargin

      TestUtils.checkExecutedPlanContains[VeloxIcebergAppendDataExec](spark, sqlStr)

      checkAnswer(
        spark.sql("select * from iceberg_tb2 order by a"),
        Seq(Row(1, 1), Row(2, 2))
      )
    }
  }

  test("check iceberg write c2r") {
    withTable("iceberg_tbl") {
      spark.sql("""
                  |create table if not exists iceberg_tbl (a int, pt int) using iceberg
                  |tblproperties (
                  |  'format-version' = '2',
                  |  'write.delete.mode' = 'copy-on-write',
                  |  'write.update.mode' = 'copy-on-write',
                  |  'write.merge.mode' = 'copy-on-write'
                  |)
                  |partitioned by (pt)
                  |""".stripMargin)

      def checkColumnarToRow(df: DataFrame, num: Int): Unit = {
        assert(
          collect(
            df.queryExecution.executedPlan.asInstanceOf[CommandResultExec].commandPhysicalPlan) {
            case p if p.isInstanceOf[ColumnarToRowExecBase] => p
          }.size == num)
      }

      // insert partitioned table
      var df = spark.sql("insert into table iceberg_tbl values (1, 1), (2, 1), (3, 1), (4, 2)")
      checkAnswer(
        spark.sql("select * from iceberg_tbl order by a"),
        Seq(Row(1, 1), Row(2, 1), Row(3, 1), Row(4, 2)))
      checkColumnarToRow(df, 0)

      // delete partitioned table
      df = spark.sql("delete from iceberg_tbl where a = 1")
      checkAnswer(
        spark.sql("select * from iceberg_tbl order by a"),
        Seq(Row(2, 1), Row(3, 1), Row(4, 2)))
      checkColumnarToRow(df, 0)

      // overwrite partitioned table
      df = spark.sql("insert overwrite table iceberg_tbl values (5, 1)")
      checkAnswer(spark.sql("select * from iceberg_tbl order by a"), Seq(Row(5, 1)))
      checkColumnarToRow(df, 0)
    }
  }

  test("iceberg dynamic insert overwrite partition") {
    withTable("iceberg_tbl") {
      spark.sql("""
                  |create table if not exists iceberg_tbl (a int, pt int) using iceberg
                  |partitioned by (pt)
                  |""".stripMargin)

      spark.sql("insert into table iceberg_tbl values (1, 1), (2, 2)")

      withSQLConf("spark.sql.sources.partitionOverwriteMode" -> "dynamic") {
        val df = spark.sql("insert overwrite table iceberg_tbl values (11, 1)")
        assert(
          df.queryExecution.executedPlan
            .asInstanceOf[CommandResultExec]
            .commandPhysicalPlan
            .isInstanceOf[VeloxIcebergOverwritePartitionsDynamicExec])
        checkAnswer(
          spark.sql("select * from iceberg_tbl order by pt"),
          Seq(Row(11, 1), Row(2, 2))
        )
      }
    }
  }

  test("iceberg write metrics") {
    withTable("iceberg_tbl") {
      spark.sql("create table if not exists iceberg_tbl (id int) using iceberg".stripMargin)
      val df = spark.sql("insert into iceberg_tbl values 1")
      val metrics =
        df.queryExecution.executedPlan.asInstanceOf[CommandResultExec].commandPhysicalPlan.metrics
      val statusStore = spark.sharedState.statusStore
      val lastExecId = statusStore.executionsList().last.executionId
      val executionMetrics = statusStore.executionMetrics(lastExecId)

      assert(executionMetrics(metrics("numWrittenFiles").id).toLong == 1)
    }
  }

  test("iceberg write file name") {
    withTable("iceberg_tbl") {
      spark.sql("create table if not exists iceberg_tbl (id int) using iceberg")
      spark.sql("insert into iceberg_tbl values 1")

      val filePath = spark
        .sql("select * from default.iceberg_tbl.files")
        .select("file_path")
        .collect()
        .apply(0)
        .getString(0)

      val fileName = filePath.split('/').last
      // Expected format: {partitionId:05d}-{taskId}-{operationId}-{fileCount:05d}.parquet
      // Example: 00000-0-query_id-0-00001.parquet
      assert(
        fileName.matches("\\d{5}-\\d+-.*-\\d{5}\\.parquet"),
        s"File name does not match expected format: $fileName")
    }
  }

  test("iceberg stream write to table") {
    withTable("iceberg_tbl") {
      withTempDir {
        checkpointDir =>
          spark.sql("CREATE TABLE iceberg_tbl (a INT, b STRING) USING iceberg")
          TestUtils.checkExecutedPlanContains[VeloxIcebergWriteToDataSourceV2Exec](spark) {
            val inputData = MemoryStream[(Int, String)]
            val stream = inputData
              .toDS()
              .toDF("a", "b")
              .writeStream
              .option("checkpointLocation", checkpointDir.getCanonicalPath)
              .format("iceberg")
              .toTable("iceberg_tbl")

            val query = () => spark.sql("SELECT * FROM iceberg_tbl ORDER BY a")
            try {
              inputData.addData((1, "a"))
              stream.processAllAvailable()
              checkAnswer(query(), Seq(Row(1, "a")))

              inputData.addData((2, "b"))
              stream.processAllAvailable()
              checkAnswer(query(), Seq(Row(1, "a"), Row(2, "b")))
            } finally {
              stream.stop()
            }
          }

      }
    }
  }

  test("iceberg native write fallback when validation fails - sort order") {
    withTable("iceberg_sorted_tbl") {
      spark.sql("CREATE TABLE iceberg_sorted_tbl (a INT, b STRING) USING iceberg")
      spark.sql("ALTER TABLE iceberg_sorted_tbl WRITE ORDERED BY a")

      val df = spark.sql("INSERT INTO iceberg_sorted_tbl VALUES (1, 'hello'), (2, 'world')")

      // Should fallback to vanilla Spark's AppendDataExec.
      val commandPlan =
        df.queryExecution.executedPlan.asInstanceOf[CommandResultExec].commandPhysicalPlan
      assert(commandPlan.isInstanceOf[AppendDataExec])
      assert(!commandPlan.isInstanceOf[VeloxIcebergAppendDataExec])

      checkAnswer(
        spark.sql("SELECT * FROM iceberg_sorted_tbl ORDER BY a"),
        Seq(Row(1, "hello"), Row(2, "world")))

      // Verify fallbackSummary reports the sort order fallback reason.
      val summary = df.fallbackSummary()
      assert(
        summary.fallbackNodeToReason.exists(
          _.values.exists(_.contains("Not support write table with sort order"))))
    }
  }

  test("iceberg read cow table - update after schema evolution") {
    withTable("iceberg_cow_update_evolved_tb") {
      spark.sql("""
                  |create table iceberg_cow_update_evolved_tb (
                  |  id int,
                  |  name string,
                  |  age int
                  |) using iceberg
                  |tblproperties (
                  |  'format-version' = '2',
                  |  'write.delete.mode' = 'copy-on-write',
                  |  'write.update.mode' = 'copy-on-write',
                  |  'write.merge.mode' = 'copy-on-write'
                  |)
                  |""".stripMargin)

      spark.sql("""
                  |alter table iceberg_cow_update_evolved_tb
                  |add columns (salary decimal(10, 2))
                  |""".stripMargin)

      spark.sql("""
                  |insert into table iceberg_cow_update_evolved_tb values
                  |  (1, 'Name1', 23, 3400.00),
                  |  (2, 'Name2', 30, 5500.00),
                  |  (3, 'Name3', 35, 6500.00)
                  |""".stripMargin)

      val df = spark.sql("""
                           |update iceberg_cow_update_evolved_tb
                           |set name = 'Name4'
                           |where id = 1
                           |""".stripMargin)

      assert(
        df.queryExecution.executedPlan
          .asInstanceOf[CommandResultExec]
          .commandPhysicalPlan
          .isInstanceOf[VeloxIcebergReplaceDataExec])

      checkAnswer(
        spark.sql("""
                    |select id, name, age, salary
                    |from iceberg_cow_update_evolved_tb
                    |order by id
                    |""".stripMargin),
        Seq(
          Row(1, "Name4", 23, new java.math.BigDecimal("3400.00")),
          Row(2, "Name2", 30, new java.math.BigDecimal("5500.00")),
          Row(3, "Name1", 35, new java.math.BigDecimal("6500.00"))
        )
      )
    }
  }
  ignore("disabled test") {
    test("iceberg native write respects target file size bytes") {
      withTable("iceberg_small_target_tbl") {
        spark.sql(
          """
            |CREATE TABLE iceberg_small_target_tbl (
            |  id INT,
            |  payload STRING
            |) USING iceberg
            |TBLPROPERTIES (
            |  'write.format.default' = 'parquet',
            |  'write.parquet.compression-codec' = 'uncompressed',
            |  'write.parquet.row-group-size-bytes' = '4096',
            |  'write.parquet.page-size-bytes' = '1024B',
            |  'write.target-file-size-bytes' = '8192'
            |)
            |""".stripMargin)

        checkAnswer(
          spark.sql(
            """
              |SHOW TBLPROPERTIES iceberg_small_target_tbl
              |('write.target-file-size-bytes')
              |""".stripMargin),
          Seq(Row("write.target-file-size-bytes", "8192"))
        )

        val df = spark.sql(
          """
            |INSERT INTO iceberg_small_target_tbl
            |SELECT /*+ COALESCE(1) */
            |  CAST(id AS INT),
            |  concat(
            |    CAST(id AS STRING),
            |    '-',
            |    sha2(CAST(id AS STRING), 256),
            |    '-',
            |    sha2(CAST(id + 1000 AS STRING), 256)
            |  )
            |FROM range(1000)
            |""".stripMargin)

        val commandPlan =
          df.queryExecution.executedPlan.asInstanceOf[CommandResultExec].commandPhysicalPlan

        assert(commandPlan.isInstanceOf[VeloxIcebergAppendDataExec])

        checkAnswer(
          spark.sql("SELECT COUNT(*) FROM iceberg_small_target_tbl"),
          Seq(Row(1000L)))

        val files = spark.sql(
          """
            |SELECT file_size_in_bytes
            |FROM default.iceberg_small_target_tbl.files
            |""".stripMargin).collect().map(_.getLong(0))

        assert(files.nonEmpty)

        assert(
          files.length > 1,
          s"Expected write.target-file-size-bytes=8192 to create multiple files, " +
            s"but got files=${files.mkString("[", ", ", "]")}")

        assert(
          files.max < 64L * 1024L,
          s"Expected small target file size to keep max file size reasonably small, " +
            s"but got files=${files.mkString("[", ", ", "]")}")
      }
    }
  }

  test("iceberg parquet writer respects dictionary page size bytes") {
    val table = "iceberg_dict_page_size_tbl"

    def parquetFiles(table: String): Seq[String] = {
      spark.sql(s"""
                   |SELECT file_path
                   |FROM default.$table.files
                   |""".stripMargin).collect().map(_.getString(0)).toSeq
    }

    def pageEncoding(page: DataPage): Encoding = {
      page.accept(new DataPage.Visitor[Encoding] {
        override def visit(dataPageV1: DataPageV1): Encoding = dataPageV1.getValueEncoding
        override def visit(dataPageV2: DataPageV2): Encoding = dataPageV2.getDataEncoding
      })
    }

    def dataPageEncodings(table: String, columnName: String): Seq[Encoding] = {
      val conf = spark.sparkContext.hadoopConfiguration

      parquetFiles(table).flatMap {
        file =>
          val inputFile = HadoopInputFile.fromPath(new Path(file), conf)
          val reader = ParquetFileReader.open(inputFile, ParquetReadOptions.builder().build())

          try {
            val column = reader
              .getFooter
              .getFileMetaData
              .getSchema
              .getColumns
              .asScala
              .find(_.getPath.toSeq == Seq(columnName))
              .getOrElse {
                fail(s"Column $columnName was not found in Parquet file $file")
              }

            val encodings = scala.collection.mutable.ArrayBuffer.empty[Encoding]

            var rowGroup = reader.readNextRowGroup()
            while (rowGroup != null) {
              val pageReader = rowGroup.getPageReader(column)
              pageReader.readDictionaryPage()

              var page = pageReader.readPage()
              while (page != null) {
                encodings += pageEncoding(page)
                page = pageReader.readPage()
              }

              rowGroup = reader.readNextRowGroup()
            }

            encodings
          } finally {
            reader.close()
          }
      }
    }

    withSQLConf(
      "spark.sql.shuffle.partitions" -> "1"
    ) {
      withTable(table) {
        spark.sql(s"""
                     |CREATE TABLE $table (
                     |  value SMALLINT
                     |) USING iceberg
                     |TBLPROPERTIES (
                     |  'write.format.default' = 'parquet',
                     |  'write.parquet.compression-codec' = 'uncompressed',
                     |  'write.parquet.dict-size-bytes' = '1B'
                     |)
                     |""".stripMargin)

        val df = spark.sql(s"""
                              |INSERT INTO $table
                              |SELECT CAST(id + 1 AS SMALLINT)
                              |FROM range(0, 10000, 1, 1)
                              |""".stripMargin)

        assert(
          df.queryExecution.executedPlan
            .asInstanceOf[CommandResultExec]
            .commandPhysicalPlan
            .isInstanceOf[VeloxIcebergAppendDataExec])

        checkAnswer(
          spark.sql(s"SELECT count(*) FROM $table"),
          Seq(Row(10000L)))

        val encodings = dataPageEncodings(table, "value")

        assert(encodings.nonEmpty, "Expected at least one Parquet data page")
        assert(
          encodings.head == Encoding.RLE_DICTIONARY,
          s"Expected the first data page to use dictionary encoding, " +
            s"but got encodings=${encodings.mkString("[", ", ", "]")}"
        )
        assert(
          encodings.contains(Encoding.PLAIN),
          s"Expected write.parquet.dict-size-bytes=1B to make later data pages fall back " +
            s"to PLAIN, but got encodings=${encodings.mkString("[", ", ", "]")}"
        )
      }
    }
  }

  // Ignored due to velox parquet row-group flush semantics change after velox#16998.
  ignore("iceberg parquet writer default row group size test") {
    val table = "iceberg_default_row_group_size"
    val defaultRowGroupBytes = 128L * 1024 * 1024

    def parquetFiles(table: String): Seq[String] = {
      spark.sql(s"""
      SELECT file_path
      FROM default.$table.files
    """).collect().map(_.getString(0)).toSeq
    }

    case class RowGroupInfo(
        file: String,
        ordinal: Int,
        rowCount: Long,
        totalByteSize: Long,
        compressedSize: Long)

    def collectRowGroups(table: String): Seq[RowGroupInfo] = {
      val conf = spark.sparkContext.hadoopConfiguration

      parquetFiles(table).flatMap {
        file =>
          val path = new Path(file)
          val inputFile = HadoopInputFile.fromPath(path, conf)
          val options = ParquetReadOptions.builder().build()

          val stream = inputFile.newStream()
          val footer =
            try {
              ParquetFileReader.readFooter(inputFile, options, stream)
            } finally {
              stream.close()
            }

          footer.getBlocks.asScala.zipWithIndex.map {
            case (block, index) =>
              val compressedSize =
                block.getColumns.asScala.map(_.getTotalSize).sum

              RowGroupInfo(
                file = file,
                ordinal = index,
                rowCount = block.getRowCount,
                totalByteSize = block.getTotalByteSize,
                compressedSize = compressedSize)
          }
      }
    }

    withSQLConf(
      MAX_TARGET_FILE_SIZE_SESSION.key -> "0",
      "spark.sql.shuffle.partitions" -> "1"
    ) {
      withTable(table) {
        spark.sql(s"""
        CREATE TABLE $table (
          id BIGINT,
          payload STRING
        ) USING iceberg
        TBLPROPERTIES (
          'write.parquet.compression-codec' = 'uncompressed'
        )
      """)

        val df = spark.sql(s"""
        INSERT INTO $table
        SELECT
          id,
          array_join(
            transform(
              sequence(0, 63),
              x -> md5(concat(CAST(id AS STRING), ':', CAST(x AS STRING)))
            ),
            ''
          ) AS payload
        FROM range(0, 90000, 1, 1)
      """)

        assert(
          df.queryExecution.executedPlan
            .asInstanceOf[CommandResultExec]
            .commandPhysicalPlan
            .isInstanceOf[VeloxIcebergAppendDataExec])

        checkAnswer(
          spark.sql(s"SELECT count(*) FROM $table"),
          Seq(Row(90000L)))
        val rowGroups = collectRowGroups(table)

        assert(
          rowGroups.size == 2,
          "Expected 2 row groups")

        assert(
          rowGroups.head.totalByteSize < defaultRowGroupBytes,
          "Expected row group to contain less than default value")

        assert(
          rowGroups(1).totalByteSize < defaultRowGroupBytes,
          "Expected row group to contain less than default value")
      }
    }
  }
}

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

import org.apache.gluten.config.VeloxConfig.MAX_TARGET_FILE_SIZE_SESSION

import org.apache.spark.sql.Row

import org.apache.hadoop.fs.Path
import org.apache.iceberg.shaded.org.apache.parquet.ParquetReadOptions
import org.apache.iceberg.shaded.org.apache.parquet.column.Encoding
import org.apache.iceberg.shaded.org.apache.parquet.column.page.{DataPage, DataPageV1, DataPageV2}
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.util.HadoopInputFile

import scala.jdk.CollectionConverters._

class VeloxIcebergParquetWriteSuite extends VeloxIcebergTestBase {

  private val defaultRowGroupBytes = 128L * 1024 * 1024

  private case class RowGroupInfo(
      file: String,
      ordinal: Int,
      rowCount: Long,
      totalByteSize: Long,
      compressedSize: Long)

  private def parquetFiles(table: String): Seq[String] = {
    spark.sql(s"""
                 |SELECT file_path
                 |FROM default.$table.files
                 |""".stripMargin).collect().map(_.getString(0)).toSeq
  }

  private def pageEncoding(page: DataPage): Encoding = {
    page.accept(new DataPage.Visitor[Encoding] {
      override def visit(dataPageV1: DataPageV1): Encoding = dataPageV1.getValueEncoding
      override def visit(dataPageV2: DataPageV2): Encoding = dataPageV2.getDataEncoding
    })
  }

  private def dataPageEncodings(table: String, columnName: String): Seq[Encoding] = {
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

  private def collectRowGroups(table: String): Seq[RowGroupInfo] = {
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

  ignore("respect target file size") {
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

      checkCommandPlan[VeloxIcebergAppendDataExec](df)

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

  test("respect dictionary page size") {
    val table = "iceberg_dict_page_size_tbl"

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

        checkCommandPlan[VeloxIcebergAppendDataExec](df)

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

  test("use the default row group size") {
    val table = "iceberg_default_row_group_size"

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

        checkCommandPlan[VeloxIcebergAppendDataExec](df)

        checkAnswer(
          spark.sql(s"SELECT count(*) FROM $table"),
          Seq(Row(90000L)))
        val rowGroups =
          collectRowGroups(table).sortBy(info => (info.file, info.ordinal))

        assert(
          rowGroups.map(_.file).distinct.size == 1,
          s"Expected one Parquet file, found: ${rowGroups.map(_.file).distinct}")

        assert(
          rowGroups.size == 2,
          s"Expected 2 row groups, found ${rowGroups.size}: $rowGroups")

        assert(
          rowGroups.map(_.rowCount).sum == 90000L,
          s"Expected 90000 rows across all row groups: $rowGroups")

        val firstRowGroup = rowGroups.head
        val finalRowGroup = rowGroups.last

        assert(
          firstRowGroup.compressedSize >= defaultRowGroupBytes,
          s"Expected the first row group to reach the default row-group size " +
            s"$defaultRowGroupBytes, but found ${firstRowGroup.compressedSize}"
        )

        assert(
          finalRowGroup.compressedSize < defaultRowGroupBytes,
          s"Expected the final row group to be smaller than the default row-group " +
            s"size $defaultRowGroupBytes, but found ${finalRowGroup.compressedSize}"
        )
      }
    }
  }
}

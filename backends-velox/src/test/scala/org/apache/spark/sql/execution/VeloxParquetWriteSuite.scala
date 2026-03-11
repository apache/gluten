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
package org.apache.spark.sql.execution

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.VeloxWholeStageTransformerSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.util.Utils

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

class VeloxParquetWriteSuite extends VeloxWholeStageTransformerSuite with WriteUtils {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  // The parquet compression codec extensions
  private val parquetCompressionCodecExtensions = {
    Map(
      "none" -> "",
      "uncompressed" -> "",
      "snappy" -> ".snappy",
      "gzip" -> ".gz",
      "lzo" -> ".lzo",
      "lz4" -> (if (isSparkVersionGE("3.5")) ".lz4hadoop" else ".lz4"),
      "brotli" -> ".br",
      "zstd" -> ".zstd"
    )
  }

  private def getParquetFileExtension(codec: String): String = {
    s"${parquetCompressionCodecExtensions(codec)}.parquet"
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set(GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")
  }

  test("test Array(Struct) fallback") {
    withTempPath {
      f =>
        val path = f.getCanonicalPath
        checkNativeWrite(
          s"INSERT OVERWRITE DIRECTORY '$path' USING PARQUET SELECT array(struct(1), null) as var1",
          expectNative = false)
    }
  }

  test("test write parquet with compression codec") {
    // compression codec details see `VeloxParquetDatasource.cc`
    Seq("snappy", "gzip", "zstd", "lz4", "none", "uncompressed")
      .foreach {
        codec =>
          TPCHTableDataFrames.foreach {
            case (_, df) =>
              withTempPath {
                f =>
                  df.write
                    .format("parquet")
                    .option("compression", codec)
                    .save(f.getCanonicalPath)
                  val expectedCodec = codec match {
                    case "none" => "uncompressed"
                    case _ => codec
                  }
                  val parquetFiles = f.list((_, name) => name.contains("parquet"))
                  assert(parquetFiles.nonEmpty, expectedCodec)
                  assert(
                    parquetFiles.forall {
                      file =>
                        val path = new Path(f.getCanonicalPath, file)
                        assert(file.endsWith(getParquetFileExtension(codec)))
                        val in = HadoopInputFile.fromPath(path, spark.sessionState.newHadoopConf())
                        Utils.tryWithResource(ParquetFileReader.open(in)) {
                          reader =>
                            val column = reader.getFooter.getBlocks.get(0).getColumns.get(0)
                            expectedCodec.equalsIgnoreCase(column.getCodec.toString)
                        }
                    },
                    expectedCodec
                  )

                  val parquetDf = spark.read
                    .format("parquet")
                    .load(f.getCanonicalPath)
                  assert(df.schema.equals(parquetDf.schema))
                  checkAnswer(parquetDf, df)
              }
          }
      }
  }

  test("test insert into") {
    withTable("t") {
      spark.sql("CREATE TABLE t (id INT) USING PARQUET")
      checkNativeWrite("INSERT INTO t VALUES 1")
      checkAnswer(spark.sql("SELECT * FROM t"), Row(1))
    }
  }

  test("test ctas") {
    withTable("velox_ctas") {
      spark
        .range(100)
        .toDF("id")
        .createOrReplaceTempView("ctas_temp")
      checkNativeWrite(
        "CREATE TABLE velox_ctas USING PARQUET AS SELECT * FROM ctas_temp",
        expectNative = isSparkVersionGE("3.4"))
    }
  }

  test("test insert overwrite dir") {
    withTempPath {
      f =>
        val path = f.getCanonicalPath
        spark
          .range(100)
          .selectExpr("id as c1", "id % 7 as p")
          .createOrReplaceTempView("temp")
        checkNativeWrite(s"INSERT OVERWRITE DIRECTORY '$path' USING PARQUET SELECT * FROM temp")
    }
  }

  test("test dynamic and static partition write table") {
    withTable("t") {
      spark.sql(
        "CREATE TABLE t (c int, d long, e long)" +
          " USING PARQUET partitioned by (c, d)")
      checkNativeWrite(
        "INSERT OVERWRITE TABLE t partition(c=1, d)" +
          " SELECT 3 as e, 2 as d")
      checkAnswer(spark.table("t"), Row(3, 1, 2))
      checkAnswer(spark.sql("SHOW PARTITIONS t"), Seq(Row("c=1/d=2")))
    }
  }

  test("test parquet bucket write") {
    withTable("bucket") {
      spark
        .range(100)
        .selectExpr("id as c1", "id % 7 as p")
        .createOrReplaceTempView("bucket_temp")
      checkNativeWrite(
        "CREATE TABLE bucket USING PARQUET CLUSTERED BY (p) INTO 7 BUCKETS " +
          "AS SELECT * FROM bucket_temp",
        expectNative = false)
    }
  }

  test("test write parquet with custom block size, block rows and page size") {
    val blockSize = 64 * 1024 * 1024L // 64MB
    val blockRows = 10000000L // 10M
    val pageSize = 1024 * 1024L // 1MB

    withTempPath {
      f =>
        spark
          .range(100)
          .toDF("id")
          .write
          .format("parquet")
          .option(GlutenConfig.PARQUET_BLOCK_SIZE, blockSize.toString)
          .option(GlutenConfig.PARQUET_BLOCK_ROWS, blockRows.toString)
          .option(GlutenConfig.PARQUET_DATAPAGE_SIZE, pageSize.toString)
          .save(f.getCanonicalPath)

        val parquetDf = spark.read.parquet(f.getCanonicalPath)
        checkAnswer(parquetDf, spark.range(100).toDF("id"))
    }
  }

  test("test write parquet with block size, block rows and page size exceeding INT_MAX") {
    val largeBlockSize = 3L * 1024 * 1024 * 1024 // 3GB
    val largeBlockRows = 3L * 1000 * 1000 * 1000 // 3 billion
    val largePageSize = 3L * 1024 * 1024 * 1024 // 3GB

    withTempPath {
      f =>
        spark
          .range(100)
          .toDF("id")
          .write
          .format("parquet")
          .option(GlutenConfig.PARQUET_BLOCK_SIZE, largeBlockSize.toString)
          .option(GlutenConfig.PARQUET_BLOCK_ROWS, largeBlockRows.toString)
          .option(GlutenConfig.PARQUET_DATAPAGE_SIZE, largePageSize.toString)
          .save(f.getCanonicalPath)

        val parquetDf = spark.read.parquet(f.getCanonicalPath)
        checkAnswer(parquetDf, spark.range(100).toDF("id"))
    }
  }
}

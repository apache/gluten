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
import org.apache.gluten.execution.BoltWholeStageTransformerSuite
import org.apache.gluten.test.FallbackUtil

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.junit.Assert

class BoltParquetWriteSuite extends BoltWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  // The parquet compression codec extensions
  private val parquetCompressionCodecExtensions = if (isSparkVersionGE("3.5")) {
    Map(
      "none" -> "",
      "uncompressed" -> "",
      "snappy" -> ".snappy",
      "gzip" -> ".gz",
      "lzo" -> ".lzo",
      "lz4" -> ".lz4hadoop", // Specific extension for version 3.5
      "brotli" -> ".br",
      "zstd" -> ".zstd"
    )
  } else {
    Map(
      "none" -> "",
      "uncompressed" -> "",
      "snappy" -> ".snappy",
      "gzip" -> ".gz",
      "lzo" -> ".lzo",
      "lz4" -> ".lz4",
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
        val testAppender = new LogAppender("native write tracker")
        withLogAppender(testAppender) {
          spark.sql("select array(struct(1), null) as var1").write.mode("overwrite").save(path)
        }
        assert(
          !testAppender.loggingEvents.exists(
            _.getMessage.toString.contains("Use Gluten parquet write for hive")))
    }
  }

  test("test write parquet with compression codec") {
    // compression codec details see `BoltParquetDatasource.cc`
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

  test("test ctas") {
    withTable("bolt_ctas") {
      spark
        .range(100)
        .toDF("id")
        .createOrReplaceTempView("ctas_temp")
      val df = spark.sql("CREATE TABLE bolt_ctas USING PARQUET AS SELECT * FROM ctas_temp")
      Assert.assertTrue(FallbackUtil.hasFallback(df.queryExecution.executedPlan))
    }
  }

  test("test parquet dynamic partition write") {
    withTempPath {
      f =>
        val path = f.getCanonicalPath
        spark
          .range(100)
          .selectExpr("id as c1", "id % 7 as p")
          .createOrReplaceTempView("temp")
        val df = spark.sql(s"INSERT OVERWRITE DIRECTORY '$path' USING PARQUET SELECT * FROM temp")
        Assert.assertTrue(FallbackUtil.hasFallback(df.queryExecution.executedPlan))
    }
  }

  test("test parquet bucket write") {
    withTable("bucket") {
      spark
        .range(100)
        .selectExpr("id as c1", "id % 7 as p")
        .createOrReplaceTempView("bucket_temp")
      val df = spark.sql(
        "CREATE TABLE bucket USING PARQUET CLUSTERED BY (p) INTO 7 BUCKETS " +
          "AS SELECT * FROM bucket_temp")
      Assert.assertTrue(FallbackUtil.hasFallback(df.queryExecution.executedPlan))
    }
  }
}

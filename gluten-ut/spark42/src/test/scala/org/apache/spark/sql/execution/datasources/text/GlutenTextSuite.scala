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
package org.apache.spark.sql.execution.datasources.text

import org.apache.spark.{SparkConf, SparkIllegalArgumentException, TestUtils}
import org.apache.spark.sql.{AnalysisException, DataFrame, GlutenSQLTestsBaseTrait, QueryTest, Row, SaveMode}
import org.apache.spark.sql.execution.datasources.CommonFileDataSourceSuite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.Utils

import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.GzipCodec

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

abstract class GlutenTextSuite
  extends QueryTest
  with SharedSparkSession
  with CommonFileDataSourceSuite
  with GlutenSQLTestsBaseTrait {
  import testImplicits._

  override protected def dataSourceFormat = "text"

  testGluten("reading text file") {
    verifyFrame(spark.read.format("text").load(testFile))
  }

  testGluten("SQLContext.read.text() API") {
    verifyFrame(spark.read.text(testFile))
  }

  testGluten("SPARK-12562 verify write.text() can handle column name beyond `value`") {
    val df = spark.read.text(testFile).withColumnRenamed("value", "adwrasdf")

    val tempFile = Utils.createTempDir()
    tempFile.delete()
    df.write.text(tempFile.getCanonicalPath)
    verifyFrame(spark.read.text(tempFile.getCanonicalPath))

    Utils.deleteRecursively(tempFile)
  }

  testGluten("error handling for invalid schema") {
    val tempFile = Utils.createTempDir()
    tempFile.delete()

    val df = spark.range(2)
    intercept[AnalysisException] {
      df.write.text(tempFile.getCanonicalPath)
    }

    intercept[AnalysisException] {
      spark.range(2).select(df("id"), df("id") + 1).write.text(tempFile.getCanonicalPath)
    }
  }

  testGluten("reading partitioned data using read.textFile()") {
    val ds = spark.read.textFile(textPartitioned)
    val data = ds.collect()

    assert(ds.schema == new StructType().add("value", StringType))
    assert(data.length == 2)
  }

  testGluten("support for partitioned reading using read.text()") {
    val df = spark.read.text(textPartitioned)
    val data = df.filter("year = '2015'").select("value").collect()

    assert(data(0) == Row("2015-test"))
    assert(data.length == 1)
  }

  testGluten("SPARK-13503 Support to specify the option for compression codec for TEXT") {
    val testDf = spark.read.text(testFile)
    val extensionNameMap = Map("bzip2" -> ".bz2", "deflate" -> ".deflate", "gzip" -> ".gz")
    extensionNameMap.foreach {
      case (codecName, extension) =>
        val tempDir = Utils.createTempDir()
        val tempDirPath = tempDir.getAbsolutePath
        testDf.write.option("compression", codecName).mode(SaveMode.Overwrite).text(tempDirPath)
        val compressedFiles = new File(tempDirPath).listFiles()
        assert(compressedFiles.exists(_.getName.endsWith(s".txt$extension")))
        verifyFrame(spark.read.text(tempDirPath))
    }

    withTempDir {
      dir =>
        checkError(
          exception = intercept[SparkIllegalArgumentException] {
            testDf.write
              .option("compression", "illegal")
              .mode(SaveMode.Overwrite)
              .text(dir.getAbsolutePath)
          },
          condition = "CODEC_NOT_AVAILABLE.WITH_AVAILABLE_CODECS_SUGGESTION",
          parameters = Map(
            "codecName" -> "illegal",
            "availableCodecs" -> "bzip2, deflate, uncompressed, snappy, none, lz4, gzip")
        )
    }
  }

  testGluten("SPARK-13543 Write the output as uncompressed via option()") {
    val extraOptions = Map[String, String](
      "mapreduce.output.fileoutputformat.compress" -> "true",
      "mapreduce.output.fileoutputformat.compress.type" -> CompressionType.BLOCK.toString,
      "mapreduce.map.output.compress" -> "true",
      "mapreduce.output.fileoutputformat.compress.codec" -> classOf[GzipCodec].getName,
      "mapreduce.map.output.compress.codec" -> classOf[GzipCodec].getName
    )
    withTempDir {
      dir =>
        val testDf = spark.read.text(testFile)
        val tempDirPath = dir.getAbsolutePath
        testDf.write
          .option("compression", "none")
          .options(extraOptions)
          .mode(SaveMode.Overwrite)
          .text(tempDirPath)
        val compressedFiles = new File(tempDirPath).listFiles()
        assert(compressedFiles.exists(!_.getName.endsWith(".txt.gz")))
        verifyFrame(spark.read.options(extraOptions).text(tempDirPath))
    }
  }

  testGluten("case insensitive option") {
    val extraOptions = Map[String, String](
      "mApReDuCe.output.fileoutputformat.compress" -> "true",
      "mApReDuCe.output.fileoutputformat.compress.type" -> CompressionType.BLOCK.toString,
      "mApReDuCe.map.output.compress" -> "true",
      "mApReDuCe.output.fileoutputformat.compress.codec" -> classOf[GzipCodec].getName,
      "mApReDuCe.map.output.compress.codec" -> classOf[GzipCodec].getName
    )
    withTempDir {
      dir =>
        val testDf = spark.read.text(testFile)
        val tempDirPath = dir.getAbsolutePath
        testDf.write
          .option("CoMpReSsIoN", "none")
          .options(extraOptions)
          .mode(SaveMode.Overwrite)
          .text(tempDirPath)
        val compressedFiles = new File(tempDirPath).listFiles()
        assert(compressedFiles.exists(!_.getName.endsWith(".txt.gz")))
        verifyFrame(spark.read.options(extraOptions).text(tempDirPath))
    }
  }

  testGluten("SPARK-14343: select partitioning column") {
    withTempPath {
      dir =>
        val path = dir.getCanonicalPath
        val ds1 = spark.range(1).selectExpr("CONCAT('val_', id)")
        ds1.write.text(s"$path/part=a")
        ds1.write.text(s"$path/part=b")

        checkAnswer(
          spark.read.format("text").load(path).select($"part"),
          Row("a") :: Row("b") :: Nil)
    }
  }

  testGluten("SPARK-15654: should not split gz files") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        val df1 = spark.range(0, 1000).selectExpr("CAST(id AS STRING) AS s")
        df1.write.option("compression", "gzip").mode("overwrite").text(path)

        val expected = df1.collect()
        Seq(10, 100, 1000).foreach {
          bytes =>
            withSQLConf(SQLConf.FILES_MAX_PARTITION_BYTES.key -> bytes.toString) {
              val df2 = spark.read.format("text").load(path)
              checkAnswer(df2, expected)
            }
        }
    }
  }

  def testLineSeparator(lineSep: String): Unit = {
    test(s"SPARK-23577: Support line separator - lineSep: '$lineSep'") {
      // Read
      val values = Seq("a", "b", "\nc")
      val data = values.mkString(lineSep)
      val dataWithTrailingLineSep = s"$data$lineSep"
      Seq(data, dataWithTrailingLineSep).foreach {
        lines =>
          withTempPath {
            path =>
              Files.write(path.toPath, lines.getBytes(StandardCharsets.UTF_8))
              val df = spark.read.option("lineSep", lineSep).text(path.getAbsolutePath)
              checkAnswer(df, Seq("a", "b", "\nc").toDF())
          }
      }

      // Write
      withTempPath {
        path =>
          values.toDF().coalesce(1).write.option("lineSep", lineSep).text(path.getAbsolutePath)
          val partFile =
            TestUtils.recursiveList(path).filter(f => f.getName.startsWith("part-")).head
          val readBack = new String(Files.readAllBytes(partFile.toPath), StandardCharsets.UTF_8)
          assert(readBack === s"a${lineSep}b$lineSep\nc$lineSep")
      }

      // Roundtrip
      withTempPath {
        path =>
          val df = values.toDF()
          df.write.option("lineSep", lineSep).text(path.getAbsolutePath)
          val readBack = spark.read.option("lineSep", lineSep).text(path.getAbsolutePath)
          checkAnswer(df, readBack)
      }
    }
  }

  // scalastyle:off nonascii
  Seq("|", "^", "::", "!!!@3", 0x1e.toChar.toString, "아").foreach {
    lineSep => testLineSeparator(lineSep)
  }
  // scalastyle:on nonascii

  // Rewrite for file locating.
  private def testFile: String = {
    getWorkspaceFilePath(
      "sql",
      "core",
      "src",
      "test",
      "resources").toString + "/test-data/text-suite.txt"
  }

  // Added for file locating.
  private def textPartitioned: String = {
    getWorkspaceFilePath(
      "sql",
      "core",
      "src",
      "test",
      "resources").toString + "/test-data/text-partitioned"
  }

  /** Verifies data and schema. */
  private def verifyFrame(df: DataFrame): Unit = {
    // schema
    assert(df.schema == new StructType().add("value", StringType))

    // verify content
    val data = df.collect()
    assert(data(0) == Row("This is a test file for the text data source"))
    assert(data(1) == Row("1+1"))
    // scalastyle:off nonascii
    assert(data(2) == Row("数据砖头"))
    // scalastyle:on nonascii
    assert(data(3) == Row("\"doh\""))
    assert(data.length == 4)
  }
}

class GlutenTextV1Suite extends GlutenTextSuite with GlutenSQLTestsBaseTrait {
  override def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "text")
}

class GlutenTextV2Suite extends GlutenTextSuite with GlutenSQLTestsBaseTrait {
  override def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")
}

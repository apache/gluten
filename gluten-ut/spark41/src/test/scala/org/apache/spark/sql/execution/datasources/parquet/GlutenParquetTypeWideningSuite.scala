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
package org.apache.spark.sql.execution.datasources.parquet

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{DataFrame, GlutenSQLTestsTrait}
import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DecimalType.{ByteDecimal, IntDecimal, LongDecimal, ShortDecimal}

import org.apache.hadoop.fs.Path
import org.apache.parquet.column.{Encoding, ParquetProperties}
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetOutputFormat}

import java.io.File

class GlutenParquetTypeWideningSuite extends ParquetTypeWideningSuite with GlutenSQLTestsTrait {

  import testImplicits._

  // Disable native writer so that writeParquetFiles() uses Spark's Parquet writer.
  // This suite tests the READ path. The native writer doesn't produce
  // DELTA_BINARY_PACKED/DELTA_BYTE_ARRAY encodings that the parent test's
  // V2 encoding assertions expect.
  override def sparkConf: SparkConf =
    super.sparkConf.set(GlutenConfig.NATIVE_WRITER_ENABLED.key, "false")

  // ====== Private methods copied from ParquetTypeWideningSuite ======
  // These are private in the parent class, so we must copy them to use in overridden tests.
  // The key change: removed withAllParquetReaders wrapper since Velox native reader
  // always behaves like the vectorized reader.

  private def checkAllParquetReaders(
      values: Seq[String],
      fromType: DataType,
      toType: DataType,
      expectError: Boolean): Unit = {
    val timestampRebaseModes = toType match {
      case _: TimestampNTZType | _: DateType =>
        Seq(LegacyBehaviorPolicy.CORRECTED, LegacyBehaviorPolicy.LEGACY)
      case _ =>
        Seq(LegacyBehaviorPolicy.CORRECTED)
    }
    for {
      dictionaryEnabled <- Seq(true, false)
      timestampRebaseMode <- timestampRebaseModes
    }
      withClue(
        s"with dictionary encoding '$dictionaryEnabled' with timestamp rebase mode " +
          s"'$timestampRebaseMode''") {
        withAllParquetWriters {
          withTempDir {
            dir =>
              val expected =
                writeParquetFiles(dir, values, fromType, dictionaryEnabled, timestampRebaseMode)
              if (expectError) {
                val exception = intercept[SparkException] {
                  readParquetFiles(dir, toType).collect()
                }
                assert(
                  exception.getCause
                    .isInstanceOf[SchemaColumnConvertNotSupportedException] ||
                    exception.getCause
                      .isInstanceOf[org.apache.parquet.io.ParquetDecodingException] ||
                    exception.getCause.getMessage.contains("PARQUET_CONVERSION_FAILURE"))
              } else {
                checkAnswer(readParquetFiles(dir, toType), expected.select($"a".cast(toType)))
              }
          }
        }
      }
  }

  private def readParquetFiles(dir: File, dataType: DataType): DataFrame = {
    spark.read.schema(s"a ${dataType.sql}").parquet(dir.getAbsolutePath)
  }

  private def writeParquetFiles(
      dir: File,
      values: Seq[String],
      dataType: DataType,
      dictionaryEnabled: Boolean,
      timestampRebaseMode: LegacyBehaviorPolicy.Value = LegacyBehaviorPolicy.CORRECTED)
      : DataFrame = {
    val repeatedValues = List.fill(if (dictionaryEnabled) 10 else 1)(values).flatten
    val df = repeatedValues.toDF("a").select(col("a").cast(dataType))
    withSQLConf(
      ParquetOutputFormat.ENABLE_DICTIONARY -> dictionaryEnabled.toString,
      SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> timestampRebaseMode.toString) {
      df.write.mode("overwrite").parquet(dir.getAbsolutePath)
    }

    if (dictionaryEnabled && !DecimalType.isByteArrayDecimalType(dataType)) {
      assertAllParquetFilesDictionaryEncoded(dir)
    }

    val isParquetV2 = spark.conf
      .getOption(ParquetOutputFormat.WRITER_VERSION)
      .contains(ParquetProperties.WriterVersion.PARQUET_2_0.toString)
    if (isParquetV2) {
      if (dictionaryEnabled) {
        assertParquetV2Encoding(dir, Encoding.PLAIN)
      } else if (DecimalType.is64BitDecimalType(dataType)) {
        assertParquetV2Encoding(dir, Encoding.DELTA_BINARY_PACKED)
      } else if (DecimalType.isByteArrayDecimalType(dataType)) {
        assertParquetV2Encoding(dir, Encoding.DELTA_BYTE_ARRAY)
      }
    }
    df
  }

  private def assertAllParquetFilesDictionaryEncoded(dir: File): Unit = {
    dir.listFiles(_.getName.endsWith(".parquet")).foreach {
      file =>
        val parquetMetadata = ParquetFileReader.readFooter(
          spark.sessionState.newHadoopConf(),
          new Path(dir.toString, file.getName),
          ParquetMetadataConverter.NO_FILTER)
        parquetMetadata.getBlocks.forEach {
          block =>
            block.getColumns.forEach {
              col =>
                assert(
                  col.hasDictionaryPage,
                  "This test covers dictionary encoding but column " +
                    s"'${col.getPath.toDotString}' in the test data is not dictionary encoded.")
            }
        }
    }
  }

  private def assertParquetV2Encoding(dir: File, expected_encoding: Encoding): Unit = {
    dir.listFiles(_.getName.endsWith(".parquet")).foreach {
      file =>
        val parquetMetadata = ParquetFileReader.readFooter(
          spark.sessionState.newHadoopConf(),
          new Path(dir.toString, file.getName),
          ParquetMetadataConverter.NO_FILTER)
        parquetMetadata.getBlocks.forEach {
          block =>
            block.getColumns.forEach {
              col =>
                assert(
                  col.getEncodings.contains(expected_encoding),
                  s"Expected column '${col.getPath.toDotString}' " +
                    s"to use encoding $expected_encoding " +
                    s"but found ${col.getEncodings}."
                )
            }
        }
    }
  }

  // ====== Override tests ======
  // Velox native reader always behaves like Spark's vectorized reader (no parquet-mr fallback).
  // In the parent tests, `expectError` is conditional on PARQUET_VECTORIZED_READER_ENABLED:
  // parquet-mr allows conversions that the vectorized reader rejects.
  // Since Velox always rejects, we override with expectError = true.

  for {
    (values: Seq[String], fromType: DataType, toType: DecimalType) <- Seq(
      (Seq("1", "2"), ByteType, DecimalType(1, 0)),
      (Seq("1", "2"), ByteType, ByteDecimal),
      (Seq("1", "2"), ShortType, ByteDecimal),
      (Seq("1", "2"), ShortType, ShortDecimal),
      (Seq("1", "2"), IntegerType, ShortDecimal),
      (Seq("1", "2"), ByteType, DecimalType(ByteDecimal.precision + 1, 1)),
      (Seq("1", "2"), ShortType, DecimalType(ShortDecimal.precision + 1, 1)),
      (Seq("1", "2"), LongType, IntDecimal),
      (Seq("1", "2"), ByteType, DecimalType(ByteDecimal.precision - 1, 0)),
      (Seq("1", "2"), ShortType, DecimalType(ShortDecimal.precision - 1, 0)),
      (Seq("1", "2"), IntegerType, DecimalType(IntDecimal.precision - 1, 0)),
      (Seq("1", "2"), LongType, DecimalType(LongDecimal.precision - 1, 0)),
      (Seq("1", "2"), ByteType, DecimalType(ByteDecimal.precision, 1)),
      (Seq("1", "2"), ShortType, DecimalType(ShortDecimal.precision, 1)),
      (Seq("1", "2"), IntegerType, DecimalType(IntDecimal.precision, 1)),
      (Seq("1", "2"), LongType, DecimalType(LongDecimal.precision, 1))
    )
  }
    testGluten(s"unsupported parquet conversion $fromType -> $toType") {
      checkAllParquetReaders(values, fromType, toType, expectError = true)
    }

  for {
    (fromPrecision, toPrecision) <-
      Seq(7 -> 5, 10 -> 5, 20 -> 5, 12 -> 10, 20 -> 10, 22 -> 20)
  }
    testGluten(
      s"parquet decimal precision change Decimal($fromPrecision, 2) -> Decimal($toPrecision, 2)") {
      checkAllParquetReaders(
        values = Seq("1.23", "10.34"),
        fromType = DecimalType(fromPrecision, 2),
        toType = DecimalType(toPrecision, 2),
        expectError = true)
    }

  for {
    ((fromPrecision, fromScale), (toPrecision, toScale)) <-
    // Narrowing precision and scale by the same amount.
    Seq(
      (7, 4) -> (5, 2),
      (10, 7) -> (5, 2),
      (20, 17) -> (5, 2),
      (12, 4) -> (10, 2),
      (20, 17) -> (10, 2),
      (22, 4) -> (20, 2)) ++
      // Increasing precision and decreasing scale.
      Seq((10, 6) -> (12, 4)) ++
      // Decreasing precision and increasing scale.
      Seq((12, 4) -> (10, 6), (22, 5) -> (20, 7)) ++
      // Increasing precision by a smaller amount than scale.
      Seq((5, 2) -> (6, 4), (10, 4) -> (12, 7))
  }
    testGluten(
      s"parquet decimal precision and scale change Decimal($fromPrecision, $fromScale) -> " +
        s"Decimal($toPrecision, $toScale)") {
      checkAllParquetReaders(
        values = Seq("1.23", "10.34"),
        fromType = DecimalType(fromPrecision, fromScale),
        toType = DecimalType(toPrecision, toScale),
        expectError = true)
    }
}

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
package org.apache.gluten.utils

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.crypto.{ColumnEncryptionProperties, FileEncryptionProperties}
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.ColumnPath
import org.apache.parquet.schema.{MessageType, PrimitiveType, Type, Types}
import org.junit.Assert._

import java.nio.charset.StandardCharsets
import java.util.Base64

import scala.collection.JavaConverters._

/**
 * This suite attempt to test parquet encryption for fallback of scan operator. Will check the
 * following:
 *   1. Plain Parquet File:
 *      - Writes a Parquet file with no encryption.
 *      - Asserts that parquet is not encrypted
 *
 * 2. Encrypted Parquet File (with encrypted footer):
 *   - Writes a Parquet file with column-level encryption and an encrypted footer.
 *   - Asserts that the file is encrypted.
 *
 * 3. Encrypted Parquet File (with plaintext footer):
 *   - Writes a Parquet file with column-level encryption but a plaintext (unencrypted) footer.
 *   - Ensures the file is still detected as encrypted despite the plaintext footer.
 */

class ParquetEncryptionDetectionSuite extends SharedSparkSession {

  private val masterKey =
    Base64.getEncoder.encodeToString("0123456789012345".getBytes(StandardCharsets.UTF_8))
  private val columnKey =
    Base64.getEncoder.encodeToString("1234567890123456".getBytes(StandardCharsets.UTF_8))

  private val schema: MessageType = Types
    .buildMessage()
    .addField(
      Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED).named("id"))
    .addField(
      Types
        .primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
        .named("name"))
    .named("TestSchema")

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.PARQUET_UNEXPECTED_METADATA_FALLBACK_ENABLED.key, "true")
  }

  private def writeParquet(
      path: String,
      encryptionProperties: Option[FileEncryptionProperties],
      data: Seq[Map[String, Any]]
  ): Unit = {
    val configuration = new Configuration()
    val writerBuilder = ExampleParquetWriter
      .builder(new Path(path))
      .withConf(configuration)
      .withType(schema)
      .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE)

    encryptionProperties.foreach(writerBuilder.withEncryption)

    val writer = writerBuilder.build()
    try {
      data.foreach {
        row =>
          val group = new SimpleGroup(schema)
          row.foreach {
            case (key, value) =>
              value match {
                case i: Int => group.add(key, i)
                case s: String => group.add(key, s)
              }
          }
          writer.write(group)
      }
    } finally {
      writer.close()
    }
  }

  def isFileEncrypted(filePath: String): Boolean = {
    val parquetOptions = new ParquetOptions(CaseInsensitiveMap(Map()), SQLConf.get)
    ParquetMetadataUtils
      .validateMetadata(Seq(filePath), new Configuration(), parquetOptions, 10)
      .isDefined
  }

  test("Detect encrypted Parquet with encrypted footer") {
    withTempDir {
      tempDir =>
        val filePath = s"${tempDir.getAbsolutePath}/encrypted_footer.parquet"
        val encryptionProps = FileEncryptionProperties
          .builder(Base64.getDecoder.decode(masterKey))
          .withEncryptedColumns(
            Map(
              ColumnPath.get("name") -> ColumnEncryptionProperties
                .builder(ColumnPath.get("name"))
                .withKey(Base64.getDecoder.decode(columnKey))
                .build()).asJava)
          .build()

        writeParquet(filePath, Some(encryptionProps), Seq(Map("id" -> 1, "name" -> "Alice")))
        assertTrue(isFileEncrypted(filePath))
    }
  }

  test("Detect encrypted Parquet without encrypted footer (plaintext footer)") {
    withTempDir {
      tempDir =>
        val filePath = s"${tempDir.getAbsolutePath}/plaintext_footer.parquet"
        val encryptionProps = FileEncryptionProperties
          .builder(Base64.getDecoder.decode(masterKey))
          .withEncryptedColumns(
            Map(
              ColumnPath.get("name") -> ColumnEncryptionProperties
                .builder(ColumnPath.get("name"))
                .withKey(Base64.getDecoder.decode(columnKey))
                .build()).asJava)
          .withPlaintextFooter()
          .build()

        writeParquet(filePath, Some(encryptionProps), Seq(Map("id" -> 1, "name" -> "Bob")))
        assertTrue(isFileEncrypted(filePath))
    }
  }

  test("Detect plain (unencrypted) Parquet file") {
    withTempDir {
      tempDir =>
        val filePath = s"${tempDir.getAbsolutePath}/plain.parquet"

        writeParquet(filePath, None, Seq(Map("id" -> 1, "name" -> "Charlie")))
        assertFalse(isFileEncrypted(filePath))
    }
  }
}

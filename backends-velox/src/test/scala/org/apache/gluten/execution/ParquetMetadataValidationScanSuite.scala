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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, LocatedFileStatus, Path, RawLocalFileSystem, RemoteIterator}
import org.apache.parquet.crypto.{ColumnEncryptionProperties, FileEncryptionProperties}
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.ColumnPath
import org.apache.parquet.schema.{PrimitiveType, Type, Types}

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.Collections
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * Parquet scan validation must not touch the scan root paths when
 * spark.gluten.sql.fallbackUnexpectedMetadataParquet is off (the default). Otherwise every scan
 * validation pays a storage round trip per root path for a check whose result is never used.
 */
class ParquetMetadataValidationScanSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  private val dataSchema = new StructType().add("id", IntegerType).add("name", StringType)

  // Same argument shapes as BasicScanExecTransformer.doValidateInternal. The counting file system
  // replaces the local one only in this Hadoop conf; file:// paths skip the native scheme check.
  private def validateParquetScan(rootPaths: Seq[String]): ValidationResult = {
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.file.impl", classOf[CountingLocalFileSystem].getName)
    hadoopConf.setBoolean("fs.file.impl.disable.cache", true)
    CountingLocalFileSystem.reset()
    BackendsApiManager.getSettings.validateScanExec(
      ReadFileFormat.ParquetReadFormat,
      dataSchema.fields,
      dataSchema,
      rootPaths,
      Map.empty[String, String],
      hadoopConf,
      Set.empty[ReadFileFormat]
    )
  }

  private def describe(result: ValidationResult): String =
    if (result.ok()) "succeeded" else result.reason()

  private def writeEncryptedParquet(dir: File): Unit = {
    val schema = Types
      .buildMessage()
      .addField(
        Types
          .primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
          .named("id"))
      .addField(
        Types
          .primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
          .named("name"))
      .named("TestSchema")
    val encryption = FileEncryptionProperties
      .builder("0123456789012345".getBytes(StandardCharsets.UTF_8))
      .withEncryptedColumns(
        Collections.singletonMap(
          ColumnPath.get("name"),
          ColumnEncryptionProperties
            .builder(ColumnPath.get("name"))
            .withKey("1234567890123456".getBytes(StandardCharsets.UTF_8))
            .build()
        ))
      .build()
    val writer = ExampleParquetWriter
      .builder(new Path(new File(dir, "encrypted.parquet").getAbsolutePath))
      .withConf(new Configuration())
      .withType(schema)
      .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
      .withEncryption(encryption)
      .build()
    try {
      val group = new SimpleGroup(schema)
      group.add("id", 1)
      group.add("name", "Alice")
      writer.write(group)
    } finally {
      writer.close()
    }
  }

  test("metadata validation off (default): no file system calls on the scan root paths") {
    assert(!GlutenConfig.get.parquetMetadataValidationEnabled)
    withTempDir {
      dir =>
        writeEncryptedParquet(dir)
        val missing = new File(dir, "missing").getAbsolutePath
        val result = validateParquetScan(Seq(dir.getAbsolutePath, missing))
        assert(result.ok(), describe(result))
        assert(
          CountingLocalFileSystem.calls.isEmpty,
          s"unexpected file system calls: ${CountingLocalFileSystem.calls}")
    }
  }

  test("metadata validation on: a missing root path is skipped after exists()") {
    withSQLConf(GlutenConfig.PARQUET_UNEXPECTED_METADATA_FALLBACK_ENABLED.key -> "true") {
      withTempDir {
        dir =>
          val result = validateParquetScan(Seq(new File(dir, "missing").getAbsolutePath))
          assert(result.ok(), describe(result))
          assert(CountingLocalFileSystem.calls.contains("exists"))
          assert(!CountingLocalFileSystem.calls.contains("listFiles"))
      }
    }
  }

  test("metadata validation on: encrypted Parquet under an existing root path falls back") {
    withSQLConf(GlutenConfig.PARQUET_UNEXPECTED_METADATA_FALLBACK_ENABLED.key -> "true") {
      withTempDir {
        dir =>
          writeEncryptedParquet(dir)
          val result = validateParquetScan(Seq(dir.getAbsolutePath))
          assert(!result.ok())
          assert(
            result.reason().contains("Detected unsupported metadata in parquet files"),
            result.reason())
          assert(CountingLocalFileSystem.calls.contains("exists"))
          assert(CountingLocalFileSystem.calls.contains("listFiles"))
      }
    }
  }
}

/** Local file system that records the path operations made through it. */
class CountingLocalFileSystem extends RawLocalFileSystem {
  private def record(op: String): Unit = CountingLocalFileSystem.record(op)

  override def exists(f: Path): Boolean = {
    record("exists")
    super.exists(f)
  }

  override def getFileStatus(f: Path): FileStatus = {
    record("getFileStatus")
    super.getFileStatus(f)
  }

  override def listStatus(f: Path): Array[FileStatus] = {
    record("listStatus")
    super.listStatus(f)
  }

  override def listFiles(f: Path, recursive: Boolean): RemoteIterator[LocatedFileStatus] = {
    record("listFiles")
    super.listFiles(f, recursive)
  }
}

object CountingLocalFileSystem {
  private val ops = new ConcurrentLinkedQueue[String]()

  def record(op: String): Unit = ops.add(op)

  def reset(): Unit = ops.clear()

  def calls: Seq[String] = ops.toArray(new Array[String](0)).toSeq
}

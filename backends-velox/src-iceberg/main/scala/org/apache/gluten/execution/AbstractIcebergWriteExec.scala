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

import org.apache.gluten.IcebergNestedFieldVisitor
import org.apache.gluten.config.GlutenConfig.COLUMNAR_PARQUET_WRITE_BLOCK_SIZE
import org.apache.gluten.config.VeloxConfig.{MAX_TARGET_FILE_SIZE_SESSION, PARQUET_DICT_SIZE_BYTES, PARQUET_PAGE_SIZE_BYTES}
import org.apache.gluten.connector.write.{ColumnarBatchDataWriterFactory, ColumnarStreamingDataWriterFactory, IcebergDataWriteFactory}

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import org.apache.iceberg.spark.source.IcebergWriteUtil
import org.apache.iceberg.types.{Types, TypeUtil}
import org.apache.iceberg.types.Type.TypeID

import java.util

import scala.collection.JavaConverters._

abstract class AbstractIcebergWriteExec extends IcebergWriteExec {

  override protected def supportsSortedWrite: Boolean = true

  private val parquetPageRowLimitSession =
    "spark.gluten.sql.columnar.backend.velox.parquet_writer_page_row_limit"

  private val parquetPageVersionSession =
    "spark.gluten.sql.columnar.backend.velox.parquet_writer_datapage_version"

  private val parquetCompressionLevelSession =
    "spark.gluten.sql.columnar.backend.velox.parquet_writer_compression_level"

  // the writer factory works for both batch and streaming
  private def createIcebergDataWriteFactory(schema: StructType): IcebergDataWriteFactory = {
    val writeSchema = IcebergWriteUtil.getWriteSchema(write)
    val nestedField = TypeUtil.visit(writeSchema, new IcebergNestedFieldVisitor)
    // Filter out metadata columns from the Spark output schema and reorder to match Iceberg schema
    // Spark 4.0 may include metadata columns in the output schema during UPDATE operations,
    // but these should not be written to the Iceberg table
    val writeFieldNames = writeSchema.columns().asScala.map(_.name()).toSet
    val filteredSchema = StructType(
      schema.fields.filter(field => writeFieldNames.contains(field.name))
    )

    val icebergProperties = new util.HashMap[String, String]

    // Fix for https://github.com/apache/gluten/issues/13429
    // Spark's TimestampType maps to Iceberg's `timestamptz` (shouldAdjustToUTC = true).
    // Velox's Parquet writer derives isAdjustedToUTC from whether the Arrow timestamp type
    // carries a non-empty timezone string (ArrowSchema.cpp:240). Without the serde property
    // below, the Arrow schema is built with an empty timezone → isAdjustedToUTC=false in
    // the Parquet footer, violating the Iceberg spec and breaking strict readers such as
    // apache/iceberg-go during compaction ("cannot promote timestamp to timestamptz").
    //
    // Limitation: this sets a file-level timezone flag. Tables containing both `timestamptz`
    // and `timestamp_ntz` columns are not currently supported by the native Iceberg write
    // path (validation in IcebergWriteExec blocks them). If per-column support is needed in
    // future, consider Option 2/3 discussed in https://github.com/apache/gluten/issues/13429.
    val hasTimestampTz = writeSchema
      .columns()
      .asScala
      .exists {
        col =>
          col.`type`().typeId() == TypeID.TIMESTAMP &&
          col.`type`().asInstanceOf[Types.TimestampType].shouldAdjustToUTC()
      }
    if (hasTimestampTz) {
      icebergProperties.put("parquet.writer.timestamp.timezone", "UTC")
    }

    Seq(
      PARQUET_PAGE_SIZE_BYTES.key -> getParquetPageSizeBytes,
      COLUMNAR_PARQUET_WRITE_BLOCK_SIZE.key -> getParquetRowGroupSizeBytes,
      MAX_TARGET_FILE_SIZE_SESSION.key -> getTargetFileSizeBytes,
      PARQUET_DICT_SIZE_BYTES.key -> getDictSizeBytes
    ).foreach {
      case (key, value) =>
        val overrideValue = SQLConf.get.getConfString(key, null)
        if (overrideValue == null) {
          icebergProperties.put(key, value)
        } else {
          icebergProperties.put(key, normalizeCapacityString(overrideValue))
        }
    }

    Seq(
      parquetPageRowLimitSession -> getParquetPageRowLimit,
      parquetPageVersionSession -> getParquetPageVersion).foreach {
      case (key, value) =>
        icebergProperties.put(key, SQLConf.get.getConfString(key, value))
    }

    if (Seq("gzip", "zstd").exists(_.equalsIgnoreCase(getCodec))) {
      Option(SQLConf.get.getConfString(parquetCompressionLevelSession, null))
        .orElse(getParquetCompressionLevel)
        .foreach(level => icebergProperties.put(parquetCompressionLevelSession, level))
    }

    IcebergDataWriteFactory(
      filteredSchema,
      getFileFormat(IcebergWriteUtil.getFileFormat(write)),
      IcebergWriteUtil.getDirectory(write),
      getCodec,
      getPartitionSpec,
      IcebergWriteUtil.getSortOrder(write),
      nestedField,
      icebergProperties,
      IcebergWriteUtil.getQueryId(write)
    )
  }

  override protected def createBatchWriterFactory(
      schema: StructType): ColumnarBatchDataWriterFactory = {
    createIcebergDataWriteFactory(schema)
  }

  override protected def createStreamingWriterFactory(
      schema: StructType): ColumnarStreamingDataWriterFactory = {
    createIcebergDataWriteFactory(schema)
  }
}

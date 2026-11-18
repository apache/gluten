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
package org.apache.spark.sql.delta

import org.apache.gluten.backendsapi.velox.VeloxValidatorApi

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.execution.datasources.parquet.{ParquetOptions, ParquetUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.column.ParquetProperties.WriterVersion
import org.apache.parquet.crypto.EncryptionPropertiesFactory
import org.apache.parquet.hadoop.ParquetOutputFormat

import java.util.Locale

import scala.util.Try

/**
 * Decides whether the Velox writer writes what vanilla Delta would: the same rows, schema, physical
 * types, field IDs and partition columns, honoring the codec, file-size limit, Parquet version and
 * encryption settings. Low-level layout (row group and page sizes, encodings) is Velox's own.
 *
 * It reads the settings vanilla's writer uses: the options Delta computed for this write, the
 * session SQL configuration, and the job's Hadoop configuration, which parquet-mr reads and to
 * which Delta adds the table properties. It fails closed: an option it does not know falls back to
 * vanilla Delta.
 */
object GlutenDeltaNativeWriteGate {

  /** Checked before Delta's writeFiles starts, on the data to write. */
  def unsupportedSchema(schema: StructType): Option[String] = {
    // The native write path inserts a RowToVeloxColumnarExec transition whose
    // SparkArrowUtil.toArrowSchema call throws for types without an Arrow mapping (e.g. variant).
    VeloxValidatorApi
      .validateSchema(schema)
      .map(reason => s"Schema is not supported by Velox ($reason)")
      .orElse(Option.when(exists(schema, _ == NullType)) {
        // Delta drops VOID columns or rejects the write; the Velox writer has no VOID type.
        "VOID columns are not supported by the Velox writer"
      })
      .orElse(Option.when(exists(schema, _ == TimestampNTZType)) {
        // The Velox writer annotates TIMESTAMP_NTZ as isAdjustedToUTC=true; Delta requires false.
        "TIMESTAMP_NTZ columns are not supported by the Velox writer"
      })
  }

  /**
   * Checked where Delta calls its file writer, with the job Hadoop configuration and the options
   * Delta computed for it.
   */
  def unsupportedWrite(
      sqlConf: SQLConf,
      hadoopConf: Configuration,
      options: Map[String, String],
      dataSchema: StructType,
      partitionColumns: Seq[Attribute],
      metadata: Metadata): Option[String] = {
    // The columns written to the data files: partition values go to the directory path only,
    // unless Delta writes them to the files too.
    val fileSchema = if (isTrue(options, writePartitionColumns)) {
      dataSchema
    } else {
      StructType(dataSchema.filterNot(field => partitionColumns.exists(_.name == field.name)))
    }
    unsupportedSchema(dataSchema)
      .orElse(unsupportedOption(options))
      .orElse(Option(hadoopConf.get(cryptoFactoryClass)).map {
        // parquet-mr encrypts the files when the job conf names a crypto factory.
        _ => "Parquet encryption is not supported by the Velox writer"
      })
      .orElse(unsupportedCodec(sqlConf, options))
      .orElse(unsupportedWriterVersion(hadoopConf))
      .orElse(Option.when(partitionColumns.isEmpty && maxRecordsPerFile(sqlConf, options) > 0) {
        // Only the partitioned native writer splits a batch across files.
        "maxRecordsPerFile is not supported by the unpartitioned Velox writer"
      })
      .orElse(unsupportedDatetimeEncoding(sqlConf, hadoopConf, options, fileSchema))
      .orElse(unsupportedLegacyFormat(sqlConf, hadoopConf, fileSchema))
      .orElse(Option.when(partitionColumns.nonEmpty && isTrue(options, writePartitionColumns)) {
        "Partition columns in data files are not written by the Velox writer"
      })
      .orElse(Option.when(partitionColumns.nonEmpty && sqlConf.maxConcurrentOutputFileWriters > 0) {
        "Concurrent output file writers are not supported by the native Delta writer"
      })
      .orElse(Option.when(metadata.columnMappingMode != NoMapping) {
        // Delta writes Parquet field IDs for both name and id column mapping.
        "Parquet field IDs for column mapping are not written by the Velox writer"
      })
      .orElse(Option.when(ParquetUtils.hasFieldIds(fileSchema)) {
        // Vanilla writes a column's parquet.field.id metadata as its Parquet field ID.
        "Parquet field IDs are not written by the Velox writer"
      })
      .orElse(Option.when(IcebergCompat.isAnyEnabled(metadata)) {
        "Iceberg compatibility is not supported by the Velox writer"
      })
  }

  private val writePartitionColumns = DeltaOptions.WRITE_PARTITION_COLUMNS
  // SQLConf.VARIANT_INFER_SHREDDING_SCHEMA, which Spark 4.0 does not have.
  private val variantInferShreddingSchema = "spark.sql.variant.inferShreddingSchema"
  // The codecs the Velox writer maps (VeloxWriterUtils); it would write others as SNAPPY.
  private val supportedCodecs = Set("UNCOMPRESSED", "SNAPPY", "GZIP", "LZ4", "ZSTD")
  private val cryptoFactoryClass = EncryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME
  private val parquetV1 = WriterVersion.PARQUET_1_0

  private def unsupportedOption(options: Map[String, String]): Option[String] =
    options.collectFirst {
      case (key, value) if !supportedOption(lower(key), value) =>
        s"Writer option $key=$value is not supported by the Velox writer"
    }

  private def supportedOption(key: String, value: String): Boolean = key match {
    // Checked by unsupportedCodec and maxRecordsPerFile, together with the session conf.
    case k if k == lower(DeltaOptions.COMPRESSION) => true
    case k if k == lower(DeltaOptions.MAX_RECORDS_PER_FILE) => true
    case k if k == lower(writePartitionColumns) => true
    // Velox writes Parquet v2 data pages, but not the encodings of parquet-mr's v2 files.
    case k if k == lower(DeltaOptions.PARQUET_VERSION) => value.equalsIgnoreCase("v1")
    // Checked by unsupportedDatetimeEncoding, together with the session conf.
    case k if k == lower(DeltaOptions.PARQUET_OUTPUT_TIMESTAMP_TYPE) => true
    // On Spark 4.1+ Delta always passes this option; variant data itself falls back on schema.
    case k if k == lower(variantInferShreddingSchema) => value.equalsIgnoreCase("false")
    case _ => false
  }

  /** The codec vanilla's ParquetFileFormat would use: the option, else the session conf. */
  private def unsupportedCodec(sqlConf: SQLConf, options: Map[String, String]): Option[String] = {
    val codec = new ParquetOptions(CaseInsensitiveMap(options), sqlConf).compressionCodecClassName
    Option.when(!supportedCodecs.contains(codec.toUpperCase(Locale.ROOT))) {
      s"Compression codec $codec is not supported by the Velox writer"
    }
  }

  /**
   * parquet-mr takes the Parquet version from the job conf, where a table property or a cluster
   * setting puts it; Spark 4.2 also merges the writer option into it. Only v1 is written natively.
   */
  private def unsupportedWriterVersion(hadoopConf: Configuration): Option[String] =
    Option(hadoopConf.get(ParquetOutputFormat.WRITER_VERSION))
      .filterNot(version => Try(WriterVersion.fromString(version)).toOption.contains(parquetV1))
      .map(version => s"Parquet writer version $version is not supported by the Velox writer")

  private def maxRecordsPerFile(sqlConf: SQLConf, options: Map[String, String]): Long =
    find(
      options,
      DeltaOptions.MAX_RECORDS_PER_FILE).map(_.toLong).getOrElse(sqlConf.maxRecordsPerFile)

  /**
   * Velox writes timestamps as INT64 microseconds and never rebases dates or timestamps. Vanilla
   * writes INT96 by default (spark.sql.parquet.outputTimestampType). Spark 4.0/4.1 take the type
   * from the session conf, Spark 4.2 from the job conf, where a table property overrides the
   * session. Those, and the writer option of that name, must all ask for microseconds.
   */
  private def unsupportedDatetimeEncoding(
      sqlConf: SQLConf,
      hadoopConf: Configuration,
      options: Map[String, String],
      schema: StructType): Option[String] = {
    val hasTimestamp = exists(schema, _ == TimestampType)
    val hasDatetime = hasTimestamp || exists(schema, _ == DateType)
    val micros = SQLConf.ParquetOutputTimestampType.TIMESTAMP_MICROS.toString
    val timestampTypes = Seq(sqlConf.parquetOutputTimestampType.toString) ++
      Option(hadoopConf.get(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key)) ++
      find(options, DeltaOptions.PARQUET_OUTPUT_TIMESTAMP_TYPE)
    val rebaseMode = sqlConf.getConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE).toString
    if (hasTimestamp && !timestampTypes.forall(_.equalsIgnoreCase(micros))) {
      Some(s"Timestamps are written as ${timestampTypes.mkString("/")}, not by the Velox writer")
    } else if (hasDatetime && !rebaseMode.equalsIgnoreCase("CORRECTED")) {
      Some(s"Datetime rebase mode $rebaseMode is not supported by the Velox writer")
    } else {
      None
    }
  }

  /**
   * spark.sql.parquet.writeLegacyFormat changes how decimals, arrays and maps are written. The
   * Velox writer follows the session conf, and writes legacy decimals but not legacy arrays and
   * maps. Spark 4.2 takes the setting from the job conf, where a table property overrides the
   * session, so a job conf that disagrees with the session falls back.
   */
  private def unsupportedLegacyFormat(
      sqlConf: SQLConf,
      hadoopConf: Configuration,
      schema: StructType): Option[String] = {
    val key = SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key
    val legacy = sqlConf.writeLegacyParquetFormat
    if (legacy && exists(schema, isNested)) {
      Some("The legacy Parquet layout of arrays and maps is not written by the Velox writer")
    } else {
      Option(hadoopConf.get(key))
        .filterNot(_.equalsIgnoreCase(legacy.toString))
        .map(value => s"$key=$value in the job conf is not followed by the Velox writer")
    }
  }

  private def find(options: Map[String, String], key: String): Option[String] =
    options.collectFirst { case (k, v) if k.equalsIgnoreCase(key) => v }

  private def isTrue(options: Map[String, String], key: String): Boolean =
    find(options, key).exists(_.equalsIgnoreCase("true"))

  private def lower(key: String): String = key.toLowerCase(Locale.ROOT)

  private def isNested(dataType: DataType): Boolean = dataType match {
    case _: ArrayType | _: MapType => true
    case _ => false
  }

  /** Whether `p` holds for the type or any type nested in it. */
  private def exists(dataType: DataType, p: DataType => Boolean): Boolean = p(dataType) || {
    dataType match {
      case udt: UserDefinedType[_] => exists(udt.sqlType, p)
      case struct: StructType => struct.fields.exists(f => exists(f.dataType, p))
      case array: ArrayType => exists(array.elementType, p)
      case map: MapType => exists(map.keyType, p) || exists(map.valueType, p)
      case _ => false
    }
  }
}

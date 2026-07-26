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
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.VeloxConfig.{MAX_TARGET_FILE_SIZE_SESSION, PARQUET_DICT_SIZE_BYTES, PARQUET_PAGE_SIZE_BYTES}
import org.apache.gluten.connector.write.{ColumnarBatchDataWriterFactory, ColumnarStreamingDataWriterFactory, IcebergDeltaDataWriteFactory}
import org.apache.gluten.proto.{IcebergExistingDeletionVector, IcebergNativeWriteInfo, IcebergNativeWriteMode}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DeltaWrite, RowLevelOperation}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.WriteDeltaExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.iceberg.{DeleteFile, PartitionField, PartitionSpec, Schema, Table, TableProperties}
import org.apache.iceberg.avro.AvroSchemaUtil
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.spark.source.SparkPositionDeltaWriteUtil
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.types.TypeUtil

import java.util

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

case class VeloxIcebergWriteDeltaExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: DeltaWrite,
    nativeDataSchema: StructType,
    nativeWriteInfo: Array[Byte],
    referencedDataFiles: util.Map[String, SparkPositionDeltaWriteUtil.ReferencedDataFile])
  extends ColumnarV2TableWriteExec {

  @transient private var referencedDataFilesBroadcast: Broadcast[util.Map[
    String,
    SparkPositionDeltaWriteUtil.ReferencedDataFile]] = _

  private lazy val writeInfo = SparkPositionDeltaWriteUtil.writeInfo(write)
  private lazy val table = writeInfo.table()
  private lazy val icebergWriteSchema: Schema =
    Option(writeInfo.dataSchema()).getOrElse(table.schema())

  override protected def createBatchWriterFactory(
      schema: StructType): ColumnarBatchDataWriterFactory = {
    createDataWriteFactory()
  }

  override protected def createStreamingWriterFactory(
      schema: StructType): ColumnarStreamingDataWriterFactory = {
    createDataWriteFactory()
  }

  private def createDataWriteFactory(): IcebergDeltaDataWriteFactory = {
    val nestedField = TypeUtil.visit(icebergWriteSchema, new IcebergNestedFieldVisitor)
    val properties = new util.HashMap[String, String]
    Seq(
      PARQUET_PAGE_SIZE_BYTES.key -> tableProperty(
        TableProperties.PARQUET_PAGE_SIZE_BYTES,
        TableProperties.PARQUET_PAGE_SIZE_BYTES_DEFAULT.toString),
      MAX_TARGET_FILE_SIZE_SESSION.key -> tableProperty(
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT.toString),
      PARQUET_DICT_SIZE_BYTES.key -> tableProperty(
        TableProperties.PARQUET_DICT_SIZE_BYTES,
        TableProperties.PARQUET_DICT_SIZE_BYTES_DEFAULT.toString)
    ).foreach {
      case (key, value) =>
        if (SQLConf.get.getConfString(key, null) == null) {
          properties.put(key, normalizeCapacityString(value))
        }
    }

    IcebergDeltaDataWriteFactory(
      nativeDataSchema,
      1,
      dataDirectory,
      parquetCodec,
      table.spec(),
      table.sortOrder(),
      nestedField,
      properties,
      writeInfo.queryId(),
      nativeWriteInfo,
      table.specs(),
      table.io(),
      getReferencedDataFilesBroadcast
    )
  }

  private def getReferencedDataFilesBroadcast
      : Broadcast[util.Map[String, SparkPositionDeltaWriteUtil.ReferencedDataFile]] = synchronized {
    if (referencedDataFilesBroadcast == null) {
      referencedDataFilesBroadcast = writeInfo.sparkContext().broadcast(referencedDataFiles)
    }
    referencedDataFilesBroadcast
  }

  private def tableProperty(key: String, defaultValue: String): String = {
    Option(writeInfo.writeProperties().get(key))
      .orElse(Option(table.properties().get(key)))
      .getOrElse(defaultValue)
  }

  private def parquetCodec: String = {
    val codec = tableProperty(
      TableProperties.PARQUET_COMPRESSION,
      TableProperties.PARQUET_COMPRESSION_DEFAULT)
    if (codec.equalsIgnoreCase("uncompressed")) "none" else codec
  }

  private def dataDirectory: String = {
    VeloxIcebergWriteDeltaExec.dataDirectory(table)
  }

  private def normalizeCapacityString(value: String): String = {
    val trimmed = value.trim
    if (trimmed.lastOption.exists(_.isDigit)) s"${trimmed}B" else trimmed
  }

  override def doValidateInternal(): ValidationResult = {
    Option(SparkPositionDeltaWriteUtil.validate(write)) match {
      case Some(reason) => return ValidationResult.failed(reason)
      case None =>
    }
    if (table.sortOrder().isSorted) {
      return ValidationResult.failed("Native Iceberg mutation write does not support sort order")
    }
    if (
      referencedDataFiles
        .values()
        .asScala
        .exists(_.spec().specId() != table.spec().specId())
    ) {
      return ValidationResult.failed(
        "Native Iceberg mutation write does not support files from an older partition spec")
    }
    VeloxIcebergWriteDeltaExec.validateReferencedDataFileLocations(
      dataDirectory,
      referencedDataFiles.keySet().asScala) match {
      case Some(reason) => return ValidationResult.failed(reason)
      case None =>
    }
    VeloxIcebergWriteDeltaExec.validateWriteSchemaAndPartition(
      icebergWriteSchema,
      table.spec(),
      query.schema) match {
      case Some(reason) => return ValidationResult.failed(reason)
      case None =>
    }
    BackendsApiManager.getValidatorApiInstance.doSchemaValidate(query.schema) match {
      case Some(reason) => ValidationResult.failed(reason)
      case None => ValidationResult.succeeded
    }
  }

  override protected def run(): Seq[InternalRow] = {
    try {
      super.run()
    } finally {
      synchronized {
        if (referencedDataFilesBroadcast != null) {
          referencedDataFilesBroadcast.destroy()
          referencedDataFilesBroadcast = null
        }
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(query = newChild)
}

object VeloxIcebergWriteDeltaExec {
  def apply(original: WriteDeltaExec): Option[VeloxIcebergWriteDeltaExec] = {
    try {
      val reason = SparkPositionDeltaWriteUtil.validate(original.write)
      if (reason != null) {
        return None
      }

      val info = SparkPositionDeltaWriteUtil.writeInfo(original.write)
      val referenced = SparkPositionDeltaWriteUtil.referencedDataFiles(original.write)
      if (info.table().sortOrder().isSorted) {
        return None
      }
      if (referenced.values().asScala.exists(_.spec().specId() != info.table().spec().specId())) {
        return None
      }
      if (
        validateReferencedDataFileLocations(
          dataDirectory(info.table()),
          referenced.keySet().asScala).nonEmpty
      ) {
        return None
      }

      val mode = writeMode(info.command())
      val dataSchema = nativeDataSchema(info, mode)
      if (
        validateWriteSchemaAndPartition(
          Option(info.dataSchema()).getOrElse(info.table().schema()),
          info.table().spec(),
          original.query.schema).nonEmpty
      ) {
        return None
      }
      val existingDeletionVectors = referenced
        .values()
        .asScala
        .flatMap(ref => Option(ref.existingDeletionVector()))
        .toSeq
      val nativeInfo = buildNativeWriteInfo(
        info.command(),
        original.query.schema,
        dataSchema,
        info.deleteSparkType(),
        existingDeletionVectors)

      Some(
        VeloxIcebergWriteDeltaExec(
          original.query,
          original.refreshCache,
          original.write,
          dataSchema,
          nativeInfo.toByteArray,
          referenced))
    } catch {
      case NonFatal(_) => None
    }
  }

  private[execution] def buildNativeWriteInfo(
      command: RowLevelOperation.Command,
      inputSchema: StructType,
      dataSchema: StructType,
      deleteSchema: StructType,
      existingDeletionVectors: Seq[DeleteFile]): IcebergNativeWriteInfo = {
    if (deleteSchema == null || deleteSchema.length < 2) {
      throw new IllegalArgumentException("Missing Iceberg file-path or row-position schema")
    }

    val mode = writeMode(command)
    val dataChannels =
      if (mode == IcebergNativeWriteMode.ICEBERG_NATIVE_WRITE_MODE_DELETION_VECTOR) {
        Seq.empty
      } else {
        resolveDataChannels(inputSchema, dataSchema)
      }
    val nativeInfo = IcebergNativeWriteInfo
      .newBuilder()
      .setWriteMode(mode)
      .addAllDataColumnIndices(dataChannels.map(Int.box).asJava)
      .setOperationColumnIndex(resolveOperationChannel(inputSchema))
      .setFilePathColumnIndex(resolveChannel(inputSchema, deleteSchema.fields.apply(0).name))
      .setRowPositionColumnIndex(resolveChannel(inputSchema, deleteSchema.fields.apply(1).name))

    existingDeletionVectors.foreach {
      deletionVector =>
        nativeInfo.addExistingDeletionVectors(toExistingDeletionVector(deletionVector))
    }
    nativeInfo.build()
  }

  private def writeMode(command: RowLevelOperation.Command): IcebergNativeWriteMode = {
    command match {
      case RowLevelOperation.Command.DELETE =>
        IcebergNativeWriteMode.ICEBERG_NATIVE_WRITE_MODE_DELETION_VECTOR
      case RowLevelOperation.Command.UPDATE | RowLevelOperation.Command.MERGE =>
        IcebergNativeWriteMode.ICEBERG_NATIVE_WRITE_MODE_MERGE
      case other =>
        throw new IllegalArgumentException(s"Unsupported Iceberg row-level command: $other")
    }
  }

  private def toExistingDeletionVector(
      deletionVector: DeleteFile): IcebergExistingDeletionVector = {
    IcebergExistingDeletionVector
      .newBuilder()
      .setReferencedDataFile(deletionVector.referencedDataFile())
      .setPuffinPath(deletionVector.location().toString)
      .setContentOffset(deletionVector.contentOffset())
      .setContentLength(deletionVector.contentSizeInBytes())
      .setRecordCount(deletionVector.recordCount())
      .setFileSizeInBytes(deletionVector.fileSizeInBytes())
      .build()
  }

  private def nativeDataSchema(
      info: SparkPositionDeltaWriteUtil.WriteInfo,
      mode: IcebergNativeWriteMode): StructType = {
    if (
      mode != IcebergNativeWriteMode.ICEBERG_NATIVE_WRITE_MODE_DELETION_VECTOR &&
      info.dataSparkType() != null &&
      info.dataSparkType().nonEmpty
    ) {
      info.dataSparkType()
    } else {
      SparkSchemaUtil.convert(info.table().schema())
    }
  }

  private def resolveOperationChannel(schema: StructType): Int = {
    if (schema.isEmpty) {
      throw new IllegalArgumentException("Missing Spark row-operation column")
    }
    schema.fields.head.dataType match {
      case ByteType | ShortType | IntegerType | LongType => 0
      case other =>
        throw new IllegalArgumentException(
          s"Spark row-operation column must be integral, found $other")
    }
  }

  private def resolveDataChannels(inputSchema: StructType, dataSchema: StructType): Seq[Int] = {
    dataSchema.fields.map(field => resolveChannel(inputSchema, field.name)).toSeq
  }

  private def resolveChannel(schema: StructType, name: String): Int = {
    val matches = schema.fields.zipWithIndex.collect {
      case (field, index) if field.name == name => index
    }
    if (matches.size != 1) {
      throw new IllegalArgumentException(
        s"Expected exactly one channel named '$name', found ${matches.size}")
    }
    matches.head
  }

  private[execution] def validateWriteSchemaAndPartition(
      schema: Schema,
      spec: PartitionSpec,
      inputSchema: StructType): Option[String] = {
    if (hasUnsupportedDataType(schema.asStruct())) {
      return Some("Native Iceberg mutation write does not support UUID or FIXED data types")
    }
    if (spec.isPartitioned) {
      val topLevelIds = spec.schema().columns().asScala.map(_.fieldId()).toSet
      if (
        spec
          .fields()
          .asScala
          .exists(
            field =>
              !isSupportedPartitionType(spec, field) ||
                !topLevelIds.contains(field.sourceId()) ||
                field.transform().isVoid)
      ) {
        return Some(
          "Native Iceberg mutation write does not support this partition type, " +
            "void transforms, or nested partition columns")
      }
    }
    if (
      inputSchema.fields.exists(
        field => AvroSchemaUtil.makeCompatibleName(field.name) != field.name)
    ) {
      return Some("Native Iceberg mutation write does not support incompatible column names")
    }
    None
  }

  private[execution] def validateReferencedDataFileLocations(
      dataDirectory: String,
      referencedDataFilePaths: Iterable[String]): Option[String] = {
    val prefix = if (dataDirectory.endsWith("/")) dataDirectory else s"$dataDirectory/"
    referencedDataFilePaths
      .find(
        path =>
          path == null ||
            !path.startsWith(prefix) ||
            path
              .substring(prefix.length)
              .split('/')
              .contains(".."))
      .map(
        path =>
          "Native Iceberg mutation write requires referenced data files under the configured " +
            s"data directory $dataDirectory, found ${Option(path).getOrElse("<null>")}")
  }

  private def dataDirectory(table: Table): String = {
    val location = table.locationProvider().newDataLocation("")
    if (!location.endsWith("/")) {
      throw new IllegalArgumentException(
        s"Iceberg location provider returned an invalid data directory: $location")
    }
    location.substring(0, location.length - 1)
  }

  private def isSupportedPartitionType(spec: PartitionSpec, field: PartitionField): Boolean = {
    val typeId = spec.schema().findType(field.sourceId()).typeId()
    typeId != TypeID.DOUBLE && typeId != TypeID.FLOAT
  }

  private def hasUnsupportedDataType(dataType: org.apache.iceberg.types.Type): Boolean = {
    dataType.typeId() match {
      case TypeID.UUID | TypeID.FIXED => true
      case TypeID.STRUCT =>
        dataType
          .asStructType()
          .fields()
          .asScala
          .exists(field => hasUnsupportedDataType(field.`type`()))
      case TypeID.LIST => hasUnsupportedDataType(dataType.asListType().elementType())
      case TypeID.MAP =>
        hasUnsupportedDataType(dataType.asMapType().keyType()) ||
        hasUnsupportedDataType(dataType.asMapType().valueType())
      case _ => false
    }
  }
}

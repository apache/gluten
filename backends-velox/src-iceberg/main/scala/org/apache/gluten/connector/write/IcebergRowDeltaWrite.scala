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
package org.apache.gluten.connector.write

import org.apache.gluten.execution.RowToVeloxColumnarExec

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.connector.write._

import org.apache.iceberg._
import org.apache.iceberg.encryption.PlaintextEncryptionManager
import org.apache.iceberg.expressions.Expression
import org.apache.iceberg.io.{FileIO, InputFile, OutputFile, OutputFileFactory, RollingDataWriter}
import org.apache.iceberg.spark.{SparkSchemaUtil, SparkWriteConf}
import org.apache.iceberg.spark.source.{GlutenEqualityDeleteWriterUtil, SerializableTableWithSize}
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.util.StructProjection

import java.util.{HashMap, Locale, UUID}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

case class IcebergRowDeltaCommitMessage(
    dataFiles: Array[DataFile],
    deleteFiles: Array[DeleteFile],
    referencedDataFiles: Array[String])
  extends WriterCommitMessage

class IcebergRowDeltaWrite(
    table: Table,
    readSnapshotId: Option[Long],
    readSequenceNumber: Long,
    deleteSchema: Schema,
    dataSchema: Schema,
    equalityIds: Seq[Int],
    conflictFilter: Expression,
    isolation: IsolationLevel,
    conf: SparkWriteConf,
    operation: String)
  extends Write {

  override def toBatch: BatchWrite = {
    val deleteFormat = Option(table.properties().get(TableProperties.DELETE_DEFAULT_FILE_FORMAT))
      .map(FileFormat.fromString).getOrElse(conf.dataFileFormat())
    val properties = new HashMap[String, String](table.properties())
    properties.putAll(conf.writeProperties())
    val codec = properties.getOrDefault(
      TableProperties.DELETE_PARQUET_COMPRESSION,
      TableProperties.PARQUET_COMPRESSION_DEFAULT).toLowerCase(Locale.ROOT)
    IcebergRowDeltaWrite.nativeUnsupportedReason(table, deleteSchema, deleteFormat, codec).foreach {
      reason => throw new IllegalArgumentException(reason)
    }
    val native = new IcebergEqualityDeleteWrite(
      table,
      readSnapshotId.getOrElse(0L),
      deleteSchema,
      PartitionSpec.unpartitioned(),
      equalityIds,
      conflictFilter,
      conf,
      codec
    ).nativeFactory(SparkSchemaUtil.convert(deleteSchema))
    val factory = IcebergRowDeltaWriterFactory(
      SerializableTableWithSize.copyOf(table),
      deleteSchema,
      dataSchema,
      equalityIds,
      conf.outputSpecId(),
      deleteFormat,
      conf.dataFileFormat(),
      properties,
      conf.targetDataFileSize(),
      UUID.randomUUID().toString,
      native
    )
    new IcebergEqualityDeleteBatchWrite(
      table,
      readSnapshotId.getOrElse(0L),
      conflictFilter,
      conf.caseSensitive(),
      SparkSession.active.sparkContext.applicationId,
      isolation,
      conf.branch(),
      if (conf.wapEnabled()) conf.wapId() else null,
      conf.extraSnapshotMetadata().asScala.toMap,
      Some(readSequenceNumber + 1),
      operation != "delete",
      Some(factory)
    )
  }
}

object IcebergRowDeltaWrite {
  private[write] def primitiveTypes(t: Type): Seq[Type] =
    if (t.isStructType)
      t.asStructType().fields().asScala.flatMap(f => primitiveTypes(f.`type`())).toSeq
    else Seq(t)

  private val nativeEqualityTypes = Set(
    TypeID.BOOLEAN,
    TypeID.INTEGER,
    TypeID.LONG,
    TypeID.DATE,
    TypeID.TIMESTAMP,
    TypeID.STRING,
    TypeID.DECIMAL,
    TypeID.BINARY)
  def supportsNativeEqualityType(t: Type): Boolean = nativeEqualityTypes.contains(t.typeId())

  def nativeUnsupportedReason(
      table: Table,
      schema: Schema,
      format: FileFormat,
      codec: String): Option[String] = {
    val types = primitiveTypes(schema.asStruct())
    if (format != FileFormat.PARQUET) {
      Some(s"Velox does not have an Iceberg equality-delete writer for $format")
    } else if (!table.encryption().isInstanceOf[PlaintextEncryptionManager]) {
      Some("The native equality-delete writer does not support Iceberg encryption")
    } else if (
      !Set("snappy", "gzip", "zstd", "lz4", "lz4_raw", "none", "uncompressed").contains(codec)
    ) {
      Some(s"The native equality-delete writer does not support compression codec $codec")
    } else if (types.exists(t => !supportsNativeEqualityType(t))) {
      Some(
        s"Unsupported native equality-delete types: ${types.filterNot(supportsNativeEqualityType).mkString(", ")}")
    } else None
  }
}

private case class IcebergRowDeltaWriterFactory(
    table: Table,
    deleteSchema: Schema,
    dataSchema: Schema,
    equalityIds: Seq[Int],
    outputSpecId: Int,
    deleteFormat: FileFormat,
    dataFormat: FileFormat,
    properties: java.util.Map[String, String],
    dataTargetSize: Long,
    queryId: String,
    native: IcebergDataWriteFactory)
  extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
    new IcebergRowDeltaTaskWriter(this, partitionId, taskId)
}

private class IcebergRowDeltaTaskWriter(
    factory: IcebergRowDeltaWriterFactory,
    partitionId: Int,
    taskId: Long)
  extends DataWriter[InternalRow]
  with Logging {
  private val table = factory.table
  private val deleteType = SparkSchemaUtil.convert(factory.deleteSchema)
  private val dataType = SparkSchemaUtil.convert(factory.dataSchema)
  private val partitionType = Partitioning.partitionType(table)
  private val sparkPartitionType = SparkSchemaUtil.convert(partitionType)
    .asInstanceOf[org.apache.spark.sql.types.StructType]
  private val dataFiles = mutable.ArrayBuffer.empty[DataFile]
  private val deleteFiles = mutable.ArrayBuffer.empty[DeleteFile]
  private val referencedFiles = mutable.HashSet.empty[String]
  private val createdPaths = mutable.HashSet.empty[String]
  // Track incomplete files for abort cleanup.
  private val io = new FileIO {
    override def newInputFile(path: String): InputFile = table.io().newInputFile(path)
    override def newOutputFile(path: String): OutputFile = {
      createdPaths += path
      table.io().newOutputFile(path)
    }
    override def deleteFile(path: String): Unit = table.io().deleteFile(path)
  }
  private val fileWriters = GlutenEqualityDeleteWriterUtil.writerFactory(
    table,
    factory.dataSchema,
    factory.deleteSchema,
    factory.equalityIds.toArray,
    factory.dataFormat,
    factory.deleteFormat,
    factory.properties)
  private def outputFactory(format: FileFormat, suffix: String): OutputFileFactory =
    OutputFileFactory.builderFor(table, partitionId, taskId).format(format)
      .operationId(factory.queryId).suffix(suffix).ioSupplier(() => io).build()
  private val dataOutputs = outputFactory(factory.dataFormat, "data")
  private val outputSpec = table.specs().get(factory.outputSpecId)
  private val partitionKey = new PartitionKey(outputSpec, factory.dataSchema)
  private val projection = UnsafeProjection.create(deleteType)
  private var nextWriterId = 0
  private var closed = false
  private var committed = false
  private var aborted = false

  private trait TaskFileWriter {
    def write(row: InternalRow): Unit
    def finish(): Unit
    def abort(): Unit
  }
  private case class WriterKey(delete: Boolean, specId: Int, partition: PartitionData)
  private val writers = new java.util.LinkedHashMap[WriterKey, TaskFileWriter](32, 0.75f, true)

  override def write(row: InternalRow): Unit = {
    if (!row.isNullAt(0)) {
      val spec = table.specs().get(row.getInt(2))
      require(spec != null, s"Unknown source partition spec ${row.getInt(2)}")
      val partition = new PartitionData(spec.partitionType())
      if (spec.isPartitioned) {
        val all = GlutenEqualityDeleteWriterUtil.wrap(
          row.getStruct(3, sparkPartitionType.length),
          sparkPartitionType,
          partitionType)
        val selected = StructProjection.create(partitionType, spec.partitionType()).wrap(all)
        spec.partitionType().fields().asScala.indices.foreach {
          i => partition.set(i, selected.get(i, classOf[Object]))
        }
      }
      writer(WriterKey(true, spec.specId(), partition.copy()), spec)
        .write(row.getStruct(0, deleteType.length))
      referencedFiles += row.getUTF8String(4).toString
    }
    if (!row.isNullAt(1)) {
      val data = row.getStruct(1, dataType.length)
      partitionKey.partition(GlutenEqualityDeleteWriterUtil.wrap(
        data,
        dataType,
        factory.dataSchema.asStruct()))
      val partition = new PartitionData(outputSpec.partitionType()).copyFor(partitionKey)
      writer(WriterKey(false, outputSpec.specId(), partition), outputSpec).write(data)
    }
  }

  private def writer(key: WriterKey, spec: PartitionSpec): TaskFileWriter = {
    val existing = writers.get(key)
    if (existing != null) return existing
    if (writers.size() >= 32) {
      val iterator = writers.entrySet().iterator()
      val entry = iterator.next()
      entry.getValue.finish()
      iterator.remove()
    }
    val result = if (key.delete) {
      nativeWriter(factory.native, spec, key.partition)
    } else {
      val writer = new RollingDataWriter[InternalRow](
        fileWriters,
        dataOutputs,
        io,
        factory.dataTargetSize,
        spec,
        key.partition)
      new TaskFileWriter {
        override def write(row: InternalRow): Unit = writer.write(row)
        override def finish(): Unit = {
          writer.close()
          dataFiles ++= writer.result().dataFiles().asScala
        }
        override def abort(): Unit = writer.close()
      }
    }
    writers.put(key, result)
    result
  }

  private def nativeWriter(
      base: IcebergDataWriteFactory,
      spec: PartitionSpec,
      partition: PartitionData): TaskFileWriter = {
    nextWriterId += 1
    val suffix = s"${factory.queryId}-$nextWriterId"
    val path = table.locationProvider().newDataLocation(spec, partition, suffix)
    val native = base.copy(directory = path.stripSuffix(suffix).stripSuffix("/"), queryId = suffix)
      .createEqualityDeleteWriter(partitionId, taskId, factory.equalityIds)
    new TaskFileWriter {
      private val buffer = mutable.ArrayBuffer.empty[UnsafeRow]
      private var bytes = 0L
      private var finished = false
      override def write(row: InternalRow): Unit = {
        val copy = projection(row).copy()
        buffer += copy
        bytes += copy.getSizeInBytes
        if (buffer.size >= 1024 || bytes >= 1024 * 1024) flush()
      }
      private def flush(): Unit = if (buffer.nonEmpty) {
        RowToVeloxColumnarExec.toColumnarBatchIterator(
          buffer.iterator,
          deleteType,
          1024,
          1024 * 1024).foreach(native.write)
        buffer.clear()
        bytes = 0
      }
      override def finish(): Unit = if (!finished) {
        flush()
        val commit = native.commit().asInstanceOf[IcebergEqualityDeleteCommitMessage]
        commit.deleteFiles.foreach {
          file =>
            createdPaths += file.path().toString
            // Restore the source partition omitted by the unpartitioned native sink.
            deleteFiles += FileMetadata.deleteFileBuilder(spec)
              .ofEqualityDeletes(factory.equalityIds: _*)
              .withPath(file.path().toString).withFormat(file.format())
              .withPartition(partition).withFileSizeInBytes(file.fileSizeInBytes())
              .withSplitOffsets(file.splitOffsets())
              .withMetrics(new Metrics(
                file.recordCount(),
                file.columnSizes(),
                file.valueCounts(),
                file.nullValueCounts(),
                file.nanValueCounts(),
                file.lowerBounds(),
                file.upperBounds()))
              .build()
        }
        finished = true
        native.close()
      }
      override def abort(): Unit = if (!finished) {
        try native.abort()
        finally native.close()
        finished = true
      }
    }
  }

  override def commit(): WriterCommitMessage = {
    writers.values().asScala.foreach(_.finish())
    writers.clear()
    committed = true
    IcebergRowDeltaCommitMessage(dataFiles.toArray, deleteFiles.toArray, referencedFiles.toArray)
  }

  override def abort(): Unit = {
    if (aborted) return
    aborted = true
    writers.values().asScala.foreach {
      writer =>
        try writer.abort()
        catch { case NonFatal(e) => logWarning("Failed to close aborted Iceberg writer", e) }
    }
    writers.clear()
    createdPaths.foreach {
      path =>
        try table.io().deleteFile(path)
        catch { case NonFatal(e) => logWarning(s"Failed to remove uncommitted file $path", e) }
    }
  }

  override def close(): Unit = if (!closed) {
    closed = true
    if (!committed) abort()
    table match {
      case closeable: AutoCloseable => closeable.close()
      case _ =>
    }
  }
}

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

import org.apache.gluten.IcebergNestedFieldVisitor
import org.apache.gluten.config.GlutenConfig.COLUMNAR_PARQUET_WRITE_BLOCK_SIZE
import org.apache.gluten.config.VeloxConfig.{MAX_TARGET_FILE_SIZE_SESSION, PARQUET_DICT_SIZE_BYTES => NATIVE_DICT_SIZE, PARQUET_PAGE_SIZE_BYTES => NATIVE_PAGE_SIZE}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.iceberg.{BaseRowDelta, ContentFile, DeleteFile, HasTableOperations, IsolationLevel, MetricsConfig, PartitionSpec, RowDelta, Schema, SnapshotSummary, SortOrder, Table}
import org.apache.iceberg.TableProperties._
import org.apache.iceberg.exceptions.CleanableFailure
import org.apache.iceberg.expressions.Expression
import org.apache.iceberg.spark.{CommitMetadata, SparkWriteConf}
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.types.Types.TimestampType
import org.apache.iceberg.types.TypeUtil

import java.util.{HashMap, Locale, UUID}

import scala.collection.JavaConverters._

class IcebergEqualityDeleteWrite(
    val table: Table,
    val readSnapshotId: Long,
    val deleteSchema: Schema,
    val spec: PartitionSpec,
    val equalityFieldIds: Seq[Int],
    val conflictFilter: Expression,
    val writeConf: SparkWriteConf,
    val codec: String)
  extends Write {

  private val queryId = UUID.randomUUID().toString

  override def toBatch: BatchWrite = new IcebergEqualityDeleteBatchWrite(
    table,
    readSnapshotId,
    conflictFilter,
    writeConf.caseSensitive(),
    SparkSession.active.sparkContext.applicationId)

  def createWriterFactory(schema: StructType): ColumnarBatchDataWriterFactory =
    IcebergEqualityDeleteWriterFactory(nativeFactory(schema), equalityFieldIds)

  private[write] def nativeFactory(schema: StructType): IcebergDataWriteFactory = {
    val properties = table.properties()
    val conf = SQLConf.get
    val nativeProperties = new HashMap[String, String]()
    val timestampsWithoutZone = TypeUtil.indexById(deleteSchema.asStruct()).values().asScala
      .filter(
        f =>
          f.`type`().typeId() == TypeID.TIMESTAMP &&
            !f.`type`().asInstanceOf[TimestampType].shouldAdjustToUTC())
      .map(_.fieldId()).toSeq.sorted
    nativeProperties.put("gluten.iceberg.timestamp-timezone", "UTC")
    nativeProperties.put(
      "gluten.iceberg.timestamp-without-timezone-field-ids",
      timestampsWithoutZone.mkString("[", ",", "]"))
    def property(deleteKey: String, dataKey: String, default: String): String =
      properties.getOrDefault(deleteKey, properties.getOrDefault(dataKey, default))
    def put(key: String, value: String, capacity: Boolean = false): Unit = {
      val configured = conf.getConfString(key, value).trim
      nativeProperties.put(
        key,
        if (capacity && configured.lastOption.exists(_.isDigit)) s"${configured}B" else configured)
    }
    put(MAX_TARGET_FILE_SIZE_SESSION.key, writeConf.targetDeleteFileSize().toString, true)
    put(
      COLUMNAR_PARQUET_WRITE_BLOCK_SIZE.key,
      property(
        DELETE_PARQUET_ROW_GROUP_SIZE_BYTES,
        PARQUET_ROW_GROUP_SIZE_BYTES,
        PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT.toString),
      true
    )
    put(
      NATIVE_PAGE_SIZE.key,
      property(
        DELETE_PARQUET_PAGE_SIZE_BYTES,
        PARQUET_PAGE_SIZE_BYTES,
        PARQUET_PAGE_SIZE_BYTES_DEFAULT.toString),
      true)
    put(
      NATIVE_DICT_SIZE.key,
      property(
        DELETE_PARQUET_DICT_SIZE_BYTES,
        PARQUET_DICT_SIZE_BYTES,
        PARQUET_DICT_SIZE_BYTES_DEFAULT.toString),
      true)
    put(
      "spark.gluten.sql.columnar.backend.velox.parquet_writer_page_row_limit",
      property(
        DELETE_PARQUET_PAGE_ROW_LIMIT,
        PARQUET_PAGE_ROW_LIMIT,
        PARQUET_PAGE_ROW_LIMIT_DEFAULT.toString)
    )
    put(
      "spark.gluten.sql.columnar.backend.velox.parquet_writer_datapage_version",
      properties.getOrDefault("write.parquet.page-version", "v1").toUpperCase(Locale.ROOT)
    )
    Option(writeConf.writeProperties().get(DELETE_PARQUET_COMPRESSION_LEVEL))
      .foreach(put("spark.gluten.sql.columnar.backend.velox.parquet_writer_compression_level", _))

    val directory = table.locationProvider().newDataLocation("").stripSuffix("/")
    // Iceberg follows Parquet codec names; Velox calls the raw variant "lz4".
    val nativeCodec = codec match {
      case "lz4" => "lz4_hadoop"
      case "lz4_raw" => "lz4"
      case "uncompressed" => "none"
      case other => other
    }
    IcebergDataWriteFactory(
      schema,
      1,
      directory,
      nativeCodec,
      spec,
      SortOrder.unsorted(),
      TypeUtil.visit(deleteSchema, new IcebergNestedFieldVisitor),
      nativeProperties,
      queryId,
      Some(IcebergEqualityDeleteMetrics(deleteSchema, MetricsConfig.forTable(table)))
    )
  }
}

private case class IcebergEqualityDeleteWriterFactory(
    delegate: IcebergDataWriteFactory,
    equalityFieldIds: Seq[Int])
  extends ColumnarBatchDataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[ColumnarBatch] =
    delegate.createEqualityDeleteWriter(partitionId, taskId, equalityFieldIds)
}

class IcebergEqualityDeleteBatchWrite(
    table: Table,
    readSnapshotId: Long,
    conflictFilter: Expression,
    caseSensitive: Boolean,
    applicationId: String,
    isolationLevel: IsolationLevel = IsolationLevel.SERIALIZABLE,
    branch: String = null,
    wapId: String = null,
    snapshotMetadata: Map[String, String] = Map.empty,
    deleteSequenceNumber: Option[Long] = None,
    validateDeletedFiles: Boolean = false,
    factory: Option[DataWriterFactory] = None)
  extends BatchWrite
  with Logging {

  private var cleanupOnAbort = true

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    factory.getOrElse(
      throw new UnsupportedOperationException(
        "Equality deletes require the columnar writer factory"))

  private def files(messages: Array[WriterCommitMessage]): Seq[ContentFile[_]] =
    messages.iterator.filter(_ != null).flatMap {
      case message: IcebergEqualityDeleteCommitMessage => message.deleteFiles.toSeq
      case message: IcebergRowDeltaCommitMessage => message.dataFiles.toSeq ++ message.deleteFiles
      case other => throw new IllegalArgumentException(s"Unexpected delete commit: $other")
    }.toSeq

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    if (files(messages).isEmpty) return
    val delta: RowDelta = deleteSequenceNumber match {
      case Some(sequence) =>
        new BaseRowDelta(table.name(), table.asInstanceOf[HasTableOperations].operations()) {
          override def addDeletes(file: DeleteFile): RowDelta = {
            // Anchor deletes to the read snapshot so concurrent inserts survive.
            add(file, sequence)
            this
          }
        }
      case None => table.newRowDelta()
    }
    if (readSnapshotId != 0L) delta.validateFromSnapshot(readSnapshotId)
    delta.caseSensitive(caseSensitive).conflictDetectionFilter(conflictFilter)
      .validateNoConflictingDeleteFiles()
    if (isolationLevel == IsolationLevel.SERIALIZABLE) delta.validateNoConflictingDataFiles()
    if (validateDeletedFiles) delta.validateDeletedFiles()
    messages.filter(_ != null).foreach {
      case message: IcebergEqualityDeleteCommitMessage =>
        message.deleteFiles.foreach(delta.addDeletes)
      case message: IcebergRowDeltaCommitMessage =>
        message.dataFiles.foreach(delta.addRows)
        message.deleteFiles.foreach(delta.addDeletes)
        delta.validateDataFilesExist(message.referencedDataFiles.toSeq.asJava)
      case other => throw new IllegalArgumentException(s"Unexpected delete commit: $other")
    }
    delta.set("spark.app.id", applicationId)
    snapshotMetadata.foreach { case (key, value) => delta.set(key, value) }
    CommitMetadata.commitProperties().asScala.foreach { case (key, value) => delta.set(key, value) }
    if (wapId != null) {
      delta.set(SnapshotSummary.STAGED_WAP_ID_PROP, wapId)
      delta.stageOnly()
    }
    if (branch != null) delta.toBranch(branch)
    // An unknown commit status may mean success; retain files unless cleanup is safe.
    cleanupOnAbort = false
    try {
      delta.commit()
    } catch {
      case failure: Exception =>
        cleanupOnAbort = failure.isInstanceOf[CleanableFailure]
        throw failure
    }
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    if (cleanupOnAbort) {
      files(messages).foreach {
        file =>
          try {
            table.io().deleteFile(file.path().toString)
          } catch {
            case scala.util.control.NonFatal(e) =>
              logWarning(s"Failed to remove uncommitted equality-delete file ${file.path()}", e)
          }
      }
    }
  }
}

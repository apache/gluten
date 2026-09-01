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
package org.apache.gluten.extension

import org.apache.gluten.execution.DeltaScanTransformer
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.extension.columnar.offload.OffloadSingleNode

import org.apache.spark.sql.delta.{DeltaParquetFileFormat, SnapshotDescriptor}
import org.apache.spark.sql.delta.commands.DeletionVectorUtils.deletionVectorsReadable
import org.apache.spark.sql.delta.files.{CdcAddFileIndex, TahoeBatchFileIndex, TahoeFileIndex, TahoeRemoveFileIndex}
import org.apache.spark.sql.delta.stats.PreparedDeltaFileIndex
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.util.SparkVersionUtil

case class OffloadDeltaScan(enableNativeDmlRowIndexScan: Boolean) extends OffloadSingleNode {
  private val DeletionVectorsUseMetadataRowIndexKey =
    "spark.databricks.delta.deletionVectors.useMetadataRowIndex"

  // Spark 3.5+ exposes this as ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME.
  private val parquetTemporaryRowIndexColumnName = "_tmp_metadata_row_index"
  // Row-index columns Delta generates as top-level scan outputs.
  private val generatedRowIndexColumnNames =
    Set(DeltaParquetFileFormat.ROW_INDEX_COLUMN_NAME, parquetTemporaryRowIndexColumnName)
  // ParquetFileFormat.ROW_INDEX, the generated field Delta adds to the file metadata struct in
  // PreprocessTableWithDVs when deletionVectors.useMetadataRowIndex is on. Only meaningful nested
  // under _metadata -- a user column may legitimately be called row_index.
  private val metadataRowIndexFieldName = "row_index"
  // TahoeBatchFileIndex.actionType as set by Delta's DELETE, UPDATE and MERGE commands.
  private val dmlActionTypes = Set("delete", "update", "merge")

  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case scan: FileSourceScanExec if isDeltaLogScan(scan) =>
      FallbackTags.add(scan, "fallback Delta _delta_log scan")
      scan
    case scan: FileSourceScanExec if shouldFallbackDeletionVectorDmlScan(scan) =>
      FallbackTags.add(scan, "fallback Delta DV DML row-index scan by configuration")
      scan
    case scan: FileSourceScanExec if shouldFallbackSpark34DeletionVectorScan(scan) =>
      FallbackTags.add(scan, "fallback Spark 3.4 Delta DV scan")
      scan
    case scan: FileSourceScanExec
        if shouldFallbackDeletionVectorScanWithoutMetadataRowIndex(scan) =>
      FallbackTags.add(scan, "fallback Delta DV scan without metadata row index")
      scan
    case scan: FileSourceScanExec if DeltaScanUtils.isDeltaScan(scan) =>
      DeltaScanTransformer(scan)
    case other => other
  }

  /**
   * The scoped escape hatch: with the config off, the DELETE/UPDATE/MERGE target scan that produces
   * file paths and row indexes for deletion-vector writes stays on Spark, while every other scan
   * keeps offloading. The whole check lives here, on the scan alone: Delta builds every DML target
   * relation over a [[TahoeBatchFileIndex]] carrying the command name, which survives AQE stage
   * splits and arbitrary join placement, and only DV-writing DML reads a row-index column from that
   * relation; DML that rewrites whole files does not, and remains eligible for native execution.
   */
  private def shouldFallbackDeletionVectorDmlScan(scan: FileSourceScanExec): Boolean = {
    !enableNativeDmlRowIndexScan && isDmlTargetScan(scan) && scanReadsRowIndexColumn(scan)
  }

  private def isDmlTargetScan(scan: FileSourceScanExec): Boolean = {
    scan.relation.location match {
      case index: TahoeBatchFileIndex => dmlActionTypes.contains(index.actionType)
      case _ => false
    }
  }

  private def isRowIndexColumn(name: String, dataType: DataType): Boolean = {
    generatedRowIndexColumnNames.contains(name) ||
    (name == FileFormat.METADATA_NAME && (dataType match {
      case struct: StructType => struct.fieldNames.contains(metadataRowIndexFieldName)
      case _ => false
    }))
  }

  private def scanReadsRowIndexColumn(scan: FileSourceScanExec): Boolean = {
    val outputFields = scan.output.iterator.map(attribute => (attribute.name, attribute.dataType))
    val requiredFields =
      scan.requiredSchema.fields.iterator.map(field => (field.name, field.dataType))
    (outputFields ++ requiredFields).exists {
      case (name, dataType) => isRowIndexColumn(name, dataType)
    }
  }

  private def isDeltaLogScan(scan: FileSourceScanExec): Boolean = {
    scan.relation.location.rootPaths.exists {
      path =>
        val root = path.toString
        root.contains("/_delta_log") || root.contains("\\_delta_log") || root.endsWith("_delta_log")
    }
  }

  private def shouldFallbackSpark34DeletionVectorScan(scan: FileSourceScanExec): Boolean = {
    if (SparkVersionUtil.gteSpark35) {
      return false
    }

    containsDeletionVector(scan)
  }

  private def shouldFallbackDeletionVectorScanWithoutMetadataRowIndex(
      scan: FileSourceScanExec): Boolean = {
    if (!SparkVersionUtil.gteSpark35) {
      return false
    }

    // Delta DML tests force this path and rely on Spark's injected
    // row-index filter column for correctness. Keep it on Spark until the native path can
    // prove the same contract for DML-generated DVs.
    val useMetadataRowIndex =
      scan.relation.sparkSession.sessionState.conf
        .getConfString(DeletionVectorsUseMetadataRowIndexKey, "true")
        .toBoolean
    !useMetadataRowIndex && containsDeletionVector(scan)
  }

  private def containsDeletionVector(scan: FileSourceScanExec): Boolean = {
    scan.relation.location match {
      // CDF indexes expose the exact actions in the requested range. Use those instead of the
      // table-level protocol capability so a DV-capable table can still offload DV-free ranges.
      case index: TahoeRemoveFileIndex =>
        index.filesByVersion.exists(_.actions.exists(_.deletionVector != null))
      case index: CdcAddFileIndex =>
        index.addFiles.exists(_.deletionVector != null)
      case preparedIndex: PreparedDeltaFileIndex =>
        preparedIndex.preparedScan.files.exists(_.deletionVector != null)
      case index: TahoeFileIndex =>
        val snapshot = index.asInstanceOf[SnapshotDescriptor]
        deletionVectorsReadable(snapshot.protocol, snapshot.metadata)
      case _ =>
        false
    }
  }
}

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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.delta.DeltaParquetFileFormat._
import org.apache.spark.sql.delta.commands.DeletionVectorUtils.deletionVectorsReadable
import org.apache.spark.sql.delta.files.{TahoeFileIndex, TahoeLogFileIndex}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.datasources.FileFormat.METADATA_NAME
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.StructType

/**
 * Rewrites Delta scans over DV-enabled tables to request the backend-specific skip-row metadata
 * column only when the snapshot actually contains DVs.
 */
trait PreprocessTableWithDVs extends SubqueryTransformerHelper {
  def preprocessTablesWithDVs(plan: LogicalPlan): LogicalPlan = {
    plan.transformDown { case ScanWithDeletionVectors(dvScan) => dvScan }
  }
}

object ScanWithDeletionVectors {
  def unapply(a: LogicalRelation): Option[LogicalPlan] = a match {
    case scan @ LogicalRelation(
          relation @ HadoopFsRelation(
            index: TahoeFileIndex,
            _,
            _,
            _,
            format: DeltaParquetFileFormat,
            _),
          _,
          _,
          _) =>
      dvEnabledScanFor(scan, relation, format, index)
    case scan @ LogicalRelation(
          relation @ HadoopFsRelation(
            index: TahoeFileIndex,
            _,
            _,
            _,
            format: GlutenDeltaParquetFileFormat,
            _),
          _,
          _,
          _) =>
      dvEnabledScanFor(scan, relation, format, index)
    case _ => None
  }

  def dvEnabledScanFor(
      scan: LogicalRelation,
      hadoopRelation: HadoopFsRelation,
      fileFormat: DeltaParquetFileFormat,
      index: TahoeFileIndex): Option[LogicalPlan] = {
    if (!deletionVectorsReadable(index.protocol, index.metadata)) {
      return None
    }

    if (index.isInstanceOf[TahoeLogFileIndex]) {
      return None
    }

    if (fileFormat.hasTablePath) {
      return None
    }

    val filesWithDVs = index
      .matchingFiles(partitionFilters = Seq(TrueLiteral), dataFilters = Seq(TrueLiteral))
      .filter(_.deletionVector != null)
    if (filesWithDVs.isEmpty) {
      return None
    }

    val planOutput = scan.output
    val spark = SparkSession.getActiveSession.get
    val newScan = createScanWithSkipRowColumn(spark, scan, fileFormat, index, hadoopRelation)
    val rowIndexFilter = createRowIndexFilterNode(newScan)
    Some(Project(planOutput, rowIndexFilter))
  }

  def dvEnabledScanFor(
      scan: LogicalRelation,
      hadoopRelation: HadoopFsRelation,
      fileFormat: GlutenDeltaParquetFileFormat,
      index: TahoeFileIndex): Option[LogicalPlan] = {
    if (!deletionVectorsReadable(index.protocol, index.metadata)) {
      return None
    }

    if (index.isInstanceOf[TahoeLogFileIndex]) {
      return None
    }

    if (fileFormat.hasTablePath) {
      return None
    }

    val filesWithDVs = index
      .matchingFiles(partitionFilters = Seq(TrueLiteral), dataFilters = Seq(TrueLiteral))
      .filter(_.deletionVector != null)
    if (filesWithDVs.isEmpty) {
      return None
    }

    val planOutput = scan.output
    val spark = SparkSession.getActiveSession.get
    val newScan = createScanWithSkipRowColumn(spark, scan, fileFormat, index, hadoopRelation)
    val rowIndexFilter = createRowIndexFilterNode(newScan)
    Some(Project(planOutput, rowIndexFilter))
  }

  private def addRowIndexIfMissing(attribute: AttributeReference): AttributeReference = {
    require(attribute.name == METADATA_NAME)

    val dataType = attribute.dataType.asInstanceOf[StructType]
    if (dataType.fieldNames.contains(ParquetFileFormat.ROW_INDEX)) {
      return attribute
    }

    val newDatatype = dataType.add(ParquetFileFormat.ROW_INDEX_FIELD)
    attribute.copy(dataType = newDatatype)(
      exprId = attribute.exprId,
      qualifier = attribute.qualifier)
  }

  private def createScanWithSkipRowColumn(
      spark: SparkSession,
      inputScan: LogicalRelation,
      fileFormat: DeltaParquetFileFormat,
      tahoeFileIndex: TahoeFileIndex,
      hadoopFsRelation: HadoopFsRelation): LogicalRelation = {
    val useMetadataRowIndex =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX)

    val skipRowField = IS_ROW_DELETED_STRUCT_FIELD
    val scanOutputWithMetadata = if (useMetadataRowIndex) {
      if (inputScan.output.map(_.name).contains(METADATA_NAME)) {
        inputScan.output.collect {
          case a: AttributeReference if a.name == METADATA_NAME => addRowIndexIfMissing(a)
          case o => o
        }
      } else {
        inputScan.output :+ fileFormat.createFileMetadataCol()
      }
    } else {
      inputScan.output
    }

    val newScanOutput =
      scanOutputWithMetadata :+ AttributeReference(skipRowField.name, skipRowField.dataType)()
    val newDataSchema = hadoopFsRelation.dataSchema.add(skipRowField)
    val newFileFormat = fileFormat.copyWithDVInfo(
      tablePath = tahoeFileIndex.path.toString,
      optimizationsEnabled = useMetadataRowIndex)

    val newRelation = hadoopFsRelation.copy(fileFormat = newFileFormat, dataSchema = newDataSchema)(
      hadoopFsRelation.sparkSession)

    inputScan.copy(relation = newRelation, output = newScanOutput)
  }

  private def createScanWithSkipRowColumn(
      spark: SparkSession,
      inputScan: LogicalRelation,
      fileFormat: GlutenDeltaParquetFileFormat,
      tahoeFileIndex: TahoeFileIndex,
      hadoopFsRelation: HadoopFsRelation): LogicalRelation = {
    val useMetadataRowIndex =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX)

    val skipRowField = GlutenDeltaParquetFileFormat.IS_ROW_DELETED_STRUCT_FIELD
    val scanOutputWithMetadata = if (useMetadataRowIndex) {
      if (inputScan.output.map(_.name).contains(METADATA_NAME)) {
        inputScan.output.collect {
          case a: AttributeReference if a.name == METADATA_NAME => addRowIndexIfMissing(a)
          case o => o
        }
      } else {
        inputScan.output :+ fileFormat.createFileMetadataCol()
      }
    } else {
      inputScan.output
    }

    val newScanOutput =
      scanOutputWithMetadata :+ AttributeReference(skipRowField.name, skipRowField.dataType)()
    val newDataSchema = hadoopFsRelation.dataSchema.add(skipRowField)
    val newFileFormat = fileFormat.copyWithDVInfo(
      tablePath = tahoeFileIndex.path.toString,
      optimizationsEnabled = useMetadataRowIndex)

    val newRelation = hadoopFsRelation.copy(fileFormat = newFileFormat, dataSchema = newDataSchema)(
      hadoopFsRelation.sparkSession)

    inputScan.copy(relation = newRelation, output = newScanOutput)
  }

  private def createRowIndexFilterNode(newScan: LogicalRelation): Filter = {
    val skipRowColumnRefs = newScan.output.filter(_.name == IS_ROW_DELETED_COLUMN_NAME)
    require(
      skipRowColumnRefs.size == 1,
      s"Expected only one column with name=$IS_ROW_DELETED_COLUMN_NAME")
    val skipRowColumnRef = skipRowColumnRefs.head
    Filter(EqualTo(skipRowColumnRef, Literal(RowIndexFilter.KEEP_ROW_VALUE)), newScan)
  }
}

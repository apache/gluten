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

import org.apache.spark.sql.catalyst.expressions.{Expression, GetStructField, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregation.BitmapAggregator
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.delta.DeltaParquetFileFormat
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.delta.stats.PreparedDeltaFileIndex
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.types.{DataType, StructType}

object DeltaDeletionVectorDmlUtils {
  private val DmlRowIndexScanTag: TreeNodeTag[Boolean] =
    TreeNodeTag[Boolean]("org.apache.gluten.delta.dml.row.index.scan")

  // Spark 3.5+ exposes this as ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME.
  private val parquetTemporaryRowIndexColumnName = "_tmp_metadata_row_index"
  private val deletionVectorRowIndexColumnNames =
    Set(
      "__delta_internal_row_index",
      DeltaParquetFileFormat.ROW_INDEX_COLUMN_NAME,
      parquetTemporaryRowIndexColumnName,
      "row_index",
      "rowIndexCol")
  private val filePathColumnNames = Set("file_path", "filePath")

  val tagDmlRowIndexScans: Rule[SparkPlan] = (plan: SparkPlan) => {
    def visit(
        node: SparkPlan,
        hasRowIndexReference: Boolean,
        hasFilePathReference: Boolean,
        hasBitmapAggregation: Boolean): Unit = {
      val nextHasBitmapAggregation =
        hasBitmapAggregation || node.expressions.exists(referencesDeletionVectorBitmapAggregator)
      // The row-index/file-path signature is only meaningful below Delta's BitmapAggregator.
      // Avoid collecting expression references for every node in ordinary (non-DML) queries.
      val nextHasRowIndexReference =
        nextHasBitmapAggregation &&
          (hasRowIndexReference || node.expressions.exists(referencesRowIndexColumn))
      val nextHasFilePathReference =
        nextHasBitmapAggregation &&
          (hasFilePathReference || node.expressions.exists(referencesFilePathColumn))

      node.children.foreach {
        case scan: FileSourceScanExec
            if nextHasBitmapAggregation &&
              nextHasRowIndexReference &&
              nextHasFilePathReference &&
              isDeletionVectorDmlRowIndexScanCandidate(scan) =>
          scan.setTagValue(DmlRowIndexScanTag, true)
        case child =>
          visit(
            child,
            nextHasRowIndexReference,
            nextHasFilePathReference,
            nextHasBitmapAggregation)
      }
    }

    visit(
      plan,
      hasRowIndexReference = false,
      hasFilePathReference = false,
      hasBitmapAggregation = false)
    plan
  }

  def copyDmlRowIndexScanTag(from: SparkPlan, to: SparkPlan): Unit = {
    if (from.getTagValue(DmlRowIndexScanTag).contains(true)) {
      to.setTagValue(DmlRowIndexScanTag, true)
    }
  }

  def isDeltaScan(scan: FileSourceScanExec): Boolean = {
    isDeltaFileIndex(scan) || isDeltaParquetScan(scan)
  }

  def isDeltaParquetScan(scan: FileSourceScanExec): Boolean = {
    val fileFormatClass = scan.relation.fileFormat.getClass
    fileFormatClass == classOf[DeltaParquetFileFormat] ||
    fileFormatClass.getSimpleName == "GlutenDeltaParquetFileFormat"
  }

  def isDeltaFileIndex(scan: FileSourceScanExec): Boolean = {
    scan.relation.location.isInstanceOf[TahoeFileIndex] ||
    scan.relation.location.isInstanceOf[PreparedDeltaFileIndex]
  }

  def isDeletionVectorDmlRowIndexScan(scan: FileSourceScanExec): Boolean = {
    scan.getTagValue(DmlRowIndexScanTag).contains(true) &&
    isDeletionVectorDmlRowIndexScanCandidate(scan)
  }

  def isDeletionVectorDmlRowIndexScan(plan: SparkPlan): Boolean = {
    plan.getTagValue(DmlRowIndexScanTag).contains(true)
  }

  private def isDeletionVectorDmlRowIndexScanCandidate(scan: FileSourceScanExec): Boolean = {
    if (!isDeltaScan(scan)) {
      return false
    }

    scanContainsColumnName(scan, deletionVectorRowIndexColumnNames) &&
    scanContainsColumnName(scan, filePathColumnNames)
  }

  private def scanContainsColumnName(
      scan: FileSourceScanExec,
      columnNames: Set[String]): Boolean = {
    def nestedFieldNames(dataType: DataType): Seq[String] = dataType match {
      case struct: StructType =>
        struct.fields.flatMap(field => field.name +: nestedFieldNames(field.dataType)).toSeq
      case _ => Seq.empty
    }

    val outputColumnNames =
      scan.output.flatMap(attribute => attribute.name +: nestedFieldNames(attribute.dataType))
    val requiredColumnNames = scan.requiredSchema.fields.flatMap {
      field => field.name +: nestedFieldNames(field.dataType)
    }
    (outputColumnNames ++ requiredColumnNames).exists(columnNames.contains)
  }

  private def referencedColumnNames(expr: Expression): Set[String] = {
    val attributeNames = expr.references.iterator.map(_.name)
    val nestedFieldNames = expr.collect {
      case field: GetStructField => field.name
      case named: NamedExpression => Some(named.name)
    }.flatten
    (attributeNames ++ nestedFieldNames).toSet
  }

  private def referencesRowIndexColumn(expr: Expression): Boolean = {
    referencedColumnNames(expr).exists(deletionVectorRowIndexColumnNames.contains)
  }

  private def referencesFilePathColumn(expr: Expression): Boolean = {
    referencedColumnNames(expr).exists(filePathColumnNames.contains)
  }

  private def referencesDeletionVectorBitmapAggregator(expr: Expression): Boolean = {
    expr.exists(_.isInstanceOf[BitmapAggregator])
  }
}

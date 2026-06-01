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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.delta.DeltaParquetFileFormat
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.delta.stats.PreparedDeltaFileIndex
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}

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
        hasFilePathReference: Boolean): Unit = {
      val nextHasRowIndexReference =
        hasRowIndexReference || node.expressions.exists(referencesRowIndexColumn)
      val nextHasFilePathReference =
        hasFilePathReference || node.expressions.exists(referencesFilePathColumn)

      node.children.foreach {
        case scan: FileSourceScanExec
            if nextHasRowIndexReference &&
              nextHasFilePathReference &&
              isDeletionVectorDmlRowIndexScanCandidate(scan) =>
          scan.setTagValue(DmlRowIndexScanTag, true)
        case child =>
          visit(child, nextHasRowIndexReference, nextHasFilePathReference)
      }
    }

    visit(plan, hasRowIndexReference = false, hasFilePathReference = false)
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
    val scanColumnNames = (scan.output.map(_.name) ++ scan.requiredSchema.fieldNames).toSet
    scanColumnNames.exists(columnNames.contains) || columnNames.exists(scan.treeString.contains)
  }

  private def referencesRowIndexColumn(expr: Expression): Boolean = {
    val expressionText = expr.toString()
    expr.references.exists(attr => deletionVectorRowIndexColumnNames.contains(attr.name)) ||
    deletionVectorRowIndexColumnNames.exists(expressionText.contains)
  }

  private def referencesFilePathColumn(expr: Expression): Boolean = {
    val expressionText = expr.toString()
    expr.references.exists(attr => filePathColumnNames.contains(attr.name)) ||
    filePathColumnNames.exists(expressionText.contains)
  }
}

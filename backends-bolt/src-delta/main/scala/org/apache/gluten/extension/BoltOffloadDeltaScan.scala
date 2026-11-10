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

import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.extension.columnar.offload.OffloadSingleNode

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, ExpressionSet, Literal}
import org.apache.spark.sql.delta.DeltaParquetFileFormat
import org.apache.spark.sql.delta.commands.DeletionVectorUtils.deletionVectorsReadable
import org.apache.spark.sql.delta.files.{TahoeBatchFileIndex, TahoeFileIndex, TahoeRemoveFileIndex}
import org.apache.spark.sql.delta.stats.PreparedDeltaFileIndex
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.types.ByteType

/** Keeps actual Delta deletion-vector reads on Spark until Bolt supports the DV split protocol. */
case class BoltOffloadDeltaScan() extends OffloadSingleNode {
  private val delegate = OffloadDeltaScan(enableNativeDmlRowIndexScan = false)

  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case scan: FileSourceScanExec if readsDeletionVector(scan) =>
      FallbackTags.add(scan, "Bolt does not support native Delta deletion-vector reads")
      scan
    case other => delegate.offload(other)
  }

  private def isDeletedRowFilter(expression: Expression): Boolean = expression match {
    case EqualTo(attribute: AttributeReference, Literal(value: Byte, ByteType))
        if attribute.name == DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME &&
          attribute.dataType == ByteType && value == 0 => true
    case _ => false
  }

  private def readsDeletionVector(scan: FileSourceScanExec): Boolean = {
    scan.relation.location match {
      case index: TahoeRemoveFileIndex =>
        index.filesByVersion.exists(_.actions.exists(_.deletionVector != null))
      case index: PreparedDeltaFileIndex =>
        // Delta injects this row predicate after preparing files; the shared native DV
        // rule removes it before file selection. Compare the same effective filters here.
        val filters = ExpressionSet(
          scan.partitionFilters ++ scan.dataFilters.filterNot(isDeletedRowFilter))
        val prepared = index.preparedScan
        // These are the same reuse conditions as PreparedDeltaFileIndex.matchingFiles.
        // Otherwise Delta reselects files, so its prepared list cannot prove this scan DV-free.
        if (filters == prepared.allFilters || filters == prepared.filtersUsedForSkipping) {
          prepared.files.exists(_.deletionVector != null)
        } else {
          deletionVectorsReadable(index.protocol, index.metadata)
        }
      case index: TahoeBatchFileIndex =>
        index.addFiles.exists(_.deletionVector != null)
      case index: TahoeFileIndex =>
        // Other indexes have no reusable selected-file list. Avoid an extra driver-side
        // listing and conservatively retain Spark for DV-capable snapshots. In particular,
        // do not cache by index: the same index can serve scans with different filters.
        deletionVectorsReadable(index.protocol, index.metadata)
      case _ => false
    }
  }
}

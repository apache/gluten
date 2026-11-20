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
package org.apache.gluten.delta

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, FileSourceConstantMetadataAttribute, PlanExpression}
import org.apache.spark.sql.delta.{DefaultRowCommitVersion, DeltaParquetFileFormat, RowId}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.delta.stats.PreparedDeltaFileIndex
import org.apache.spark.sql.execution.datasources.HadoopFsRelation

object DeltaRowTrackingScanInfo {

  /**
   * Whether the scan reads `_metadata.base_row_id` or `_metadata.default_row_commit_version` as a
   * non-nullable field while a file it may read has no value for it. Delta's
   * `DeltaParquetFileFormat.fileConstantMetadataExtractors` fails such a read with "Missing ...
   * value for file"; the native scan would return NULL.
   *
   * The files are the ones selected at planning time, before dynamic partition pruning, so this may
   * flag a scan whose pruned files all have the values. That scan then runs on Spark and succeeds.
   */
  def missingRequiredRowTrackingMetadata(
      relation: HadoopFsRelation,
      output: Seq[Attribute],
      partitionFilters: Seq[Expression]): Boolean = {
    val nonNullable = relation.fileFormat match {
      case format: DeltaParquetFileFormat => !format.nullableRowTrackingConstantFields
      case _ => false
    }
    if (!nonNullable) {
      return false
    }
    val readFields = output.collect { case FileSourceConstantMetadataAttribute(a) => a.name }.toSet
    val readsBaseRowId = readFields.contains(RowId.BASE_ROW_ID)
    val readsDefaultRowCommitVersion =
      readFields.contains(DefaultRowCommitVersion.METADATA_STRUCT_FIELD_NAME)
    if (!readsBaseRowId && !readsDefaultRowCommitVersion) {
      return false
    }
    candidateFiles(relation, partitionFilters).exists {
      file =>
        (readsBaseRowId && file.baseRowId.isEmpty) ||
        (readsDefaultRowCommitVersion && file.defaultRowCommitVersion.isEmpty)
    }
  }

  private def candidateFiles(
      relation: HadoopFsRelation,
      partitionFilters: Seq[Expression]): Seq[AddFile] = relation.location match {
    case index: PreparedDeltaFileIndex => index.preparedScan.files
    case index: TahoeFileIndex =>
      val staticPartitionFilters =
        partitionFilters.filterNot(_.exists(_.isInstanceOf[PlanExpression[_]]))
      index.matchingFiles(staticPartitionFilters, Nil)
    case _ => Nil
  }
}

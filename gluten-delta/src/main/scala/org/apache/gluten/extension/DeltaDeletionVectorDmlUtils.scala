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

import org.apache.spark.sql.delta.DeltaParquetFileFormat
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.delta.stats.PreparedDeltaFileIndex
import org.apache.spark.sql.execution.FileSourceScanExec

object DeltaDeletionVectorDmlUtils {
  // Spark 3.5+ exposes this as ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME.
  private val parquetTemporaryRowIndexColumnName = "_tmp_metadata_row_index"
  private val deletionVectorRowIndexColumnNames =
    Set(
      "__delta_internal_row_index",
      DeltaParquetFileFormat.ROW_INDEX_COLUMN_NAME,
      parquetTemporaryRowIndexColumnName)
  private val filePathColumnNames = Set("file_path", "filePath")

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
    if (!isDeltaScan(scan)) {
      return false
    }

    val scanColumnNames = (scan.output.map(_.name) ++ scan.requiredSchema.fieldNames).toSet
    scanColumnNames.exists(deletionVectorRowIndexColumnNames.contains) &&
    scanColumnNames.exists(filePathColumnNames.contains)
  }
}

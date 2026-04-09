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

import org.apache.spark.sql.delta.DeltaParquetFileFormat
import org.apache.spark.sql.delta.DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.delta.stats.PreparedDeltaFileIndex
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat

case class OffloadDeltaScan() extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case scan: FileSourceScanExec if isDeltaScan(scan) && isDeltaLogScan(scan) =>
      FallbackTags.add(scan, "fallback Delta _delta_log scan")
      scan
    case scan: FileSourceScanExec if isDvPreparedDeltaScan(scan) =>
      DeltaScanTransformer(scan)
    case scan: FileSourceScanExec if isDeltaScan(scan) =>
      FallbackTags.add(scan, "fallback plain Delta scan without DV preprocessing")
      scan
    case other => other
  }

  private def isDeltaScan(scan: FileSourceScanExec): Boolean = {
    isDeltaFileIndex(scan) || isDeltaParquetScan(scan)
  }

  private def isDeltaParquetScan(scan: FileSourceScanExec): Boolean = {
    val fileFormatClass = scan.relation.fileFormat.getClass
    fileFormatClass == classOf[DeltaParquetFileFormat] ||
    fileFormatClass.getSimpleName == "GlutenDeltaParquetFileFormat"
  }

  private def isDeltaFileIndex(scan: FileSourceScanExec): Boolean = {
    scan.relation.location.isInstanceOf[TahoeFileIndex] ||
    scan.relation.location.isInstanceOf[PreparedDeltaFileIndex]
  }

  private def isDvPreparedDeltaScan(scan: FileSourceScanExec): Boolean = {
    isDeltaScan(scan) && hasDeletionVectorMarkers(scan)
  }

  private def hasDeletionVectorMarkers(scan: FileSourceScanExec): Boolean = {
    scan.output.exists(_.name == IS_ROW_DELETED_COLUMN_NAME) ||
    scan.requiredSchema.fieldNames.contains(IS_ROW_DELETED_COLUMN_NAME) ||
    scan.output.exists(_.name == ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME) ||
    scan.requiredSchema.fieldNames.contains(ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME)
  }

  private def isDeltaLogScan(scan: FileSourceScanExec): Boolean = {
    scan.relation.location.rootPaths.exists {
      path =>
        val root = path.toString
        root.contains("/_delta_log") || root.contains("\\_delta_log") || root.endsWith("_delta_log")
    }
  }
}

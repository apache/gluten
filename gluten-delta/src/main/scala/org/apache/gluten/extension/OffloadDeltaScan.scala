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

import org.apache.spark.sql.delta.SnapshotDescriptor
import org.apache.spark.sql.delta.commands.DeletionVectorUtils.deletionVectorsReadable
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.delta.stats.PreparedDeltaFileIndex
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.util.SparkVersionUtil

case class OffloadDeltaScan(
    enableNativeDeltaWriteKey: String,
    enableNativeDeletionVectorDmlRowIndexScanKey: String)
  extends OffloadSingleNode {
  private val DeletionVectorsUseMetadataRowIndexKey =
    "spark.databricks.delta.deletionVectors.useMetadataRowIndex"

  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case scan: FileSourceScanExec if isDeltaLogScan(scan) =>
      FallbackTags.add(scan, "fallback Delta _delta_log scan")
      scan
    case scan: FileSourceScanExec if shouldFallbackSpark34DeletionVectorScan(scan) =>
      FallbackTags.add(scan, "fallback Spark 3.4 Delta DV scan")
      scan
    case scan: FileSourceScanExec if shouldFallbackDeletionVectorDmlScan(scan) =>
      FallbackTags.add(scan, "fallback Delta DV DML row-index scan")
      scan
    case scan: FileSourceScanExec
        if shouldFallbackDeletionVectorScanWithoutMetadataRowIndex(scan) =>
      FallbackTags.add(scan, "fallback Delta DV scan without metadata row index")
      scan
    case scan: FileSourceScanExec if isDeltaScan(scan) =>
      val transformer = DeltaScanTransformer(scan)
      DeltaDeletionVectorDmlUtils.copyDmlRowIndexScanTag(scan, transformer)
      transformer
    case other => other
  }

  private def isDeltaScan(scan: FileSourceScanExec): Boolean = {
    DeltaDeletionVectorDmlUtils.isDeltaScan(scan)
  }

  private def shouldFallbackDeletionVectorDmlScan(scan: FileSourceScanExec): Boolean = {
    val enableNativeDeltaWrite =
      scan.relation.sparkSession.sessionState.conf
        .getConfString(enableNativeDeltaWriteKey, "false")
        .toBoolean
    val enableNativeDmlRowIndexScan =
      scan.relation.sparkSession.sessionState.conf
        .getConfString(enableNativeDeletionVectorDmlRowIndexScanKey, "false")
        .toBoolean
    if (enableNativeDeltaWrite && enableNativeDmlRowIndexScan) {
      return false
    }

    // DELETE/UPDATE/MERGE with persistent deletion vectors needs the target scan to expose
    // per-file row indexes so Delta can build updated DV bitmaps. Keep this experimental target
    // scan on Spark by default until its native row-index correctness is established independently
    // of the native BitmapAggregator and write path.
    DeltaDeletionVectorDmlUtils.isDeletionVectorDmlRowIndexScan(scan)
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

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

import org.apache.spark.sql.delta.DeltaParquetFileFormat
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.delta.commands.DeletionVectorUtils.deletionVectorsReadable
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.delta.stats.PreparedDeltaFileIndex
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}

/**
 * Delta 2.4 DELETE/UPDATE on DV-enabled tables execute helper scans inside an active transaction.
 * Those scans must stay on the JVM path; otherwise Gluten can conclude that zero rows matched.
 */
case class OffloadDeltaDmlScan() extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case scan: FileSourceScanExec if shouldFallbackDeltaDmlScan(scan) =>
      FallbackTags.add(scan, "fallback Delta 2.4 DV scan inside active DML transaction")
      scan
    case other => other
  }

  private def shouldFallbackDeltaDmlScan(scan: FileSourceScanExec): Boolean = {
    isDeltaScan(scan) &&
    OptimisticTransaction.getActive().exists {
      txn => deletionVectorsReadable(txn.snapshot.protocol, txn.snapshot.metadata)
    }
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
}

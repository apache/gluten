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
package org.apache.gluten.component

import org.apache.gluten.execution.{CHPaimonCountTransformer, CHPaimonScanTransformer, PaimonScanTransformer}
import org.apache.gluten.extension.columnar.offload.OffloadSingleNode

import org.apache.spark.sql.execution.{LocalTableScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

import org.apache.paimon.spark.read.PaimonLocalScan

case object CHOffloadPaimonScan extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan = {
    plan match {
      case scan: LocalTableScanExec
          if scan.logicalLink.exists(_.exists {
            case relation: DataSourceV2ScanRelation =>
              relation.scan.isInstanceOf[PaimonLocalScan]
            case _ => false
          }) => CHPaimonCountTransformer.replace(scan)
      case scan: BatchScanExec if PaimonScanTransformer.supportsBatchScan(scan.scan) =>
        CHPaimonScanTransformer(scan)
      case other => other
    }
  }
}

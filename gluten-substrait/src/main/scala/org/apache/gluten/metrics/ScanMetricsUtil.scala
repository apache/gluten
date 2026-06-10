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
package org.apache.gluten.metrics

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.sql.execution.metric.SQLMetric

/**
 * Velox-only utilities to reduce scan operator SQL metrics under driver memory pressure.
 *
 * By default all detailed metrics are registered. Disable full collection via
 * `spark.gluten.sql.scan.detailedMetrics.enabled=false` to keep only essential metrics. Also
 * enabled automatically when `spark.gluten.sql.debug` is true.
 */
object ScanMetricsUtil {

  def detailedScanMetricsEnabled: Boolean = GlutenConfig.get.detailedScanMetricsEnabled

  /** Velox BatchScanExecTransformer - executor-side metrics (default subset). */
  val VELOX_BATCH_SCAN_MINIMAL_METRICS: Set[String] = Set(
    "rawInputRows",
    "rawInputBytes",
    "numOutputRows",
    "outputBytes",
    "scanTime",
    "wallNanos",
    "peakMemoryBytes",
    "ioWaitTime",
    "storageReadBytes"
  )

  /** Velox FileSourceScan / HiveTableScan - executor + Spark-aligned driver metrics. */
  val VELOX_FILE_SCAN_MINIMAL_METRICS: Set[String] = VELOX_BATCH_SCAN_MINIMAL_METRICS ++ Set(
    "numFiles",
    "metadataTime",
    "filesSize",
    "numPartitions",
    "pruningTime"
  )

  val VELOX_HIVE_SCAN_MINIMAL_METRICS: Set[String] = VELOX_FILE_SCAN_MINIMAL_METRICS

  /** Driver-only FileSource/Hive scan metrics (not passed to executor MetricsUpdater). */
  val VELOX_FILE_SCAN_DRIVER_METRICS: Set[String] = Set(
    "numFiles",
    "metadataTime",
    "filesSize",
    "numPartitions",
    "pruningTime"
  )

  def filterExecutorMetrics(metrics: Map[String, SQLMetric]): Map[String, SQLMetric] =
    metrics.filterKeys(!VELOX_FILE_SCAN_DRIVER_METRICS.contains(_)).toMap

  def filterScanMetrics(
      metrics: Map[String, SQLMetric],
      minimalKeys: Set[String]): Map[String, SQLMetric] = {
    if (detailedScanMetricsEnabled) {
      metrics
    } else {
      metrics.filterKeys(minimalKeys.contains).toMap
    }
  }

  def inc(metric: Option[SQLMetric], value: Long): Unit = metric.foreach(_ += value)
}

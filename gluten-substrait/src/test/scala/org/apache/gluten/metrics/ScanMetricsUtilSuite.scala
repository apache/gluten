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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.test.SharedSparkSession

class ScanMetricsUtilSuite extends SparkFunSuite with SharedSparkSession with SQLHelper {

  test("filterScanMetrics keeps minimal Velox batch scan keys when disabled") {
    withSQLConf(
      GlutenConfig.SCAN_DETAILED_METRICS_ENABLED.key -> "false",
      GlutenConfig.DEBUG_ENABLED.key -> "false") {
      val full = Map(
        "numInputRows" -> SQLMetrics.createMetric(sparkContext, "in"),
        "rawInputRows" -> SQLMetrics.createMetric(sparkContext, "raw in rows"),
        "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "out"),
        "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu")
      )
      val filtered =
        ScanMetricsUtil.filterScanMetrics(full, ScanMetricsUtil.VELOX_BATCH_SCAN_MINIMAL_METRICS)
      assert(!filtered.contains("numInputRows"))
      assert(!filtered.contains("cpuCount"))
      assert(filtered.contains("rawInputRows"))
      assert(filtered.contains("numOutputRows"))
    }
  }

  test("filterScanMetrics returns all keys by default") {
    withSQLConf(
      GlutenConfig.SCAN_DETAILED_METRICS_ENABLED.key -> "true",
      GlutenConfig.DEBUG_ENABLED.key -> "false") {
      val full = Map(
        "numInputRows" -> SQLMetrics.createMetric(sparkContext, "in"),
        "rawInputRows" -> SQLMetrics.createMetric(sparkContext, "raw in rows"))
      val filtered =
        ScanMetricsUtil.filterScanMetrics(full, ScanMetricsUtil.VELOX_BATCH_SCAN_MINIMAL_METRICS)
      assert(filtered.size == full.size)
    }
  }

  test("filterScanMetrics returns all keys when debug is enabled") {
    withSQLConf(
      GlutenConfig.SCAN_DETAILED_METRICS_ENABLED.key -> "false",
      GlutenConfig.DEBUG_ENABLED.key -> "true") {
      val full = Map("cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu"))
      val filtered =
        ScanMetricsUtil.filterScanMetrics(full, ScanMetricsUtil.VELOX_BATCH_SCAN_MINIMAL_METRICS)
      assert(filtered.size == full.size)
    }
  }
}

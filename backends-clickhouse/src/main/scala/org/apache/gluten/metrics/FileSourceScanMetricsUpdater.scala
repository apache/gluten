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

import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.utils.SparkInputMetricsUtil.InputMetricsWrapper

/**
 * Note: "val metrics" is made transient to avoid sending driver-side metrics to tasks, e.g.
 * "pruning time" from scan.
 */
class FileSourceScanMetricsUpdater(@transient val metrics: Map[String, SQLMetric])
  extends MetricsUpdater {

  val scanTime: SQLMetric = metrics("scanTime")
  val outputRows: SQLMetric = metrics("numOutputRows")
  val outputVectors: SQLMetric = metrics("outputVectors")
  val outputBytes: SQLMetric = metrics("outputBytes")
  val inputRows: SQLMetric = metrics("numInputRows")
  val inputBytes: SQLMetric = metrics("inputBytes")
  val extraTime: SQLMetric = metrics("extraTime")
  val inputWaitTime: SQLMetric = metrics("inputWaitTime")
  val outputWaitTime: SQLMetric = metrics("outputWaitTime")
  val selectedMarksPK: SQLMetric = metrics("selectedMarksPk")
  val selectedMarks: SQLMetric = metrics("selectedMarks")
  val totalMarksPK: SQLMetric = metrics("totalMarksPk")
  val readCacheHits: SQLMetric = metrics("readCacheHits")
  val missCacheHits: SQLMetric = metrics("missCacheHits")
  val readCacheBytes: SQLMetric = metrics("readCacheBytes")
  val readMissBytes: SQLMetric = metrics("readMissBytes")
  val readCacheMillisecond: SQLMetric = metrics("readCacheMillisecond")
  val missCacheMillisecond: SQLMetric = metrics("missCacheMillisecond")
  val parquetMetadataCacheHits: SQLMetric = metrics("parquetMetadataCacheHits")
  val parquetMetadataCacheMisses: SQLMetric = metrics("parquetMetadataCacheMisses")
  val isParquetReaderV3: SQLMetric = metrics("isParquetReaderV3")

  override def updateInputMetrics(inputMetrics: InputMetricsWrapper): Unit = {
    // inputMetrics.bridgeIncBytesRead(metrics("inputBytes").value)
    // inputMetrics.bridgeIncRecordsRead(metrics("numInputRows").value)
  }

  override def updateNativeMetrics(opMetrics: IOperatorMetrics): Unit = {
    if (opMetrics != null) {
      val operatorMetrics = opMetrics.asInstanceOf[OperatorMetrics]
      if (!operatorMetrics.metricsList.isEmpty) {
        val metricsData = operatorMetrics.metricsList.get(0)
        scanTime += (metricsData.time / 1000L).toLong
        inputWaitTime += (metricsData.inputWaitTime / 1000L).toLong
        outputWaitTime += (metricsData.outputWaitTime / 1000L).toLong
        outputVectors += metricsData.outputVectors

        metricsData.getSteps.forEach(
          step => {
            selectedMarksPK += step.selectedMarksPk
            selectedMarks += step.selectedMarks
            totalMarksPK += step.totalMarksPk
            readCacheHits += step.readCacheHits
            missCacheHits += step.missCacheHits
            readCacheBytes += step.readCacheBytes
            readMissBytes += step.readMissBytes
            readCacheMillisecond += step.readCacheMillisecond
            missCacheMillisecond += step.missCacheMillisecond
            parquetMetadataCacheHits += step.parquetMetadataCacheHits
            parquetMetadataCacheMisses += step.parquetMetadataCacheMisses
            isParquetReaderV3 += step.parquetReaderVersion
          })

        MetricsUtil.updateExtraTimeMetric(
          metricsData,
          extraTime,
          outputRows,
          outputBytes,
          inputRows,
          inputBytes,
          FileSourceScanMetricsUpdater.INCLUDING_PROCESSORS,
          FileSourceScanMetricsUpdater.CH_PLAN_NODE_NAME
        )
      }
    }
  }
}

object FileSourceScanMetricsUpdater {
  // in mergetree format, the processor name is `MergeTreeSelect(pool: XXX, algorithm: XXX)`
  val INCLUDING_PROCESSORS = Array("MergeTreeSelect(pool", "SubstraitFileSource")
  val CH_PLAN_NODE_NAME = Array("MergeTreeSelect(pool", "SubstraitFileSource")
}

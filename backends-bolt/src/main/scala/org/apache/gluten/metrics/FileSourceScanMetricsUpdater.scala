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

  val rawInputRows: SQLMetric = metrics("rawInputRows")
  val rawInputBytes: SQLMetric = metrics("rawInputBytes")
  val outputRows: SQLMetric = metrics("numOutputRows")
  val outputVectors: SQLMetric = metrics("outputVectors")
  val outputBytes: SQLMetric = metrics("outputBytes")
  val wallNanos: SQLMetric = metrics("wallNanos")
  val cpuCount: SQLMetric = metrics("cpuCount")
  val scanTime: SQLMetric = metrics("scanTime")
  val peakMemoryBytes: SQLMetric = metrics("peakMemoryBytes")
  val numMemoryAllocations: SQLMetric = metrics("numMemoryAllocations")

  // Number of dynamic filters received.
  val numDynamicFiltersAccepted: SQLMetric = metrics("numDynamicFiltersAccepted")
  val skippedSplits: SQLMetric = metrics("skippedSplits")
  val processedSplits: SQLMetric = metrics("processedSplits")
  val preloadSplits: SQLMetric = metrics("preloadSplits")
  val dataSourceAddSplitTime: SQLMetric = metrics("dataSourceAddSplitTime")
  val dataSourceReadTime: SQLMetric = metrics("dataSourceReadTime")
  val skippedStrides: SQLMetric = metrics("skippedStrides")
  val processedStrides: SQLMetric = metrics("processedStrides")
  val remainingFilterTime: SQLMetric = metrics("remainingFilterTime")
  val ioWaitTime: SQLMetric = metrics("ioWaitTime")
  val storageReadBytes: SQLMetric = metrics("storageReadBytes")
  val localReadBytes: SQLMetric = metrics("localReadBytes")
  val ramReadBytes: SQLMetric = metrics("ramReadBytes")
  val loadLazyVectorTime: SQLMetric = metrics("loadLazyVectorTime")

  override def updateInputMetrics(inputMetrics: InputMetricsWrapper): Unit = {
    inputMetrics.bridgeIncBytesRead(rawInputBytes.value)
    inputMetrics.bridgeIncRecordsRead(rawInputRows.value)
  }

  override def updateNativeMetrics(opMetrics: IOperatorMetrics): Unit = {
    if (opMetrics != null) {
      val operatorMetrics = opMetrics.asInstanceOf[OperatorMetrics]
      rawInputRows += operatorMetrics.rawInputRows
      rawInputBytes += operatorMetrics.rawInputBytes
      outputRows += operatorMetrics.outputRows
      outputVectors += operatorMetrics.outputVectors
      outputBytes += operatorMetrics.outputBytes
      wallNanos += operatorMetrics.wallNanos
      cpuCount += operatorMetrics.cpuCount
      scanTime += operatorMetrics.scanTime
      peakMemoryBytes += operatorMetrics.peakMemoryBytes
      numMemoryAllocations += operatorMetrics.numMemoryAllocations
      // Number of dynamic filters received.
      numDynamicFiltersAccepted += operatorMetrics.numDynamicFiltersAccepted
      skippedSplits += operatorMetrics.skippedSplits
      processedSplits += operatorMetrics.processedSplits
      skippedStrides += operatorMetrics.skippedStrides
      processedStrides += operatorMetrics.processedStrides
      remainingFilterTime += operatorMetrics.remainingFilterTime
      ioWaitTime += operatorMetrics.ioWaitTime
      storageReadBytes += operatorMetrics.storageReadBytes
      localReadBytes += operatorMetrics.localReadBytes
      ramReadBytes += operatorMetrics.ramReadBytes
      preloadSplits += operatorMetrics.preloadSplits
      dataSourceAddSplitTime += operatorMetrics.dataSourceAddSplitTime
      dataSourceReadTime += operatorMetrics.dataSourceReadTime
      loadLazyVectorTime += operatorMetrics.loadLazyVectorTime
    }
  }
}

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
 * See [[FileSourceScanMetricsUpdater]]: @transient metrics map with per-metric fields captured on
 * the driver for executor serialization.
 */
class BatchScanMetricsUpdater(@transient val metrics: Map[String, SQLMetric])
  extends MetricsUpdater {

  private def metric(key: String): Option[SQLMetric] = Option(metrics).flatMap(_.get(key))

  private val numInputRows: Option[SQLMetric] = metric("numInputRows")
  private val inputVectors: Option[SQLMetric] = metric("inputVectors")
  private val inputBytes: Option[SQLMetric] = metric("inputBytes")
  private val rawInputRows: Option[SQLMetric] = metric("rawInputRows")
  private val rawInputBytes: Option[SQLMetric] = metric("rawInputBytes")
  private val outputRows: Option[SQLMetric] = metric("numOutputRows")
  private val outputVectors: Option[SQLMetric] = metric("outputVectors")
  private val outputBytes: Option[SQLMetric] = metric("outputBytes")
  private val cpuCount: Option[SQLMetric] = metric("cpuCount")
  private val scanTime: Option[SQLMetric] = metric("scanTime")
  private val wallNanos: Option[SQLMetric] = metric("wallNanos")
  private val peakMemoryBytes: Option[SQLMetric] = metric("peakMemoryBytes")
  private val numMemoryAllocations: Option[SQLMetric] = metric("numMemoryAllocations")
  private val numDynamicFiltersAccepted: Option[SQLMetric] = metric("numDynamicFiltersAccepted")
  private val skippedSplits: Option[SQLMetric] = metric("skippedSplits")
  private val processedSplits: Option[SQLMetric] = metric("processedSplits")
  private val skippedStrides: Option[SQLMetric] = metric("skippedStrides")
  private val processedStrides: Option[SQLMetric] = metric("processedStrides")
  private val remainingFilterTime: Option[SQLMetric] = metric("remainingFilterTime")
  private val ioWaitTime: Option[SQLMetric] = metric("ioWaitTime")
  private val storageReadBytes: Option[SQLMetric] = metric("storageReadBytes")
  private val storageReads: Option[SQLMetric] = metric("storageReads")
  private val localReadBytes: Option[SQLMetric] = metric("localReadBytes")
  private val ramReadBytes: Option[SQLMetric] = metric("ramReadBytes")
  private val preloadSplits: Option[SQLMetric] = metric("preloadSplits")
  private val pageLoadTime: Option[SQLMetric] = metric("pageLoadTime")
  private val dataSourceAddSplitTime: Option[SQLMetric] = metric("dataSourceAddSplitTime")
  private val dataSourceReadTime: Option[SQLMetric] = metric("dataSourceReadTime")
  private val loadLazyVectorTime: Option[SQLMetric] = metric("loadLazyVectorTime")

  override def updateInputMetrics(inputMetrics: InputMetricsWrapper): Unit = {
    rawInputBytes.foreach(m => inputMetrics.bridgeIncBytesRead(m.value))
    rawInputRows.foreach(m => inputMetrics.bridgeIncRecordsRead(m.value))
  }

  override def updateNativeMetrics(opMetrics: IOperatorMetrics): Unit = {
    if (opMetrics != null) {
      val operatorMetrics = opMetrics.asInstanceOf[OperatorMetrics]
      ScanMetricsUtil.inc(numInputRows, operatorMetrics.inputRows)
      ScanMetricsUtil.inc(inputVectors, operatorMetrics.inputVectors)
      ScanMetricsUtil.inc(inputBytes, operatorMetrics.inputBytes)
      ScanMetricsUtil.inc(rawInputRows, operatorMetrics.rawInputRows)
      ScanMetricsUtil.inc(rawInputBytes, operatorMetrics.rawInputBytes)
      ScanMetricsUtil.inc(outputRows, operatorMetrics.outputRows)
      ScanMetricsUtil.inc(outputVectors, operatorMetrics.outputVectors)
      ScanMetricsUtil.inc(outputBytes, operatorMetrics.outputBytes)
      ScanMetricsUtil.inc(cpuCount, operatorMetrics.cpuCount)
      ScanMetricsUtil.inc(scanTime, operatorMetrics.scanTime)
      ScanMetricsUtil.inc(wallNanos, operatorMetrics.wallNanos)
      ScanMetricsUtil.inc(peakMemoryBytes, operatorMetrics.peakMemoryBytes)
      ScanMetricsUtil.inc(numMemoryAllocations, operatorMetrics.numMemoryAllocations)
      ScanMetricsUtil.inc(numDynamicFiltersAccepted, operatorMetrics.numDynamicFiltersAccepted)
      ScanMetricsUtil.inc(skippedSplits, operatorMetrics.skippedSplits)
      ScanMetricsUtil.inc(processedSplits, operatorMetrics.processedSplits)
      ScanMetricsUtil.inc(skippedStrides, operatorMetrics.skippedStrides)
      ScanMetricsUtil.inc(processedStrides, operatorMetrics.processedStrides)
      ScanMetricsUtil.inc(remainingFilterTime, operatorMetrics.remainingFilterTime)
      ScanMetricsUtil.inc(ioWaitTime, operatorMetrics.ioWaitTime)
      ScanMetricsUtil.inc(storageReadBytes, operatorMetrics.storageReadBytes)
      ScanMetricsUtil.inc(storageReads, operatorMetrics.storageReads)
      ScanMetricsUtil.inc(localReadBytes, operatorMetrics.localReadBytes)
      ScanMetricsUtil.inc(ramReadBytes, operatorMetrics.ramReadBytes)
      ScanMetricsUtil.inc(preloadSplits, operatorMetrics.preloadSplits)
      ScanMetricsUtil.inc(pageLoadTime, operatorMetrics.pageLoadTime)
      ScanMetricsUtil.inc(dataSourceAddSplitTime, operatorMetrics.dataSourceAddSplitTime)
      ScanMetricsUtil.inc(dataSourceReadTime, operatorMetrics.dataSourceReadTime)
      ScanMetricsUtil.inc(loadLazyVectorTime, operatorMetrics.loadLazyVectorTime)
    }
  }
}

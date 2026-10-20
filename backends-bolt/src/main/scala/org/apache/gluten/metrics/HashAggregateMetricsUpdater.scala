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

import org.apache.gluten.substrait.AggregationParams

import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.utils.SparkMetricsUtil
import org.apache.spark.task.TaskResources

import scala.collection.JavaConverters._

trait HashAggregateMetricsUpdater extends MetricsUpdater {
  def updateAggregationMetrics(
      aggregationMetrics: java.util.ArrayList[OperatorMetrics],
      aggParams: AggregationParams): Unit
}

class HashAggregateMetricsUpdaterImpl(val metrics: Map[String, SQLMetric])
  extends HashAggregateMetricsUpdater {
  val aggOutputRows: SQLMetric = metrics("aggOutputRows")
  val aggOutputVectors: SQLMetric = metrics("aggOutputVectors")
  val aggOutputBytes: SQLMetric = metrics("aggOutputBytes")
  val aggCpuCount: SQLMetric = metrics("aggCpuCount")
  val aggWallNanos: SQLMetric = metrics("aggWallNanos")
  val aggPeakMemoryBytes: SQLMetric = metrics("aggPeakMemoryBytes")
  val aggNumMemoryAllocations: SQLMetric = metrics("aggNumMemoryAllocations")
  val aggSpilledBytes: SQLMetric = metrics("aggSpilledBytes")
  val aggSpilledRows: SQLMetric = metrics("aggSpilledRows")
  val aggSpilledPartitions: SQLMetric = metrics("aggSpilledPartitions")
  val aggSpilledFiles: SQLMetric = metrics("aggSpilledFiles")
  val flushRowCount: SQLMetric = metrics("flushRowCount")
  val loadedToValueHook: SQLMetric = metrics("loadedToValueHook")

  val rowConstructionCpuCount: SQLMetric = metrics("rowConstructionCpuCount")
  val rowConstructionWallNanos: SQLMetric = metrics("rowConstructionWallNanos")

  val extractionCpuCount: SQLMetric = metrics("extractionCpuCount")
  val extractionWallNanos: SQLMetric = metrics("extractionWallNanos")

  val loadLazyVectorTime: SQLMetric = metrics("loadLazyVectorTime")

  override def updateAggregationMetrics(
      aggregationMetrics: java.util.ArrayList[OperatorMetrics],
      aggParams: AggregationParams): Unit = {
    var idx = 0

    if (aggParams.extractionNeeded) {
      extractionCpuCount += aggregationMetrics.get(idx).cpuCount
      extractionWallNanos += aggregationMetrics.get(idx).wallNanos
      idx += 1
    }

    val aggMetrics = aggregationMetrics.get(idx)
    aggOutputRows += aggMetrics.outputRows
    aggOutputVectors += aggMetrics.outputVectors
    aggOutputBytes += aggMetrics.outputBytes
    aggCpuCount += aggMetrics.cpuCount
    aggWallNanos += aggMetrics.wallNanos
    aggPeakMemoryBytes += aggMetrics.peakMemoryBytes
    aggNumMemoryAllocations += aggMetrics.numMemoryAllocations
    aggSpilledBytes += aggMetrics.spilledBytes
    aggSpilledRows += aggMetrics.spilledRows
    aggSpilledPartitions += aggMetrics.spilledPartitions
    aggSpilledFiles += aggMetrics.spilledFiles
    flushRowCount += aggMetrics.flushRowCount
    loadedToValueHook += aggMetrics.loadedToValueHook
    idx += 1

    if (aggParams.rowConstructionNeeded) {
      rowConstructionCpuCount += aggregationMetrics.get(idx).cpuCount
      rowConstructionWallNanos += aggregationMetrics.get(idx).wallNanos
      idx += 1
    }

    loadLazyVectorTime += aggregationMetrics.asScala.last.loadLazyVectorTime

    if (TaskResources.inSparkTask()) {
      SparkMetricsUtil.incMemoryBytesSpilled(
        TaskResources.getLocalTaskContext().taskMetrics(),
        aggMetrics.spilledInputBytes)
      SparkMetricsUtil.incDiskBytesSpilled(
        TaskResources.getLocalTaskContext().taskMetrics(),
        aggMetrics.spilledBytes)
    }
  }
}

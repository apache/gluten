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

import org.apache.gluten.metrics.Metrics.SingleMetric
import org.apache.gluten.substrait.JoinParams

import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.utils.SparkMetricsUtil
import org.apache.spark.task.TaskResources

import java.util

import scala.collection.JavaConverters._

trait JoinMetricsUpdater extends MetricsUpdater {
  def updateJoinMetrics(
      joinMetrics: java.util.ArrayList[OperatorMetrics],
      singleMetrics: SingleMetric,
      joinParams: JoinParams): Unit
}

abstract class JoinMetricsUpdaterBase(val metrics: Map[String, SQLMetric])
  extends JoinMetricsUpdater {
  val postProjectionCpuCount: SQLMetric = metrics("postProjectionCpuCount")
  val postProjectionWallNanos: SQLMetric = metrics("postProjectionWallNanos")
  val numOutputRows: SQLMetric = metrics("numOutputRows")
  val numOutputVectors: SQLMetric = metrics("numOutputVectors")
  val numOutputBytes: SQLMetric = metrics("numOutputBytes")

  final override def updateJoinMetrics(
      joinMetrics: util.ArrayList[OperatorMetrics],
      singleMetrics: SingleMetric,
      joinParams: JoinParams): Unit = {
    assert(joinParams.postProjectionNeeded)
    val postProjectMetrics = joinMetrics.remove(0)
    postProjectionCpuCount += postProjectMetrics.cpuCount
    postProjectionWallNanos += postProjectMetrics.wallNanos
    numOutputRows += postProjectMetrics.outputRows
    numOutputVectors += postProjectMetrics.outputVectors
    numOutputBytes += postProjectMetrics.outputBytes

    updateJoinMetricsInternal(joinMetrics, joinParams)
  }

  protected def updateJoinMetricsInternal(
      joinMetrics: util.ArrayList[OperatorMetrics],
      joinParams: JoinParams): Unit
}

class HashJoinMetricsUpdater(override val metrics: Map[String, SQLMetric])
  extends JoinMetricsUpdaterBase(metrics) {
  val hashBuildInputRows: SQLMetric = metrics("hashBuildInputRows")
  val hashBuildOutputRows: SQLMetric = metrics("hashBuildOutputRows")
  val hashBuildOutputVectors: SQLMetric = metrics("hashBuildOutputVectors")
  val hashBuildOutputBytes: SQLMetric = metrics("hashBuildOutputBytes")
  val hashBuildCpuCount: SQLMetric = metrics("hashBuildCpuCount")
  val hashBuildWallNanos: SQLMetric = metrics("hashBuildWallNanos")
  val hashBuildPeakMemoryBytes: SQLMetric = metrics("hashBuildPeakMemoryBytes")
  val hashBuildNumMemoryAllocations: SQLMetric = metrics("hashBuildNumMemoryAllocations")
  val hashBuildSpilledBytes: SQLMetric = metrics("hashBuildSpilledBytes")
  val hashBuildSpilledRows: SQLMetric = metrics("hashBuildSpilledRows")
  val hashBuildSpilledPartitions: SQLMetric = metrics("hashBuildSpilledPartitions")
  val hashBuildSpilledFiles: SQLMetric = metrics("hashBuildSpilledFiles")

  val hashProbeInputRows: SQLMetric = metrics("hashProbeInputRows")
  val hashProbeOutputRows: SQLMetric = metrics("hashProbeOutputRows")
  val hashProbeOutputVectors: SQLMetric = metrics("hashProbeOutputVectors")
  val hashProbeOutputBytes: SQLMetric = metrics("hashProbeOutputBytes")
  val hashProbeCpuCount: SQLMetric = metrics("hashProbeCpuCount")
  val hashProbeWallNanos: SQLMetric = metrics("hashProbeWallNanos")
  val hashProbePeakMemoryBytes: SQLMetric = metrics("hashProbePeakMemoryBytes")
  val hashProbeNumMemoryAllocations: SQLMetric = metrics("hashProbeNumMemoryAllocations")
  val hashProbeSpilledBytes: SQLMetric = metrics("hashProbeSpilledBytes")
  val hashProbeSpilledRows: SQLMetric = metrics("hashProbeSpilledRows")
  val hashProbeSpilledPartitions: SQLMetric = metrics("hashProbeSpilledPartitions")
  val hashProbeSpilledFiles: SQLMetric = metrics("hashProbeSpilledFiles")

  // The number of rows which were passed through without any processing
  // after filter was pushed down.
  val hashProbeReplacedWithDynamicFilterRows: SQLMetric =
    metrics("hashProbeReplacedWithDynamicFilterRows")

  // The number of dynamic filters this join generated for push down.
  val hashProbeDynamicFiltersProduced: SQLMetric =
    metrics("hashProbeDynamicFiltersProduced")

  val streamPreProjectionCpuCount: SQLMetric = metrics("streamPreProjectionCpuCount")
  val streamPreProjectionWallNanos: SQLMetric = metrics("streamPreProjectionWallNanos")

  val buildPreProjectionCpuCount: SQLMetric = metrics("buildPreProjectionCpuCount")
  val buildPreProjectionWallNanos: SQLMetric = metrics("buildPreProjectionWallNanos")

  val loadLazyVectorTime: SQLMetric = metrics("loadLazyVectorTime")

  override protected def updateJoinMetricsInternal(
      joinMetrics: java.util.ArrayList[OperatorMetrics],
      joinParams: JoinParams): Unit = {
    var idx = 0
    // HashProbe
    val hashProbeMetrics = joinMetrics.get(idx)
    hashProbeInputRows += hashProbeMetrics.inputRows
    hashProbeOutputRows += hashProbeMetrics.outputRows
    hashProbeOutputVectors += hashProbeMetrics.outputVectors
    hashProbeOutputBytes += hashProbeMetrics.outputBytes
    hashProbeCpuCount += hashProbeMetrics.cpuCount
    hashProbeWallNanos += hashProbeMetrics.wallNanos
    hashProbePeakMemoryBytes += hashProbeMetrics.peakMemoryBytes
    hashProbeNumMemoryAllocations += hashProbeMetrics.numMemoryAllocations
    hashProbeSpilledBytes += hashProbeMetrics.spilledBytes
    hashProbeSpilledRows += hashProbeMetrics.spilledRows
    hashProbeSpilledPartitions += hashProbeMetrics.spilledPartitions
    hashProbeSpilledFiles += hashProbeMetrics.spilledFiles
    hashProbeReplacedWithDynamicFilterRows += hashProbeMetrics.numReplacedWithDynamicFilterRows
    hashProbeDynamicFiltersProduced += hashProbeMetrics.numDynamicFiltersProduced
    idx += 1

    // HashBuild
    val hashBuildMetrics = joinMetrics.get(idx)
    hashBuildInputRows += hashBuildMetrics.inputRows
    hashBuildOutputRows += hashBuildMetrics.outputRows
    hashBuildOutputVectors += hashBuildMetrics.outputVectors
    hashBuildOutputBytes += hashBuildMetrics.outputBytes
    hashBuildCpuCount += hashBuildMetrics.cpuCount
    hashBuildWallNanos += hashBuildMetrics.wallNanos
    hashBuildPeakMemoryBytes += hashBuildMetrics.peakMemoryBytes
    hashBuildNumMemoryAllocations += hashBuildMetrics.numMemoryAllocations
    hashBuildSpilledBytes += hashBuildMetrics.spilledBytes
    hashBuildSpilledRows += hashBuildMetrics.spilledRows
    hashBuildSpilledPartitions += hashBuildMetrics.spilledPartitions
    hashBuildSpilledFiles += hashBuildMetrics.spilledFiles
    idx += 1

    if (joinParams.buildPreProjectionNeeded) {
      buildPreProjectionCpuCount += joinMetrics.get(idx).cpuCount
      buildPreProjectionWallNanos += joinMetrics.get(idx).wallNanos
      idx += 1
    }

    if (joinParams.streamPreProjectionNeeded) {
      streamPreProjectionCpuCount += joinMetrics.get(idx).cpuCount
      streamPreProjectionWallNanos += joinMetrics.get(idx).wallNanos
      idx += 1
    }
    if (TaskResources.inSparkTask()) {
      SparkMetricsUtil.incMemoryBytesSpilled(
        TaskResources.getLocalTaskContext().taskMetrics(),
        hashProbeMetrics.spilledInputBytes)
      SparkMetricsUtil.incDiskBytesSpilled(
        TaskResources.getLocalTaskContext().taskMetrics(),
        hashProbeMetrics.spilledBytes)
      SparkMetricsUtil.incMemoryBytesSpilled(
        TaskResources.getLocalTaskContext().taskMetrics(),
        hashBuildMetrics.spilledInputBytes)
      SparkMetricsUtil.incDiskBytesSpilled(
        TaskResources.getLocalTaskContext().taskMetrics(),
        hashBuildMetrics.spilledBytes)
    }

    loadLazyVectorTime += joinMetrics.asScala.last.loadLazyVectorTime
  }
}

class SortMergeJoinMetricsUpdater(override val metrics: Map[String, SQLMetric])
  extends JoinMetricsUpdaterBase(metrics) {
  val cpuCount: SQLMetric = metrics("cpuCount")
  val wallNanos: SQLMetric = metrics("wallNanos")
  val peakMemoryBytes: SQLMetric = metrics("peakMemoryBytes")
  val numMemoryAllocations: SQLMetric = metrics("numMemoryAllocations")

  val streamPreProjectionCpuCount: SQLMetric = metrics("streamPreProjectionCpuCount")
  val streamPreProjectionWallNanos: SQLMetric = metrics("streamPreProjectionWallNanos")
  val bufferPreProjectionCpuCount: SQLMetric = metrics("bufferPreProjectionCpuCount")
  val bufferPreProjectionWallNanos: SQLMetric = metrics("bufferPreProjectionWallNanos")

  override protected def updateJoinMetricsInternal(
      joinMetrics: util.ArrayList[OperatorMetrics],
      joinParams: JoinParams): Unit = {
    var idx = 0
    val smjMetrics = joinMetrics.get(0)
    cpuCount += smjMetrics.cpuCount
    wallNanos += smjMetrics.wallNanos
    peakMemoryBytes += smjMetrics.peakMemoryBytes
    numMemoryAllocations += smjMetrics.numMemoryAllocations
    idx += 1

    if (joinParams.buildPreProjectionNeeded) {
      bufferPreProjectionCpuCount += joinMetrics.get(idx).cpuCount
      bufferPreProjectionWallNanos += joinMetrics.get(idx).wallNanos
      idx += 1
    }

    if (joinParams.streamPreProjectionNeeded) {
      streamPreProjectionCpuCount += joinMetrics.get(idx).cpuCount
      streamPreProjectionWallNanos += joinMetrics.get(idx).wallNanos
      idx += 1
    }
  }
}

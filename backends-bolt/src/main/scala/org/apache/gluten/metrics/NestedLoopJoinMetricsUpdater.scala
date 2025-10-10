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

import org.apache.gluten.substrait.JoinParams

import org.apache.spark.sql.execution.metric.SQLMetric

import java.util

import scala.collection.JavaConverters._

class NestedLoopJoinMetricsUpdater(override val metrics: Map[String, SQLMetric])
  extends JoinMetricsUpdaterBase(metrics) {

  val nestedLoopJoinBuildInputRows: SQLMetric = metrics("nestedLoopJoinBuildInputRows")
  val nestedLoopJoinBuildOutputRows: SQLMetric = metrics("nestedLoopJoinBuildOutputRows")
  val nestedLoopJoinBuildOutputVectors: SQLMetric = metrics("nestedLoopJoinBuildOutputVectors")
  val nestedLoopJoinBuildOutputBytes: SQLMetric = metrics("nestedLoopJoinBuildOutputBytes")
  val nestedLoopJoinBuildCpuCount: SQLMetric = metrics("nestedLoopJoinBuildCpuCount")
  val nestedLoopJoinBuildWallNanos: SQLMetric = metrics("nestedLoopJoinBuildWallNanos")
  val nestedLoopJoinBuildPeakMemoryBytes: SQLMetric = metrics("nestedLoopJoinBuildPeakMemoryBytes")
  val nestedLoopJoinBuildNumMemoryAllocations: SQLMetric = metrics(
    "nestedLoopJoinBuildNumMemoryAllocations")

  val nestedLoopJoinProbeInputRows: SQLMetric = metrics("nestedLoopJoinProbeInputRows")
  val nestedLoopJoinProbeOutputRows: SQLMetric = metrics("nestedLoopJoinProbeOutputRows")
  val nestedLoopJoinProbeOutputVectors: SQLMetric = metrics("nestedLoopJoinProbeOutputVectors")
  val nestedLoopJoinProbeOutputBytes: SQLMetric = metrics("nestedLoopJoinProbeOutputBytes")
  val nestedLoopJoinProbeCpuCount: SQLMetric = metrics("nestedLoopJoinProbeCpuCount")
  val nestedLoopJoinProbeWallNanos: SQLMetric = metrics("nestedLoopJoinProbeWallNanos")
  val nestedLoopJoinProbePeakMemoryBytes: SQLMetric = metrics("nestedLoopJoinProbePeakMemoryBytes")
  val nestedLoopJoinProbeNumMemoryAllocations: SQLMetric = metrics(
    "nestedLoopJoinProbeNumMemoryAllocations")

  val loadLazyVectorTime: SQLMetric = metrics("loadLazyVectorTime")

  override protected def updateJoinMetricsInternal(
      joinMetrics: util.ArrayList[OperatorMetrics],
      joinParams: JoinParams): Unit = {
    // nestedLoopJoinProbe
    val nestedLoopJoinProbeMetrics = joinMetrics.get(0)
    nestedLoopJoinProbeInputRows += nestedLoopJoinProbeMetrics.inputRows
    nestedLoopJoinProbeOutputRows += nestedLoopJoinProbeMetrics.outputRows
    nestedLoopJoinProbeOutputVectors += nestedLoopJoinProbeMetrics.outputVectors
    nestedLoopJoinProbeOutputBytes += nestedLoopJoinProbeMetrics.outputBytes
    nestedLoopJoinProbeCpuCount += nestedLoopJoinProbeMetrics.cpuCount
    nestedLoopJoinProbeWallNanos += nestedLoopJoinProbeMetrics.wallNanos
    nestedLoopJoinProbePeakMemoryBytes += nestedLoopJoinProbeMetrics.peakMemoryBytes
    nestedLoopJoinProbeNumMemoryAllocations += nestedLoopJoinProbeMetrics.numMemoryAllocations

    // nestedLoopJoinBuild
    val nestedLoopJoinBuildMetrics = joinMetrics.get(1)
    nestedLoopJoinBuildInputRows += nestedLoopJoinBuildMetrics.inputRows
    nestedLoopJoinBuildOutputRows += nestedLoopJoinBuildMetrics.outputRows
    nestedLoopJoinBuildOutputVectors += nestedLoopJoinBuildMetrics.outputVectors
    nestedLoopJoinBuildOutputBytes += nestedLoopJoinBuildMetrics.outputBytes
    nestedLoopJoinBuildCpuCount += nestedLoopJoinBuildMetrics.cpuCount
    nestedLoopJoinBuildWallNanos += nestedLoopJoinBuildMetrics.wallNanos
    nestedLoopJoinBuildPeakMemoryBytes += nestedLoopJoinBuildMetrics.peakMemoryBytes
    nestedLoopJoinBuildNumMemoryAllocations += nestedLoopJoinBuildMetrics.numMemoryAllocations

    loadLazyVectorTime += joinMetrics.asScala.last.loadLazyVectorTime
  }
}

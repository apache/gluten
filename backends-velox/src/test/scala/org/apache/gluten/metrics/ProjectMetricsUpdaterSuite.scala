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

import org.scalatest.funsuite.AnyFunSuite

class ProjectMetricsUpdaterSuite extends AnyFunSuite {

  private def projectMetrics(): Map[String, SQLMetric] = Seq(
    "numOutputRows",
    "outputVectors",
    "outputBytes",
    "cpuCount",
    "wallNanos",
    "peakMemoryBytes",
    "numMemoryAllocations",
    "loadLazyVectorTime").map(_ -> new SQLMetric("sum")).toMap

  private def operatorMetrics(
      outputRows: Long,
      expressionStats: (String, Long)*): OperatorMetrics = {
    val metrics = new OperatorMetrics()
    metrics.outputRows = outputRows
    expressionStats.foreach {
      case (name, rows) => metrics.expressionStats.put(name, java.lang.Long.valueOf(rows))
    }
    metrics
  }

  test("a native counter is credited with the rows its function processed") {
    val updated = new SQLMetric("sum")
    val copied = new SQLMetric("sum")
    val updater = new ProjectMetricsUpdater(
      projectMetrics(),
      Seq("increment_metric_0" -> updated, "increment_metric_1" -> copied))
    updater.updateNativeMetrics(
      operatorMetrics(
        10,
        "increment_metric_0" -> 3L,
        "increment_metric_1" -> 7L,
        "increment_metric_2" -> 10L))
    assert(updated.value === 3)
    assert(copied.value === 7)
  }

  test("a native counter that never ran is credited with zero rows") {
    val metric = new SQLMetric("sum")
    val updater = new ProjectMetricsUpdater(projectMetrics(), Seq("increment_metric_0" -> metric))
    updater.updateNativeMetrics(operatorMetrics(10))
    assert(metric.value === 0)
  }

  test("a counter removed from the native plan is credited with the output rows") {
    val metric = new SQLMetric("sum")
    val updater = new ProjectMetricsUpdater(projectMetrics(), Seq("increment_metric" -> metric))
    updater.updateNativeMetrics(operatorMetrics(10, "increment_metric_0" -> 3L))
    assert(metric.value === 10)
  }
}

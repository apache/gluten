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

class GenerateMetricsUpdater(val metrics: Map[String, SQLMetric]) extends MetricsUpdater {
  override def updateNativeMetrics(operatorMetrics: IOperatorMetrics): Unit = {
    if (operatorMetrics != null) {
      val nativeMetrics = operatorMetrics.asInstanceOf[OperatorMetrics]
      metrics("numOutputRows") += nativeMetrics.outputRows
      metrics("numOutputVectors") += nativeMetrics.outputVectors
      metrics("numOutputBytes") += nativeMetrics.outputBytes
      metrics("cpuCount") += nativeMetrics.cpuCount
      metrics("wallNanos") += nativeMetrics.wallNanos
      metrics("peakMemoryBytes") += nativeMetrics.peakMemoryBytes
      metrics("numMemoryAllocations") += nativeMetrics.numMemoryAllocations
      metrics("loadLazyVectorTime") += nativeMetrics.loadLazyVectorTime
    }
  }
}

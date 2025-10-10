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

import scala.collection.JavaConverters._

class UnionMetricsUpdater(val metrics: Map[String, SQLMetric]) extends MetricsUpdater {
  override def updateNativeMetrics(opMetrics: IOperatorMetrics): Unit = {
    throw new UnsupportedOperationException()
  }

  def updateUnionMetrics(unionMetrics: java.util.ArrayList[OperatorMetrics]): Unit = {
    // Union was interpreted to LocalExchange + LocalPartition. Use metrics from LocalExchange.
    val localExchangeMetrics = unionMetrics.get(0)
    metrics("numInputRows") += localExchangeMetrics.inputRows
    metrics("inputVectors") += localExchangeMetrics.inputVectors
    metrics("inputBytes") += localExchangeMetrics.inputBytes
    metrics("cpuCount") += localExchangeMetrics.cpuCount
    metrics("wallNanos") += localExchangeMetrics.wallNanos
    metrics("loadLazyVectorTime") += unionMetrics.asScala.last.loadLazyVectorTime
  }
}

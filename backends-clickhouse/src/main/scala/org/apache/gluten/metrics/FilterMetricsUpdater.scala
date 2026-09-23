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

import org.apache.gluten.metrics.ProjectMetricsUpdater.{DELTA_INPUT_ROW_METRIC_NAMES, UNSUPPORTED_METRIC_NAMES}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.metric.SQLMetric

class FilterMetricsUpdater(
    val metrics: Map[String, SQLMetric],
    val extraMetrics: Seq[(String, SQLMetric)]
) extends MetricsUpdater
  with Logging {

  override def updateNativeMetrics(opMetrics: IOperatorMetrics): Unit = {
    if (opMetrics != null) {
      val operatorMetrics = opMetrics.asInstanceOf[OperatorMetrics]
      if (!operatorMetrics.metricsList.isEmpty) {
        var numInputRows = Seq(metrics("numInputRows"))
        extraMetrics.foreach {
          case (name, metric) =>
            name match {
              case "increment_metric_dummy" =>
                metric.name match {
                  case Some(input) if DELTA_INPUT_ROW_METRIC_NAMES.contains(input) =>
                    numInputRows = numInputRows :+ metric
                  case Some(unSupport) if UNSUPPORTED_METRIC_NAMES.contains(unSupport) =>
                    logTrace(s"Unsupported metric name: $unSupport")
                  case Some(other) =>
                    logTrace(s"Unknown metric name: $other")
                  case _ => // do nothing
                }
              case o: String =>
                logTrace(s"Unknown metric name: $o")
              case _ => // do nothing
            }
        }

        val metricsData = operatorMetrics.metricsList.get(0)
        metrics("totalTime") += (metricsData.time / 1000L).toLong
        metrics("inputWaitTime") += (metricsData.inputWaitTime / 1000L).toLong
        metrics("outputWaitTime") += (metricsData.outputWaitTime / 1000L).toLong
        metrics("outputVectors") += metricsData.outputVectors

        MetricsUtil.updateExtraTimeMetric(
          metricsData,
          metrics("extraTime"),
          metrics("numOutputRows"),
          metrics("outputBytes"),
          numInputRows,
          metrics("inputBytes"),
          FilterMetricsUpdater.INCLUDING_PROCESSORS,
          FilterMetricsUpdater.INCLUDING_PROCESSORS
        )
      }
    }
  }
}

object FilterMetricsUpdater {
  val INCLUDING_PROCESSORS = Array("FilterTransform")
  val CH_PLAN_NODE_NAME = Array("FilterTransform")
}

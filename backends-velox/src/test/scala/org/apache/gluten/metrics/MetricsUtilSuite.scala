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

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.funsuite.AnyFunSuite

import java.util.{Arrays => JArrays}

import scala.collection.JavaConverters._

class MetricsUtilSuite extends AnyFunSuite {

  private val mapper = new ObjectMapper()

  private def expressionStats(json: String): Map[String, Long] =
    MetricsUtil
      .expressionStatsFromJson(mapper.readTree(json))
      .asScala
      .map { case (name, rows) => name -> rows.longValue() }
      .toMap

  private def operatorMetrics(expressionStats: (String, Long)*): OperatorMetrics = {
    val metrics = new OperatorMetrics()
    expressionStats.foreach {
      case (name, rows) => metrics.expressionStats.put(name, java.lang.Long.valueOf(rows))
    }
    metrics
  }

  test("expression stats are read from an operator's metrics") {
    val json =
      """{"outputRows": 4, "expressionStats": {"increment_metric_0": 3, "increment_metric_1": 0}}"""
    assert(
      expressionStats(json) === Map("increment_metric_0" -> 3L, "increment_metric_1" -> 0L))
  }

  test("an operator without expression stats has none") {
    assert(expressionStats("""{"outputRows": 4}""").isEmpty)
    assert(expressionStats("""{"outputRows": 4, "expressionStats": {}}""").isEmpty)
  }

  test("merging operator metrics sums the expression stats of each function") {
    val merged = MetricsUtil.mergeMetrics(
      JArrays.asList(
        operatorMetrics("increment_metric_0" -> 3L, "increment_metric_1" -> 1L),
        operatorMetrics("increment_metric_0" -> 2L),
        operatorMetrics()))
    assert(
      merged.expressionStats.asScala.map { case (name, rows) => name -> rows.longValue() } ===
        Map("increment_metric_0" -> 5L, "increment_metric_1" -> 1L))
    // A concrete map, like the one every OperatorMetrics starts with.
    assert(merged.expressionStats.isInstanceOf[java.util.HashMap[_, _]])
  }
}

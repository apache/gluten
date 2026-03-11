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
package org.apache.gluten.integration.metrics

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric

trait MetricMapper {
  def map(node: SparkPlan, key: String, metric: SQLMetric): Seq[MetricTag]
}

object MetricMapper {
  val dummy: MetricMapper = (node: SparkPlan, key: String, metric: SQLMetric) => Nil

  case class SimpleMetricMapper(tags: Seq[MetricTag], selfTimeKeys: Map[String, Set[String]])
    extends MetricMapper {
    override def map(node: SparkPlan, key: String, metric: SQLMetric): Seq[MetricTag] = {
      val className = node.getClass.getSimpleName
      if (selfTimeKeys.contains(className)) {
        if (selfTimeKeys(className).contains(key)) {
          return tags
        }
      }
      Nil
    }
  }

  implicit class TypedMetricMapperOps(mapper: MetricMapper) {
    private def unwrap(mapper: MetricMapper): Seq[MetricMapper] =
      mapper match {
        case c: ChainedTypeMetricMapper => c.mappers
        case other => Seq(other)
      }

    def and(other: MetricMapper): MetricMapper = {
      new ChainedTypeMetricMapper(unwrap(mapper) ++ unwrap(other))
    }
  }

  private class ChainedTypeMetricMapper(val mappers: Seq[MetricMapper]) extends MetricMapper {
    assert(!mappers.exists(_.isInstanceOf[ChainedTypeMetricMapper]))
    override def map(node: SparkPlan, key: String, metric: SQLMetric): Seq[MetricTag] = {
      mappers.flatMap(m => m.map(node, key, metric))
    }
  }
}

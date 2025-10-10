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
package org.apache.spark.metrics

import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.util.AccumulatorV2

class TaskStatsAccumulator extends AccumulatorV2[String, String] {
  private var stats: String = ""

  override def isZero: Boolean = stats.isEmpty

  override def copy(): AccumulatorV2[String, String] = {
    val newAcc = new TaskStatsAccumulator()
    newAcc.stats = this.stats
    newAcc
  }

  override def reset(): Unit = {
    stats = ""
  }

  override def add(v: String): Unit = {
    stats = v
  }

  override def merge(other: AccumulatorV2[String, String]): Unit = {
    other match {
      case o: TaskStatsAccumulator =>
        // Overwrite stats. Can be empty if no stats were collected.
        stats = o.stats
      case _ =>
        throw new IllegalArgumentException("Cannot merge with non-BoltTaskStatsAccumulator")
    }
  }

  override def value: String = stats

  override def toInfo(update: Option[Any], value: Option[Any]): AccumulableInfo = {
    // If `update` is None, it means the `toInfo` method was called from stage completion, and we
    // don't send the stats to the stage metrics.
    val v = update.map(_ => stats)
    // `update` field is always empty. `value` field shows the stats of the current task.
    AccumulableInfo(id, name, None, v, internal = false, countFailedValues = false)
  }
}

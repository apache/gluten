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
package org.apache.gluten.delta

import org.apache.spark.TaskContext
import org.apache.spark.sql.execution.metric.SQLMetric

import java.io.ObjectInputStream

/** Metrics updated while an executor materializes an on-disk deletion-vector payload. */
final case class DeletionVectorReadMetrics(
    readTimeNanos: SQLMetric,
    readBytes: SQLMetric,
    readAttempts: SQLMetric) {

  @transient @volatile private var registeredInTask = false

  /**
   * Spark can deserialize an input partition before installing `TaskContext`, so accumulators
   * nested in that partition cannot register from `AccumulatorV2.readObject`. Register them when
   * deferred I/O first runs inside the task instead. The shared metrics object makes this
   * once-per-task even when a partition contains multiple deletion vectors.
   */
  def registerForCurrentTask(): Unit = {
    if (!registeredInTask && TaskContext.get() != null) {
      this.synchronized {
        if (!registeredInTask) {
          registeredInTask = TaskAccumulatorRegistry.registerForCurrentTask(
            readTimeNanos,
            readBytes,
            readAttempts)
        }
      }
    }
  }

  /**
   * `defaultReadObject` deserializes the nested SQL metrics first. Spark's
   * `AccumulatorV2.readObject` registers each one when a task context exists, so mirror that state
   * here to avoid registering them a second time. Without a task context the metrics remain
   * unregistered and `registerForCurrentTask` handles them when materialization begins.
   */
  private def readObject(input: ObjectInputStream): Unit = {
    input.defaultReadObject()
    registeredInTask = TaskContext.get() != null
  }
}

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
package org.apache.spark.util

import org.apache.spark.{SparkConf, TaskContext, TaskContextImpl}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{TaskMemoryManager, UnifiedMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.storage.BlockManagerUtil

import java.util.Properties

import scala.collection.JavaConverters._

object SparkTaskUtil {
  def setTaskContext(taskContext: TaskContext): Unit = {
    TaskContext.setTaskContext(taskContext)
  }

  def unsetTaskContext(): Unit = {
    TaskContext.unset()
  }

  def getTaskMemoryManager(taskContext: TaskContext): TaskMemoryManager = {
    taskContext.taskMemoryManager()
  }

  def createTestTaskContext(properties: Properties): TaskContext = {
    val conf = new SparkConf()
    conf.setAll(properties.asScala)
    val memoryManager = UnifiedMemoryManager(conf, 1)
    BlockManagerUtil.setTestMemoryStore(conf, memoryManager, isDriver = false)
    val stageId = -1.asInstanceOf[Object]
    val stageAttemptNumber = -1.asInstanceOf[Object]
    val partitionId = -1.asInstanceOf[Object]
    val taskAttemptId = -1L.asInstanceOf[Object]
    val attemptNumber = -1.asInstanceOf[Object]
    val numPartitions = -1.asInstanceOf[Object] // Added in Spark 3.4.
    val taskMemoryManager = new TaskMemoryManager(memoryManager, -1L).asInstanceOf[Object]
    val localProperties = properties.asInstanceOf[Object]
    val metricsSystem =
      MetricsSystem.createMetricsSystem("GLUTEN_UNSAFE", conf).asInstanceOf[Object]
    val taskMetrics = TaskMetrics.empty.asInstanceOf[Object]
    val cpus = 1.asInstanceOf[Object] // Added in Spark 3.3.
    val resources = Map.empty.asInstanceOf[Object]

    val ctor = {
      val ctors = classOf[TaskContextImpl].getDeclaredConstructors
      assert(ctors.size == 1)
      ctors.head
    }

    if (SparkVersionUtil.eqSpark33) {
      return ctor
        .newInstance(
          stageId,
          stageAttemptNumber,
          partitionId,
          taskAttemptId,
          attemptNumber,
          taskMemoryManager,
          localProperties,
          metricsSystem,
          taskMetrics,
          cpus,
          resources
        )
        .asInstanceOf[TaskContext]
    }

    // Since Spark 3.4.
    ctor
      .newInstance(
        stageId,
        stageAttemptNumber,
        partitionId,
        taskAttemptId,
        attemptNumber,
        numPartitions,
        taskMemoryManager,
        localProperties,
        metricsSystem,
        taskMetrics,
        cpus,
        resources
      )
      .asInstanceOf[TaskContext]
  }
}

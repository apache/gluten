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
package org.apache.gluten

import org.apache.gluten.component.Component
import org.apache.gluten.config.GlutenCoreConfig
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.extension.GlutenSessionExtensions
import org.apache.gluten.initializer.CodedInputStreamClassInitializer
import org.apache.gluten.task.TaskListener

import org.apache.spark.{SparkConf, SparkContext, TaskFailedReason}
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.internal.SparkConfigUtil._
import org.apache.spark.sql.internal.StaticSQLConf.SPARK_SESSION_EXTENSIONS
import org.apache.spark.task.TaskResources
import org.apache.spark.util.SparkResourceUtil

import java.util
import java.util.Collections

class GlutenPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = {
    new GlutenDriverPlugin()
  }

  override def executorPlugin(): ExecutorPlugin = {
    new GlutenExecutorPlugin()
  }
}

private[gluten] class GlutenDriverPlugin extends DriverPlugin with Logging {
  import GlutenDriverPlugin._

  override def init(sc: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    val conf = pluginContext.conf()
    // Spark SQL extensions
    val extensionSeq = conf.get(SPARK_SESSION_EXTENSIONS).getOrElse(Seq.empty)
    if (!extensionSeq.toSet.contains(GlutenSessionExtensions.GLUTEN_SESSION_EXTENSION_NAME)) {
      conf.set(
        SPARK_SESSION_EXTENSIONS,
        extensionSeq :+ GlutenSessionExtensions.GLUTEN_SESSION_EXTENSION_NAME)
    }

    setPredefinedConfigs(conf)

    val components = Component.sorted()
    printComponentInfo(components)
    components.foreach(_.onDriverStart(sc, pluginContext))
    Collections.emptyMap()
  }

  override def registerMetrics(appId: String, pluginContext: PluginContext): Unit = {
    Component.sorted().foreach(_.registerMetrics(appId, pluginContext))
  }

  override def shutdown(): Unit = {
    Component.sorted().reverse.foreach(_.onDriverShutdown())
  }
}

private object GlutenDriverPlugin extends Logging {
  private def checkOffHeapSettings(conf: SparkConf): Unit = {
    if (conf.get(GlutenCoreConfig.DYNAMIC_OFFHEAP_SIZING_ENABLED)) {
      // When dynamic off-heap sizing is enabled, off-heap mode is not strictly required to be
      // enabled. Skip the check.
      return
    }

    if (conf.get(GlutenCoreConfig.COLUMNAR_MEMORY_UNTRACKED)) {
      // When untracked memory mode is enabled, off-heap mode is not strictly required to be
      // enabled. Skip the check.
      return
    }

    val minOffHeapSize = "1MB"
    if (
      !conf.getBoolean(GlutenCoreConfig.SPARK_OFFHEAP_ENABLED_KEY, defaultValue = false) ||
      conf.getSizeAsBytes(GlutenCoreConfig.SPARK_OFFHEAP_SIZE_KEY, 0) < JavaUtils.byteStringAsBytes(
        minOffHeapSize)
    ) {
      throw new GlutenException(
        s"Must set '${GlutenCoreConfig.SPARK_OFFHEAP_ENABLED_KEY}' to true " +
          s"and set '${GlutenCoreConfig.SPARK_OFFHEAP_SIZE_KEY}' to be greater " +
          s"than $minOffHeapSize")
    }
  }

  private def setPredefinedConfigs(conf: SparkConf): Unit = {
    // check memory off-heap enabled and size.
    checkOffHeapSettings(conf)

    // Get the off-heap size set by user.
    val offHeapSize =
      if (conf.getBoolean(GlutenCoreConfig.DYNAMIC_OFFHEAP_SIZING_ENABLED.key, false)) {
        val onHeapSize: Long =
          if (conf.contains(GlutenCoreConfig.SPARK_ONHEAP_SIZE_KEY)) {
            conf.getSizeAsBytes(GlutenCoreConfig.SPARK_ONHEAP_SIZE_KEY)
          } else {
            // 1GB default
            1024 * 1024 * 1024
          }

        if (conf.contains(GlutenCoreConfig.SPARK_OFFHEAP_ENABLED_KEY)) {
          logWarning(
            s"Dynamic off-heap sizing is enabled. Ignoring user-defined " +
              s"'${GlutenCoreConfig.SPARK_OFFHEAP_SIZE_KEY}' setting.")
        }
        if (conf.contains(GlutenCoreConfig.SPARK_OFFHEAP_SIZE_KEY)) {
          logWarning(
            s"Dynamic off-heap sizing is enabled. Ignoring user-defined " +
              s"'${GlutenCoreConfig.SPARK_OFFHEAP_SIZE_KEY}' setting.")
        }
        conf.set(GlutenCoreConfig.SPARK_OFFHEAP_SIZE_KEY, "0")
        conf.set(GlutenCoreConfig.SPARK_OFFHEAP_ENABLED_KEY, "false")

        ((onHeapSize - (300 * 1024 * 1024)) *
          conf.getDouble(GlutenCoreConfig.DYNAMIC_OFFHEAP_SIZING_MEMORY_FRACTION.key, 0.6d)).toLong
      } else {
        conf.getSizeAsBytes(GlutenCoreConfig.SPARK_OFFHEAP_SIZE_KEY)
      }

    // Set off-heap size in bytes.
    conf.set(GlutenCoreConfig.COLUMNAR_OFFHEAP_SIZE_IN_BYTES, offHeapSize)

    // Set off-heap size in bytes per task.
    val taskSlots = SparkResourceUtil.getTaskSlots(conf)
    conf.set(GlutenCoreConfig.NUM_TASK_SLOTS_PER_EXECUTOR, taskSlots)
    val offHeapPerTask = offHeapSize / taskSlots
    conf.set(GlutenCoreConfig.COLUMNAR_TASK_OFFHEAP_SIZE_IN_BYTES, offHeapPerTask)

    // Pessimistic off-heap sizes, with the assumption that all non-borrowable storage memory
    // determined by spark.memory.storageFraction was used.
    val fraction = 1.0d - conf.getDouble("spark.memory.storageFraction", 0.5d)
    val conservativeOffHeapPerTask = (offHeapSize * fraction).toLong / taskSlots
    conf.set(
      GlutenCoreConfig.COLUMNAR_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES,
      conservativeOffHeapPerTask)
  }

  private def printComponentInfo(components: Seq[Component]): Unit = {
    val loggingInfo = components
      .map {
        c =>
          val infoStr =
            if (c.info().isEmpty) ""
            else "\n" + c.info().map { case (k, v) => s"  $k = $v" }.mkString("\n")
          s"Component ${c.name()}$infoStr"
      }
      .mkString(
        "Gluten components:\n==============================================================\n",
        "\n",
        "\n=============================================================="
      )
    logInfo(loggingInfo)
  }
}

private[gluten] class GlutenExecutorPlugin extends ExecutorPlugin {
  private val taskListeners: Seq[TaskListener] = Seq(TaskResources)

  /** Initialize the executor plugin. */
  override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
    CodedInputStreamClassInitializer.modifyDefaultRecursionLimitUnsafe
    // Initialize Backend.
    Component.sorted().foreach(_.onExecutorStart(ctx))
  }

  /** Clean up and terminate this plugin. For example: close the native engine. */
  override def shutdown(): Unit = {
    Component.sorted().reverse.foreach(_.onExecutorShutdown())
    super.shutdown()
  }

  override def onTaskStart(): Unit = {
    taskListeners.foreach(_.onTaskStart())
  }

  override def onTaskSucceeded(): Unit = {
    taskListeners.reverse.foreach(_.onTaskSucceeded())
  }

  override def onTaskFailed(failureReason: TaskFailedReason): Unit = {
    taskListeners.reverse.foreach(_.onTaskFailed(failureReason))
  }
}

private object GlutenPlugin {}

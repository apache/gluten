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
package org.apache.gluten.memory

import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.memory.GlobalOffHeapMemory

import org.scalatest.funsuite.AnyFunSuite

import java.util.{Collections, Map => JMap}

import scala.util.Try

/**
 * A global off-heap release can run while a SparkContext is being created, e.g. when the Finalizer
 * thread frees a broadcast buffer left by a previous session. In Spark 4.0+ SparkEnv is published
 * before its memory manager is initialized, so such a call must not force the lazy BlockManager
 * memory store, otherwise the BlockManager caches a null memory manager and SparkContext creation
 * fails.
 */
class GlobalOffHeapMemoryEnvInitSuite extends AnyFunSuite {
  import GlobalOffHeapMemoryEnvInitSuite._

  test("global off-heap release during SparkContext init does not break the BlockManager") {
    pluginCallResult = None
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("GlobalOffHeapMemoryEnvInitSuite")
      .set("spark.plugins", classOf[ReleaseOnInitPlugin].getName)
      .set("spark.ui.enabled", "false")
    val sc = new SparkContext(conf)
    try {
      assert(pluginCallResult.isDefined, "plugin was not initialized")
      assert(pluginCallResult.get.isSuccess, s"release during init failed: ${pluginCallResult.get}")
      assert(SparkEnv.get.memoryManager != null)
      assert(sc.parallelize(1 to 10, 2).sum() == 55)
    } finally {
      sc.stop()
    }
  }
}

object GlobalOffHeapMemoryEnvInitSuite {
  @volatile var pluginCallResult: Option[Try[Unit]] = None

  class ReleaseOnInitPlugin extends SparkPlugin {
    override def driverPlugin(): DriverPlugin = new DriverPlugin {
      override def init(sc: SparkContext, ctx: PluginContext): JMap[String, String] = {
        // Runs after SparkEnv.set(...) and, on Spark 4.0+, before initializeMemoryManager(...).
        // Failures are swallowed, as they would be on an unrelated thread.
        pluginCallResult = Some(Try(GlobalOffHeapMemory.release(0)))
        Collections.emptyMap()
      }
    }

    override def executorPlugin(): ExecutorPlugin = null
  }
}

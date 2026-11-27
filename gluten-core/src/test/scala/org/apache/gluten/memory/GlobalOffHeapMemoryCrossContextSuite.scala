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

import org.apache.gluten.config.GlutenCoreConfig

import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.memory.{GlobalOffHeapMemory, GlobalOffHeapMemoryTestHelper, MemoryMode}
import org.apache.spark.memory.MemoryMode.{OFF_HEAP, ON_HEAP}
import org.apache.spark.sql.internal.SQLConf

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import java.util.{Collections, Map => JMap}

import scala.util.Try

/**
 * Global off-heap memory can be released after the SparkContext that lent it has stopped, e.g. by
 * the Finalizer thread. Such a release must not be charged to a later SparkContext.
 */
class GlobalOffHeapMemoryCrossContextSuite extends AnyFunSuite with BeforeAndAfterEach {
  import GlobalOffHeapMemoryCrossContextSuite._

  private val block = 8L * 1024 * 1024

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    GlobalOffHeapMemoryTestHelper.resetLedger()
    onPluginInit = () => ()
    pluginCallResult = None
  }

  private def withContext[T](conf: SparkConf = baseConf)(body: SparkContext => T): T = {
    val sc = new SparkContext(conf)
    try {
      body(sc)
    } finally {
      sc.stop()
    }
  }

  private def withPlugin[T](body: SparkContext => T): T =
    withContext(baseConf.clone().set("spark.plugins", classOf[InitPlugin].getName))(body)

  private def storageUsed(sc: SparkContext, mode: MemoryMode): Long =
    GlobalOffHeapMemoryTestHelper.storageUsed(sc, mode)

  private def withDynamicSizing[T](body: => T): T = {
    val conf = new SQLConf
    conf.setConfString(GlutenCoreConfig.DYNAMIC_OFFHEAP_SIZING_ENABLED.key, "true")
    SQLConf.withExistingConf(conf)(body)
  }

  test("release of memory lent by a stopped context does not free the next context's memory") {
    withContext() {
      a =>
        GlobalOffHeapMemory.acquire(block)
        assert(storageUsed(a, OFF_HEAP) == block)
    }
    withContext() {
      b =>
        GlobalOffHeapMemory.acquire(block)
        assert(storageUsed(b, OFF_HEAP) == block)
        // Settles the loan of the stopped context first.
        GlobalOffHeapMemory.release(block)
        assert(storageUsed(b, OFF_HEAP) == block)
        GlobalOffHeapMemory.release(block)
        assert(storageUsed(b, OFF_HEAP) == 0)
    }
  }

  test("release of memory lent by a stopped context during the next context's startup") {
    val baseline = GlobalOffHeapMemory.currentBytes()
    withContext()(_ => GlobalOffHeapMemory.acquire(block))

    onPluginInit = () => GlobalOffHeapMemory.release(block)
    withPlugin {
      b =>
        assert(pluginCallResult.isDefined, "plugin was not initialized")
        assert(
          pluginCallResult.get.isSuccess,
          s"release during init failed: ${pluginCallResult.get}")
        assert(SparkEnv.get.memoryManager != null)
        assert(b.parallelize(1 to 10, 2).sum() == 55)
        assert(GlobalOffHeapMemory.currentBytes() == baseline)
        assert(storageUsed(b, OFF_HEAP) == 0)
    }
  }

  test("release of memory granted during startup does not free the context's own memory") {
    // Booked on Spark 3.5; granted unbooked on Spark 4.0+, before the memory manager is ready.
    onPluginInit = () => GlobalOffHeapMemory.acquire(block)
    withPlugin {
      b =>
        assert(pluginCallResult.exists(_.isSuccess), s"plugin init failed: $pluginCallResult")
        GlobalOffHeapMemory.acquire(block)
        // Settles the memory granted during startup first.
        GlobalOffHeapMemory.release(block)
        assert(storageUsed(b, OFF_HEAP) == block)
        GlobalOffHeapMemory.release(block)
        assert(storageUsed(b, OFF_HEAP) == 0)
    }
  }

  test("release goes to the memory mode it was borrowed in") {
    withContext() {
      a =>
        withDynamicSizing(GlobalOffHeapMemory.acquire(block))
        assert(storageUsed(a, ON_HEAP) == block)
    }
    withContext() {
      b =>
        GlobalOffHeapMemory.acquire(block)
        val onHeapUsed = storageUsed(b, ON_HEAP)
        assert(storageUsed(b, OFF_HEAP) == block)
        // Settles the ON_HEAP loan of the stopped context: neither of B's pools changes.
        GlobalOffHeapMemory.release(block)
        assert(storageUsed(b, ON_HEAP) == onHeapUsed)
        assert(storageUsed(b, OFF_HEAP) == block)
        GlobalOffHeapMemory.release(block)
        assert(storageUsed(b, OFF_HEAP) == 0)

        // Borrowed ON_HEAP, released while the mode reads OFF_HEAP.
        withDynamicSizing(GlobalOffHeapMemory.acquire(block))
        assert(storageUsed(b, ON_HEAP) == onHeapUsed + block)
        GlobalOffHeapMemory.release(block)
        assert(storageUsed(b, ON_HEAP) == onHeapUsed)
        assert(storageUsed(b, OFF_HEAP) == 0)
    }
  }
}

object GlobalOffHeapMemoryCrossContextSuite {
  private val baseConf = new SparkConf()
    .setMaster("local[1]")
    .setAppName("GlobalOffHeapMemoryCrossContextSuite")
    .set("spark.ui.enabled", "false")
    .set("spark.memory.offHeap.enabled", "true")
    .set("spark.memory.offHeap.size", "64m")

  @volatile var onPluginInit: () => Unit = () => ()
  @volatile var pluginCallResult: Option[Try[Unit]] = None

  class InitPlugin extends SparkPlugin {
    override def driverPlugin(): DriverPlugin = new DriverPlugin {
      override def init(sc: SparkContext, ctx: PluginContext): JMap[String, String] = {
        // Runs after SparkEnv.set(...) and, on Spark 4.0+, before initializeMemoryManager(...).
        pluginCallResult = Some(Try(onPluginInit()))
        Collections.emptyMap()
      }
    }

    override def executorPlugin(): ExecutorPlugin = null
  }
}

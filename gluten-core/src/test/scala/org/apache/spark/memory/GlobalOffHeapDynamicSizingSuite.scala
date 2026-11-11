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
package org.apache.spark.memory

import org.apache.gluten.config.GlutenCoreConfig
import org.apache.gluten.memory.memtarget.{DynamicOffHeapSizingMemoryTarget, MemoryTargets}

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.storage.BlockManager
import org.apache.spark.storage.memory.MemoryStore

import org.mockito.Mockito.{mock, when}
import org.scalatest.funsuite.AnyFunSuite

/**
 * Covers how the global (non-task) off-heap reservations pick their backing memory pool when
 * dynamic off-heap sizing is on.
 *
 * The suite deliberately runs *without* an active SQL session, because that is the situation of the
 * thread that builds the global memory target in production: the Gluten plugin initializes it from
 * the driver/executor plugin callback, long before any SparkSession is attached to the thread. A
 * conf lookup that falls back to SQLConf defaults there would silently read "disabled" and leave
 * the global target half-wired - charging the on-heap pools from one place while the dynamic-sizing
 * ledger is bypassed in another.
 */
class GlobalOffHeapDynamicSizingSuite extends AnyFunSuite {

  test("global target joins the dynamic ledger with no active SQL session") {
    withGlobalMemory(dynamicSizing = true) {
      memoryManager =>
        val target = MemoryTargets.global()
        assert(target.isInstanceOf[DynamicOffHeapSizingMemoryTarget])
        val ledgerBefore = DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting()
        assume(canBorrowFromHeapBudget(60L), "JVM heap is already at its maximum size")
        assert(target.borrow(60) == 60)
        try {
          assert(target.usedBytes() == 60)
          // Dynamic sizing reserves from the ON-heap pools ...
          assert(memoryManager.onHeapStorageMemoryUsed == 60)
          assert(memoryManager.offHeapStorageMemoryUsed == 0)
          // ... and the bytes are visible to the task-level targets through the shared ledger.
          assert(
            DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting() == ledgerBefore + 60)
          assert(SparkMemoryUtil.getCurrentAvailableOffHeapMemory == 40)
          assert(target.repay(20) == 20)
          assert(target.usedBytes() == 40)
          assert(memoryManager.onHeapStorageMemoryUsed == 40)
          assert(
            DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting() == ledgerBefore + 40)
          assert(SparkMemoryUtil.getCurrentAvailableOffHeapMemory == 60)
        } finally {
          target.repay(target.usedBytes())
        }
        assert(memoryManager.onHeapStorageMemoryUsed == 0)
        assert(DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting() == ledgerBefore)
    }
  }

  test("global target stays on the off-heap pools when dynamic sizing is disabled") {
    withGlobalMemory(dynamicSizing = false) {
      memoryManager =>
        val target = MemoryTargets.global()
        assert(target.isInstanceOf[GlobalOffHeapMemoryTarget])
        val ledgerBefore = DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting()
        assert(target.borrow(60) == 60)
        try {
          assert(memoryManager.offHeapStorageMemoryUsed == 60)
          assert(memoryManager.onHeapStorageMemoryUsed == 0)
          // No dynamic wrapper, hence nothing is booked in the shared ledger.
          assert(DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting() == ledgerBefore)
          assert(SparkMemoryUtil.getCurrentAvailableOffHeapMemory == 40)
        } finally {
          target.repay(60)
        }
    }
  }

  test("a rejected reservation leaves neither the pools nor the ledger changed") {
    withGlobalMemory(dynamicSizing = true) {
      memoryManager =>
        val target = MemoryTargets.global().asInstanceOf[DynamicOffHeapSizingMemoryTarget]
        val ledgerBefore = DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting()
        // 101 bytes exceed the 100-byte on-heap budget configured below.
        assert(target.borrow(101) == 0)
        assert(target.usedBytes() == 0)
        assert(target.target().usedBytes() == 0)
        assert(memoryManager.onHeapStorageMemoryUsed == 0)
        assert(DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting() == ledgerBefore)
    }
  }

  test("the cluster-wide SparkConf wins over an unrelated active SQLConf") {
    // The flag is a static (cluster-wide) conf: a session-local SQLConf must never be able to
    // flip it, otherwise the wrapper decision and the pool choice could disagree.
    withGlobalMemory(dynamicSizing = true) {
      _ =>
        val sqlConf = new SQLConf
        sqlConf.setConfString(GlutenCoreConfig.DYNAMIC_OFFHEAP_SIZING_ENABLED.key, "false")
        SQLConf.withExistingConf(sqlConf) {
          assert(GlutenCoreConfig.isDynamicOffHeapSizingEnabled)
          assert(MemoryTargets.global().isInstanceOf[DynamicOffHeapSizingMemoryTarget])
        }
    }
  }

  /**
   * DynamicOffHeapSizingMemoryTarget sizes its budget against the real JVM heap, so a fixed-size
   * heap (-Xms == -Xmx) would make any reservation fail for reasons unrelated to this suite.
   */
  private def canBorrowFromHeapBudget(size: Long): Boolean = {
    val runtime = Runtime.getRuntime
    runtime.totalMemory() + size < runtime.maxMemory()
  }

  /**
   * Installs a SparkEnv whose SparkConf carries the dynamic-sizing flag, mirroring an executor that
   * was launched with (or without) the cluster-wide option. No SQLConf is put in scope on purpose -
   * see the class-level comment.
   */
  private def withGlobalMemory(dynamicSizing: Boolean)(
      body: UnifiedMemoryManager => Unit): Unit = {
    val previousEnv = SparkEnv.get
    val sparkConf = new SparkConf(false)
      .set(GlutenCoreConfig.DYNAMIC_OFFHEAP_SIZING_ENABLED.key, dynamicSizing.toString)
      .set("spark.testing", "true")
      .set("spark.testing.memory", "100")
      .set("spark.testing.reservedMemory", "0")
      .set("spark.memory.fraction", "1.0")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "100")
    val memoryManager = UnifiedMemoryManager(sparkConf, 1)
    val memoryStore = mock(classOf[MemoryStore])
    memoryManager.setMemoryStore(memoryStore)
    val blockManager = mock(classOf[BlockManager])
    when(blockManager.memoryStore).thenReturn(memoryStore)
    val env = mock(classOf[SparkEnv])
    when(env.conf).thenReturn(sparkConf)
    when(env.memoryManager).thenReturn(memoryManager)
    when(env.blockManager).thenReturn(blockManager)
    SparkEnv.set(env)
    try {
      body(memoryManager)
    } finally {
      SparkEnv.set(previousEnv)
    }
  }
}

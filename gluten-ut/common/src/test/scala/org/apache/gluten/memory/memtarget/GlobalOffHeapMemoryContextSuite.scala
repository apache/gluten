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
package org.apache.gluten.memory.memtarget

import org.apache.gluten.config.GlutenCoreConfig

import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.memory.{GlobalOffHeapMemory, SparkMemoryUtil}
import org.apache.spark.sql.internal.SQLConf

import org.scalatest.funsuite.AnyFunSuite

class GlobalOffHeapMemoryContextSuite extends AnyFunSuite {
  private val onHeapSize = 100L
  private val offHeapSize = 64L * 1024 * 1024 * 1024

  Seq(false, true).foreach {
    firstDynamicSizing =>
      test(
        s"global target follows SparkContexts: $firstDynamicSizing then ${!firstDynamicSizing}") {
        assumeHeapBudget()
        val target = withSparkContext(firstDynamicSizing) {
          val target = MemoryTargets.throwOnOom(MemoryTargets.global())
          checkReservations(target, firstDynamicSizing)
          target
        }
        withSparkContext(!firstDynamicSizing) {
          checkReservations(target, !firstDynamicSizing)
        }
      }

      test(s"global reservations follow SparkEnv rather than SQLConf: $firstDynamicSizing") {
        assumeHeapBudget()
        withSparkContext(firstDynamicSizing) {
          val sqlConf = new SQLConf
          sqlConf.setConfString(
            GlutenCoreConfig.DYNAMIC_OFFHEAP_SIZING_ENABLED.key,
            (!firstDynamicSizing).toString)
          SQLConf.withExistingConf(sqlConf) {
            assert(GlutenCoreConfig.isDynamicOffHeapSizingEnabled == firstDynamicSizing)
            checkReservations(MemoryTargets.global(), firstDynamicSizing)
          }
        }
      }
  }

  test("global singleton follows SparkContexts: false, true, false") {
    assumeHeapBudget()
    val target = withSparkContext(dynamicSizing = false) {
      checkReservations(GlobalOffHeapMemory.target, dynamicSizing = false)
      GlobalOffHeapMemory.target
    }
    Seq(true, false).foreach {
      dynamicSizing =>
        withSparkContext(dynamicSizing) {
          assert(GlobalOffHeapMemory.target eq target)
          checkReservations(target, dynamicSizing)
        }
    }
  }

  test("a rejected global reservation changes neither the Spark pools nor the shared ledger") {
    assumeHeapBudget()
    withSparkContext(dynamicSizing = true) {
      val target = MemoryTargets.global()
      val ledgerBefore = DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting()
      try {
        assert(target.borrow(onHeapSize + 1) == 0)
        assert(target.usedBytes() == 0)
        assert(SparkEnv.get.memoryManager.onHeapStorageMemoryUsed == 0)
        assert(SparkEnv.get.memoryManager.offHeapStorageMemoryUsed == 0)
        assert(SparkMemoryUtil.getCurrentAvailableOffHeapMemory == onHeapSize)
        assert(DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting() == ledgerBefore)
        checkReservations(target, dynamicSizing = true)
      } finally {
        target.repay(target.usedBytes())
      }
    }
  }

  private def assumeHeapBudget(): Unit = {
    val runtime = Runtime.getRuntime
    assume(runtime.totalMemory() + onHeapSize + 1 < runtime.maxMemory())
    assume(runtime.maxMemory() <= offHeapSize)
  }

  private def checkReservations(target: MemoryTarget, dynamicSizing: Boolean): Unit = {
    val memoryManager = SparkEnv.get.memoryManager
    val ledgerBefore = DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting()
    val globalBefore = target.usedBytes()
    val onHeapBefore = memoryManager.onHeapStorageMemoryUsed
    val offHeapBefore = memoryManager.offHeapStorageMemoryUsed
    val availableBefore = if (dynamicSizing) onHeapSize else offHeapSize
    val size = if (dynamicSizing) 60L else Runtime.getRuntime.maxMemory()
    val bookedSize = if (dynamicSizing) size else 0L
    var outstanding = 0L
    assert(SparkMemoryUtil.getCurrentAvailableOffHeapMemory == availableBefore)
    try {
      outstanding = target.borrow(size)
      assert(outstanding == size)
      assert(target.usedBytes() == globalBefore + size)
      assert(memoryManager.onHeapStorageMemoryUsed == onHeapBefore + bookedSize)
      assert(memoryManager.offHeapStorageMemoryUsed == offHeapBefore + size - bookedSize)
      assert(
        DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting() == ledgerBefore + bookedSize)
      assert(SparkMemoryUtil.getCurrentAvailableOffHeapMemory == availableBefore - outstanding)

      val freed = target.repay(size / 2)
      outstanding -= freed
      assert(freed == size / 2)
      assert(target.usedBytes() == globalBefore + outstanding)
      assert(
        memoryManager.onHeapStorageMemoryUsed ==
          onHeapBefore + (if (dynamicSizing) outstanding else 0L))
      assert(
        memoryManager.offHeapStorageMemoryUsed ==
          offHeapBefore + (if (dynamicSizing) 0L else outstanding))
      assert(SparkMemoryUtil.getCurrentAvailableOffHeapMemory == availableBefore - outstanding)
      assert(
        DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting() ==
          ledgerBefore + (if (dynamicSizing) outstanding else 0))
    } finally {
      assert(target.repay(outstanding) == outstanding)
    }
    assert(target.usedBytes() == globalBefore)
    assert(memoryManager.onHeapStorageMemoryUsed == onHeapBefore)
    assert(memoryManager.offHeapStorageMemoryUsed == offHeapBefore)
    assert(DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting() == ledgerBefore)
    assert(SparkMemoryUtil.getCurrentAvailableOffHeapMemory == availableBefore)
  }

  private def withSparkContext[Result](dynamicSizing: Boolean)(body: => Result): Result = {
    val conf = new SparkConf(false)
      .setMaster("local[1]")
      .setAppName(getClass.getSimpleName)
      .set("spark.ui.enabled", "false")
      .set("spark.driver.host", "127.0.0.1")
      .set("spark.driver.bindAddress", "127.0.0.1")
      .set("spark.plugins", "")
      .set("spark.testing", "true")
      .set("spark.testing.memory", onHeapSize.toString)
      .set("spark.testing.reservedMemory", "0")
      .set("spark.memory.fraction", "1.0")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", offHeapSize.toString)
      .set(GlutenCoreConfig.DYNAMIC_OFFHEAP_SIZING_ENABLED.key, dynamicSizing.toString)
    val context = new SparkContext(conf)
    try {
      body
    } finally {
      context.stop()
    }
  }
}

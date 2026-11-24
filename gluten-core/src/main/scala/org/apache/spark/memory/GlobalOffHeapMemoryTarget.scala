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
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.memory.{MemoryUsageRecorder, SimpleMemoryUsageRecorder}
import org.apache.gluten.memory.memtarget.{KnownNameAndStats, MemoryTarget, MemoryTargetUtil, MemoryTargetVisitor}
import org.apache.gluten.proto.MemoryUsageStats

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId

import java.lang.ref.WeakReference
import java.lang.reflect.Field
import java.util.{ArrayDeque, UUID}

import scala.collection.mutable.ArrayBuffer

/**
 * Books global off-heap memory as storage memory of the current SparkEnv's memory manager.
 *
 * Memory can be repaid long after it was borrowed, e.g. by a finalizer after the SparkContext that
 * lent it has stopped and a new one has started. So each borrow is recorded as a loan of the
 * SparkEnv that lent it, and a repay only gives memory back to that SparkEnv while it is still
 * running. Memory granted without booking, e.g. while a SparkEnv starts up, is recorded as a loan
 * without a lender. Loans are settled oldest first.
 *
 * Settling oldest first means a loan that is never repaid, e.g. capacity the native global pool
 * keeps after its SparkEnv stopped, absorbs up to its size of later repays: that much stays booked
 * in the later SparkEnv until it stops. Spark then under-uses memory rather than over-commits it.
 */
class GlobalOffHeapMemoryTarget private[memory]
  extends MemoryTarget
  with KnownNameAndStats
  with Logging {
  import GlobalOffHeapMemoryTarget._

  private val targetName = MemoryTargetUtil.toUniqueName("GlobalOffHeap")
  private val recorder: MemoryUsageRecorder = new SimpleMemoryUsageRecorder()
  // Oldest first. Spark's MemoryManager is never called while holding this lock: its methods are
  // synchronized and may lead to a repay on another thread, so taking the locks in both orders
  // could deadlock.
  private val loans = new ArrayDeque[Loan]()

  private def mode: MemoryMode = {
    if (GlutenCoreConfig.get.dynamicOffHeapSizingEnabled) MemoryMode.ON_HEAP
    else MemoryMode.OFF_HEAP
  }

  private val FIELD_MEMORY_MANAGER: Field = {
    val f =
      try {
        classOf[TaskMemoryManager].getDeclaredField("memoryManager")
      } catch {
        case e: Exception =>
          throw new GlutenException(
            "Unable to find field TaskMemoryManager#memoryManager via reflection",
            e)
      }
    f.setAccessible(true)
    f
  }

  override def borrow(size: Long): Long = {
    val m = mode
    val env = SparkEnv.get
    val tc = TaskContext.get()
    if (env == null && tc != null) {
      // Test code that mocks the task context without booting up SparkEnv. Kept out of the ledger.
      val mm = taskMemoryManager(tc)
      if (!acquire(mm, m, size)) {
        return 0
      }
    } else {
      readyMemoryManager(env) match {
        case Some(mm) =>
          if (!acquire(mm, m, size)) {
            return 0
          }
          record(env, m, size)
        case None =>
          record(null, m, size)
      }
    }
    recorder.inc(size)
    size
  }

  override def repay(size: Long): Long = {
    val tc = TaskContext.get()
    if (SparkEnv.get == null && tc != null) {
      taskMemoryManager(tc).releaseStorageMemory(size, mode)
    } else {
      // A lender stopping after settle() only logs a warning on its discarded memory manager.
      settle(size).foreach { case (mm, m, bytes) => mm.releaseStorageMemory(bytes, m) }
    }
    recorder.inc(-size)
    size
  }

  private def acquire(mm: MemoryManager, m: MemoryMode, size: Long): Boolean = {
    val acquired = mm.acquireStorageMemory(BlockId(s"test_${UUID.randomUUID()}"), size, m)
    if (!acquired) {
      logOom(mm, m, size)
    }
    acquired
  }

  /**
   * Records a loan of `lender`, or one granted without booking if `lender` is null. The mode is
   * read per borrow, so borrows of one SparkEnv can alternate between modes: each switch adds a
   * loan, and repays follow the order of borrows rather than the mode.
   */
  private def record(lender: SparkEnv, m: MemoryMode, size: Long): Unit = {
    if (size > 0) {
      loans.synchronized {
        val last = loans.peekLast()
        if (last != null && last.isLentBy(lender) && last.mode == m) {
          last.bytes += size
        } else {
          loans.addLast(new Loan(lender, m, size))
        }
      }
    }
  }

  /**
   * Takes `size` bytes off the loans, oldest first, and returns what goes back to running lenders.
   * Loans of a stopped SparkEnv, whose memory manager is discarded, and loans without a lender are
   * only written off.
   */
  private def settle(size: Long): Seq[(MemoryManager, MemoryMode, Long)] = loans.synchronized {
    val current = SparkEnv.get
    val releases = ArrayBuffer[(MemoryManager, MemoryMode, Long)]()
    var remaining = size
    while (remaining > 0 && !loans.isEmpty) {
      val loan = loans.peekFirst()
      val bytes = math.min(remaining, loan.bytes)
      loan.bytes -= bytes
      remaining -= bytes
      if (loan.bytes == 0) {
        loans.pollFirst()
      }
      loan.runningLender(current).foreach(mm => releases += ((mm, loan.mode, bytes)))
    }
    releases.toSeq
  }

  /** Forgets all loans, so tests do not depend on what earlier tests left unpaid. */
  private[memory] def resetForTesting(): Unit = loans.synchronized {
    loans.clear()
  }

  override def usedBytes(): Long = recorder.current()

  override def accept[T](visitor: MemoryTargetVisitor[T]): T = visitor.visit(this)

  /** The memory manager of `env` if it can lend now; the memory is granted unbooked otherwise. */
  private def readyMemoryManager(env: SparkEnv): Option[MemoryManager] = {
    if (env == null) {
      logWarning(
        "Memory manager not found because the code is unlikely be run in a Spark application")
      return None
    }
    if (env.isStopped) {
      logDebug("SparkEnv is stopped, skipping global off-heap accounting")
      return None
    }
    val mm = env.memoryManager
    if (mm == null) {
      // Spark 4.0+ publishes SparkEnv before it initializes the memory manager, and
      // BlockManager#memoryManager is a lazy val, so forcing the memory store now would cache
      // a null memory manager and fail the SparkContext in BlockManager#initialize.
      logWarning(
        "SparkEnv memory manager is not initialized yet, skipping global off-heap accounting")
      return None
    }
    // SPARK-46947: https://github.com/apache/spark/pull/45052.
    ensureMemoryStoreInitialized(env)
    Some(mm)
  }

  private def taskMemoryManager(tc: TaskContext): MemoryManager =
    FIELD_MEMORY_MANAGER.get(tc.taskMemoryManager()).asInstanceOf[MemoryManager]

  private def ensureMemoryStoreInitialized(env: SparkEnv): Unit = {
    assert(env.blockManager.memoryStore != null)
  }

  private def logOom(mm: MemoryManager, m: MemoryMode, size: Long): Unit = {
    if (m == MemoryMode.ON_HEAP) {
      val storageUsed = mm.onHeapStorageMemoryUsed
      val executionUsed = mm.onHeapExecutionMemoryUsed
      val onHeapMemoryTotal = storageUsed + executionUsed
      logError(
        s"Spark on-heap memory is exhausted. " +
          s"Requested: $size, " +
          s"Storage: $storageUsed / $onHeapMemoryTotal, " +
          s"Execution: $executionUsed / $onHeapMemoryTotal")
      return
    }
    val storageUsed = mm.offHeapStorageMemoryUsed
    val executionUsed = mm.offHeapExecutionMemoryUsed
    val offHeapMemoryTotal = storageUsed + executionUsed
    logError(
      s"Spark off-heap memory is exhausted." +
        s" Storage: $storageUsed / $offHeapMemoryTotal," +
        s" execution: $executionUsed / $offHeapMemoryTotal")
  }

  override def name(): String = targetName

  override def stats(): MemoryUsageStats = recorder.toStats
}

private object GlobalOffHeapMemoryTarget {

  /**
   * Storage memory booked in the memory manager of `lender`, or granted unbooked if `lender` is
   * null. Guarded by the target's ledger lock.
   */
  private class Loan(lender: SparkEnv, val mode: MemoryMode, var bytes: Long) {
    // Weak, so a leaked buffer does not keep a stopped SparkEnv alive.
    private val lenderRef = new WeakReference(lender)

    def isLentBy(env: SparkEnv): Boolean = lenderRef.get() eq env

    /** The memory manager to give bytes back to, if the lender is still the running SparkEnv. */
    def runningLender(current: SparkEnv): Option[MemoryManager] = {
      val env = lenderRef.get()
      if (env == null || env.isStopped || (env ne current)) None else Option(env.memoryManager)
    }
  }
}

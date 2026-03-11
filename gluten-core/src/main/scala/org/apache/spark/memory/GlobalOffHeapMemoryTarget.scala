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

import java.lang.reflect.Field
import java.util.UUID

class GlobalOffHeapMemoryTarget private[memory]
  extends MemoryTarget
  with KnownNameAndStats
  with Logging {
  private val targetName = MemoryTargetUtil.toUniqueName("GlobalOffHeap")
  private val recorder: MemoryUsageRecorder = new SimpleMemoryUsageRecorder()
  private val mode: MemoryMode = {
    val enabled = Option(SparkEnv.get)
      .map(_.conf.getBoolean(GlutenCoreConfig.DYNAMIC_OFFHEAP_SIZING_ENABLED.key, false))
      .getOrElse(false)
    if (enabled) MemoryMode.ON_HEAP
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
    memoryManagerOption()
      .map {
        mm =>
          val succeeded =
            mm.acquireStorageMemory(BlockId(s"test_${UUID.randomUUID()}"), size, mode)

          if (succeeded) {
            recorder.inc(size)
            size
          } else {
            // OOM.
            // Throw OOM.
            if (mode == MemoryMode.ON_HEAP) {
              val storageUsed = mm.onHeapStorageMemoryUsed
              val executionUsed = mm.onHeapExecutionMemoryUsed
              val onHeapMemoryTotal = storageUsed + executionUsed
              logError(
                s"Spark on-heap memory is exhausted. " +
                  s"Requested: $size, " +
                  s"Storage: $storageUsed / $onHeapMemoryTotal, " +
                  s"Execution: $executionUsed / $onHeapMemoryTotal")
              return 0;
            }
            val storageUsed = mm.offHeapStorageMemoryUsed
            val executionUsed = mm.offHeapExecutionMemoryUsed
            val offHeapMemoryTotal = storageUsed + executionUsed
            logError(
              s"Spark off-heap memory is exhausted." +
                s" Storage: $storageUsed / $offHeapMemoryTotal," +
                s" execution: $executionUsed / $offHeapMemoryTotal")
            return 0;
          }
      }
      .getOrElse(size)
  }

  override def repay(size: Long): Long = {
    memoryManagerOption()
      .map {
        mm =>
          mm.releaseStorageMemory(size, mode)
          recorder.inc(-size)
          size
      }
      .getOrElse(size)
  }

  override def usedBytes(): Long = recorder.current()

  override def accept[T](visitor: MemoryTargetVisitor[T]): T = visitor.visit(this)

  private[memory] def memoryManagerOption(): Option[MemoryManager] = {
    val env = SparkEnv.get
    if (env != null) {
      // SPARK-46947: https://github.com/apache/spark/pull/45052.
      ensureMemoryStoreInitialized(env)
      return Some(env.memoryManager)
    }
    val tc = TaskContext.get()
    if (tc != null) {
      // This may happen in test code that mocks the task context without booting up SparkEnv.
      return Some(FIELD_MEMORY_MANAGER.get(tc.taskMemoryManager()).asInstanceOf[MemoryManager])
    }
    logWarning(
      "Memory manager not found because the code is unlikely be run in a Spark application")
    None
  }

  private def ensureMemoryStoreInitialized(env: SparkEnv): Unit = {
    assert(env.blockManager.memoryStore != null)
  }

  override def name(): String = targetName

  override def stats(): MemoryUsageStats = recorder.toStats
}

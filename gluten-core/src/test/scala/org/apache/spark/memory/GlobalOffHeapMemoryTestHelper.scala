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

import org.apache.gluten.memory.memtarget.ThrowOnOomMemoryTarget

import org.apache.spark.SparkContext

/** Exposes private[spark] memory state to Gluten test suites. */
object GlobalOffHeapMemoryTestHelper {

  /** Forgets the loans of the global off-heap memory target. */
  def resetLedger(): Unit = GlobalOffHeapMemory.target match {
    case t: ThrowOnOomMemoryTarget =>
      t.target().asInstanceOf[GlobalOffHeapMemoryTarget].resetForTesting()
    case _ =>
  }

  def storageUsed(sc: SparkContext, mode: MemoryMode): Long = mode match {
    case MemoryMode.ON_HEAP => sc.env.memoryManager.onHeapStorageMemoryUsed
    case MemoryMode.OFF_HEAP => sc.env.memoryManager.offHeapStorageMemoryUsed
  }
}

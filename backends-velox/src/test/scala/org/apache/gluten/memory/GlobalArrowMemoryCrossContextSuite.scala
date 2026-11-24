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

import org.apache.gluten.memory.arrow.alloc.{ArrowBufferAllocators, ManagedAllocationListener}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.memory.VeloxGlobalOffHeapMemoryTestHelper

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

class GlobalArrowMemoryCrossContextSuite extends AnyFunSuite with BeforeAndAfterEach {

  private val conf = new SparkConf()
    .setMaster("local[1]")
    .setAppName("GlobalArrowMemoryCrossContextSuite")
    .set("spark.ui.enabled", "false")
    .set("spark.memory.offHeap.enabled", "true")
    .set("spark.memory.offHeap.size", "128m")

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    VeloxGlobalOffHeapMemoryTestHelper.resetLedger()
  }

  test(
    "closing a global Arrow buffer of a stopped context does not free the next context's memory") {
    val allocator = ArrowBufferAllocators.globalInstance()
    // Whole blocks, so each allocation borrows exactly its own size whatever is already reserved.
    val block = ManagedAllocationListener.BLOCK_SIZE

    val a = new SparkContext(conf)
    val bufferOfA =
      try {
        allocator.buffer(2 * block)
      } finally {
        a.stop()
      }
    var bufferOfAOpen = true
    val b = new SparkContext(conf)
    try {
      val used = VeloxGlobalOffHeapMemoryTestHelper.offHeapStorageUsed(b)
      val bufferOfB = allocator.buffer(block)
      try {
        assert(VeloxGlobalOffHeapMemoryTestHelper.offHeapStorageUsed(b) == used + block)
        bufferOfA.close()
        bufferOfAOpen = false
        assert(VeloxGlobalOffHeapMemoryTestHelper.offHeapStorageUsed(b) == used + block)
      } finally {
        bufferOfB.close()
      }
      assert(VeloxGlobalOffHeapMemoryTestHelper.offHeapStorageUsed(b) == used)
    } finally {
      if (bufferOfAOpen) {
        bufferOfA.close()
      }
      b.stop()
    }
  }
}

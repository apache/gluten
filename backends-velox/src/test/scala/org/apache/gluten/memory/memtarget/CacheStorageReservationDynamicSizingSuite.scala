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

import org.apache.gluten.memory.CacheStorageReservation

import org.apache.spark.sql.internal.SQLConf

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * With dynamic off-heap sizing there is no fixed native budget: allocations are admitted against
 * the committed heap plus what Gluten has already handed out, weighed against the process maximum.
 * Only allocations passing through [[DynamicOffHeapSizingMemoryTarget]] move that counter, so a
 * reservation made around it is invisible to the check -- and queries then believe there is room
 * the cache has already taken.
 *
 * Lives in this package to reach the counter, which is otherwise not observable.
 *
 * The reservation memoizes its target on first use, so the flag is set before any reservation call
 * rather than per test.
 */
class CacheStorageReservationDynamicSizingSuite extends AnyFunSuite with BeforeAndAfterAll {

  private val flag = "spark.gluten.memory.dynamic.offHeap.sizing.enabled"

  override protected def beforeAll(): Unit = {
    SQLConf.get.setConfString(flag, "true")
    DynamicOffHeapSizingMemoryTarget.resetUsedOffHeapBytesForTesting()
  }

  override protected def afterAll(): Unit = {
    DynamicOffHeapSizingMemoryTarget.resetUsedOffHeapBytesForTesting()
    SQLConf.get.unsetConf(flag)
  }

  test("memory reserved for the cache is weighed against the process budget") {
    val before = DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting()

    assert(CacheStorageReservation.borrow(4096) === 4096)
    assert(
      DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting() === before + 4096,
      "the cache must count towards what queries are admitted against")

    assert(CacheStorageReservation.repay(4096) === 4096)
    assert(DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting() === before)
  }
}

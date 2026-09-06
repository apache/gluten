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

import org.apache.gluten.memory.memtarget.{MemoryTarget, MemoryTargets}

/**
 * Reserves executor-wide memory from Spark's storage pool on behalf of the Velox cache.
 *
 * Deliberately built on the bare target rather than [[GlobalOffHeapMemory]], whose `throwOnOom`
 * wrapper turns a refusal into an exception and would fail the query. A refusal here must simply
 * mean "the cache does not grow", so `borrow` reports how much was granted and never throws.
 *
 * Reserving prefers the free part of the execution pool, but `acquireStorageMemory` may fall back
 * to evicting cached blocks when the storage pool is full. In practice the cache is rarely the one
 * asking by then: it only grows while well under its share of the spare storage, a fixed step at a
 * time, and backs off sharply the moment storage tightens. Whatever it does take at the boundary is
 * small and given up on the next round.
 *
 * The reverse does not hold -- Spark cannot reclaim this reservation by itself, as it is not a real
 * block -- so whoever borrows must give it back explicitly.
 */
trait CacheStorageReservation {

  /**
   * Returns the bytes granted, which is either all of `bytes` or 0 -- Spark's storage pool has no
   * partial grant. Never throws.
   */
  def borrow(bytes: Long): Long

  /** Returns the bytes handed back. */
  def repay(bytes: Long): Long
}

object CacheStorageReservation extends CacheStorageReservation {

  /**
   * Wrapped the way query allocations are, so the cache is weighed against the same budget.
   *
   * With dynamic off-heap sizing the executor has no fixed native budget: allocations are admitted
   * against the committed heap plus what Gluten has already handed out, measured against the
   * process maximum. That counter only moves for allocations that pass through this wrapper, so a
   * cache reserved without it would be invisible to the check -- and a cache is large enough for
   * that to matter.
   *
   * Without dynamic sizing the wrapper is not applied and this is the bare target.
   */
  private lazy val target: MemoryTarget =
    MemoryTargets.dynamicOffHeapSizingIfEnabled(MemoryTargets.global())

  override def borrow(bytes: Long): Long = {
    if (bytes <= 0) 0L else target.borrow(bytes)
  }

  override def repay(bytes: Long): Long = {
    if (bytes <= 0) 0L else target.repay(bytes)
  }
}

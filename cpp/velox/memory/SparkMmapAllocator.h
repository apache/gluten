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

#pragma once

#include <atomic>

#include "velox/common/memory/MmapAllocator.h"

namespace gluten {

/// Backs the Velox AsyncDataCache with a capacity that Spark can move at
/// runtime.
///
/// The capacity is reported and enforced separately, and both are needed.
/// Reporting it through capacity() is what makes AsyncDataCache evict rather
/// than grow, since canTryAllocate() measures headroom against it -- but that
/// check runs unlocked by design, so it steers the cache without bounding it.
/// Enforcing it means passing the same value to the three admission points
/// MmapAllocator takes it as a parameter on, which puts the cap behind the
/// atomic add and roll back the base class already does.
class SparkMmapAllocator : public facebook::velox::memory::MmapAllocator {
 public:
  explicit SparkMmapAllocator(const Options& options)
      : MmapAllocator(options),
        staticCapacityBytes_(options.capacity),
        governedCapacityBytes_(pageAlignedDown(options.capacity)) {}

  /// Returns the capacity the cache layer may use, never below the bytes
  /// currently allocated.
  ///
  /// The clamp is mandatory, not defensive: callers compute
  /// capacity() - numAllocated() in unsigned arithmetic
  /// (AsyncDataCache::canTryAllocate, CachedBufferedInput::shouldPreload), so a
  /// lower value would wrap around and turn the cap into an unlimited
  /// allowance. Clamped, the headroom is exactly zero, which is what makes the
  /// cache evict.
  size_t capacity() const override;

  /// Returns the capacity as set, without the clamp against allocated bytes.
  /// This is the value setCapacity() stores.
  size_t governedCapacity() const {
    return governedCapacityBytes_.load(std::memory_order_relaxed);
  }

  /// Returns the capacity configured at construction, the ceiling for
  /// setCapacity().
  ///
  /// The configured size, not what MmapAllocator rounded it up to -- the base
  /// class rounds to a multiple of 64 size classes, 64MiB with the defaults.
  /// The JVM reasons about the configured one, and capping below what the base
  /// class physically holds is the safe direction.
  size_t staticCapacity() const {
    return staticCapacityBytes_;
  }

  /// Moves the capacity from 'from' to 'to' and returns the capacity now in
  /// effect: 'to' unless entries could not be evicted, in which case it stays
  /// at the allocated bytes and the caller sees how much was really given up.
  ///
  /// Both values are absolute rather than a delta, so the caller needs no
  /// baseline from here -- the JVM decides the target from what it has borrowed
  /// from Spark, which is the record of what the cache occupies.
  ///
  /// 'from' is that reservation. It is expected to cover the capacity but not
  /// required to, since lowering the capacity cannot reach an allocation
  /// already in flight. That is reported rather than refused: refusing would
  /// block the very move that makes the cache evict its way back down, and the
  /// gap closes on its own once the lower capacity is in force.
  ///
  /// A target eviction could not reach is not stored, so the bound never falls
  /// below what the caller has reserved. The caller repeats the move once the
  /// entries are released.
  size_t setCapacity(size_t from, size_t to);

  /// Returns the bytes held by cache entries. This is the O(1) half of
  /// totalUsedBytes(); the other half, numMallocBytes(), locks and walks every
  /// thread and must stay off the allocation path.
  size_t allocatedBytes() const;

 private:
  /// The three points where the base class admits an allocation, each delegating
  /// straight back with the governed bound in place of the configured one. This
  /// is the enforcing half described above; capacity() is the reporting half.
  bool allocateNonContiguousWithoutRetry(
      const facebook::velox::memory::MemoryAllocator::SizeMix& sizeMix,
      facebook::velox::memory::Allocation& out) override {
    return allocateNonContiguousWithCapacity(sizeMix, out, governedPages());
  }

  bool allocateContiguousWithoutRetry(
      facebook::velox::memory::MachinePageCount numPages,
      facebook::velox::memory::Allocation* collateral,
      facebook::velox::memory::ContiguousAllocation& allocation,
      facebook::velox::memory::MachinePageCount maxPages = 0) override {
    return allocateContiguousWithCapacity(numPages, collateral, allocation, maxPages, governedPages());
  }

  bool growContiguousWithoutRetry(
      facebook::velox::memory::MachinePageCount increment,
      facebook::velox::memory::ContiguousAllocation& allocation) override {
    return growContiguousWithCapacity(increment, allocation, governedPages());
  }

  /// The governed capacity as a page count, which is what the base class
  /// compares against. Rounded down, as the stored value already is.
  facebook::velox::memory::MachinePageCount governedPages() const {
    return facebook::velox::memory::AllocationTraits::numPages(governedCapacity());
  }

  /// Rounds down to a whole number of pages, cancelling the rounding up done
  /// where headroom is read: numPages() rounds up, so an unaligned bound would
  /// be read as the next whole page and admit past what the JVM reserved.
  static size_t pageAlignedDown(size_t bytes) {
    return bytes - bytes % facebook::velox::memory::AllocationTraits::kPageSize;
  }

  const size_t staticCapacityBytes_;
  std::atomic<size_t> governedCapacityBytes_;
};

} // namespace gluten

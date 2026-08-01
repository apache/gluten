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

/// Measures what the governed cache capacity costs on the read path.
///
/// SparkMmapAllocator::capacity() replaces a load of a constant member with an
/// atomic load and a max against the allocated pages. The base class calls it
/// nowhere, so the only caller that matters is AsyncDataCache::canTryAllocate(),
/// reached once per cache allocation:
///
///   AsyncDataCacheEntry::initialize()
///     -> MemoryAllocator::allocateNonContiguous()
///          -> AsyncDataCache::makeSpace()
///               -> AsyncDataCache::canTryAllocate()   <-- capacity() here
///
/// The benchmarks walk that path from the inside out:
///   Capacity        - the override on its own, called virtually as velox does
///   CanTryAllocate  - the expression canTryAllocate() computes around it
///   CacheAllocate   - a whole cache allocation, the unit the cost is paid per
///
/// Read the two inner numbers against the outer one rather than on their own.
/// Cache allocations come one per load quantum, and Gluten refuses to start
/// with a load quantum above 8MB whenever the cache is on (VeloxListenerApi
/// throws), so CacheAllocate is how often the inner cost is actually paid.
///
/// Each variant runs twice, at a capacity that is still the configured one and
/// at a lowered one. The lowered case is the one where max() has to pick the
/// allocated bytes rather than the governed capacity, so running both shows
/// whether the cost depends on which side wins.

#include <cstdint>
#include <memory>

#include <benchmark/benchmark.h>

#include "memory/SparkMmapAllocator.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/memory/Memory.h"

using namespace facebook::velox;
using namespace facebook::velox::memory;

namespace gluten {
namespace {

constexpr size_t kCapacityBytes = 1UL << 30; // 1GB
// Gluten caps the load quantum at 8MB whenever the Velox cache is enabled, so
// this is what one cache entry allocation actually asks for.
constexpr size_t kLoadQuantum = 8UL << 20;

enum class Variant {
  // Stock MmapAllocator, the behaviour before the governed capacity.
  kBase,
  // SparkMmapAllocator still reporting the configured capacity.
  kGoverned,
  // SparkMmapAllocator with the capacity lowered, so capacity() has to clamp
  // against the allocated bytes instead of returning the governed value.
  kGovernedLowered,
};

std::shared_ptr<MmapAllocator> makeAllocator(Variant variant) {
  MmapAllocator::Options options;
  options.capacity = kCapacityBytes;
  if (variant == Variant::kBase) {
    return std::make_shared<MmapAllocator>(options);
  }
  auto allocator = std::make_shared<SparkMmapAllocator>(options);
  if (variant == Variant::kGovernedLowered) {
    allocator->setCapacity(allocator->staticCapacity(), kLoadQuantum * 4);
  }
  return allocator;
}

// Shared across threads for the concurrent benchmark. Function local statics are
// initialized exactly once and are safe to race on, which the thread_index() == 0
// idiom is not: google-benchmark does not barrier the setup that runs before the
// timing loop.
const MemoryAllocator* sharedAllocator(Variant variant) {
  static auto base = makeAllocator(Variant::kBase);
  static auto governed = makeAllocator(Variant::kGoverned);
  static auto lowered = makeAllocator(Variant::kGovernedLowered);
  switch (variant) {
    case Variant::kBase:
      return base.get();
    case Variant::kGoverned:
      return governed.get();
    default:
      return lowered.get();
  }
}

// Reproduces the arithmetic of AsyncDataCache::canTryAllocate(), which is the
// whole reason capacity() must never report less than the allocated bytes: the
// subtraction is unsigned.
bool canTryAllocate(const MemoryAllocator* allocator, uint64_t requestBytes) {
  return requestBytes <=
      AllocationTraits::pageBytes(AllocationTraits::numPages(allocator->capacity()) - allocator->numAllocated());
}

// capacity() on its own, dispatched through MemoryAllocator* as at the call
// site, so every variant pays for the same virtual call.
void BM_Capacity(benchmark::State& state, Variant variant) {
  auto allocator = makeAllocator(variant);
  const MemoryAllocator* base = allocator.get();
  for (auto _ : state) {
    benchmark::DoNotOptimize(base->capacity());
  }
  state.SetItemsProcessed(state.iterations());
}

// The full admission test. The base allocator reads numAllocated() here too, so
// the atomic the override adds is already in cache by the time it is used.
void BM_CanTryAllocate(benchmark::State& state, Variant variant) {
  auto allocator = makeAllocator(variant);
  const MemoryAllocator* base = allocator.get();
  for (auto _ : state) {
    benchmark::DoNotOptimize(canTryAllocate(base, kLoadQuantum));
  }
  state.SetItemsProcessed(state.iterations());
}

// Same test under concurrency. numAllocated_ is written by every allocation, so
// this checks that reading it a second time adds no cache line traffic.
void BM_CanTryAllocateConcurrent(benchmark::State& state, Variant variant) {
  const MemoryAllocator* allocator = sharedAllocator(variant);
  for (auto _ : state) {
    benchmark::DoNotOptimize(canTryAllocate(allocator, kLoadQuantum));
  }
  state.SetItemsProcessed(state.iterations());
}

// One cache entry allocation and its matching free, which is the unit the
// capacity() call is paid per. Goes through AsyncDataCache::makeSpace() exactly
// as AsyncDataCacheEntry::initialize() does.
//
// The free is inside the timed region on purpose. Pausing the timer around it
// costs more than the whole allocation, and since every variant pays the same
// free the difference between them stays readable.
void BM_CacheAllocate(benchmark::State& state, Variant variant) {
  auto allocator = makeAllocator(variant);
  auto cache = cache::AsyncDataCache::create(allocator.get());
  const auto numPages = AllocationTraits::numPages(kLoadQuantum);

  Allocation allocation;
  for (auto _ : state) {
    if (!allocator->allocateNonContiguous(numPages, allocation)) {
      state.SkipWithError("allocation failed");
      break;
    }
    allocator->freeNonContiguous(allocation);
  }
  cache->shutdown();
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_CAPTURE(BM_Capacity, base, Variant::kBase);
BENCHMARK_CAPTURE(BM_Capacity, governed, Variant::kGoverned);
BENCHMARK_CAPTURE(BM_Capacity, governed_lowered, Variant::kGovernedLowered);

BENCHMARK_CAPTURE(BM_CanTryAllocate, base, Variant::kBase);
BENCHMARK_CAPTURE(BM_CanTryAllocate, governed, Variant::kGoverned);
BENCHMARK_CAPTURE(BM_CanTryAllocate, governed_lowered, Variant::kGovernedLowered);

BENCHMARK_CAPTURE(BM_CanTryAllocateConcurrent, base, Variant::kBase)->ThreadRange(1, 8);
BENCHMARK_CAPTURE(BM_CanTryAllocateConcurrent, governed, Variant::kGoverned)->ThreadRange(1, 8);

BENCHMARK_CAPTURE(BM_CacheAllocate, base, Variant::kBase);
BENCHMARK_CAPTURE(BM_CacheAllocate, governed, Variant::kGoverned);

} // namespace
} // namespace gluten

int main(int argc, char** argv) {
  facebook::velox::memory::MemoryManager::initialize(facebook::velox::memory::MemoryManager::Options{});
  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();
  return 0;
}

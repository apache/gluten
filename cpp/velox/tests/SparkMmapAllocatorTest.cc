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

#include "memory/SparkMmapAllocator.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/FileIds.h"
#include "velox/common/caching/SsdCache.h"

using namespace facebook::velox::memory;
using namespace facebook::velox::cache;

namespace gluten {
namespace {

constexpr size_t kCapacityBytes = 64 << 20;

class SparkMmapAllocatorTest : public testing::Test {
 protected:
  void SetUp() override {
    MmapAllocator::Options options;
    options.capacity = kCapacityBytes;
    allocator_ = std::make_shared<SparkMmapAllocator>(options);
  }

  // Returns the bytes the cache layer would consider free, computed the way
  // AsyncDataCache::canTryAllocate does it: in unsigned arithmetic.
  size_t headroomAsCacheSeesIt() const {
    return AllocationTraits::pageBytes(AllocationTraits::numPages(allocator_->capacity()) - allocator_->numAllocated());
  }

  std::shared_ptr<SparkMmapAllocator> allocator_;
};

TEST_F(SparkMmapAllocatorTest, startsAtStaticCapacity) {
  EXPECT_EQ(allocator_->staticCapacity(), kCapacityBytes);
  EXPECT_EQ(allocator_->capacity(), kCapacityBytes);
}

TEST_F(SparkMmapAllocatorTest, lowersTheCap) {
  EXPECT_EQ(allocator_->setCapacity(kCapacityBytes, 8 << 20), 8 << 20);
  EXPECT_EQ(allocator_->capacity(), 8 << 20);
  EXPECT_EQ(allocator_->governedCapacity(), 8 << 20);
}

TEST_F(SparkMmapAllocatorTest, raisesTheCapBack) {
  allocator_->setCapacity(kCapacityBytes, kCapacityBytes / 2);
  EXPECT_EQ(allocator_->setCapacity(kCapacityBytes / 2, kCapacityBytes * 3 / 4), kCapacityBytes * 3 / 4);
}

TEST_F(SparkMmapAllocatorTest, refusesToExceedTheConfiguredSize) {
  EXPECT_THROW(allocator_->setCapacity(kCapacityBytes, kCapacityBytes + 1), facebook::velox::VeloxRuntimeError);
}

// The clamp exists to keep canTryAllocate's unsigned subtraction from wrapping
// around, which would turn the cap into an unlimited allowance.
TEST_F(SparkMmapAllocatorTest, capacityNeverFallsBelowAllocatedBytes) {
  Allocation allocation;
  ASSERT_TRUE(allocator_->allocateNonContiguous(64, allocation));
  const size_t allocated = allocator_->allocatedBytes();
  ASSERT_GT(allocated, 0);

  EXPECT_EQ(allocator_->setCapacity(kCapacityBytes, 0), allocated);
  EXPECT_EQ(allocator_->capacity(), allocated);
  EXPECT_EQ(headroomAsCacheSeesIt(), 0);

  allocator_->freeNonContiguous(allocation);
}

// Same scenario, asserted from the caller's point of view: the headroom must
// stay small rather than wrap to a huge number.
TEST_F(SparkMmapAllocatorTest, headroomDoesNotUnderflowAfterShrinking) {
  Allocation allocation;
  ASSERT_TRUE(allocator_->allocateNonContiguous(256, allocation));
  allocator_->setCapacity(kCapacityBytes, 1);

  EXPECT_LE(headroomAsCacheSeesIt(), kCapacityBytes);

  allocator_->freeNonContiguous(allocation);
}

// A target the allocation blocks is not stored, so freeing does not land on it
// by itself -- the caller repeats the move once it has the room.
TEST_F(SparkMmapAllocatorTest, freeingDoesNotApplyATargetThatWasNeverStored) {
  Allocation allocation;
  ASSERT_TRUE(allocator_->allocateNonContiguous(256, allocation));
  const auto allocated = allocator_->allocatedBytes();
  const size_t target = 4 << 10;
  ASSERT_LT(target, allocated);

  // Held up at the allocated bytes, which is what the caller books.
  EXPECT_EQ(allocator_->setCapacity(kCapacityBytes, target), allocated);
  allocator_->freeNonContiguous(allocation);
  EXPECT_EQ(allocator_->capacity(), allocated);

  // Repeating it now succeeds.
  EXPECT_EQ(allocator_->setCapacity(allocated, target), target);
}

// The four properties the JVM side relies on when booking memory with Spark.

// (1) The reported capacity is max(governed, allocated), bounded by the
// configured size.
TEST_F(SparkMmapAllocatorTest, reportedCapacityCoversWhatIsInUse) {
  Allocation allocation;
  ASSERT_TRUE(allocator_->allocateNonContiguous(256, allocation));
  allocator_->setCapacity(kCapacityBytes, 0);

  EXPECT_EQ(allocator_->capacity(), allocator_->allocatedBytes());
  EXPECT_LE(allocator_->capacity(), allocator_->staticCapacity());

  allocator_->freeNonContiguous(allocation);
}

// (2) Once the cache is at its cap the headroom is zero, so the allocated bytes
// can only fall. This is what keeps usage inside what was booked with Spark.
TEST_F(SparkMmapAllocatorTest, noHeadroomOnceTheCapIsReached) {
  Allocation allocation;
  ASSERT_TRUE(allocator_->allocateNonContiguous(256, allocation));
  allocator_->setCapacity(kCapacityBytes, 0);

  EXPECT_EQ(headroomAsCacheSeesIt(), 0);

  const auto before = allocator_->allocatedBytes();
  allocator_->freeNonContiguous(allocation);
  EXPECT_LT(allocator_->allocatedBytes(), before);
}

// (3) A target at or above what is in use takes effect exactly, so the caller
// gets the capacity it borrowed for rather than something the clamp altered.
TEST_F(SparkMmapAllocatorTest, targetsAboveUsageTakeEffectExactly) {
  Allocation allocation;
  ASSERT_TRUE(allocator_->allocateNonContiguous(256, allocation));
  const auto allocated = allocator_->allocatedBytes();

  const size_t target = allocated + (16 << 20);
  EXPECT_EQ(allocator_->setCapacity(kCapacityBytes, target), target);

  allocator_->freeNonContiguous(allocation);
}

// (4) A target below what is in use is not stored at all, so the bound never
// falls below what the caller has reserved.
//
// Storing it would leave a bound the caller cannot see -- what comes back is
// held up at the allocated bytes -- and once its own target stopped moving,
// nothing would ever raise the bound again: the cache would go on being
// squeezed towards a figure that no longer applied while the reservation stayed
// where it was.
TEST_F(SparkMmapAllocatorTest, targetsBelowUsageAreNotStored) {
  Allocation allocation;
  ASSERT_TRUE(allocator_->allocateNonContiguous(256, allocation));
  const auto pinnedBytes = allocator_->allocatedBytes();
  ASSERT_GT(pinnedBytes, 0);

  const size_t target = pinnedBytes / 2;
  EXPECT_EQ(allocator_->setCapacity(kCapacityBytes, target), pinnedBytes);
  EXPECT_EQ(allocator_->governedCapacity(), pinnedBytes);

  allocator_->freeNonContiguous(allocation);
  EXPECT_EQ(allocator_->capacity(), pinnedBytes);
}

// What happens when the reservation does not cover what the cache holds.
//
// It can happen because lowering the capacity does not reach an allocation
// already in flight: the bound is read once on the way in, so a call that
// started earlier still admits against the previous value. The cache is then
// holding memory Spark has not been told about.
//
// The move is reported rather than refused. Refusing would remove the only way
// out -- lowering the capacity is what makes the cache evict, so a throw would
// leave it holding exactly the memory it is being asked to give up, and the
// same throw would greet every later attempt. What comes back is what the cache
// really holds, and the caller settles the difference with Spark.
TEST_F(SparkMmapAllocatorTest, reportsRatherThanRefusesWhenAboveTheReservation) {
  Allocation allocation;
  ASSERT_TRUE(allocator_->allocateNonContiguous(256, allocation));
  const auto allocated = allocator_->allocatedBytes();
  ASSERT_GT(allocated, 0);

  // A target under what is in use, so the reported capacity is held up by the
  // allocation.
  allocator_->setCapacity(kCapacityBytes, allocated / 2);
  ASSERT_EQ(allocator_->capacity(), allocated);

  // Claiming to have reserved less than that is accepted rather than refused,
  // and what comes back is what is really held -- which is what the caller
  // books. The bound is held up at the allocated bytes, not left at the target.
  EXPECT_EQ(allocator_->setCapacity(allocated - 1, allocated / 4), allocated);
  EXPECT_EQ(allocator_->governedCapacity(), allocated);

  // The cache cannot grow meanwhile: the headroom it reads is zero, so it
  // evicts rather than taking more.
  EXPECT_EQ(headroomAsCacheSeesIt(), 0);

  allocator_->freeNonContiguous(allocation);
}

// Reproduces executor startup with a cache size that is not a whole multiple of
// the quantum MmapAllocator rounds its capacity up to (64 * largestSizeClass
// pages = 64MiB with the defaults, MmapAllocator.cpp:37-41).
//
// The JVM reads memCacheSize from the config and passes it as the capacity the
// cache is coming down from (PeriodicMemoryChecker.reserveInitial), while this
// class records what velox rounded that up to. The two disagree for any size
// that is not a whole quantum, and setCapacity compares them.
TEST_F(SparkMmapAllocatorTest, startsFromTheConfiguredSizeNotTheRoundedOne) {
  constexpr size_t kConfigured = 100 << 20; // 100MiB is not a whole 64MiB quantum
  MmapAllocator::Options options;
  options.capacity = kConfigured;
  SparkMmapAllocator allocator(options);

  EXPECT_EQ(allocator.staticCapacity(), kConfigured);
  EXPECT_EQ(allocator.capacity(), kConfigured);
  // What reserveInitial does: come down from the configured size to what Spark granted.
  EXPECT_NO_THROW(allocator.setCapacity(kConfigured, 50 << 20));
}

// The JVM computes targets in bytes and nothing rounds them to pages, but
// AsyncDataCache::canTryAllocate admits against numPages(capacity()), which
// rounds *up* (Allocation.h:48-50). The cache may therefore allocate past the
// governed capacity -- and so past what was reserved with Spark.
TEST_F(SparkMmapAllocatorTest, doesNotAdmitBeyondAnUnalignedCapacity) {
  const size_t reserved = (4 << 20) + 1; // a byte past a page boundary
  allocator_->setCapacity(kCapacityBytes, reserved);

  Allocation allocation;
  ASSERT_TRUE(allocator_->allocateNonContiguous(AllocationTraits::numPages(allocator_->capacity()), allocation));

  EXPECT_LE(allocator_->allocatedBytes(), reserved);
  // Usage above the reservation makes every later move fail, so the cache can
  // no longer grow.
  EXPECT_NO_THROW(allocator_->setCapacity(reserved, reserved * 2));

  allocator_->freeNonContiguous(allocation);
}

// AsyncDataCache::shutdown() is what releases the pages, and nothing else will.
//
// The allocator and the cache own each other: AsyncDataCache::create registers
// the cache with the allocator, which keeps a shared_ptr to it
// (MmapAllocator.h:397), while the cache refers back by raw pointer. Dropping
// every external reference to the cache therefore destroys nothing, the entries
// keep their pages, and ~MmapAllocator aborts the process on
// `numAllocated_ == 0`.
//
// So VeloxBackend::tearDown() calling shutdown() is not a tidy-up: it is the
// only thing standing between an executor shutdown and an abort. Declaration
// order cannot substitute for it.
TEST_F(SparkMmapAllocatorTest, releasesPagesOnlyOnCacheShutdown) {
  MmapAllocator::Options options;
  options.capacity = kCapacityBytes;
  auto allocator = std::make_shared<SparkMmapAllocator>(options);
  auto cache = AsyncDataCache::create(allocator.get());

  // Large enough to go through the allocator rather than the entry's tinyData_.
  facebook::velox::StringIdLease file(facebook::velox::fileIds(), std::string_view("released-on-shutdown"));
  auto pin = cache->findOrCreate(RawFileCacheKey{file.id(), 0}, 64 << 10);
  ASSERT_FALSE(pin.empty());
  pin.entry()->setExclusiveToShared();
  pin.clear();
  ASSERT_GT(allocator->allocatedBytes(), 0);

  // Letting go of the cache frees nothing: the allocator still holds it.
  cache.reset();
  EXPECT_GT(allocator->allocatedBytes(), 0);

  // Only this does, and it has to reach the cache through the allocator, since
  // that is now its sole owner.
  static_cast<AsyncDataCache*>(allocator->cache())->shutdown();
  EXPECT_EQ(allocator->allocatedBytes(), 0);
}

// Fills a cache from several threads at once and returns the most it ever held.
//
// Sized so the cache spends the run at its grant rather than sawtoothing below
// it: the grant is far larger than the 1MB minimum eviction, and the entries are
// small enough that the boundary is crossed constantly.
constexpr int kThreads = 16;
constexpr uint64_t kEntrySize = 8 << 10; // above kTinyDataSize, so it reaches the allocator
constexpr size_t kGranted = 32 << 20;

size_t peakWhileFilledConcurrently(
    SparkMmapAllocator* allocator,
    facebook::velox::cache::AsyncDataCache* cache,
    size_t granted) {
  constexpr int kEntriesPerThread = 3000;

  facebook::velox::StringIdLease file(facebook::velox::fileIds(), std::string_view("filled-together"));
  std::atomic<size_t> highWater{0};
  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t]() {
      for (int i = 0; i < kEntriesPerThread; ++i) {
        const uint64_t offset = (static_cast<uint64_t>(t) * kEntriesPerThread + i) * kEntrySize;
        auto pin = cache->findOrCreate(RawFileCacheKey{file.id(), offset}, kEntrySize);
        if (pin.empty()) {
          continue;
        }
        pin.entry()->setExclusiveToShared();
        pin.clear();
        const auto seen = allocator->allocatedBytes();
        auto previous = highWater.load(std::memory_order_relaxed);
        while (seen > previous && !highWater.compare_exchange_weak(previous, seen, std::memory_order_relaxed)) {
        }
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_GT(highWater.load(), granted / 2) << "the cache should have filled up";
  return highWater.load();
}

// The grant holds while several readers fill the cache at once.
//
// It is worth saying why this is worth testing. AsyncDataCache::makeSpace()
// decides whether there is room without holding a lock -- deliberately, so that
// what one thread evicts another can take -- so readers that pass that check
// together would leave the cache above the grant if the check were the only
// thing standing in the way.
//
// It is not. Passing the grant to the base class as the admission capacity puts
// the cap behind the same atomic add and roll back it already uses, at the
// point the pages are handed out, so a reader that raced past the check still
// leaves empty-handed.
//
// The cache pages are therefore bounded exactly. The counter is a different
// matter: a refusal is an add followed by a subtraction, so a reader sampling
// numAllocated() mid-flight sees requests that are in the middle of being
// turned down. Measured against cachedPages(), which is only updated after an
// allocation succeeds, the real pages never exceeded the grant while the
// counter peaked one entry above it. The counter reads high, never low, so
// anything deciding on it errs towards evicting.
TEST_F(SparkMmapAllocatorTest, holdsTheGrantWhileReadersFillTheCacheTogether) {
  MmapAllocator::Options options;
  options.capacity = kCapacityBytes;
  auto allocator = std::make_shared<SparkMmapAllocator>(options);
  auto cache = AsyncDataCache::create(allocator.get());
  ASSERT_EQ(allocator->setCapacity(kCapacityBytes, kGranted), kGranted);

  const auto peak = peakWhileFilledConcurrently(allocator.get(), cache.get(), kGranted);

  // What the cache is really holding, at its highest. Never above the grant.
  EXPECT_LE(AllocationTraits::pageBytes(cache->cachedPages()), kGranted)
      << "granted " << kGranted << ", cached " << AllocationTraits::pageBytes(cache->cachedPages());
  EXPECT_LE(allocator->allocatedBytes(), kGranted)
      << "granted " << kGranted << ", at rest " << allocator->allocatedBytes();
  // One in-flight refusal per thread is all the counter may be inflated by.
  EXPECT_LE(peak, kGranted + kThreads * kEntrySize) << "granted " << kGranted << ", peak " << peak;
  cache->shutdown();
}

TEST_F(SparkMmapAllocatorTest, allocationsBeyondTheCapAreRefused) {
  constexpr size_t kGrant = 1 << 20;
  allocator_->setCapacity(kCapacityBytes, kGrant);

  Allocation allocation;
  // Refused by the allocator, not merely by the check AsyncDataCache does
  // beforehand. The cap is enforced where the pages are handed out, so a caller
  // that skips that check -- or races past it -- still cannot take more than was
  // reserved.
  const auto tooMany = AllocationTraits::numPages(kCapacityBytes / 2);
  ASSERT_FALSE(allocator_->allocateNonContiguous(tooMany, allocation));
  EXPECT_EQ(allocator_->allocatedBytes(), 0);
  EXPECT_EQ(headroomAsCacheSeesIt(), kGrant);

  // What fits is still served, all of it.
  ASSERT_TRUE(allocator_->allocateNonContiguous(AllocationTraits::numPages(kGrant), allocation));
  EXPECT_EQ(allocator_->allocatedBytes(), kGrant);
  EXPECT_EQ(headroomAsCacheSeesIt(), 0);

  allocator_->freeNonContiguous(allocation);
}

} // namespace
} // namespace gluten

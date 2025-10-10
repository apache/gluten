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

#include "compute/BoltBackend.h"
#include "config/BoltConfig.h"
#include "memory/BoltMemoryManager.h"
#include "bolt/common/base/tests/GTestUtils.h"

namespace gluten {

using namespace bytedance::bolt;

class MockAllocationListener : public gluten::AllocationListener {
 public:
  void allocationChanged(int64_t diff) override {
    currentBytes_ += diff;
    peakBytes_ = std::max(peakBytes_, currentBytes_);
  }
  int64_t currentBytes() override {
    return currentBytes_;
  }
  int64_t peakBytes() override {
    return peakBytes_;
  }
  uint64_t currentBytes_{0L};
  uint64_t peakBytes_{0L};
};

namespace {
static const uint64_t kMB = 1 << 20;
} // namespace

class MemoryManagerTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    std::unordered_map<std::string, std::string> conf = {
        {kMemoryReservationBlockSize, std::to_string(kMemoryReservationBlockSizeDefault)},
        {kBoltMemInitCapacity, std::to_string(kBoltMemInitCapacityDefault)},
        {kSparkOffHeapMemory, "7516192768"}};
    gluten::BoltBackend::create(AllocationListener::noop(), conf);
  }

  void SetUp() override {
    vmm_ = std::make_unique<BoltMemoryManager>(
        gluten::kBoltBackendKind,
        std::make_unique<MockAllocationListener>(),
        *BoltBackend::get()->getBackendConf(),
        "memory-manager-test");
    listener_ = vmm_->getListener();
    allocator_ = vmm_->allocator();
  }

  std::unique_ptr<BoltMemoryManager> vmm_;
  AllocationListener* listener_;
  MemoryAllocator* allocator_;

  std::shared_ptr<MemoryAllocator> stdAllocator_ = std::make_shared<StdMemoryAllocator>();

  struct Allocation {
    void* buffer;
    size_t size;
    memory::MemoryPool* pool;
  };
};

TEST_F(MemoryManagerTest, memoryPoolWithBlockReseravtion) {
  auto pool = vmm_->getLeafMemoryPool();
  std::vector<Allocation> allocations;
  std::vector<uint64_t> sizes{
      kMemoryReservationBlockSizeDefault - 1 * kMB, kMemoryReservationBlockSizeDefault - 2 * kMB};
  for (const auto& size : sizes) {
    auto buf = pool->allocate(size);
    allocations.push_back({buf, size, pool.get()});
  }
  EXPECT_EQ(listener_->currentBytes(), 2 * kMemoryReservationBlockSizeDefault);
  EXPECT_EQ(listener_->peakBytes(), listener_->currentBytes());

  for (auto& allocation : allocations) {
    allocation.pool->free(allocation.buffer, allocation.size);
  }

  auto currentBytes = listener_->currentBytes();
  ASSERT_EQ(vmm_->shrink(0), currentBytes);
  ASSERT_EQ(listener_->currentBytes(), 0);
}

TEST_F(MemoryManagerTest, memoryAllocatorWithBlockReservation) {
  auto initBytes = listener_->currentBytes();

  std::vector<Allocation> allocations;
  std::vector<uint64_t> sizes{
      kMemoryReservationBlockSizeDefault - 1 * kMB, kMemoryReservationBlockSizeDefault - 2 * kMB};
  for (auto i = 0; i < sizes.size(); i++) {
    auto size = sizes[i];
    auto currentBytes = allocator_->getBytes();
    Allocation allocation{.size = size};
    allocator_->allocate(size, &allocation.buffer);
    allocations.push_back(allocation);

    EXPECT_EQ(allocator_->getBytes(), currentBytes + size);
    EXPECT_EQ(allocator_->peakBytes(), allocator_->getBytes());
    EXPECT_EQ(listener_->currentBytes(), (i + 1) * kMemoryReservationBlockSizeDefault + initBytes);
    EXPECT_EQ(listener_->peakBytes(), listener_->currentBytes());
  }

  auto currentBytes = allocator_->getBytes();
  auto allocation = allocations.back();
  allocations.pop_back();
  allocator_->free(allocation.buffer, allocation.size);
  EXPECT_EQ(allocator_->getBytes(), currentBytes - allocation.size);
  EXPECT_EQ(listener_->currentBytes(), kMemoryReservationBlockSizeDefault + initBytes);

  currentBytes = allocator_->getBytes();
  allocation = allocations.back();
  allocations.pop_back();
  allocator_->free(allocation.buffer, allocation.size);
  EXPECT_EQ(allocator_->getBytes(), currentBytes - allocation.size);
  EXPECT_EQ(listener_->currentBytes(), initBytes);

  ASSERT_EQ(allocator_->getBytes(), 0);
}

namespace {
class AllocationListenerWrapper : public AllocationListener {
 public:
  explicit AllocationListenerWrapper() {}

  void set(AllocationListener* const delegate) {
    if (delegate_ != nullptr) {
      throw std::runtime_error("Invalid state");
    }
    delegate_ = delegate;
  }

  void allocationChanged(int64_t diff) override {
    delegate_->allocationChanged(diff);
  }
  int64_t currentBytes() override {
    return delegate_->currentBytes();
  }
  int64_t peakBytes() override {
    return delegate_->peakBytes();
  }

 private:
  AllocationListener* delegate_{nullptr};
};

class SpillableAllocationListener : public AllocationListener {
 public:
  virtual uint64_t shrink(uint64_t bytes) = 0;
  virtual uint64_t spill(uint64_t bytes) = 0;
};

class MockSparkTaskMemoryManager {
 public:
  explicit MockSparkTaskMemoryManager(const uint64_t maxBytes);

  AllocationListener* newListener(std::function<uint64_t(uint64_t)> shrink, std::function<uint64_t(uint64_t)> spill);

  uint64_t acquire(uint64_t bytes);
  void release(uint64_t bytes);
  uint64_t currentBytes() {
    return currentBytes_;
  }

 private:
  mutable std::recursive_mutex mutex_;
  std::vector<std::unique_ptr<SpillableAllocationListener>> listeners_{};

  const uint64_t maxBytes_;
  uint64_t currentBytes_{0L};
};

class MockSparkAllocationListener : public SpillableAllocationListener {
 public:
  explicit MockSparkAllocationListener(
      MockSparkTaskMemoryManager* const manager,
      std::function<uint64_t(uint64_t)> shrink,
      std::function<uint64_t(uint64_t)> spill)
      : manager_(manager), shrink_(shrink), spill_(spill) {}

  void allocationChanged(int64_t diff) override {
    if (diff == 0) {
      return;
    }
    if (diff > 0) {
      auto granted = manager_->acquire(diff);
      if (granted < diff) {
        throw std::runtime_error("OOM");
      }
      currentBytes_ += granted;
      return;
    }
    manager_->release(-diff);
    currentBytes_ -= (-diff);
  }

  uint64_t shrink(uint64_t bytes) override {
    return shrink_(bytes);
  }

  uint64_t spill(uint64_t bytes) override {
    return spill_(bytes);
  }

  int64_t currentBytes() override {
    return currentBytes_;
  }

 private:
  MockSparkTaskMemoryManager* const manager_;
  std::function<uint64_t(uint64_t)> shrink_;
  std::function<uint64_t(uint64_t)> spill_;
  std::atomic<uint64_t> currentBytes_{0L};
};

MockSparkTaskMemoryManager::MockSparkTaskMemoryManager(const uint64_t maxBytes) : maxBytes_(maxBytes) {}

AllocationListener* MockSparkTaskMemoryManager::newListener(
    std::function<uint64_t(uint64_t)> shrink,
    std::function<uint64_t(uint64_t)> spill) {
  listeners_.push_back(std::make_unique<MockSparkAllocationListener>(this, shrink, spill));
  return listeners_.back().get();
}

uint64_t MockSparkTaskMemoryManager::acquire(uint64_t bytes) {
  std::unique_lock l(mutex_);
  auto freeBytes = maxBytes_ - currentBytes_;
  if (bytes <= freeBytes) {
    currentBytes_ += bytes;
    return bytes;
  }
  // Shrink listeners.
  int64_t bytesNeeded = bytes - freeBytes;
  for (const auto& listener : listeners_) {
    bytesNeeded -= listener->shrink(bytesNeeded);
    if (bytesNeeded < 0) {
      break;
    }
  }
  if (bytesNeeded > 0) {
    for (const auto& listener : listeners_) {
      bytesNeeded -= listener->spill(bytesNeeded);
      if (bytesNeeded < 0) {
        break;
      }
    }
  }

  if (bytesNeeded > 0) {
    uint64_t granted = bytes - bytesNeeded;
    currentBytes_ += granted;
    return granted;
  }

  currentBytes_ += bytes;
  return bytes;
}

void MockSparkTaskMemoryManager::release(uint64_t bytes) {
  std::unique_lock l(mutex_);
  currentBytes_ -= bytes;
}

class MockMemoryReclaimer : public bytedance::bolt::memory::MemoryReclaimer {
 public:
  explicit MockMemoryReclaimer(std::vector<void*>& buffs, int32_t size)
      : bytedance::bolt::memory::MemoryReclaimer(0), buffs_(buffs), size_(size) {}

  bool reclaimableBytes(const memory::MemoryPool& pool, uint64_t& reclaimableBytes) const override {
    uint64_t total = 0;
    for (const auto& buf : buffs_) {
      if (buf == nullptr) {
        continue;
      }
      total += size_;
    }
    if (total == 0) {
      return false;
    }
    reclaimableBytes = total;
    return true;
  }

  uint64_t reclaim(memory::MemoryPool* pool, uint64_t targetBytes, uint64_t maxWaitMs, Stats& stats) override {
    uint64_t total = 0;
    for (auto& buf : buffs_) {
      if (buf == nullptr) {
        // When:
        // 1. Called by allocation from the same pool so buff is not allocated yet.
        // 2. Already called once.
        continue;
      }
      pool->free(buf, size_);
      buf = nullptr;
      total += size_;
    }
    return total;
  }

 private:
  std::vector<void*>& buffs_;
  int32_t size_;
};

void assertCapacitiesMatch(MockSparkTaskMemoryManager& tmm, std::vector<std::unique_ptr<BoltMemoryManager>>& vmms) {
  uint64_t sum = 0;
  for (const auto& vmm : vmms) {
    if (vmm == nullptr) {
      continue;
    }
    sum += vmm->getAggregateMemoryPool()->capacity();
  }
  if (tmm.currentBytes() != sum) {
    ASSERT_EQ(tmm.currentBytes(), sum);
  }
}
} // namespace

class MultiMemoryManagerTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    std::unordered_map<std::string, std::string> conf = {
        {kMemoryReservationBlockSize, std::to_string(kMemoryReservationBlockSizeDefault)},
        {kBoltMemInitCapacity, std::to_string(kBoltMemInitCapacityDefault)},
        {kSparkOffHeapMemory, "7516192768"}};
    gluten::BoltBackend::create(AllocationListener::noop(), conf);
  }

  std::unique_ptr<BoltMemoryManager> newBoltMemoryManager(std::unique_ptr<AllocationListener> listener) {
    return std::make_unique<BoltMemoryManager>(
        gluten::kBoltBackendKind,
        std::move(listener),
        *BoltBackend::get()->getBackendConf(),
        "multi-memory-manager-test");
  }
};

TEST_F(MultiMemoryManagerTest, spill) {
  const uint64_t maxBytes = 200 << 20;
  const uint32_t numThreads = 100;
  const uint32_t numAllocations = 200;
  const int32_t allocateSize = 10 << 20;

  MockSparkTaskMemoryManager tmm{maxBytes};
  std::vector<std::unique_ptr<BoltMemoryManager>> vmms{};
  std::vector<std::thread> threads{};
  std::vector<std::vector<void*>> buffs{};
  for (size_t i = 0; i < numThreads; ++i) {
    buffs.push_back({});
    vmms.emplace_back(nullptr);
  }

  // Emulate a shared lock to avoid ABBA deadlock.
  std::recursive_mutex mutex;

  for (size_t i = 0; i < numThreads; ++i) {
    threads.emplace_back([this, i, allocateSize, &tmm, &vmms, &mutex, &buffs]() -> void {
      auto wrapper = std::make_unique<AllocationListenerWrapper>(); // Set later.
      auto* listener = wrapper.get();

      bytedance::bolt::memory::MemoryPool* pool; // Set later.
      {
        std::unique_lock<std::recursive_mutex> l(mutex);
        vmms[i] = newBoltMemoryManager(std::move(wrapper));
        pool = vmms[i]->getLeafMemoryPool().get();
        pool->setReclaimer(std::make_unique<MockMemoryReclaimer>(buffs[i], allocateSize));
        listener->set(tmm.newListener(
            [](uint64_t bytes) -> uint64_t { return 0; },
            [i, &vmms, &mutex, &pool](uint64_t bytes) -> uint64_t {
              std::unique_lock<std::recursive_mutex> l(mutex);
              return vmms[i]->getMemoryManager()->arbitrator()->shrinkCapacity(bytes, true);
            }));
      }
      {
        std::unique_lock<std::recursive_mutex> l(mutex);
        for (size_t j = 0; j < numAllocations; ++j) {
          assertCapacitiesMatch(tmm, vmms);
          buffs[i].push_back(pool->allocate(allocateSize));
          assertCapacitiesMatch(tmm, vmms);
        }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  for (auto& vmm : vmms) {
    assertCapacitiesMatch(tmm, vmms);
    vmm->getMemoryManager()->arbitrator()->shrinkCapacity(allocateSize * numAllocations, true);
    assertCapacitiesMatch(tmm, vmms);
  }

  ASSERT_EQ(tmm.currentBytes(), 0);
}

} // namespace gluten

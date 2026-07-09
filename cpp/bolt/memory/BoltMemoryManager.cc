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

#include "BoltMemoryManager.h"
#ifdef ENABLE_JEMALLOC_STATS
#include <jemalloc/jemalloc.h>
#endif

#include "compute/BoltBackend.h"

#include "bolt/common/memory/MallocAllocator.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/common/process/StackTrace.h"
#include "bolt/exec/MemoryReclaimer.h"

#include "config/BoltConfig.h"
#include "memory/ArrowMemoryPool.h"
#include "memory/BoltGlutenMemoryManager.h"
#include "utils/Exception.h"

DECLARE_int32(gluten_bolt_async_timeout_on_task_stopping);

namespace gluten {

using namespace bytedance;

std::unordered_map<std::string, std::string> getExtraArbitratorConfigs(
    const bytedance::bolt::config::ConfigBase& backendConf) {
  auto reservationBlockSize =
      backendConf.get<uint64_t>(kMemoryReservationBlockSize, kMemoryReservationBlockSizeDefault);
  auto memInitCapacity = backendConf.get<uint64_t>(kBoltMemInitCapacity, kBoltMemInitCapacityDefault);
  auto memReclaimMaxWaitMs = backendConf.get<uint64_t>(kBoltMemReclaimMaxWaitMs, kBoltMemReclaimMaxWaitMsDefault);

  std::unordered_map<std::string, std::string> extraArbitratorConfigs;
  extraArbitratorConfigs[std::string(kMemoryPoolInitialCapacity)] = folly::to<std::string>(memInitCapacity) + "B";
  extraArbitratorConfigs[std::string(kMemoryPoolTransferCapacity)] = folly::to<std::string>(reservationBlockSize) + "B";
  extraArbitratorConfigs[std::string(kMemoryReclaimMaxWaitMs)] = folly::to<std::string>(memReclaimMaxWaitMs) + "ms";

  return extraArbitratorConfigs;
}

namespace {
template <typename T>
T getConfig(
    const std::unordered_map<std::string, std::string>& configs,
    const std::string_view& key,
    const T& defaultValue) {
  if (configs.count(std::string(key)) > 0) {
    try {
      return folly::to<T>(configs.at(std::string(key)));
    } catch (const std::exception& e) {
      BOLT_USER_FAIL("Failed while parsing SharedArbitrator configs: {}", e.what());
    }
  }
  return defaultValue;
}

/// We assume in a single Spark task. No thread-safety should be guaranteed.
class ListenableArbitrator : public bolt::memory::MemoryArbitrator {
 public:
  ListenableArbitrator(const Config& config, AllocationListener* listener)
      : MemoryArbitrator(config),
        listener_(listener),
        memoryPoolInitialCapacity_(bolt::config::toCapacity(
            getConfig<std::string>(
                config.extraConfigs,
                kMemoryPoolInitialCapacity,
                std::to_string(kDefaultMemoryPoolInitialCapacity)),
            bolt::config::CapacityUnit::BYTE)),
        memoryPoolTransferCapacity_(bolt::config::toCapacity(
            getConfig<std::string>(
                config.extraConfigs,
                kMemoryPoolTransferCapacity,
                std::to_string(kDefaultMemoryPoolTransferCapacity)),
            bolt::config::CapacityUnit::BYTE)),
        memoryReclaimMaxWaitMs_(
            std::chrono::duration_cast<std::chrono::milliseconds>(bolt::config::toDuration(getConfig<std::string>(
                                                                      config.extraConfigs,
                                                                      kMemoryReclaimMaxWaitMs,
                                                                      std::string(kDefaultMemoryReclaimMaxWaitMs))))
                .count()) {}
  std::string kind() const override {
    return kind_;
  }

  void shutdown() override {}

  void addPool(const std::shared_ptr<bolt::memory::MemoryPool>& pool) override {
    BOLT_CHECK_EQ(pool->capacity(), 0);

    std::unique_lock guard{mutex_};
    BOLT_CHECK_EQ(candidates_.count(pool.get()), 0);
    candidates_.emplace(pool.get(), pool->weak_from_this());
  }

  void removePool(bolt::memory::MemoryPool* pool) override {
    BOLT_CHECK_EQ(pool->reservedBytes(), 0);
    shrinkCapacity(pool, pool->capacity());

    std::unique_lock guard{mutex_};
    const auto ret = candidates_.erase(pool);
    BOLT_CHECK_EQ(ret, 1);
  }

  bool growCapacity(bolt::memory::MemoryPool* pool, uint64_t targetBytes) override {
    // Set arbitration context to allow memory over-use during recursive arbitration.
    // See MemoryPoolImpl::maybeIncrementReservation.
    bolt::memory::ScopedMemoryArbitrationContext ctx{};
    bolt::memory::MemoryPool* candidate;
    {
      std::unique_lock guard{mutex_};
      BOLT_CHECK_EQ(candidates_.size(), 1, "ListenableArbitrator should only be used within a single root pool");
      candidate = candidates_.begin()->first;
    }
    BOLT_CHECK(pool->root() == candidate, "Illegal state in ListenableArbitrator");

    growCapacityInternal(pool->root(), targetBytes);
    return true;
  }

  uint64_t shrinkCapacity(uint64_t targetBytes, bool allowSpill, bool allowAbort) override {
    bolt::memory::ScopedMemoryArbitrationContext ctx{};
    bytedance::bolt::exec::MemoryReclaimer::Stats status;
    bolt::memory::MemoryPool* pool = nullptr;
    {
      std::unique_lock guard{mutex_};
      BOLT_CHECK_EQ(candidates_.size(), 1, "ListenableArbitrator should only be used within a single root pool");
      pool = candidates_.begin()->first;
    }
    pool->reclaim(targetBytes, memoryReclaimMaxWaitMs_, status); // ignore the output
    return shrinkCapacityInternal(pool, 0);
  }

  uint64_t shrinkCapacity(bolt::memory::MemoryPool* pool, uint64_t targetBytes) override {
    return shrinkCapacityInternal(pool, targetBytes);
  }

  Stats stats() const override {
    Stats stats; // no-op
    return stats;
  }

  std::string toString() const override {
    return fmt::format("ARBITRATOR[{}] CAPACITY {} {}", kind_, bolt::succinctBytes(capacity()), stats().toString());
  }

 private:
  void growCapacityInternal(bolt::memory::MemoryPool* pool, uint64_t bytes) {
    auto freeByes = pool->freeBytes();
    if (freeByes > bytes) {
      if (growPool(pool, 0, bytes)) {
        return;
      }
    }
    auto reclaimedFreeBytes = shrinkPool(pool, 0);
    auto neededBytes = bolt::bits::roundUp(bytes - reclaimedFreeBytes, memoryPoolTransferCapacity_);
    try {
      listener_->allocationChanged(neededBytes);
    } catch (const std::exception&) {
      VLOG(2) << "ListenableArbitrator growCapacityInternal failed, stacktrace: "
              << bolt::process::StackTrace().toString();
      // if allocationChanged failed, we need to free the reclaimed bytes
      listener_->allocationChanged(-reclaimedFreeBytes);
      std::rethrow_exception(std::current_exception());
    }
    auto ret = growPool(pool, reclaimedFreeBytes + neededBytes, bytes);
    BOLT_CHECK(
        ret,
        "{} failed to grow {} bytes, current state {}",
        pool->name(),
        bolt::succinctBytes(bytes),
        pool->toString());
  }

  uint64_t shrinkCapacityInternal(bolt::memory::MemoryPool* pool, uint64_t bytes) {
    uint64_t freeBytes = shrinkPool(pool, bytes);
    listener_->allocationChanged(-freeBytes);
    return freeBytes;
  }

  gluten::AllocationListener* listener_ = nullptr;
  const uint64_t memoryPoolInitialCapacity_; // FIXME: Unused.
  const uint64_t memoryPoolTransferCapacity_;
  const uint64_t memoryReclaimMaxWaitMs_;

  mutable std::mutex mutex_;
  inline static std::string kind_ = "GLUTEN";
  std::unordered_map<bolt::memory::MemoryPool*, std::weak_ptr<bolt::memory::MemoryPool>> candidates_;
};

} // namespace

ArbitratorFactoryRegister::ArbitratorFactoryRegister(gluten::AllocationListener* listener) : listener_(listener) {
  static std::atomic_uint32_t id{0UL};
  kind_ = "GLUTEN_ARBITRATOR_FACTORY_" + std::to_string(id++);
  bolt::memory::MemoryArbitrator::registerFactory(
      kind_,
      [this](const bolt::memory::MemoryArbitrator::Config& config) -> std::unique_ptr<bolt::memory::MemoryArbitrator> {
        return std::make_unique<ListenableArbitrator>(config, listener_);
      });
}

ArbitratorFactoryRegister::~ArbitratorFactoryRegister() {
  bolt::memory::MemoryArbitrator::unregisterFactory(kind_);
}

BoltMemoryManager::BoltMemoryManager(
    const std::string& kind,
    std::unique_ptr<AllocationListener> listener,
    const bytedance::bolt::config::ConfigBase& backendConf,
    const std::string& name)
    : MemoryManager(kind, MemoryManagerOptions{name}), listener_(std::move(listener)) {
  auto reservationBlockSize =
      backendConf.get<uint64_t>(kMemoryReservationBlockSize, kMemoryReservationBlockSizeDefault);
  blockListener_ = std::make_unique<BlockAllocationListener>(listener_.get(), reservationBlockSize);
  defaultArrowPool_ = std::make_shared<ArrowMemoryPool>(blockListener_.get());
  arrowPools_.emplace("default", defaultArrowPool_);

  auto checkUsageLeak = backendConf.get<bool>(kCheckUsageLeak, kCheckUsageLeakDefault);

  ArbitratorFactoryRegister afr(listener_.get());
  bolt::memory::MemoryManager::Options mmOptions;
  mmOptions.alignment = bolt::memory::MemoryAllocator::kMaxAlignment;
  mmOptions.trackDefaultUsage = true; // memory usage tracking
  mmOptions.checkUsageLeak = checkUsageLeak; // leak check
  mmOptions.coreOnAllocationFailureEnabled = false;
  mmOptions.allocatorCapacity = bolt::memory::kMaxMemory;
  mmOptions.arbitratorKind = afr.getKind();
  mmOptions.extraArbitratorConfigs = getExtraArbitratorConfigs(backendConf);
  boltMemoryManager_ = std::make_unique<bolt::memory::MemoryManager>(mmOptions);

  boltAggregatePool_ = boltMemoryManager_->addRootPool(
      "root",
      bolt::memory::kMaxMemory, // the 3rd capacity
      bytedance::bolt::memory::MemoryReclaimer::create());

  boltLeafPool_ = boltAggregatePool_->addLeafChild("default_leaf");
}

namespace {
MemoryUsageStats collectBoltMemoryUsageStats(const bolt::memory::MemoryPool* pool) {
  MemoryUsageStats stats;
  stats.set_current(pool->usedBytes());
  stats.set_peak(pool->peakBytes());
  // walk down root and all children
  pool->visitChildren([&](bolt::memory::MemoryPool* pool) -> bool {
    stats.mutable_children()->emplace(pool->name(), collectBoltMemoryUsageStats(pool));
    return true;
  });
  return stats;
}

MemoryUsageStats collectGlutenAllocatorMemoryUsageStats(
    const std::unordered_map<std::string, std::weak_ptr<ArrowMemoryPool>>& arrowPools) {
  MemoryUsageStats stats;
  int64_t totalBytes = 0;
  int64_t peakBytes = 0;

  for (const auto& [name, ptr] : arrowPools) {
    auto pool = ptr.lock();
    if (pool == nullptr) {
      continue;
    }

    MemoryUsageStats poolStats;
    const auto allocated = pool->bytes_allocated();
    const auto peak = pool->max_memory();
    poolStats.set_current(allocated);
    poolStats.set_peak(peak);

    stats.mutable_children()->emplace(name, poolStats);

    totalBytes += allocated;
    peakBytes = std::max(peakBytes, peak);
  }

  stats.set_current(totalBytes);
  stats.set_peak(peakBytes);
  return stats;
}

void logMemoryUsageStats(
    MemoryUsageStats stats,
    const std::string& name,
    const std::string& logPrefix,
    std::stringstream& ss) {
  ss << logPrefix << "+- " << name << " (used: " << bolt::succinctBytes(stats.current())
     << ", peak: " << bolt::succinctBytes(stats.peak()) << ")\n";
  if (stats.children_size() > 0) {
    for (auto it = stats.children().begin(); it != stats.children().end(); ++it) {
      logMemoryUsageStats(it->second, it->first, logPrefix + "   ", ss);
    }
  }
}

int64_t shrinkBoltMemoryPool(bolt::memory::MemoryManager* mm, bolt::memory::MemoryPool* pool, int64_t size) {
  std::string poolName{pool->root()->name() + "/" + pool->name()};
  std::string logPrefix{"Shrink[" + poolName + "]: "};
  VLOG(2) << logPrefix << "Trying to shrink " << size << " bytes of data...";
  VLOG(2) << logPrefix << "Pool has reserved " << pool->usedBytes() << "/" << pool->root()->reservedBytes() << "/"
          << pool->root()->capacity() << "/" << pool->root()->maxCapacity() << " bytes.";
  if (VLOG_IS_ON(2)) {
    std::stringstream ss;
    ss << logPrefix << "Bolt memory usage stats:\n";
    logMemoryUsageStats(collectBoltMemoryUsageStats(pool), poolName, logPrefix, ss);
    VLOG(2) << ss.str();
  }
  VLOG(2) << logPrefix << "Shrinking...";
  auto shrunken = mm->arbitrator()->shrinkCapacity(pool, 0);
  VLOG(2) << logPrefix << shrunken << " bytes released from shrinking.";
  return shrunken;
}
} // namespace

std::shared_ptr<arrow::MemoryPool> BoltMemoryManager::getOrCreateArrowMemoryPool(const std::string& name) {
  std::lock_guard<std::mutex> l(mutex_);
  if (const auto it = arrowPools_.find(name); it != arrowPools_.end()) {
    if (auto pool = it->second.lock()) {
      return pool;
    }
    arrowPools_.erase(name);
  }

  auto pool = std::make_shared<ArrowMemoryPool>(blockListener_.get());
  arrowPools_.emplace(name, pool);
  return pool;
}

const MemoryUsageStats BoltMemoryManager::collectMemoryUsageStats() const {
  MemoryUsageStats stats;
  stats.set_current(listener_->currentBytes());
  stats.set_peak(listener_->peakBytes());
  stats.mutable_children()->emplace("gluten::MemoryAllocator", collectGlutenAllocatorMemoryUsageStats(arrowPools_));
  stats.mutable_children()->emplace(boltAggregatePool_->name(), collectBoltMemoryUsageStats(boltAggregatePool_.get()));
  return stats;
}

const int64_t BoltMemoryManager::shrink(int64_t size) {
  return shrinkBoltMemoryPool(boltMemoryManager_.get(), boltAggregatePool_.get(), size);
}

namespace {
void holdInternal(
    std::vector<std::shared_ptr<bytedance::bolt::memory::MemoryPool>>& heldBoltPools,
    const bolt::memory::MemoryPool* pool) {
  pool->visitChildren([&](bolt::memory::MemoryPool* child) -> bool {
    auto shared = child->shared_from_this();
    heldBoltPools.push_back(shared);
    holdInternal(heldBoltPools, child);
    return true;
  });
}
} // namespace

void BoltMemoryManager::hold() {
  holdInternal(heldBoltPools_, boltAggregatePool_.get());
}

void BoltMemoryManager::hold(const MemoryManagerLifecycleContext& context) {
  hold();
  if (BoltGlutenMemoryManager::enabled()) {
    auto holder = BoltGlutenMemoryManager::getMemoryManagerHolder(
        context.name.empty() ? name() : context.name, context.taskAttemptId, reinterpret_cast<int64_t>(this));
    holder->hold();
  }
}

void BoltMemoryManager::beforeRelease(const MemoryManagerLifecycleContext& context) {
  if (BoltGlutenMemoryManager::enabled()) {
    BoltGlutenMemoryManager::destroy(context.taskAttemptId, reinterpret_cast<int64_t>(this));
  }
}

bool BoltMemoryManager::tryDestructSafe() {
  // Bolt memory pools considered safe to destruct when no alive allocations.
  for (const auto& pool : heldBoltPools_) {
    if (pool && pool->usedBytes() != 0) {
      return false;
    }
  }
  if (boltLeafPool_ && boltLeafPool_->usedBytes() != 0) {
    return false;
  }
  if (boltAggregatePool_ && boltAggregatePool_->usedBytes() != 0) {
    return false;
  }
  heldBoltPools_.clear();
  boltLeafPool_.reset();
  boltAggregatePool_.reset();

  // Bolt memory manager considered safe to destruct when no alive pools.
  if (boltMemoryManager_) {
    if (boltMemoryManager_->numPools() > 3) {
      VLOG(2) << "Attempt to destruct BoltMemoryManager failed because there are " << boltMemoryManager_->numPools()
              << " outstanding memory pools.";
      return false;
    }
    if (boltMemoryManager_->numPools() == 3) {
      // Assert the pool is spill pool
      int32_t spillPoolCount = 0;
      int32_t cachePoolCount = 0;
      int32_t tracePoolCount = 0;
      boltMemoryManager_->deprecatedSysRootPool().visitChildren([&](bolt::memory::MemoryPool* child) -> bool {
        if (child == boltMemoryManager_->spillPool()) {
          spillPoolCount++;
        }
        if (child == boltMemoryManager_->cachePool()) {
          cachePoolCount++;
        }
        if (child == boltMemoryManager_->tracePool()) {
          tracePoolCount++;
        }
        return true;
      });
      GLUTEN_CHECK(spillPoolCount == 1, "Illegal pool count state: spillPoolCount: " + std::to_string(spillPoolCount));
      GLUTEN_CHECK(cachePoolCount == 1, "Illegal pool count state: cachePoolCount: " + std::to_string(cachePoolCount));
      GLUTEN_CHECK(tracePoolCount == 1, "Illegal pool count state: tracePoolCount: " + std::to_string(tracePoolCount));
    }
    if (boltMemoryManager_->numPools() < 3) {
      GLUTEN_CHECK(false, "Unreachable code");
    }
  }
  boltMemoryManager_.reset();

  // Applies similar rule for Arrow memory pool.
  if (!arrowPools_.empty() && std::any_of(arrowPools_.begin(), arrowPools_.end(), [&](const auto& entry) {
        auto pool = entry.second.lock();
        if (pool == nullptr) {
          return false;
        }
        return pool->bytes_allocated() != 0;
      })) {
    VLOG(2) << "Attempt to destruct BoltMemoryManager failed because there are still outstanding Arrow memory pools.";
    return false;
  }
  arrowPools_.clear();

  // Successfully destructed.
  return true;
}

BoltMemoryManager::~BoltMemoryManager() {
  static const uint32_t kWaitTimeoutMs = FLAGS_gluten_bolt_async_timeout_on_task_stopping; // 30s by default
  uint32_t accumulatedWaitMs = 0UL;
  bool destructed = false;
  for (int32_t tryCount = 0; accumulatedWaitMs < kWaitTimeoutMs; tryCount++) {
    destructed = tryDestructSafe();
    if (destructed) {
      if (tryCount > 0) {
        LOG(INFO) << "All the outstanding memory resources successfully released. ";
      }
      break;
    }
    uint32_t waitMs = 50 * static_cast<uint32_t>(pow(1.5, tryCount)); // 50ms, 75ms, 112.5ms ...
    LOG(INFO) << "There are still outstanding Bolt memory allocations. Waiting for " << waitMs
              << " ms to let possible async tasks done... ";
    usleep(waitMs * 1000);
    accumulatedWaitMs += waitMs;
  }
  if (!destructed) {
    LOG(ERROR) << "Failed to release Bolt memory manager after " << accumulatedWaitMs
               << "ms as there are still outstanding memory resources. ";
  }
#ifdef ENABLE_JEMALLOC_STATS
  malloc_stats_print(NULL, NULL, NULL);
#endif
}

BoltMemoryManager* getDefaultMemoryManager() {
  return BoltBackend::get()->getGlobalMemoryManager();
}

std::shared_ptr<bolt::memory::MemoryPool> defaultLeafBoltMemoryPool() {
  return getDefaultMemoryManager()->getLeafMemoryPool();
}

} // namespace gluten

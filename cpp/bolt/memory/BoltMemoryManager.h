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

#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/MemoryPool.h"
#include "memory/AllocationListener.h"
#include "memory/ArrowMemoryPool.h"
#include "memory/MemoryAllocator.h"
#include "memory/MemoryManager.h"

#include <bolt/common/config/Config.h>

namespace gluten {

constexpr std::string_view kMemoryPoolInitialCapacity{"memory-pool-initial-capacity"};
constexpr uint64_t kDefaultMemoryPoolInitialCapacity{256 << 20};
constexpr std::string_view kMemoryPoolTransferCapacity{"memory-pool-transfer-capacity"};
constexpr uint64_t kDefaultMemoryPoolTransferCapacity{128 << 20};
constexpr std::string_view kMemoryReclaimMaxWaitMs{"memory-reclaim-max-wait-time"};
constexpr std::string_view kDefaultMemoryReclaimMaxWaitMs{"3600000ms"};

std::unordered_map<std::string, std::string> getExtraArbitratorConfigs(
    const bytedance::bolt::config::ConfigBase& backendConf);

class ArbitratorFactoryRegister {
 public:
  explicit ArbitratorFactoryRegister(gluten::AllocationListener* listener);

  virtual ~ArbitratorFactoryRegister();

  const std::string& getKind() const {
    return kind_;
  }

 private:
  std::string kind_;
  gluten::AllocationListener* listener_;
};

// Make sure the class is thread safe
class BoltMemoryManager final : public MemoryManager {
 public:
  BoltMemoryManager(
      const std::string& kind,
      std::unique_ptr<AllocationListener> listener,
      const bytedance::bolt::config::ConfigBase& backendConf,
      const std::string& name);

  ~BoltMemoryManager() override;
  BoltMemoryManager(const BoltMemoryManager&) = delete;
  BoltMemoryManager(BoltMemoryManager&&) = delete;
  BoltMemoryManager& operator=(const BoltMemoryManager&) = delete;
  BoltMemoryManager& operator=(BoltMemoryManager&&) = delete;

  std::shared_ptr<bytedance::bolt::memory::MemoryPool> getAggregateMemoryPool() const {
    return boltAggregatePool_;
  }

  std::shared_ptr<bytedance::bolt::memory::MemoryPool> getLeafMemoryPool() const {
    return boltLeafPool_;
  }

  bytedance::bolt::memory::MemoryManager* getMemoryManager() const {
    return boltMemoryManager_.get();
  }

  arrow::MemoryPool* defaultArrowMemoryPool() override {
    return defaultArrowPool_.get();
  }

  std::shared_ptr<arrow::MemoryPool> getOrCreateArrowMemoryPool(const std::string& name) override;

  const MemoryUsageStats collectMemoryUsageStats() const override;

  const int64_t shrink(int64_t size) override;

  void hold() override;

  void hold(const MemoryManagerLifecycleContext& context) override;

  void beforeRelease(const MemoryManagerLifecycleContext& context) override;

  /// Test only
  MemoryAllocator* allocator() const {
    return defaultArrowPool_->allocator();
  }

  AllocationListener* getListener() const {
    return listener_.get();
  }

 private:
  bool tryDestructSafe();

  std::unique_ptr<AllocationListener> listener_;
  std::unique_ptr<AllocationListener> blockListener_;

  std::shared_ptr<ArrowMemoryPool> defaultArrowPool_;
  std::unordered_map<std::string, std::weak_ptr<ArrowMemoryPool>> arrowPools_;

  std::unique_ptr<bytedance::bolt::memory::MemoryManager> boltMemoryManager_;
  std::shared_ptr<bytedance::bolt::memory::MemoryPool> boltAggregatePool_;
  std::shared_ptr<bytedance::bolt::memory::MemoryPool> boltLeafPool_;
  std::vector<std::shared_ptr<bytedance::bolt::memory::MemoryPool>> heldBoltPools_;

  std::mutex mutex_;
};

BoltMemoryManager* getDefaultMemoryManager();

std::shared_ptr<bytedance::bolt::memory::MemoryPool> defaultLeafBoltMemoryPool();

} // namespace gluten

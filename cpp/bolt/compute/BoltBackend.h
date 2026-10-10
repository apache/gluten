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

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <cstdint>
#include <filesystem>
#include <tuple>

#include "bolt/common/caching/AsyncDataCache.h"
#include "bolt/common/config/Config.h"
#include "bolt/common/memory/MmapAllocator.h"

#include "memory/BoltMemoryManager.h"

namespace gluten {

// This kind string must be same with BoltBackend#name in java side.
inline static const std::string kBoltBackendKind{"bolt"};
/// As a static instance in per executor, initialized at executor startup.
/// Should not put heavily work here.
class BoltBackend {
 public:
  ~BoltBackend() {
    // multi-thread spark
    if (driverExecutor_) {
      driverExecutor_.reset();
    }
  }

  static void create(
      std::unique_ptr<AllocationListener> listener,
      const std::unordered_map<std::string, std::string>& conf);

  static BoltBackend* get();

  bytedance::bolt::cache::AsyncDataCache* getAsyncDataCache() const;

  std::shared_ptr<bytedance::bolt::config::ConfigBase> getBackendConf() const {
    return backendConf_;
  }

  folly::CPUThreadPoolExecutor* getDriverExecutor() const {
    return driverExecutor_.get();
  }

  folly::IOThreadPoolExecutor* getIOExecutor() const {
    return ioExecutor_.get();
  }

  BoltMemoryManager* getGlobalMemoryManager() const {
    return globalMemoryManager_.get();
  }

  void tearDown();

  static std::tuple<int32_t, int32_t, int32_t, int64_t, int32_t> getScanPreloadAdaptiveParam(
      const std::shared_ptr<bytedance::bolt::config::ConfigBase>& conf,
      bool log = false);

  // return sessionConf combined with backendConf, sessionConf has higher priority
  static std::shared_ptr<bytedance::bolt::config::ConfigBase> getCombinedConf(
      const std::shared_ptr<bytedance::bolt::config::ConfigBase>& sessionConf) {
    auto conf = BoltBackend::get()->getBackendConf()->rawConfigsCopy();
    for (const auto& [k, v] : sessionConf->rawConfigs()) {
      conf[k] = v;
    }
    return std::make_shared<bytedance::bolt::config::ConfigBase>(std::move(conf));
  }

 private:
  explicit BoltBackend(
      std::unique_ptr<AllocationListener> listener,
      const std::unordered_map<std::string, std::string>& conf) {
    init(std::move(listener), conf);
  }

  void init(std::unique_ptr<AllocationListener> listener, const std::unordered_map<std::string, std::string>& conf);
  void initCache();
  void initConnector(const std::shared_ptr<bytedance::bolt::config::ConfigBase>& hiveConf);
  void initUdf();
  std::unique_ptr<bytedance::bolt::cache::SsdCache> initSsdCache(uint64_t ssdSize);

  void initJolFilesystem();

  std::string getCacheFilePrefix() {
    return "cache." + boost::lexical_cast<std::string>(boost::uuids::random_generator()()) + ".";
  }

  static std::unique_ptr<BoltBackend> instance_;

  // A global Bolt memory manager for the current process.
  std::unique_ptr<BoltMemoryManager> globalMemoryManager_;
  // Instance of AsyncDataCache used for all large allocations.
  std::shared_ptr<bytedance::bolt::cache::AsyncDataCache> asyncDataCache_;

  //
  std::shared_ptr<folly::CPUThreadPoolExecutor> driverExecutor_;

  std::unique_ptr<folly::IOThreadPoolExecutor> ssdCacheExecutor_;
  std::unique_ptr<folly::IOThreadPoolExecutor> ioExecutor_;
  std::shared_ptr<bytedance::bolt::memory::MmapAllocator> cacheAllocator_;

  std::string cachePathPrefix_;
  std::string cacheFilePrefix_;

  std::shared_ptr<bytedance::bolt::config::ConfigBase> backendConf_;
};

} // namespace gluten

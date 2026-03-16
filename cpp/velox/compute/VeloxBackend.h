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
#include <folly/executors/IOThreadPoolExecutor.h>
#include <filesystem>

#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/config/Config.h"
#include "velox/common/memory/MmapAllocator.h"

#include "jni/JniHashTable.h"
#include "memory/VeloxMemoryManager.h"

namespace gluten {

// This kind string must be same with VeloxBackend#name in java side.
inline static const std::string kVeloxBackendKind{"velox"};
/// As a static instance in per executor, initialized at executor startup.
/// Should not put heavily work here.
class VeloxBackend {
 public:
  ~VeloxBackend() {}

  static void create(
      std::unique_ptr<AllocationListener> listener,
      const std::unordered_map<std::string, std::string>& conf);

  static VeloxBackend* get();

  facebook::velox::cache::AsyncDataCache* getAsyncDataCache() const;

  std::shared_ptr<facebook::velox::config::ConfigBase> getBackendConf() const {
    return backendConf_;
  }

  VeloxMemoryManager* getGlobalMemoryManager() const {
    return globalMemoryManager_.get();
  }

  folly::Executor* executor() const {
    return ioExecutor_.get();
  }

  void tearDown();

 private:
  explicit VeloxBackend(
      std::unique_ptr<AllocationListener> listener,
      const std::unordered_map<std::string, std::string>& conf) {
    init(std::move(listener), conf);
  }

  void init(std::unique_ptr<AllocationListener> listener, const std::unordered_map<std::string, std::string>& conf);
  void initCache();
  void initConnector(const std::shared_ptr<facebook::velox::config::ConfigBase>& hiveConf);
  void initUdf();
  std::unique_ptr<facebook::velox::cache::SsdCache> initSsdCache(uint64_t ssdSize);

  void initJolFilesystem();

  std::string getCacheFilePrefix() {
    return "cache." + boost::lexical_cast<std::string>(boost::uuids::random_generator()()) + ".";
  }

  static std::unique_ptr<VeloxBackend> instance_;

  // A global Velox memory manager for the current process.
  std::unique_ptr<VeloxMemoryManager> globalMemoryManager_;
  // Instance of AsyncDataCache used for all large allocations.
  std::shared_ptr<facebook::velox::cache::AsyncDataCache> asyncDataCache_;

  std::unique_ptr<folly::IOThreadPoolExecutor> ssdCacheExecutor_;
  std::unique_ptr<folly::IOThreadPoolExecutor> ioExecutor_;
  std::shared_ptr<facebook::velox::memory::MmapAllocator> cacheAllocator_;

  std::string cachePathPrefix_;
  std::string cacheFilePrefix_;

  std::shared_ptr<facebook::velox::config::ConfigBase> backendConf_;
};

} // namespace gluten

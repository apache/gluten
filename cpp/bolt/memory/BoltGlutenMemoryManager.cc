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

#include "memory/BoltGlutenMemoryManager.h"

#include <fmt/core.h>
#include <fmt/format.h>
#include <folly/String.h>
#include <unistd.h>
#include <bolt/common/base/Exceptions.h>
#include <bolt/common/memory/MemoryUtils.h>
#include <bolt/common/memory/sparksql/ConfigurationResolver.h>
#include <bolt/common/memory/sparksql/MemoryTarget.h>
#include <bolt/common/memory/sparksql/OomPrinter.h>
#include <bolt/common/memory/sparksql/Spiller.h>
#include <bolt/common/config/Config.h>
#include <bolt/common/process/StackTrace.h>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <utility>
#include <vector>

#include "compute/ResultIterator.h"
#include "memory/OnHeapUsageGetter.h"
#include "shuffle/ShuffleWriterBase.h"

using bytedance::bolt::memory::sparksql::ExecutionMemoryPool;

namespace gluten {

int64_t OperatorSpiller::spill(bytedance::bolt::memory::sparksql::MemoryTargetWeakPtr self, int64_t size) {
  auto it = iterator_.lock();
  if (it) {
    return it->spillFixedSize(size);
  }
  return 0;
}

const std::set<bytedance::bolt::memory::sparksql::SpillerPhase>& OperatorSpiller::applicablePhases() {
  return bytedance::bolt::memory::sparksql::SpillerHelper::phaseSetAll();
}

int64_t ShuffleSpiller::spill(bytedance::bolt::memory::sparksql::MemoryTargetWeakPtr self, int64_t size) {
  auto writer = shuffleWriter_.lock();
  if (writer) {
    int64_t actual = 0;
    gluten::arrowAssertOkOrThrow(writer->reclaimFixedSize(size, &actual));
    return actual;
  }
  return 0;
}

const std::set<bytedance::bolt::memory::sparksql::SpillerPhase>& ShuffleSpiller::applicablePhases() {
  return bytedance::bolt::memory::sparksql::SpillerHelper::phaseSetSpillOnly();
}

namespace {
void dynamicMemoryQuotaManagerConf(
    std::unordered_map<std::string, std::string> conf,
    bytedance::bolt::memory::sparksql::DynamicMemoryQuotaManagerOption& option,
    int64_t capacity) {
  using bytedance::bolt::memory::sparksql::ConfigurationResolver;

  option.enable = ConfigurationResolver::getBoolParamFromConf(
      conf,
      ConfigurationResolver::kDynamicMemoryQuotaManager,
      ConfigurationResolver::kDynamicMemoryQuotaManagerDefaultValue);
  std::string confValue = ConfigurationResolver::getStringParamFromConf(
      conf,
      ConfigurationResolver::kDynamicMemoryQuotaManagerRatios,
      ConfigurationResolver::kDynamicMemoryQuotaManagerRatiosDefaultValue);
  const char delimeter = confValue.back();
  confValue.pop_back();

  std::vector<std::string> ratioList;
  folly::split(delimeter, confValue, ratioList);

  using bytedance::bolt::memory::sparksql::DynamicMemoryQuotaManagerOption;

  GLUTEN_CHECK(
      ratioList.size() == DynamicMemoryQuotaManagerOption::RatioKey::KEY_NUM,
      fmt::format(
          "spark.gluten.boltMemoryManager.dynamicMemoryQuotaManager.ratios paramter"
          " format is wrong! ratioList is {}, expect size is {}",
          fmt::join(ratioList, ","),
          static_cast<int>(DynamicMemoryQuotaManagerOption::RatioKey::KEY_NUM)));

  option.quotaTriggerRatio =
      folly::to<double>(ratioList[DynamicMemoryQuotaManagerOption::RatioKey::kQuotaTriggerRatio]);
  option.rssMinRatio = folly::to<double>(ratioList[DynamicMemoryQuotaManagerOption::RatioKey::kRssMinRatio]);
  option.rssMaxRatio = folly::to<double>(ratioList[DynamicMemoryQuotaManagerOption::RatioKey::kRssMaxRatio]);
  option.extendMinRatio = folly::to<double>(ratioList[DynamicMemoryQuotaManagerOption::RatioKey::kExtendMinRatio]);
  option.extendMaxRatio = folly::to<double>(ratioList[DynamicMemoryQuotaManagerOption::RatioKey::kExtendMaxRatio]);
  option.extendScaleRatio = folly::to<double>(ratioList[DynamicMemoryQuotaManagerOption::RatioKey::kExtendScaleRatio]);
  option.sampleRatio = folly::to<double>(ratioList[DynamicMemoryQuotaManagerOption::RatioKey::kSampleRatio]);
  option.sampleSize = option.sampleRatio * capacity;
  option.changeThresholdRatio =
      folly::to<double>(ratioList[DynamicMemoryQuotaManagerOption::RatioKey::kChangeThresholdRatio]);
  option.logPrintFreq = folly::to<double>(ratioList[DynamicMemoryQuotaManagerOption::RatioKey::kLogPrintFreq]);
}
} // namespace

int BoltGlutenMemoryManager::getMaxTaskNumber(const std::unordered_map<std::string, std::string>& conf) {
  using bytedance::bolt::memory::sparksql::ConfigurationResolver;

  auto cores = ConfigurationResolver::getIntParamFromConf(
      conf, ConfigurationResolver::kSparkExecutorCores, ConfigurationResolver::kSparkExecutorCoresDefaultValue);
  auto ratio = ConfigurationResolver::getIntParamFromConf(
      conf, ConfigurationResolver::kSparkVCoreBoostRatio, ConfigurationResolver::kSparkVCoreBoostRatioDefaultValue);
  auto maxTaskNumber = cores * ratio;
  return maxTaskNumber;
}

int64_t BoltGlutenMemoryManager::getTaskMemoryCapacity(const std::unordered_map<std::string, std::string>& conf) {
  using bytedance::bolt::memory::sparksql::ConfigurationResolver;

  const char* specialCapacity = "0";
  int64_t capacity = ConfigurationResolver::getIntParamFromConf(
      conf, ConfigurationResolver::kMemoryOffHeapBytes, specialCapacity);
  if (capacity == 0) {
    std::string anotherCapacity = ConfigurationResolver::getStringParamFromConf(
        conf, ConfigurationResolver::kMemoryOffHeap, specialCapacity);
    capacity = bytedance::bolt::config::toCapacity(anotherCapacity, bytedance::bolt::config::CapacityUnit::BYTE);
  }
  if (capacity <= 0) {
    LOG(ERROR) << "Unexpected BoltMemoryManager's capacity, capacity is: " << capacity << ", stack is: " << bytedance::bolt::process::StackTrace().toString();
  }
  GLUTEN_CHECK(capacity > 0, "BoltMemoryManager expects capacity is bigger than 0");
  return capacity;
}

void BoltGlutenMemoryManager::init(const std::unordered_map<std::string, std::string>& conf) {
  std::lock_guard<std::mutex> guard(lock_);
  using bytedance::bolt::memory::sparksql::ConfigurationResolver;

  if (!hasInit_) {
    conf_ = conf;
    hasInit_ = true;

    std::string confStr;
    for (const auto& pair : conf_) {
      confStr += ("<key=" + pair.first + ", value=" + pair.second + ">");
    }
    LOG(INFO) << confStr;
    enabled_ = ConfigurationResolver::getBoolParamFromConf(
        conf_, ConfigurationResolver::kUseBoltMemoryManager, ConfigurationResolver::kUseBoltMemoryManagerDefaultValue);

    const int64_t capacity = getTaskMemoryCapacity(conf);
    int64_t minMemoryMaxWaitMs = ConfigurationResolver::getIntParamFromConf(
        conf_,
        ConfigurationResolver::kExecutionPoolMinMemoryMaxWaitMs,
        ConfigurationResolver::kExecutionPoolMinMemoryMaxWaitMsDefaultValue);

    LOG(INFO) << "BoltMemoryManager capacity=" << capacity << ", useBoltMemoryManager enabled is "
              << (enabled_ ? "true" : "false") << ", minMemoryMaxWaitMs = " << minMemoryMaxWaitMs << "ms";

    bytedance::bolt::memory::sparksql::DynamicMemoryQuotaManagerOption option;
    dynamicMemoryQuotaManagerConf(conf_, option, capacity);

    maxTaskNumber_ = getMaxTaskNumber(conf);

    // Always call ExecutionMemoryPool::init to facilitate other functions to call
    // ExecutionMemoryPool::getAvailableMemoryPerTask. Whether to enable BoltMemoryManager
    // depends on the passed enable parameter.
    ExecutionMemoryPool::init(enabled_, capacity, maxTaskNumber_, option, 0);

    // Some operations that must be performed after BoltMemoryManager is enabled need to
    // be placed in the following code.
    if (enabled_) {
      bytedance::bolt::memory::sparksql::OomPrinter::linkHolders(&holders_);
      bytedance::bolt::memory::MemoryUtils::hook = OnHeapMemUsedHookSetter::getOnHeapUsedMemory;
    }
  }
}

bool BoltGlutenMemoryManager::enabled() {
  return enabled_;
}

bytedance::bolt::memory::sparksql::BoltMemoryManagerHolder*
BoltGlutenMemoryManager::getMemoryManagerHolder(const std::string& name, int64_t taskId, int64_t memoryManagerHandle) {
  std::lock_guard<std::mutex> guard(lock_);

  GLUTEN_CHECK(enabled_, "Expect BoltMemoryManager enabled");

  bytedance::bolt::memory::sparksql::BoltMemoryManagerHolderKey key{
      .name = name, .taskId = taskId, .memoryManagerHandle = memoryManagerHandle};

  auto it = holders_.find(key);
  if (it != holders_.end()) {
    return it->second;
  } else {
    // must keep this pattern, otherwise OOM Printer will not work
    const std::string nmmName = fmt::format("{}_TID_{}_HANDLER_{}", name, taskId, memoryManagerHandle);

    auto tmm = getOrCreateTaskMemoryManagerLocked(taskId);
    bytedance::bolt::memory::sparksql::NativeMemoryManagerFactoryParam param{
        .name = nmmName,
        .memoryIsolation = false,
        .conservativeTaskOffHeapMemorySize = 0,
        .overAcquiredRatio = 0.3,
        .taskMemoryManager = tmm,
        .sessionConf = conf_};
    bytedance::bolt::memory::sparksql::BoltMemoryManagerHolder* holder =
        bytedance::bolt::memory::sparksql::NativeMemoryManagerFactory::contextInstance(param);
    holders_.emplace(
        bytedance::bolt::memory::sparksql::BoltMemoryManagerHolderKey{
            .name = name, .taskId = taskId, .memoryManagerHandle = memoryManagerHandle},
        holder);

    return holder;
  }
}

void BoltGlutenMemoryManager::destroy(int64_t taskId, int64_t memoryManagerHandle) {
  GLUTEN_CHECK(enabled_, "Expect BoltMemoryManager enabled");

  int64_t holderCount = 0, deleteCount = 0;
  std::vector<bytedance::bolt::memory::sparksql::BoltMemoryManagerHolderKey> toBeDeletedKey;
  std::vector<bytedance::bolt::memory::sparksql::BoltMemoryManagerHolder*> toBeDeletedHolders;
  {
    std::lock_guard<std::mutex> guard(lock_);
    for (const auto& pair : holders_) {
      if (pair.first.taskId == taskId) {
        holderCount++;
        if (pair.first.memoryManagerHandle == memoryManagerHandle) {
          toBeDeletedKey.emplace_back(pair.first);
          toBeDeletedHolders.emplace_back(pair.second);
        }
      }
    }
    deleteCount = toBeDeletedKey.size();
    for (const auto& key : toBeDeletedKey) {
      holders_.erase(key);
    }
  }

  // delete holder out of mutex because BoltMemoryManager destruction may wait for memory release,
  // which will block all tasks' destroying in current executor
  for (auto hold : toBeDeletedHolders) {
    if (hold != nullptr) {
      delete hold;
    }
  }

  // If the value of holderCount euqals to 1 and `deleteCount` euqals to 1, it **may be** that the task execution has
  // ended and the corresponding cleanup operation needs to be performed.
  //
  // If the task execution has not ended, there will be no side effects, because a new
  // TaskMemoryManager will be created in the getOrCreateTaskMemoryManagerLocked function.
  if (holderCount == 1 && deleteCount == 1) {
    std::lock_guard<std::mutex> guard(lock_);
    auto tmm = tmmCache_.find(taskId);
    if (tmm != tmmCache_.end()) {
      bytedance::bolt::memory::sparksql::MemoryTargetBuilder::invalidate(tmm->second, false, 0);
    }

    tmmCache_.erase(taskId);

    ExecutionMemoryPool::instance()->releaseAllMemoryForTask(taskId);
  }
}

bytedance::bolt::memory::sparksql::TaskMemoryManagerPtr BoltGlutenMemoryManager::getOrCreateTaskMemoryManagerLocked(
    int64_t taskId) {
  auto it = tmmCache_.find(taskId);
  if (it == tmmCache_.end()) {
    auto tmm =
        std::make_shared<bytedance::bolt::memory::sparksql::TaskMemoryManager>(ExecutionMemoryPool::instance(), taskId);
    tmmCache_.emplace(taskId, tmm);
    return tmm;
  }
  return it->second;
}

int64_t BoltGlutenMemoryManager::getAvailableMemoryPerTask() {
  auto freeMem = ExecutionMemoryPool::getAvailableMemoryPerTask();
  BOLT_CHECK(freeMem.has_value(), "Expect ExecutionMemoryPool::getAvailableMemoryPerTask return value");
  return freeMem.value();
}

int64_t BoltGlutenMemoryManager::getMinimumFreeMemoryForTask(int64_t taskAttemptId) {
  auto freeMem = ExecutionMemoryPool::getMinimumFreeMemoryForTask(taskAttemptId);
  BOLT_CHECK(freeMem.has_value(), "Expect ExecutionMemoryPool::getMinimumFreeMemoryForTask return value");
  return freeMem.value();
}

} // namespace gluten

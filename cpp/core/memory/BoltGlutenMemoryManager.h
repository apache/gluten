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

#include <cstdint>
#include <memory>
#include <mutex>
#include <utility>

#include <bolt/common/memory/sparksql/ExecutionMemoryPool.h>
#include <bolt/common/memory/sparksql/NativeMemoryManagerFactory.h>
#include <bolt/common/memory/sparksql/Spiller.h>
#include <bolt/common/memory/sparksql/TaskMemoryManager.h>

namespace bytedance::bolt::memory::sparksql {
class MemoryTarget;
using MemoryTargetPtr = std::shared_ptr<MemoryTarget>;
using MemoryTargetWeakPtr = std::weak_ptr<MemoryTarget>;
} // namespace bytedance::bolt::memory::sparksql

namespace gluten {

class ResultIterator;
class ShuffleWriterBase;

class OperatorSpiller final : public bytedance::bolt::memory::sparksql::Spiller {
 public:
  OperatorSpiller(std::weak_ptr<ResultIterator>& iterator) : iterator_(iterator) {}

  int64_t spill(bytedance::bolt::memory::sparksql::MemoryTargetWeakPtr self, int64_t size) override;

  const std::set<bytedance::bolt::memory::sparksql::SpillerPhase>& applicablePhases() override;

 private:
  std::weak_ptr<ResultIterator> iterator_;
};

class ShuffleSpiller final : public bytedance::bolt::memory::sparksql::Spiller {
 public:
  ShuffleSpiller(std::weak_ptr<ShuffleWriterBase>& shuffleWriter) : shuffleWriter_(shuffleWriter) {}

  int64_t spill(bytedance::bolt::memory::sparksql::MemoryTargetWeakPtr self, int64_t size) override;

  const std::set<bytedance::bolt::memory::sparksql::SpillerPhase>& applicablePhases() override;

 private:
  std::weak_ptr<ShuffleWriterBase> shuffleWriter_;
};

class BoltGlutenMemoryManager final {
 public:
  static void init(const std::unordered_map<std::string, std::string>& conf);

  static bool enabled();

  static bytedance::bolt::memory::sparksql::BoltMemoryManagerHolder*
  getMemoryManagerHolder(const std::string& name, int64_t taskId, int64_t memoryManagerHandle);

  static void destroy(int64_t taskId, int64_t memoryManagerHandle);

  static int64_t getAvailableMemoryPerTask();

  static int64_t getMinimumFreeMemoryForTask(int64_t taskAttemptId);

  static int64_t getTaskMemoryCapacity(const std::unordered_map<std::string, std::string>& conf);

  static int getMaxTaskNumber(const std::unordered_map<std::string, std::string>& conf);

 private:
  inline static int maxTaskNumber_{1};

  static bytedance::bolt::memory::sparksql::TaskMemoryManagerPtr getOrCreateTaskMemoryManagerLocked(int64_t taskId);

  inline static bool enabled_{false}, hasInit_{false};
  inline static std::mutex lock_;
  inline static std::unordered_map<std::string, std::string> conf_;
  inline static std::map<int64_t, bytedance::bolt::memory::sparksql::TaskMemoryManagerPtr> tmmCache_;
  inline static std::map<
      bytedance::bolt::memory::sparksql::BoltMemoryManagerHolderKey,
      bytedance::bolt::memory::sparksql::BoltMemoryManagerHolder*>
      holders_;
};

} // namespace gluten
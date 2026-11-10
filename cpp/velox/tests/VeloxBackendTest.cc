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

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/Future.h>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>

#include "compute/VeloxBackend.h"
#include "config/VeloxConfig.h"

namespace gluten {

struct BroadcastBuildThreadsTestParam {
  int taskSlots;
  std::optional<int> buildThreads;
  std::optional<int> expectedThreads;
};

class VeloxBackendTest : public ::testing::TestWithParam<BroadcastBuildThreadsTestParam> {
 protected:
  void SetUp() override {
    const auto& param = GetParam();
    std::unordered_map<std::string, std::string> conf{{kNumTaskSlotsPerExecutor, std::to_string(param.taskSlots)}};
    if (param.buildThreads.has_value()) {
      conf.emplace(kBroadcastHashTableBuildThreads, std::to_string(param.buildThreads.value()));
    }
    VeloxBackend::create(AllocationListener::noop(), conf);
  }

  void TearDown() override {
    VeloxBackend::get()->tearDown();
  }
};

TEST_P(VeloxBackendTest, broadcastHashTableBuildExecutorThreadCount) {
  auto* backend = VeloxBackend::get();
  auto* executor = dynamic_cast<folly::CPUThreadPoolExecutor*>(backend->broadcastHashTableBuildExecutor());
  if (!GetParam().expectedThreads.has_value()) {
    EXPECT_EQ(executor, nullptr);
    return;
  }
  ASSERT_NE(executor, nullptr);
  ASSERT_EQ(executor->numThreads(), GetParam().expectedThreads.value());
  const auto callerThread = std::this_thread::get_id();
  EXPECT_NE(folly::via(executor, [] { return std::this_thread::get_id(); }).get(), callerThread);
}

INSTANTIATE_TEST_SUITE_P(
    BuildThreads,
    VeloxBackendTest,
    ::testing::Values(
        BroadcastBuildThreadsTestParam{0, std::nullopt, std::nullopt},
        BroadcastBuildThreadsTestParam{8, std::nullopt, 8},
        BroadcastBuildThreadsTestParam{0, 0, std::nullopt},
        BroadcastBuildThreadsTestParam{0, 1, std::nullopt},
        BroadcastBuildThreadsTestParam{0, 4, 4},
        BroadcastBuildThreadsTestParam{8, 4, 4}));

} // namespace gluten

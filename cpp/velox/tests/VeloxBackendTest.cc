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
#include <thread>

#include "compute/VeloxBackend.h"
#include "config/VeloxConfig.h"

namespace gluten {

class VeloxBackendTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    VeloxBackend::create(AllocationListener::noop(), {{kNumTaskSlotsPerExecutor, "0"}});
  }

  static void TearDownTestSuite() {
    VeloxBackend::get()->tearDown();
  }
};

TEST_F(VeloxBackendTest, broadcastHashTableBuildExecutorWithZeroTaskSlots) {
  auto* backend = VeloxBackend::get();
  ASSERT_EQ(backend->ioExecutor(), nullptr);

  auto* executor = dynamic_cast<folly::CPUThreadPoolExecutor*>(backend->broadcastHashTableBuildExecutor());
  ASSERT_NE(executor, nullptr);
  ASSERT_EQ(executor->numThreads(), 1);
  const auto callerThread = std::this_thread::get_id();
  EXPECT_NE(folly::via(executor, [] { return std::this_thread::get_id(); }).get(), callerThread);
}

} // namespace gluten

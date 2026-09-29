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

#include "cudf/GpuLock.h"

#include <atomic>
#include <chrono>
#include <future>
#include <stdexcept>
#include <thread>

#include <gtest/gtest.h>

namespace gluten {

TEST(GpuLockTest, configureRejectsZeroConcurrency) {
  configureGpuTaskConcurrency(1);
  EXPECT_THROW(configureGpuTaskConcurrency(0), std::invalid_argument);
}

TEST(GpuLockTest, sameThreadLockIsReentrant) {
  configureGpuTaskConcurrency(1);
  lockGpu();
  EXPECT_TRUE(tryLockGpu());

  unlockGpu();
  EXPECT_TRUE(tryLockGpu());

  unlockGpu();
}

TEST(GpuLockTest, tryLockFailsWhilePermitHeldByAnotherThread) {
  configureGpuTaskConcurrency(1);
  lockGpu();

  auto acquired = std::async(std::launch::async, [] { return tryLockGpu(); });
  EXPECT_FALSE(acquired.get());

  unlockGpu();
}

TEST(GpuLockTest, lockBlocksUntilPermitIsReleased) {
  configureGpuTaskConcurrency(1);
  lockGpu();

  std::promise<void> workerStarted;
  auto workerStartedFuture = workerStarted.get_future();
  std::atomic<bool> workerAcquired{false};

  std::thread worker([&] {
    workerStarted.set_value();
    EXPECT_FALSE(tryLockGpu());
    lockGpu();
    workerAcquired.store(true);
    unlockGpu();
  });

  workerStartedFuture.wait();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(workerAcquired.load());

  unlockGpu();
  worker.join();

  EXPECT_TRUE(workerAcquired.load());
}

TEST(GpuLockTest, allowsUpToConfiguredConcurrentTasks) {
  configureGpuTaskConcurrency(2);

  std::promise<void> firstLocked;
  std::promise<void> secondLocked;
  auto firstLockedFuture = firstLocked.get_future();
  auto secondLockedFuture = secondLocked.get_future();
  std::promise<void> releaseWorkers;
  auto releaseWorkersFuture = releaseWorkers.get_future().share();
  std::atomic<int> acquiredCount{0};

  std::thread first([&] {
    lockGpu();
    EXPECT_TRUE(tryLockGpu());
    acquiredCount.fetch_add(1);
    firstLocked.set_value();
    releaseWorkersFuture.wait();
    unlockGpu();
  });

  std::thread second([&] {
    lockGpu();
    EXPECT_TRUE(tryLockGpu());
    acquiredCount.fetch_add(1);
    secondLocked.set_value();
    releaseWorkersFuture.wait();
    unlockGpu();
  });

  firstLockedFuture.wait();
  secondLockedFuture.wait();
  EXPECT_EQ(acquiredCount.load(), 2);

  auto thirdAcquire = std::async(std::launch::async, [] { return tryLockGpu(); });
  EXPECT_FALSE(thirdAcquire.get());

  releaseWorkers.set_value();
  first.join();
  second.join();
}

} // namespace gluten

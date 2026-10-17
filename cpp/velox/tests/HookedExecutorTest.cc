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

#include "compute/HookedExecutor.h"

#include <gtest/gtest.h>
#include <atomic>
#include <deque>
#include <functional>
#include <future>
#include <mutex>
#include <stdexcept>
#include <thread>

#include "threads/ThreadInitializer.h"

namespace gluten {
namespace {

using namespace std::chrono_literals;

class QueuedExecutor final : public folly::Executor {
 public:
  void add(folly::Func func) override {
    if (reject) {
      throw std::runtime_error("Rejected submission");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    tasks_.push_back(std::move(func));
  }

  void addWithPriority(folly::Func func, int8_t priority) override {
    lastPriority = priority;
    add(std::move(func));
  }

  uint8_t getNumPriorities() const override {
    return 3;
  }

  folly::Func take() {
    std::lock_guard<std::mutex> lock(mutex_);
    auto func = std::move(tasks_.front());
    tasks_.pop_front();
    return func;
  }

  size_t size() {
    std::lock_guard<std::mutex> lock(mutex_);
    return tasks_.size();
  }

  bool reject{false};
  int8_t lastPriority{0};

 private:
  std::mutex mutex_;
  std::deque<folly::Func> tasks_;
};

struct HookCounts {
  std::atomic<int> initialized{0};
  std::atomic<int> destroyed{0};
};

class CountingInitializer final : public ThreadInitializer {
 public:
  explicit CountingInitializer(std::shared_ptr<HookCounts> counts, bool failInitialization = false)
      : counts_(std::move(counts)), failInitialization_(failInitialization) {}

  void initialize(const std::string&) override {
    ++counts_->initialized;
    if (failInitialization_) {
      throw std::runtime_error("Thread initialization failed");
    }
  }

  void destroy(const std::string&) override {
    ++counts_->destroyed;
  }

 private:
  std::shared_ptr<HookCounts> counts_;
  bool failInitialization_;
};

struct DestructionCallback {
  explicit DestructionCallback(std::function<void()> callback) : callback(std::move(callback)) {}

  ~DestructionCallback() {
    callback();
  }

  std::function<void()> callback;
};

class HookedExecutorTest : public testing::Test {
 protected:
  std::unique_ptr<folly::Executor>
  makeExecutor(bool releaseQueuedOnStop = true, std::chrono::milliseconds timeout = 10s, bool debug = false) {
    return makeHookedExecutor(&parent_, "test", initializer_.get(), debug, timeout, releaseQueuedOnStop);
  }

  QueuedExecutor parent_;
  std::shared_ptr<HookCounts> counts_{std::make_shared<HookCounts>()};
  std::unique_ptr<ThreadInitializer> initializer_{std::make_unique<CountingInitializer>(counts_)};
};

TEST_F(HookedExecutorTest, releasesQueuedCapturesWithoutScheduling) {
  for (bool debug : {false, true}) {
    auto executor = makeExecutor(true, 10s, debug);
    auto resource = std::make_shared<int>(42);
    std::weak_ptr<int> weak = resource;
    executor->add([resource] { FAIL() << "Cancelled callable ran: " << *resource; });
    resource.reset();

    executor.reset();
    EXPECT_TRUE(weak.expired());
    EXPECT_EQ(counts_->initialized, 0);
    ASSERT_EQ(parent_.size(), 1);
    parent_.take()();
    EXPECT_EQ(counts_->destroyed, 0);
  }
}

TEST_F(HookedExecutorTest, cancelledWrappersDoNotUseDestroyedInitializer) {
  auto executor = makeExecutor();
  executor->add([] { FAIL() << "Cancelled callable ran"; });
  executor.reset();
  initializer_.reset();
  parent_.take()();
  EXPECT_EQ(counts_->initialized, 0);
  EXPECT_EQ(counts_->destroyed, 0);
}

TEST_F(HookedExecutorTest, releasesCompletedCapturesBeforeWrapperDestruction) {
  auto executor = makeExecutor();
  auto resource = std::make_shared<int>(42);
  std::weak_ptr<int> weak = resource;
  executor->add([resource] { EXPECT_EQ(*resource, 42); });
  resource.reset();
  auto wrapper = parent_.take();
  wrapper();
  EXPECT_TRUE(weak.expired());
  EXPECT_EQ(counts_->initialized, 1);
  EXPECT_EQ(counts_->destroyed, 1);
  executor.reset();
}

TEST_F(HookedExecutorTest, forwardsPrioritiesAndCancelsPriorityTasks) {
  auto executor = makeExecutor();
  EXPECT_EQ(executor->getNumPriorities(), 3);
  executor->addWithPriority([] {}, 2);
  EXPECT_EQ(parent_.lastPriority, 2);
  parent_.take()();
  executor->addWithPriority([] { FAIL() << "Cancelled priority callable ran"; }, 1);
  executor.reset();
  parent_.take()();
  EXPECT_EQ(counts_->initialized, 1);
  EXPECT_EQ(counts_->destroyed, 1);
}

TEST_F(HookedExecutorTest, cancellationIsScopedToOneRuntime) {
  auto first = makeExecutor();
  auto second = makeExecutor();
  first->add([] { FAIL() << "Cancelled callable ran"; });
  bool ran = false;
  second->add([&] { ran = true; });
  first.reset();
  parent_.take()();
  parent_.take()();
  EXPECT_TRUE(ran);
  second.reset();
  EXPECT_EQ(counts_->initialized, 1);
}

TEST_F(HookedExecutorTest, computeAndSpillKeepQueuedWorkOnTimeout) {
  auto executor = makeExecutor(false, 0ms);
  auto resource = std::make_shared<int>(42);
  std::weak_ptr<int> weak = resource;
  executor->add([resource] { EXPECT_EQ(*resource, 42); });
  resource.reset();
  executor.reset();
  EXPECT_FALSE(weak.expired());
  parent_.take()();
  EXPECT_TRUE(weak.expired());
  EXPECT_EQ(counts_->initialized, 1);
  EXPECT_EQ(counts_->destroyed, 1);
}

TEST_F(HookedExecutorTest, queuedCaptureDestructionCanSubmitMoreWork) {
  auto executor = makeExecutor();
  auto* raw = executor.get();
  auto resource = std::make_shared<int>(42);
  std::weak_ptr<int> weak = resource;
  auto onDestroy = std::make_shared<DestructionCallback>(
      [raw, resource] { raw->add([resource] { FAIL() << "Callable submitted during teardown ran: " << *resource; }); });
  executor->add([onDestroy] { FAIL() << "Cancelled callable ran"; });
  resource.reset();
  onDestroy.reset();
  executor.reset();
  EXPECT_TRUE(weak.expired());
  EXPECT_EQ(parent_.size(), 1);
  parent_.take()();
}

TEST_F(HookedExecutorTest, waitsForRunningWorkAndDiscardsItsLateSubmissions) {
  auto executor = makeExecutor();
  auto* raw = executor.get();
  std::promise<void> started;
  std::promise<void> resume;
  auto resumeFuture = resume.get_future();
  executor->add([&] {
    started.set_value();
    resumeFuture.wait();
    raw->add([] { FAIL() << "Late I/O submission ran"; });
  });
  std::thread worker([func = parent_.take()]() mutable { func(); });
  started.get_future().wait();

  std::promise<void> released;
  auto onDestroy = std::make_shared<DestructionCallback>([&] { released.set_value(); });
  executor->add([onDestroy] { FAIL() << "Queued callable ran"; });
  onDestroy.reset();
  auto stopped = std::async(std::launch::async, [executor = std::move(executor)]() mutable { executor.reset(); });
  EXPECT_EQ(released.get_future().wait_for(2s), std::future_status::ready);
  EXPECT_EQ(stopped.wait_for(0ms), std::future_status::timeout);
  resume.set_value();
  worker.join();
  stopped.get();
  ASSERT_EQ(parent_.size(), 1);
  parent_.take()();
  EXPECT_EQ(counts_->initialized, 1);
  EXPECT_EQ(counts_->destroyed, 1);
}

TEST_F(HookedExecutorTest, runningWorkRetainsBoundedWait) {
  auto executor = makeExecutor(true, 0ms);
  std::promise<void> started;
  std::promise<void> resume;
  auto resumeFuture = resume.get_future();
  executor->add([&] {
    started.set_value();
    resumeFuture.wait();
  });
  std::thread worker([func = parent_.take()]() mutable { func(); });
  started.get_future().wait();
  executor.reset();
  EXPECT_EQ(counts_->destroyed, 0);
  resume.set_value();
  worker.join();
  EXPECT_EQ(counts_->destroyed, 1);
}

TEST_F(HookedExecutorTest, waitsForRunningCaptureDestruction) {
  auto executor = makeExecutor();
  std::promise<void> destroying;
  std::promise<void> resume;
  auto resumeFuture = resume.get_future();
  auto onDestroy = std::make_shared<DestructionCallback>([&] {
    destroying.set_value();
    resumeFuture.wait();
  });
  executor->add([onDestroy] {});
  onDestroy.reset();
  std::thread worker([func = parent_.take()]() mutable { func(); });
  destroying.get_future().wait();

  std::promise<void> released;
  auto queued = std::make_shared<DestructionCallback>([&] { released.set_value(); });
  executor->add([queued] {});
  queued.reset();
  auto stopped = std::async(std::launch::async, [executor = std::move(executor)]() mutable { executor.reset(); });
  EXPECT_EQ(released.get_future().wait_for(2s), std::future_status::ready);
  EXPECT_EQ(stopped.wait_for(0ms), std::future_status::timeout);
  resume.set_value();
  worker.join();
  stopped.get();
  parent_.take()();
  EXPECT_EQ(counts_->destroyed, 1);
}

TEST_F(HookedExecutorTest, rejectedSubmissionReleasesCaptures) {
  auto executor = makeExecutor();
  parent_.reject = true;
  auto resource = std::make_shared<int>(42);
  std::weak_ptr<int> weak = resource;
  EXPECT_THROW(executor->add([resource] {}), std::runtime_error);
  resource.reset();
  EXPECT_TRUE(weak.expired());
  executor.reset();
}

TEST_F(HookedExecutorTest, callableExceptionStillReleasesCapturesAndCompletes) {
  auto executor = makeExecutor();
  auto resource = std::make_shared<int>(42);
  std::weak_ptr<int> weak = resource;
  executor->add([resource] { throw std::runtime_error("I/O failure"); });
  resource.reset();
  auto wrapper = parent_.take();
  EXPECT_THROW(wrapper(), std::runtime_error);
  EXPECT_TRUE(weak.expired());
  EXPECT_EQ(counts_->destroyed, 1);
  executor.reset();
}

TEST_F(HookedExecutorTest, initializationExceptionStillReleasesCapturesAndCompletes) {
  initializer_ = std::make_unique<CountingInitializer>(counts_, true);
  auto executor = makeExecutor();
  auto resource = std::make_shared<int>(42);
  std::weak_ptr<int> weak = resource;
  executor->add([resource] { FAIL() << "Uninitialized callable ran: " << *resource; });
  resource.reset();
  auto wrapper = parent_.take();
  EXPECT_THROW(wrapper(), std::runtime_error);
  EXPECT_TRUE(weak.expired());
  EXPECT_EQ(counts_->initialized, 1);
  EXPECT_EQ(counts_->destroyed, 0);
  executor.reset();
}

TEST_F(HookedExecutorTest, nullParentRemainsDisabled) {
  EXPECT_EQ(makeHookedExecutor(nullptr, "disabled", initializer_.get(), false, 0ms, true), nullptr);
}

} // namespace
} // namespace gluten

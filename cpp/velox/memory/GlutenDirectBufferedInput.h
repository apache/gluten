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

#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "velox/dwio/common/DirectBufferedInput.h"

namespace gluten {

namespace detail {

class ReleasableExecutor final : public folly::Executor {
 public:
  explicit ReleasableExecutor(folly::Executor* executor) : executor_(executor) {}

  void add(folly::Func func) override {
    auto task = addTask(std::move(func));
    executor_->add(wrapTask(std::move(task)));
  }

  void addWithPriority(folly::Func func, int8_t priority) override {
    auto task = addTask(std::move(func));
    executor_->addWithPriority(wrapTask(std::move(task)), priority);
  }

  uint8_t getNumPriorities() const override {
    return executor_->getNumPriorities();
  }

  // Release closures that have not started yet. These closures may own
  // AsyncLoadHolder, which keeps a task MemoryPool alive while the closure
  // stays queued on the backing executor.
  void releaseQueuedAndWaitForRunning() {
    const auto currentTaskIsOurs = currentExecutorState_ == state_.get();

    std::vector<folly::Func> queuedFuncs;
    {
      std::lock_guard<std::mutex> lock(state_->mutex);
      for (const auto& task : state_->tasks) {
        if (!task->running && task->func) {
          queuedFuncs.push_back(std::move(task->func));
        }
      }
    }

    queuedFuncs.clear();

    std::unique_lock<std::mutex> lock(state_->mutex);
    state_->cv.wait(lock, [&]() { return state_->runningTasks == (currentTaskIsOurs ? 1 : 0); });
  }

 private:
  struct State;

  struct Task {
    explicit Task(folly::Func _func) : func(std::move(_func)) {}

    folly::Func func;
    bool running{false};
  };

  struct State {
    std::mutex mutex;
    std::condition_variable cv;
    std::unordered_set<std::shared_ptr<Task>> tasks;
    size_t runningTasks{0};
  };

  struct CurrentTaskGuard {
    explicit CurrentTaskGuard(State* state) : previous(currentExecutorState_) {
      currentExecutorState_ = state;
    }

    ~CurrentTaskGuard() {
      currentExecutorState_ = previous;
    }

    void* const previous;
  };

  struct TaskGuard {
    TaskGuard(std::shared_ptr<State> _state, std::shared_ptr<Task> _task, bool _running)
        : state(std::move(_state)), task(std::move(_task)), running(_running) {}

    ~TaskGuard() {
      std::lock_guard<std::mutex> lock(state->mutex);
      if (running) {
        task->running = false;
        --state->runningTasks;
      }
      state->tasks.erase(task);
      state->cv.notify_all();
    }

    const std::shared_ptr<State> state;
    const std::shared_ptr<Task> task;
    const bool running;
  };

  std::shared_ptr<Task> addTask(folly::Func func) {
    auto task = std::make_shared<Task>(std::move(func));
    std::lock_guard<std::mutex> lock(state_->mutex);
    state_->tasks.insert(task);
    return task;
  }

  folly::Func wrapTask(std::shared_ptr<Task> task) {
    auto state = state_;
    return [state = std::move(state), task = std::move(task)]() mutable {
      folly::Func func;
      bool running = false;
      {
        std::lock_guard<std::mutex> lock(state->mutex);
        if (task->func) {
          task->running = true;
          ++state->runningTasks;
          func = std::move(task->func);
          running = true;
        }
      }

      TaskGuard taskGuard(state, task, running);
      if (func) {
        CurrentTaskGuard currentTaskGuard(state.get());
        func();
      }
    };
  }

  folly::Executor* const executor_;
  const std::shared_ptr<State> state_{std::make_shared<State>()};
  static thread_local void* currentExecutorState_;
};

inline thread_local void* ReleasableExecutor::currentExecutorState_ = nullptr;

// Owns the executor wrapper passed to DirectBufferedInput. This must be listed
// as a base before DirectBufferedInput so that it is constructed first and
// destructed last.
class ReleasableExecutorHolder {
 public:
  explicit ReleasableExecutorHolder(folly::Executor* executor)
      : rawExecutor_(executor), releasableExecutor_(makeExecutor(executor)) {}

 protected:
  // The unwrapped executor, to be handed to clones instead of this object's
  // wrapper.
  folly::Executor* rawExecutor() const {
    return rawExecutor_;
  }

  folly::Executor* executor() const {
    return releasableExecutor_.get();
  }

  void releaseQueuedAndWaitForRunning() {
    if (releasableExecutor_ != nullptr) {
      releasableExecutor_->releaseQueuedAndWaitForRunning();
    }
  }

 private:
  static std::unique_ptr<ReleasableExecutor> makeExecutor(folly::Executor* executor) {
    if (executor == nullptr) {
      return nullptr;
    }
    return std::make_unique<ReleasableExecutor>(executor);
  }

  folly::Executor* const rawExecutor_;
  const std::unique_ptr<ReleasableExecutor> releasableExecutor_;
};

} // namespace detail

class GlutenDirectBufferedInput : private detail::ReleasableExecutorHolder,
                                  public facebook::velox::dwio::common::DirectBufferedInput {
 public:
  GlutenDirectBufferedInput(
      std::shared_ptr<facebook::velox::ReadFile> readFile,
      const facebook::velox::dwio::common::MetricsLogPtr& metricsLog,
      facebook::velox::StringIdLease fileNum,
      std::shared_ptr<facebook::velox::cache::ScanTracker> tracker,
      facebook::velox::StringIdLease groupId,
      std::shared_ptr<facebook::velox::io::IoStatistics> ioStatistics,
      std::shared_ptr<facebook::velox::IoStats> ioStats,
      folly::Executor* executor,
      const facebook::velox::io::ReaderOptions& readerOptions,
      folly::F14FastMap<std::string, std::string> fileReadOps = {})
      : ReleasableExecutorHolder(executor),
        DirectBufferedInput(
            std::move(readFile),
            metricsLog,
            std::move(fileNum),
            std::move(tracker),
            std::move(groupId),
            std::move(ioStatistics),
            std::move(ioStats),
            ReleasableExecutorHolder::executor(),
            readerOptions,
            std::move(fileReadOps)) {}

  ~GlutenDirectBufferedInput() override {
    requests_.clear();
    // Cancel all the planned loads as soon as possible to avoid unnecessary IO.
    for (auto& load : coalescedLoads_) {
      if (load->state() == facebook::velox::cache::CoalescedLoad::State::kPlanned) {
        load->cancel();
      }
    }
    // Ensure running loads finish before the task memory manager is destroyed.
    for (auto& load : coalescedLoads_) {
      if (load->state() == facebook::velox::cache::CoalescedLoad::State::kLoading) {
        folly::SemiFuture<bool> waitFuture(false);
        if (!load->loadOrFuture(&waitFuture)) {
          auto& exec = folly::QueuedImmediateExecutor::instance();
          std::move(waitFuture).via(&exec).wait();
        }
      }
    }
    coalescedLoads_.clear();
    releaseQueuedAndWaitForRunning();
  }

  std::unique_ptr<facebook::velox::dwio::common::BufferedInput> clone() const override {
    // Pass the unwrapped executor: the clone has its own lifetime and wrapper.
    return std::unique_ptr<facebook::velox::dwio::common::BufferedInput>(new GlutenDirectBufferedInput(
        input_, fileNum_, tracker_, groupId_, ioStatistics_, ioStats_, rawExecutor(), options_));
  }

 private:
  // Constructor used by clone().
  GlutenDirectBufferedInput(
      std::shared_ptr<facebook::velox::dwio::common::ReadFileInputStream> input,
      facebook::velox::StringIdLease fileNum,
      std::shared_ptr<facebook::velox::cache::ScanTracker> tracker,
      facebook::velox::StringIdLease groupId,
      std::shared_ptr<facebook::velox::io::IoStatistics> ioStatistics,
      std::shared_ptr<facebook::velox::IoStats> ioStats,
      folly::Executor* executor,
      const facebook::velox::io::ReaderOptions& readerOptions)
      : ReleasableExecutorHolder(executor),
        DirectBufferedInput(
            std::move(input),
            std::move(fileNum),
            std::move(tracker),
            std::move(groupId),
            std::move(ioStatistics),
            std::move(ioStats),
            ReleasableExecutorHolder::executor(),
            readerOptions) {}
};

} // namespace gluten

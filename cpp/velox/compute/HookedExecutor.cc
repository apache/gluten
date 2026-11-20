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

#include <folly/ScopeGuard.h>
#include <glog/logging.h>
#include <condition_variable>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "threads/ThreadInitializer.h"
#include "utils/Exception.h"
#include "velox/common/process/StackTrace.h"

namespace gluten {
namespace {

class HookedExecutor final : public folly::Executor {
 public:
  HookedExecutor(
      folly::Executor* parent,
      std::string name,
      ThreadInitializer* initializer,
      bool debug,
      std::chrono::milliseconds joinTimeout,
      bool releaseQueuedOnStop)
      : parent_(parent),
        name_(std::move(name)),
        initializer_(initializer),
        debug_(debug),
        joinTimeout_(joinTimeout),
        releaseQueuedOnStop_(releaseQueuedOnStop) {}

  ~HookedExecutor() override {
    if (releaseQueuedOnStop_) {
      releaseQueued();
    }
    if (!join()) {
      LOG(WARNING) << "Timed out waiting for hooked executor " << name_ << " to finish after " << joinTimeout_.count()
                   << " ms.";
      dumpOutstandingTasks();
    }
  }

  uint8_t getNumPriorities() const override {
    return parent_ == nullptr ? 1 : parent_->getNumPriorities();
  }

  void add(folly::Func func) override {
    submit(std::move(func), 0, false);
  }

  void addWithPriority(folly::Func func, int8_t priority) override {
    submit(std::move(func), priority, true);
  }

 private:
  struct Task {
    explicit Task(folly::Func callable) : func(std::move(callable)) {}

    folly::Func func;
    bool running{false};
    uint64_t id{0};
    std::chrono::steady_clock::time_point enqueueTime;
    int8_t priority{0};
    std::string submitStacktrace;
  };

  struct State {
    std::mutex mutex;
    std::condition_variable cv;
    uint64_t nextTaskId{0};
    bool stopping{false};
    std::unordered_map<uint64_t, std::shared_ptr<Task>> tasks;
  };

  static void markDone(const std::shared_ptr<State>& state, uint64_t taskId) {
    std::lock_guard<std::mutex> lock(state->mutex);
    state->tasks.erase(taskId);
    state->cv.notify_all();
  }

  static void releaseTask(const std::shared_ptr<State>& state, const std::shared_ptr<Task>& task) {
    folly::Func func;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      if (!task->func || task->running) {
        return;
      }
      func = std::move(task->func);
    }
    func = nullptr;
    markDone(state, task->id);
  }

  void submit(folly::Func func, int8_t priority, bool withPriority) {
    GLUTEN_CHECK(parent_ != nullptr, "Parent executor is null.");
    GLUTEN_CHECK(func, "Submitted callable is null.");
    auto task = std::make_shared<Task>(std::move(func));
    task->priority = priority;
    if (debug_) {
      task->enqueueTime = std::chrono::steady_clock::now();
      task->submitStacktrace = facebook::velox::process::StackTrace().toString();
    }
    {
      std::lock_guard<std::mutex> lock(state_->mutex);
      if (state_->stopping) {
        return;
      }
      task->id = state_->nextTaskId++;
      state_->tasks.emplace(task->id, task);
    }

    // A rejected submission must not leave an outstanding callable behind.
    auto rollback = folly::makeGuard([&] { releaseTask(state_, task); });
    auto wrapped = wrap(task);
    if (withPriority) {
      parent_->addWithPriority(std::move(wrapped), priority);
    } else {
      parent_->add(std::move(wrapped));
    }
    rollback.dismiss();
  }

  folly::Func wrap(const std::shared_ptr<Task>& task) {
    auto state = state_;
    auto* initializer = initializer_;
    auto taskName = name_ + std::to_string(task->id);
    return [state = std::move(state), task, initializer, taskName = std::move(taskName)]() mutable {
      folly::Func func;
      {
        std::lock_guard<std::mutex> lock(state->mutex);
        if (!task->func) {
          // Teardown owns completion accounting for released callables.
          return;
        }
        task->running = true;
        func = std::move(task->func);
      }

      auto done = folly::makeGuard([&] {
        // Also release captures if thread initialization throws.
        func = nullptr;
        markDone(state, task->id);
      });
      GLUTEN_CHECK(initializer != nullptr, "ThreadInitializer is null.");
      initializer->initialize(taskName);
      auto destroy = folly::makeGuard([&] { initializer->destroy(taskName); });
      // Captures must die before the thread context and outstanding-task entry.
      auto localFunc = std::move(func);
      localFunc();
    };
  }

  void releaseQueued() {
    std::vector<std::pair<uint64_t, folly::Func>> queued;
    {
      std::lock_guard<std::mutex> lock(state_->mutex);
      state_->stopping = true;
      for (const auto& [taskId, task] : state_->tasks) {
        if (!task->running && task->func) {
          queued.emplace_back(taskId, std::move(task->func));
        }
      }
    }
    // Capture destruction can destroy readers or submit more work. Do not hold
    // the registry lock, and keep these tasks outstanding until it is finished.
    for (auto& [taskId, func] : queued) {
      func = nullptr;
    }
    {
      std::lock_guard<std::mutex> lock(state_->mutex);
      for (const auto& [taskId, func] : queued) {
        state_->tasks.erase(taskId);
      }
      state_->cv.notify_all();
    }
  }

  bool join() {
    std::unique_lock<std::mutex> lock(state_->mutex);
    return state_->cv.wait_for(lock, joinTimeout_, [&] { return state_->tasks.empty(); });
  }

  void dumpOutstandingTasks() const {
    if (!debug_) {
      return;
    }
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (state_->tasks.empty()) {
      LOG(WARNING) << "Hooked executor " << name_ << " timed out with no tracked in-flight tasks.";
      return;
    }
    for (const auto& [taskId, task] : state_->tasks) {
      const auto elapsedMs =
          std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - task->enqueueTime)
              .count();
      LOG(WARNING) << "Outstanding task in hooked executor " << name_ << ": taskId=" << taskId
                   << ", elapsedMs=" << elapsedMs << ", priority=" << static_cast<int32_t>(task->priority)
                   << ", submitStacktrace:\n"
                   << task->submitStacktrace;
    }
  }

  folly::Executor* const parent_;
  const std::string name_;
  ThreadInitializer* const initializer_;
  const bool debug_;
  const std::chrono::milliseconds joinTimeout_;
  const bool releaseQueuedOnStop_;
  const std::shared_ptr<State> state_{std::make_shared<State>()};
};

} // namespace

std::unique_ptr<folly::Executor> makeHookedExecutor(
    folly::Executor* parent,
    const std::string& name,
    ThreadInitializer* initializer,
    bool debug,
    std::chrono::milliseconds joinTimeout,
    bool releaseQueuedOnStop) {
  if (parent == nullptr) {
    return nullptr;
  }
  return std::make_unique<HookedExecutor>(parent, name, initializer, debug, joinTimeout, releaseQueuedOnStop);
}

} // namespace gluten

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

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace gluten {

/// A thread pool for managing reader threads that process tasks concurrently.
/// This pool manages a fixed number of worker threads that execute submitted tasks.
class ReaderThreadPool {
 public:
  using Task = std::function<void()>;

  struct PrioritizedTask {
    Task task;
    int32_t priority;

    // 0 is the highest priority, larger value means lower priority.
    bool operator<(const PrioritizedTask& other) const {
      return priority > other.priority;
    }
  };

  /// Constructor
  /// @param numThreads Number of worker threads to create
  explicit ReaderThreadPool(size_t numThreads);

  /// Destructor - stops all threads and waits for them to finish
  ~ReaderThreadPool();

  // Disable copy and move
  ReaderThreadPool(const ReaderThreadPool&) = delete;
  ReaderThreadPool& operator=(const ReaderThreadPool&) = delete;
  ReaderThreadPool(ReaderThreadPool&&) = delete;
  ReaderThreadPool& operator=(ReaderThreadPool&&) = delete;

  void submitBatch(std::vector<Task> tasks, int32_t priority);

  /// Start executing tasks from the queue
  /// Call this after all priority-0 tasks have been submitted
  void start();

  /// Stop accepting new tasks and signal all threads to finish and
  /// wait for all threads to complete their current tasks and join
  void shutdown();

  /// Get the number of active worker threads
  size_t getNumThreads() const {
    return numThreads_;
  }

  /// Check if shutdown has been requested
  bool isShutdown() const {
    return stop_.load(std::memory_order_acquire);
  }

 private:
  /// Worker thread function that processes tasks from the queue
  void workerThread();

  size_t numThreads_;
  std::vector<std::thread> workers_;
  std::priority_queue<PrioritizedTask> tasks_;

  std::mutex taskQueueMtx_;
  std::condition_variable wakeUpCV_;
  std::atomic<bool> stop_{false};
};

} // namespace gluten

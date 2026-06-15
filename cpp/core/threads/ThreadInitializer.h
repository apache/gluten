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

#include <memory>

namespace gluten {

/// Lifecycle hook invoked on each worker thread managed by a ThreadManager.
///
/// When a thread pool (e.g., folly::CPUThreadPoolExecutor) spawns or reaps a
/// thread, the ThreadInitializer gives the application a chance to attach
/// per-thread context — such as JNI thread attachment or Spark TaskContext
/// propagation — before the thread runs native work and to clean up after.
///
/// Implementations must be thread-safe; initialize() and destroy() can be
/// called concurrently from different threads.
class ThreadInitializer {
 public:
  /// Returns an initializer that does nothing (noop). Useful in benchmarks
  /// and tests where no JVM/Spark context is available.
  static std::unique_ptr<ThreadInitializer> noop();

  virtual ~ThreadInitializer() = default;

  /// Called when a worker thread is about to start executing tasks.
  /// @param threadName A human-readable name identifying the thread.
  virtual void initialize(const std::string& threadName) = 0;

  /// Called when a worker thread is about to be returned to the pool or
  /// destroyed. Must not detach the JNI thread — the thread may be reused.
  /// @param threadName The same name passed to initialize().
  virtual void destroy(const std::string& threadName) = 0;

 protected:
  ThreadInitializer() = default;
};

} // namespace gluten

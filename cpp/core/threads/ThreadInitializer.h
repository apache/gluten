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

/// Per-task hook invoked around each submitted task on executors wrapped
/// by a ThreadManager.
///
/// When a task is submitted to an executor (e.g., via HookedExecutor::wrap),
/// the ThreadInitializer gives the application a chance to attach per-task
/// context — such as JNI thread attachment or Spark TaskContext propagation —
/// before the task runs native work and to clean up after.
///
/// Implementations must be thread-safe; initialize() and destroy() can be
/// called concurrently from different threads.
class ThreadInitializer {
 public:
  /// Returns an initializer that does nothing (noop). Useful in benchmarks
  /// and tests where no JVM/Spark context is available.
  static std::unique_ptr<ThreadInitializer> noop();

  virtual ~ThreadInitializer() = default;

  /// Called before each submitted task executes. Attach per-task context
  /// such as a JNI env or Spark TaskContext.
  /// @param taskName A human-readable name identifying the task (not the thread).
  virtual void initialize(const std::string& taskName) = 0;

  /// Called after each submitted task completes. Must not detach the JNI
  /// thread — the thread may be reused for subsequent tasks.
  /// @param taskName The same name passed to initialize().
  virtual void destroy(const std::string& taskName) = 0;

 protected:
  ThreadInitializer() = default;
};

} // namespace gluten

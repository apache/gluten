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

#include <functional>
#include <memory>
#include <string>

#include "threads/ThreadInitializer.h"

namespace gluten {

/// Per-backend registry of ThreadInitializer instances.
///
/// ThreadManager follows the same Factory / Releaser registry pattern as
/// MemoryManager and Runtime: each backend registers its own implementation
/// at init time, and the factory is keyed by backend kind (e.g., "velox").
///
/// The ThreadManager owns a ThreadInitializer that is used to propagate
/// per-task context (JNI env, Spark TaskContext) to executor tasks
/// submitted via HookedExecutor. It is created once per Spark task and released
/// when the task finishes.
class ThreadManager {
 public:
  using Factory =
      std::function<ThreadManager*(const std::string& kind, std::unique_ptr<ThreadInitializer> initializer)>;
  using Releaser = std::function<void(ThreadManager*)>;

  /// Register a backend-specific factory and releaser for the given kind.
  static void registerFactory(const std::string& kind, Factory factory, Releaser releaser);

  /// Create a ThreadManager for the given backend kind with the supplied
  /// thread initializer. The returned pointer is owned by the caller.
  static ThreadManager* create(const std::string& kind, std::unique_ptr<ThreadInitializer> initializer);

  /// Release a ThreadManager previously created with create().
  static void release(ThreadManager* threadManager);

  explicit ThreadManager(const std::string& kind) : kind_(kind) {}

  virtual ~ThreadManager() = default;

  /// Return the backend kind this manager was created for.
  virtual std::string kind() {
    return kind_;
  }

  /// Return the ThreadInitializer that should be called around each
  /// submitted task (before and after execution) to attach per-task
  /// context. The returned pointer is owned by ThreadManager; callers
  /// must not delete it.
  virtual ThreadInitializer* getThreadInitializer() = 0;

 private:
  std::string kind_;
};

} // namespace gluten

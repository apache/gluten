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

class ThreadManager {
 public:
  using Factory =
      std::function<ThreadManager*(const std::string& kind, std::unique_ptr<ThreadInitializer> initializer)>;
  using Releaser = std::function<void(ThreadManager*)>;

  static void registerFactory(const std::string& kind, Factory factory, Releaser releaser);
  static ThreadManager* create(const std::string& kind, std::unique_ptr<ThreadInitializer> initializer);
  static void release(ThreadManager* threadManager);

  explicit ThreadManager(const std::string& kind) : kind_(kind) {}

  virtual ~ThreadManager() = default;

  virtual std::string kind() {
    return kind_;
  }

  virtual ThreadInitializer* getThreadInitializer() = 0;

 private:
  std::string kind_;
};

} // namespace gluten

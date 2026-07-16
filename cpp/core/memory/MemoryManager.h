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

#include "arrow/memory_pool.h"
#include "memory.pb.h"
#include "memory/AllocationListener.h"

namespace gluten {

class MemoryManager {
 public:
  using Factory = std::function<MemoryManager*(const std::string& kind, std::unique_ptr<AllocationListener> listener)>;
  using Releaser = std::function<void(MemoryManager*)>;
  static void registerFactory(const std::string& kind, Factory factory, Releaser releaser);
  static MemoryManager* create(const std::string& kind, std::unique_ptr<AllocationListener> listener);
  static void release(MemoryManager*);

  MemoryManager(const std::string& kind) : kind_(kind){};

  virtual ~MemoryManager() = default;

  virtual std::string kind() {
    return kind_;
  }

  // Get the default Arrow memory pool for this memory manager. This memory pool is held by the memory manager.
  virtual arrow::MemoryPool* defaultArrowMemoryPool() = 0;

  // Return the Arrow memory pool with the given name. The caller is responsible for managing the lifetime of the
  // returned memory pool. Memory manager only holds the weak reference to the memory pool for collecting memory usage.
  // If the memory pool with the given name does not exist, it will create a new one and return it.
  virtual std::shared_ptr<arrow::MemoryPool> getOrCreateArrowMemoryPool(const std::string& name) = 0;

  virtual const MemoryUsageStats collectMemoryUsageStats() const = 0;

  virtual const int64_t shrink(int64_t size) = 0;

  // Hold this memory manager. The underlying memory pools will be released as lately as this memory manager gets
  // destroyed. Which means, a call to this function would make sure the memory blocks directly or indirectly managed
  // by this manager, be guaranteed safe to access during the period that this manager is alive.
  virtual void hold() = 0;

 private:
  std::string kind_;
};

} // namespace gluten

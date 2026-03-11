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

#include "arrow/memory_pool.h" // IWYU pragma: keep

#include "MemoryAllocator.h" // IWYU pragma: keep

// NOLINTNEXTLINE(cert-dcl58-cpp)
namespace gluten {

using ArrowMemoryPoolReleaser = std::function<void(arrow::MemoryPool*)>;

/// This pool was not tracked by Spark, should only used in test.
class ArrowMemoryPool final : public arrow::MemoryPool {
 public:
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init, hicpp-member-init)
  explicit ArrowMemoryPool(AllocationListener* listener)
      : allocator_(std::make_unique<ListenableMemoryAllocator>(defaultMemoryAllocator().get(), listener)) {}

  ~ArrowMemoryPool() override = default;

  ArrowMemoryPool(const ArrowMemoryPool&) = delete;

  ArrowMemoryPool& operator=(const ArrowMemoryPool&) = delete;

  ArrowMemoryPool(ArrowMemoryPool&&) = delete;

  ArrowMemoryPool& operator=(ArrowMemoryPool&&) = delete;

  arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override;

  arrow::Status Reallocate(int64_t oldSize, int64_t newSize, int64_t alignment, uint8_t** ptr) override;

  void Free(uint8_t* buffer, int64_t size, int64_t alignment) override;

  int64_t bytes_allocated() const override;

  int64_t max_memory() const override;

  int64_t total_bytes_allocated() const override;

  int64_t num_allocations() const override;

  std::string backend_name() const override;

  MemoryAllocator* allocator() const;

 private:
  std::unique_ptr<MemoryAllocator> allocator_ = nullptr;
};

} // namespace gluten

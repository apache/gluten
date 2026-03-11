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

#include "ArrowMemoryPool.h"
#include "utils/Exception.h"

namespace gluten {

arrow::Status ArrowMemoryPool::Allocate(int64_t size, int64_t alignment, uint8_t** out) {
  if (!allocator_->allocateAligned(alignment, size, reinterpret_cast<void**>(out))) {
    return arrow::Status::Invalid("WrappedMemoryPool: Error allocating " + std::to_string(size) + " bytes");
  }
  return arrow::Status::OK();
}

arrow::Status ArrowMemoryPool::Reallocate(int64_t oldSize, int64_t newSize, int64_t alignment, uint8_t** ptr) {
  if (!allocator_->reallocateAligned(*ptr, alignment, oldSize, newSize, reinterpret_cast<void**>(ptr))) {
    return arrow::Status::Invalid("WrappedMemoryPool: Error reallocating " + std::to_string(newSize) + " bytes");
  }
  return arrow::Status::OK();
}

void ArrowMemoryPool::Free(uint8_t* buffer, int64_t size, int64_t alignment) {
  allocator_->free(buffer, size);
}

int64_t ArrowMemoryPool::bytes_allocated() const {
  return allocator_->getBytes();
}

int64_t ArrowMemoryPool::max_memory() const {
  return allocator_->peakBytes();
}

int64_t ArrowMemoryPool::total_bytes_allocated() const {
  throw GlutenException("Not implement");
}

int64_t ArrowMemoryPool::num_allocations() const {
  throw GlutenException("Not implement");
}

std::string ArrowMemoryPool::backend_name() const {
  return "gluten arrow allocator";
}

MemoryAllocator* ArrowMemoryPool::allocator() const {
  return allocator_.get();
}

} // namespace gluten

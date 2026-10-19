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

#include "memory/AllocationListener.h"
#include "memory/MemoryAllocator.h"
#include <gtest/gtest.h>

#include <cstdint>

using namespace gluten;

TEST(StdMemoryAllocator, allocateZeroFilledAccounting) {
  StdMemoryAllocator allocator;
  ASSERT_EQ(allocator.getBytes(), 0);

  // allocateZeroFilled with nmemb=10, size=64 should track 10*64=640 bytes.
  const int64_t expectedBytes = 10 * 64;
  void* buf = nullptr;
  bool ok = allocator.allocateZeroFilled(10, 64, &buf);
  ASSERT_TRUE(ok);
  ASSERT_NE(buf, nullptr);
  ASSERT_EQ(allocator.getBytes(), expectedBytes);

  ASSERT_TRUE(allocator.free(buf, expectedBytes));
  ASSERT_EQ(allocator.getBytes(), 0);
}

TEST(StdMemoryAllocator, allocateAligned) {
  StdMemoryAllocator allocator;
  constexpr uint64_t alignment = 64;
  constexpr int64_t size = 100; // Not a multiple of alignment
  void* buf = nullptr;

  ASSERT_TRUE(allocator.allocateAligned(alignment, size, &buf));
  ASSERT_NE(buf, nullptr);
  ASSERT_EQ(reinterpret_cast<uintptr_t>(buf) % alignment, 0);
  ASSERT_EQ(allocator.getBytes(), size);

  ASSERT_TRUE(allocator.free(buf, size));
  ASSERT_EQ(allocator.getBytes(), 0);
}

TEST(StdMemoryAllocator, reallocateAlignedShrinkAndGrow) {
  StdMemoryAllocator allocator;
  constexpr uint64_t alignment = 64;
  int64_t size = 16384;
  void* buf = nullptr;

  ASSERT_TRUE(allocator.allocateAligned(alignment, size, &buf));
  ASSERT_NE(buf, nullptr);
  ASSERT_EQ(reinterpret_cast<uintptr_t>(buf) % alignment, 0);
  ASSERT_EQ(allocator.getBytes(), size);

  auto* bytes = static_cast<uint8_t*>(buf);
  for (int64_t i = 0; i < size; ++i) {
    bytes[i] = static_cast<uint8_t>(i & 0xFF);
  }

  // Shrink to 64 bytes (multiple of alignment)
  int64_t newSize = 64;
  ASSERT_TRUE(allocator.reallocateAligned(buf, alignment, size, newSize, &buf));
  ASSERT_NE(buf, nullptr);
  ASSERT_EQ(reinterpret_cast<uintptr_t>(buf) % alignment, 0);
  ASSERT_EQ(allocator.getBytes(), newSize);
  bytes = static_cast<uint8_t*>(buf);
  for (int64_t i = 0; i < newSize; ++i) {
    ASSERT_EQ(bytes[i], static_cast<uint8_t>(i & 0xFF));
  }
  size = newSize;

  // Shrink to 10 bytes (non-multiple of alignment)
  newSize = 10;
  ASSERT_TRUE(allocator.reallocateAligned(buf, alignment, size, newSize, &buf));
  ASSERT_NE(buf, nullptr);
  ASSERT_EQ(reinterpret_cast<uintptr_t>(buf) % alignment, 0);
  ASSERT_EQ(allocator.getBytes(), newSize);
  bytes = static_cast<uint8_t*>(buf);
  for (int64_t i = 0; i < newSize; ++i) {
    ASSERT_EQ(bytes[i], static_cast<uint8_t>(i & 0xFF));
  }
  size = newSize;

  // Grow to 32768 bytes
  newSize = 32768;
  ASSERT_TRUE(allocator.reallocateAligned(buf, alignment, size, newSize, &buf));
  ASSERT_NE(buf, nullptr);
  ASSERT_EQ(reinterpret_cast<uintptr_t>(buf) % alignment, 0);
  ASSERT_EQ(allocator.getBytes(), newSize);
  bytes = static_cast<uint8_t*>(buf);
  for (int64_t i = 0; i < size; ++i) {
    ASSERT_EQ(bytes[i], static_cast<uint8_t>(i & 0xFF));
  }
  size = newSize;

  ASSERT_TRUE(allocator.free(buf, size));
  ASSERT_EQ(allocator.getBytes(), 0);
}

namespace {
class SimpleTestListener : public AllocationListener {
 public:
  void allocationChanged(int64_t diff) override {
    current_ += diff;
  }

  int64_t currentBytes() override {
    return current_;
  }

 private:
  int64_t current_{0};
};
} // namespace

TEST(ListenableMemoryAllocator, alignedOperations) {
  StdMemoryAllocator stdAlloc;
  SimpleTestListener listener;
  ListenableMemoryAllocator allocator(&stdAlloc, &listener);

  constexpr uint64_t alignment = 64;
  int64_t size = 16384;
  void* buf = nullptr;

  ASSERT_TRUE(allocator.allocateAligned(alignment, size, &buf));
  ASSERT_NE(buf, nullptr);
  ASSERT_EQ(reinterpret_cast<uintptr_t>(buf) % alignment, 0);
  ASSERT_EQ(allocator.getBytes(), size);
  ASSERT_EQ(listener.currentBytes(), size);

  // Shrink
  int64_t newSize = 128;
  ASSERT_TRUE(allocator.reallocateAligned(buf, alignment, size, newSize, &buf));
  ASSERT_NE(buf, nullptr);
  ASSERT_EQ(reinterpret_cast<uintptr_t>(buf) % alignment, 0);
  ASSERT_EQ(allocator.getBytes(), newSize);
  ASSERT_EQ(listener.currentBytes(), newSize);
  size = newSize;

  // Grow
  newSize = 32768;
  ASSERT_TRUE(allocator.reallocateAligned(buf, alignment, size, newSize, &buf));
  ASSERT_NE(buf, nullptr);
  ASSERT_EQ(reinterpret_cast<uintptr_t>(buf) % alignment, 0);
  ASSERT_EQ(allocator.getBytes(), newSize);
  ASSERT_EQ(listener.currentBytes(), newSize);
  size = newSize;

  ASSERT_TRUE(allocator.free(buf, size));
  ASSERT_EQ(allocator.getBytes(), 0);
  ASSERT_EQ(listener.currentBytes(), 0);
}

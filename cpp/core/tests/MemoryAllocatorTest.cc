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

#include "memory/MemoryAllocator.h"
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/buffer_builder.h>
#include <arrow/compute/api.h>
#include <gtest/gtest.h>
#include "memory/ArrowMemoryPool.h"

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

TEST(StdMemoryAllocator, shrinkKeepsAlignment) {
  StdMemoryAllocator allocator;
  constexpr uint64_t kAlignment = 64;
  constexpr int64_t kSize = 1024;
  // On macOS, realloc can move a shrunk block to an address that is only 16-byte aligned.
  for (int64_t newSize = kAlignment; newSize < kSize; newSize += kAlignment) {
    void* buf = nullptr;
    ASSERT_TRUE(allocator.allocateAligned(kAlignment, kSize, &buf));
    for (int64_t i = 0; i < kSize; ++i) {
      static_cast<uint8_t*>(buf)[i] = static_cast<uint8_t>(i);
    }

    void* shrunk = nullptr;
    ASSERT_TRUE(allocator.reallocateAligned(buf, kAlignment, kSize, newSize, &shrunk));
    ASSERT_EQ(reinterpret_cast<uintptr_t>(shrunk) % kAlignment, 0) << "newSize " << newSize;
    for (int64_t i = 0; i < newSize; ++i) {
      ASSERT_EQ(static_cast<uint8_t*>(shrunk)[i], static_cast<uint8_t>(i)) << "newSize " << newSize;
    }
    ASSERT_EQ(allocator.getBytes(), newSize);

    ASSERT_TRUE(allocator.free(shrunk, newSize));
    ASSERT_EQ(allocator.getBytes(), 0);
  }
}

TEST(StdMemoryAllocator, reallocateAlignedToZeroAndGrow) {
  StdMemoryAllocator allocator;
  constexpr uint64_t kAlignment = 64;
  constexpr int64_t kSize = 1024;
  void* buf = nullptr;
  ASSERT_TRUE(allocator.allocateAligned(kAlignment, kSize, &buf));

  ASSERT_TRUE(allocator.reallocateAligned(buf, kAlignment, kSize, 0, &buf));
  ASSERT_NE(buf, nullptr);
  ASSERT_EQ(reinterpret_cast<uintptr_t>(buf) % kAlignment, 0);
  ASSERT_EQ(allocator.getBytes(), 0);

  ASSERT_TRUE(allocator.reallocateAligned(buf, kAlignment, 0, 0, &buf));
  ASSERT_NE(buf, nullptr);
  ASSERT_EQ(allocator.getBytes(), 0);

  ASSERT_TRUE(allocator.reallocateAligned(buf, kAlignment, 0, kSize, &buf));
  ASSERT_NE(buf, nullptr);
  ASSERT_EQ(reinterpret_cast<uintptr_t>(buf) % kAlignment, 0);
  ASSERT_EQ(allocator.getBytes(), kSize);
  std::memset(buf, 0x5a, kSize);
  ASSERT_EQ(static_cast<uint8_t*>(buf)[kSize - 1], 0x5a);

  ASSERT_TRUE(allocator.free(buf, kSize));
  ASSERT_EQ(allocator.getBytes(), 0);
}

TEST(ListenableMemoryAllocator, reallocateAlignedToZeroAccounting) {
  class TrackingListener final : public AllocationListener {
   public:
    void allocationChanged(int64_t diff) override {
      bytes += diff;
    }
    int64_t bytes = 0;
  } listener;
  StdMemoryAllocator delegated;
  ListenableMemoryAllocator allocator(&delegated, &listener);
  constexpr uint64_t kAlignment = 64;
  constexpr int64_t kSize = 1024;
  void* buf = nullptr;
  ASSERT_TRUE(allocator.allocateAligned(kAlignment, kSize, &buf));
  ASSERT_EQ(listener.bytes, kSize);

  ASSERT_TRUE(allocator.reallocateAligned(buf, kAlignment, kSize, 0, &buf));
  ASSERT_NE(buf, nullptr);
  ASSERT_EQ(allocator.getBytes(), 0);
  ASSERT_EQ(delegated.getBytes(), 0);
  ASSERT_EQ(listener.bytes, 0);
  ASSERT_EQ(allocator.peakBytes(), kSize);

  ASSERT_TRUE(allocator.free(buf, 0));
  ASSERT_EQ(allocator.getBytes(), 0);
  ASSERT_EQ(delegated.getBytes(), 0);
  ASSERT_EQ(listener.bytes, 0);
}

TEST(ArrowMemoryPool, finishEmptyBufferWithReservedCapacity) {
  auto listener = AllocationListener::noop();
  ArrowMemoryPool pool(listener.get());
  {
    arrow::BufferBuilder builder(&pool);
    ASSERT_TRUE(builder.Reserve(1024).ok());
    ASSERT_GT(pool.bytes_allocated(), 0);

    std::shared_ptr<arrow::Buffer> buffer;
    const auto status = builder.Finish(&buffer);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_EQ(buffer->size(), 0);
    ASSERT_EQ(pool.bytes_allocated(), 0);
  }
  ASSERT_EQ(pool.bytes_allocated(), 0);
}

TEST(ArrowMemoryPool, resizeBufferToZeroAndGrow) {
  auto listener = AllocationListener::noop();
  ArrowMemoryPool pool(listener.get());
  {
    auto result = arrow::AllocateResizableBuffer(1024, &pool);
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    auto buffer = std::move(result).ValueOrDie();
    const auto status = buffer->Resize(0);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_EQ(buffer->size(), 0);
    ASSERT_EQ(pool.bytes_allocated(), 0);

    ASSERT_TRUE(buffer->Resize(1024).ok());
    ASSERT_EQ(buffer->size(), 1024);
    ASSERT_EQ(pool.bytes_allocated(), 1024);
    std::memset(buffer->mutable_data(), 0x5a, buffer->size());
    ASSERT_EQ(buffer->data()[1023], 0x5a);
  }
  ASSERT_EQ(pool.bytes_allocated(), 0);
}

TEST(StdMemoryAllocator, negativeReallocationPreservesAllocation) {
  StdMemoryAllocator allocator;
  constexpr int64_t kSize = 64;
  void* buf = nullptr;
  ASSERT_TRUE(allocator.allocateAligned(kSize, kSize, &buf));
  std::memset(buf, 0x5a, kSize);
  void* original = buf;

  ASSERT_FALSE(allocator.reallocateAligned(buf, kSize, kSize, -1, &buf));
  ASSERT_EQ(buf, original);
  ASSERT_EQ(allocator.getBytes(), kSize);
  ASSERT_EQ(static_cast<uint8_t*>(buf)[kSize - 1], 0x5a);
  ASSERT_TRUE(allocator.free(buf, kSize));
  ASSERT_EQ(allocator.getBytes(), 0);
}

TEST(ArrowMemoryPool, takeStringsWithNoValueBytes) {
  auto listener = AllocationListener::noop();
  ArrowMemoryPool pool(listener.get());

  // The Parquet dictionary writer uses Take to compute statistics for the referenced dictionary values.
  // Selecting only nulls or empty strings can shrink the output value buffer to zero bytes.
  for (bool takeNull : {false, true}) {
    SCOPED_TRACE(takeNull);
    {
      arrow::StringBuilder valuesBuilder;
      ASSERT_TRUE(valuesBuilder.Append("").ok());
      ASSERT_TRUE(valuesBuilder.Append("unreferenced dictionary value").ok());
      auto values = valuesBuilder.Finish();
      ASSERT_TRUE(values.ok()) << values.status().ToString();

      arrow::Int32Builder indicesBuilder;
      ASSERT_TRUE((takeNull ? indicesBuilder.AppendNull() : indicesBuilder.Append(0)).ok());
      auto indices = indicesBuilder.Finish();
      ASSERT_TRUE(indices.ok()) << indices.status().ToString();

      arrow::compute::ExecContext context(&pool);
      auto result = arrow::compute::Take(
          *values.ValueOrDie(), *indices.ValueOrDie(), arrow::compute::TakeOptions::Defaults(), &context);
      ASSERT_TRUE(result.ok()) << result.status().ToString();
      const auto& strings = static_cast<const arrow::StringArray&>(*result.ValueOrDie());
      ASSERT_EQ(strings.length(), 1);
      ASSERT_EQ(strings.IsNull(0), takeNull);
      if (!takeNull) {
        ASSERT_EQ(strings.GetView(0), "");
      }
    }
    ASSERT_EQ(pool.bytes_allocated(), 0);
  }
}

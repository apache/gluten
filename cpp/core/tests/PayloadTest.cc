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

#include "shuffle/Payload.h"
#include "shuffle/Utils.h"
#include "utils/Compression.h"

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <gtest/gtest.h>

#include <cstring>

namespace gluten {
namespace {

// 4096 rows: the validity buffer is 512 bytes, a multiple of 64, so Arrow's
// capacity rounding alone leaves no spare bytes after it.
constexpr uint32_t kNumRows = 4096;
const std::vector<bool> kIsValidityBuffer = {true, false};

std::shared_ptr<arrow::Buffer> makeBuffer(int64_t size, uint8_t seed) {
  auto buffer = arrow::AllocateBuffer(size).ValueOrDie();
  for (int64_t i = 0; i < size; ++i) {
    buffer->mutable_data()[i] = static_cast<uint8_t>(seed + i * 7);
  }
  return std::shared_ptr<arrow::Buffer>(std::move(buffer));
}

std::vector<std::shared_ptr<arrow::Buffer>> makeBuffers(uint8_t seed) {
  return {makeBuffer(kNumRows / 8, seed), makeBuffer(kNumRows * sizeof(int64_t), seed + 1)};
}

void expectZeroPadding(const arrow::Buffer& buffer) {
  for (int64_t i = buffer.size(); i < buffer.size() + kSimdPadding; ++i) {
    ASSERT_EQ(buffer.data()[i], 0) << "padding byte " << i << " of buffer with size " << buffer.size();
  }
}

void expectPadded(const std::vector<std::shared_ptr<arrow::Buffer>>& buffers) {
  for (const auto& buffer : buffers) {
    ASSERT_NE(buffer, nullptr);
    EXPECT_GE(buffer->capacity() - buffer->size(), kSimdPadding)
        << "size " << buffer->size() << ", capacity " << buffer->capacity();
    expectZeroPadding(*buffer);
  }
}

void expectSameData(const arrow::Buffer& actual, const arrow::Buffer& expected) {
  ASSERT_EQ(actual.size(), expected.size());
  EXPECT_EQ(std::memcmp(actual.data(), expected.data(), expected.size()), 0);
}

// Serializes and deserializes 'buffers' as a block payload.
std::vector<std::shared_ptr<arrow::Buffer>> roundTrip(
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    Payload::Type type,
    const std::shared_ptr<arrow::util::Codec>& codec) {
  auto* pool = arrow::default_memory_pool();
  auto payload = BlockPayload::fromBuffers(type, kNumRows, buffers, &kIsValidityBuffer, pool, codec.get()).ValueOrDie();
  auto out = arrow::io::BufferOutputStream::Create(1024, pool).ValueOrDie();
  EXPECT_TRUE(payload->serialize(out.get()).ok());
  auto in = std::make_shared<arrow::io::BufferReader>(out->Finish().ValueOrDie());

  uint32_t numRows = 0;
  int64_t deserializeTime = 0;
  int64_t decompressTime = 0;
  auto result = BlockPayload::deserialize(in.get(), codec, pool, numRows, deserializeTime, decompressTime).ValueOrDie();
  EXPECT_EQ(numRows, kNumRows);
  return result;
}

} // namespace

TEST(PayloadTest, allocatePaddedBuffer) {
  for (int64_t size : {0, 1, 63, 64, 512, 4096}) {
    auto buffer = allocatePaddedBuffer(size, arrow::default_memory_pool()).ValueOrDie();
    EXPECT_EQ(buffer->size(), size);
    EXPECT_GE(buffer->capacity() - buffer->size(), kSimdPadding);
    expectZeroPadding(*buffer);
  }
}

TEST(PayloadTest, resizeWithPaddingKeepsPadding) {
  auto buffer = allocatePaddedBuffer(512, arrow::default_memory_pool()).ValueOrDie();
  std::memset(buffer->mutable_data(), 0xab, 512);
  for (int64_t size : {520, 1024, 100}) {
    ASSERT_TRUE(resizeWithPadding(*buffer, size).ok());
    EXPECT_EQ(buffer->size(), size);
    EXPECT_GE(buffer->capacity() - buffer->size(), kSimdPadding);
    expectZeroPadding(*buffer);
  }
  // The leading bytes survive reallocation.
  EXPECT_EQ(buffer->data()[0], 0xab);
  EXPECT_EQ(buffer->data()[99], 0xab);
}

TEST(PayloadTest, deserializedUncompressedBuffersArePadded) {
  auto buffers = makeBuffers(1);
  auto result = roundTrip(buffers, Payload::kUncompressed, nullptr);
  ASSERT_EQ(result.size(), buffers.size());
  expectPadded(result);
  for (size_t i = 0; i < buffers.size(); ++i) {
    expectSameData(*result[i], *buffers[i]);
  }
}

TEST(PayloadTest, deserializedCompressedBuffersArePadded) {
  std::shared_ptr<arrow::util::Codec> codec = createCompressionCodec(arrow::Compression::LZ4_FRAME, CodecBackend::NONE);
  ASSERT_NE(codec, nullptr);
  auto buffers = makeBuffers(2);
  auto result = roundTrip(buffers, Payload::kCompressed, codec);
  ASSERT_EQ(result.size(), buffers.size());
  expectPadded(result);
  for (size_t i = 0; i < buffers.size(); ++i) {
    expectSameData(*result[i], *buffers[i]);
  }
}

TEST(PayloadTest, mergedBuffersArePadded) {
  auto* pool = arrow::default_memory_pool();
  auto sourceBuffers = roundTrip(makeBuffers(3), Payload::kUncompressed, nullptr);
  auto appendBuffers = roundTrip(makeBuffers(4), Payload::kUncompressed, nullptr);
  auto source = std::make_unique<InMemoryPayload>(kNumRows, &kIsValidityBuffer, nullptr, sourceBuffers);
  auto append = std::make_unique<InMemoryPayload>(kNumRows, &kIsValidityBuffer, nullptr, appendBuffers);

  auto merged = InMemoryPayload::merge(std::move(source), std::move(append), pool).ValueOrDie();
  ASSERT_EQ(merged->numRows(), 2 * kNumRows);
  std::vector<std::shared_ptr<arrow::Buffer>> mergedBuffers;
  for (uint32_t i = 0; i < merged->numBuffers(); ++i) {
    mergedBuffers.push_back(merged->readBufferAt(i).ValueOrDie());
  }
  expectPadded(mergedBuffers);
  // Merged validity is 1024 bytes and values are 64 KiB, both multiples of 64.
  EXPECT_EQ(mergedBuffers[0]->size(), 2 * kNumRows / 8);
  EXPECT_EQ(mergedBuffers[1]->size(), 2 * kNumRows * sizeof(int64_t));
}

TEST(PayloadTest, copiedBuffersArePadded) {
  auto payload = std::make_unique<InMemoryPayload>(kNumRows, &kIsValidityBuffer, nullptr, makeBuffers(5));
  ASSERT_TRUE(payload->copyBuffers(arrow::default_memory_pool()).ok());
  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  for (uint32_t i = 0; i < payload->numBuffers(); ++i) {
    buffers.push_back(payload->readBufferAt(i).ValueOrDie());
  }
  expectPadded(buffers);
}

} // namespace gluten

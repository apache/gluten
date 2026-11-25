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
#include "utils/Exception.h"

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/util/bit_util.h>
#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <vector>

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

arrow::MemoryPool* defaultPool() {
  return arrow::default_memory_pool();
}

// Buffer layout of a two-column batch, as produced by the shuffle writers:
//   column 0: int32  -> {validity, values}
//   column 1: string -> {validity, offsets, values}
const std::vector<bool> kMergeIsValidityBuffer{true, false, true, false, false};

const std::vector<bool> kSingleBufferIsValidityBuffer{false};

std::shared_ptr<arrow::ResizableBuffer> makeFilledBuffer(int64_t size, int seed) {
  GLUTEN_ASSIGN_OR_THROW(
      std::shared_ptr<arrow::ResizableBuffer> buffer, arrow::AllocateResizableBuffer(size, defaultPool()));
  for (int64_t i = 0; i < size; ++i) {
    buffer->mutable_data()[i] = static_cast<uint8_t>((seed * 31 + i * 7) & 0xff);
  }
  return buffer;
}

// Describes one payload to build: its row count, and whether its validity buffers are absent.
// An absent validity buffer means "all rows of this payload are valid" and both merge paths must
// expand it to all-ones over that payload's row range.
struct PayloadSpec {
  uint32_t numRows;
  bool nullValidity;
};

std::unique_ptr<InMemoryPayload> makeMergeInput(const PayloadSpec& spec, int seed) {
  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  buffers.reserve(kMergeIsValidityBuffer.size());
  for (size_t i = 0; i < kMergeIsValidityBuffer.size(); ++i) {
    if (kMergeIsValidityBuffer[i]) {
      if (spec.nullValidity) {
        buffers.push_back(nullptr);
      } else {
        buffers.push_back(makeFilledBuffer(arrow::bit_util::BytesForBits(spec.numRows), seed + static_cast<int>(i)));
      }
      continue;
    }
    buffers.push_back(makeFilledBuffer(static_cast<int64_t>(spec.numRows) * 4, seed + static_cast<int>(i)));
  }
  return std::make_unique<InMemoryPayload>(spec.numRows, &kMergeIsValidityBuffer, nullptr, std::move(buffers));
}

std::vector<std::unique_ptr<InMemoryPayload>> makeMergeInputs(const std::vector<PayloadSpec>& specs) {
  std::vector<std::unique_ptr<InMemoryPayload>> payloads;
  payloads.reserve(specs.size());
  for (size_t i = 0; i < specs.size(); ++i) {
    // The seed depends on the index only, so calling this twice yields two byte-identical lists.
    payloads.push_back(makeMergeInput(specs[i], static_cast<int>(i) + 1));
  }
  return payloads;
}

std::shared_ptr<arrow::Buffer> serializeToBuffer(Payload* payload) {
  GLUTEN_ASSIGN_OR_THROW(auto stream, arrow::io::BufferOutputStream::Create(1024, defaultPool()));
  GLUTEN_THROW_NOT_OK(payload->serialize(stream.get()));
  GLUTEN_ASSIGN_OR_THROW(auto buffer, stream->Finish());
  return buffer;
}

void expectPayloadEqual(InMemoryPayload* expected, InMemoryPayload* actual) {
  ASSERT_EQ(actual->numRows(), expected->numRows());
  ASSERT_EQ(actual->numBuffers(), expected->numBuffers());
  const auto numRows = expected->numRows();
  for (uint32_t i = 0; i < expected->numBuffers(); ++i) {
    GLUTEN_ASSIGN_OR_THROW(auto expectedBuffer, expected->readBufferAt(i));
    GLUTEN_ASSIGN_OR_THROW(auto actualBuffer, actual->readBufferAt(i));
    ASSERT_EQ(actualBuffer == nullptr, expectedBuffer == nullptr) << "buffer " << i;
    if (expectedBuffer == nullptr) {
      continue;
    }
    ASSERT_EQ(actualBuffer->size(), expectedBuffer->size()) << "buffer " << i;
    if (kMergeIsValidityBuffer[i]) {
      // Only the first `numRows` bits carry information. Neither code path initializes the padding
      // bits of the last byte, so they are excluded from the comparison.
      for (uint32_t row = 0; row < numRows; ++row) {
        ASSERT_EQ(
            arrow::bit_util::GetBit(actualBuffer->data(), row), arrow::bit_util::GetBit(expectedBuffer->data(), row))
            << "buffer " << i << ", row " << row;
      }
      continue;
    }
    if (expectedBuffer->size() > 0) {
      ASSERT_EQ(0, memcmp(actualBuffer->data(), expectedBuffer->data(), expectedBuffer->size())) << "buffer " << i;
    }
  }
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

// `mergeAll` is the N-way counterpart of `merge` and must be a drop-in replacement for folding a
// list of payloads pairwise with it. The specs below cover an absent validity buffer in the first
// payload (the merge result's validity buffer has to be materialized from scratch), in a later
// payload (the result's bits have to be filled with ones), and a payload without rows.
TEST(PayloadTest, mergeAllMatchesPairwiseMerge) {
  const std::vector<PayloadSpec> specs{{1024, true}, {1, false}, {512, true}, {0, true}, {777, false}, {333, true}};

  auto toFold = makeMergeInputs(specs);
  auto expected = std::move(toFold[0]);
  for (size_t i = 1; i < toFold.size(); ++i) {
    GLUTEN_ASSIGN_OR_THROW(expected, InMemoryPayload::merge(std::move(expected), std::move(toFold[i]), defaultPool()));
  }

  GLUTEN_ASSIGN_OR_THROW(auto actual, InMemoryPayload::mergeAll(makeMergeInputs(specs), defaultPool()));

  uint32_t totalRows = 0;
  for (const auto& spec : specs) {
    totalRows += spec.numRows;
  }
  ASSERT_EQ(actual->numRows(), totalRows);
  expectPayloadEqual(expected.get(), actual.get());
}

// `mergeAll` allocates its output buffers itself, so it has to keep the SIMD padding invariant
// that `merge` and the deserialization path maintain.
TEST(PayloadTest, mergeAllBuffersArePadded) {
  GLUTEN_ASSIGN_OR_THROW(
      auto merged,
      InMemoryPayload::mergeAll(makeMergeInputs({{1024, false}, {512, true}, {64, false}}), defaultPool()));
  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  for (uint32_t i = 0; i < merged->numBuffers(); ++i) {
    auto buffer = merged->readBufferAt(i).ValueOrDie();
    if (buffer != nullptr) {
      buffers.push_back(std::move(buffer));
    }
  }
  ASSERT_FALSE(buffers.empty());
  expectPadded(buffers);
}

TEST(PayloadTest, mergeAllSinglePayloadIsPassedThrough) {
  auto payloads = makeMergeInputs({{128, false}});
  const auto* expected = payloads[0].get();
  GLUTEN_ASSIGN_OR_THROW(auto merged, InMemoryPayload::mergeAll(std::move(payloads), defaultPool()));
  ASSERT_EQ(merged.get(), expected);
}

// A missing data buffer would silently drop the rows of that payload instead of merging them, so
// it must be rejected rather than merged.
TEST(PayloadTest, mergeAllRejectsNullDataBuffer) {
  auto payloads = makeMergeInputs({{64, false}, {64, false}});
  std::vector<std::shared_ptr<arrow::Buffer>> buffers(kMergeIsValidityBuffer.size(), nullptr);
  payloads[1] = std::make_unique<InMemoryPayload>(64, &kMergeIsValidityBuffer, nullptr, std::move(buffers));
  auto merged = InMemoryPayload::mergeAll(std::move(payloads), defaultPool());
  ASSERT_FALSE(merged.ok());
}

TEST(PayloadTest, mergeAllRejectsMismatchedBufferCount) {
  auto payloads = makeMergeInputs({{64, false}, {64, false}});
  std::vector<std::shared_ptr<arrow::Buffer>> buffers{makeFilledBuffer(8, 1)};
  payloads[1] = std::make_unique<InMemoryPayload>(64, &kSingleBufferIsValidityBuffer, nullptr, std::move(buffers));
  auto merged = InMemoryPayload::mergeAll(std::move(payloads), defaultPool());
  ASSERT_FALSE(merged.ok());
}

// `serializedSize` is used to size the push buffer before serializing into it, so it must return
// exactly the number of bytes `serialize` writes - for every payload type.
TEST(PayloadTest, serializedSizeMatchesSerialize) {
  auto codec = createCompressionCodec(arrow::Compression::LZ4_FRAME, CodecBackend::NONE);
  ASSERT_NE(codec, nullptr);

  for (const auto& spec : std::vector<PayloadSpec>{{1024, false}, {1024, true}, {1, false}}) {
    for (auto type : {Payload::Type::kUncompressed, Payload::Type::kToBeCompressed, Payload::Type::kCompressed}) {
      GLUTEN_ASSIGN_OR_THROW(auto payload, makeMergeInput(spec, 1)->toBlockPayload(type, defaultPool(), codec.get()));
      const auto reportedSize = payload->serializedSize();
      auto serialized = serializeToBuffer(payload.get());
      ASSERT_EQ(reportedSize, serialized->size()) << "payload type " << static_cast<int>(type);
    }
  }

  // `kRaw` carries a single, already serialized buffer.
  std::vector<std::shared_ptr<arrow::Buffer>> raw{makeFilledBuffer(2048, 1)};
  GLUTEN_ASSIGN_OR_THROW(
      auto rawPayload,
      BlockPayload::fromBuffers(
          Payload::Type::kRaw, 100, std::move(raw), &kSingleBufferIsValidityBuffer, defaultPool(), nullptr));
  const auto reportedSize = rawPayload->serializedSize();
  auto serialized = serializeToBuffer(rawPayload.get());
  ASSERT_EQ(reportedSize, serialized->size());
  ASSERT_EQ(reportedSize, 2048);
}

// Compressing into a caller-owned scratch buffer must produce the exact same bytes on the wire as
// compressing into a freshly allocated one, and the scratch buffer must only ever grow so that the
// steady state costs no allocation per payload.
TEST(PayloadTest, reusedCompressBufferProducesIdenticalPayload) {
  auto codec = createCompressionCodec(arrow::Compression::LZ4_FRAME, CodecBackend::NONE);
  ASSERT_NE(codec, nullptr);

  GLUTEN_ASSIGN_OR_THROW(
      std::shared_ptr<arrow::ResizableBuffer> reusableBuffer, arrow::AllocateResizableBuffer(0, defaultPool()));
  int64_t previousCapacity = 0;
  // Shrinking row counts in between, to cover the case where the scratch buffer is already larger
  // than the worst-case compressed length of the payload at hand.
  for (const auto& spec : std::vector<PayloadSpec>{{4096, false}, {64, false}, {2048, true}, {32, false}}) {
    GLUTEN_ASSIGN_OR_THROW(
        auto expected, makeMergeInput(spec, 1)->toBlockPayload(Payload::Type::kCompressed, defaultPool(), codec.get()));
    GLUTEN_ASSIGN_OR_THROW(
        auto actual,
        makeMergeInput(spec, 1)->toBlockPayload(
            Payload::Type::kCompressed, defaultPool(), codec.get(), reusableBuffer));

    ASSERT_EQ(actual->numRows(), expected->numRows());
    ASSERT_EQ(actual->serializedSize(), expected->serializedSize());
    auto expectedBytes = serializeToBuffer(expected.get());
    auto actualBytes = serializeToBuffer(actual.get());
    ASSERT_EQ(actualBytes->size(), expectedBytes->size());
    ASSERT_EQ(0, memcmp(actualBytes->data(), expectedBytes->data(), expectedBytes->size()));

    ASSERT_GE(reusableBuffer->capacity(), previousCapacity);
    previousCapacity = reusableBuffer->capacity();
  }
  ASSERT_GT(previousCapacity, 0);
}

} // namespace gluten

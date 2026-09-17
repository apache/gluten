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

// Regression tests for the rss_sort shuffle reader, driven through the
// public VeloxRssSortShuffleReaderDeserializer API via controllable fake
// arrow::io::InputStreams.
//
// Covers:
// - graceful EOS on an empty stream (e.g. an empty Celeborn partition);
// - EOS hit mid-page on a truncated compressed page;
// - a buggy upstream whose Read() returns a negative byte count;
// - uncompressed Presto pages spanning multiple read windows (nested struct
//   pre-scan, header crossing a refill boundary, checksummed pages) and the
//   single-window zero-copy fast path.
//
// NOTE on the two bug-probe cases (EosMidPageTerminates, NegativeReadThrows):
// on code where VeloxInputStream::next() ignores its throwIfPastEnd argument
// and silently returns on EOS, the mid-page case enters the readBytes for(;;)
// loop that never exits. FakeInputStream counts consecutive EOS reads and
// throws after kMaxConsecutiveEosReads so the loop is cut short in
// milliseconds instead of hanging until the test timeout; the assertion then
// fails on the message mismatch and surfaces "possible infinite loop" as the
// actual exception. Both cases turn green once the EOS handling fix lands.

#include <gtest/gtest.h>

#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <vector>

#include "compute/VeloxBackend.h"
#include "memory/VeloxColumnarBatch.h"
#include "memory/VeloxMemoryManager.h"
#include "shuffle/GlutenByteStream.h"
#include "shuffle/VeloxShuffleReader.h"
#include "tests/utils/TestStreamReader.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/Type.h"
#include "velox/vector/VectorStream.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

namespace gluten {

namespace {
// A minimal arrow::io::InputStream backed by a fixed in-memory payload. Once
// the payload is exhausted, Read returns 0 (EOS). With `negativeRead`, Read
// always returns -1 instead, modeling a buggy upstream that reports EOF as a
// negative byte count. With `firstReadLimit` >= 0, only the FIRST Read is
// capped to that many bytes (subsequent reads are unbounded), modeling an
// upstream (e.g. a network stream) whose first chunk ends mid-page.
//
// To keep a possible reader-side infinite loop (readBytes -> next() -> EOS ->
// silently return -> spin) from hanging the test until the CI timeout, Read
// throws after kMaxConsecutiveEosReads consecutive EOS returns. Well-behaved
// readers probe EOS only a couple of times, so the cap never trips for them.
class FakeInputStream final : public arrow::io::InputStream {
 public:
  explicit FakeInputStream(
      std::vector<uint8_t> payload = {},
      bool negativeRead = false,
      int64_t firstReadLimit = -1)
      : payload_(std::move(payload)),
        negativeRead_(negativeRead),
        firstReadLimit_(firstReadLimit) {}

  arrow::Status Close() override {
    closed_ = true;
    return arrow::Status::OK();
  }
  arrow::Result<int64_t> Tell() const override {
    return pos_;
  }
  bool closed() const override {
    return closed_;
  }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    if (negativeRead_) {
      return static_cast<int64_t>(-1);
    }
    int64_t toRead = std::min<int64_t>(nbytes, static_cast<int64_t>(payload_.size()) - pos_);
    if (firstRead_ && firstReadLimit_ >= 0) {
      toRead = std::min<int64_t>(toRead, firstReadLimit_);
      firstRead_ = false;
    }
    if (toRead > 0) {
      std::memcpy(out, payload_.data() + pos_, toRead);
      pos_ += toRead;
      consecutiveEosReads_ = 0;
    } else if (++consecutiveEosReads_ > kMaxConsecutiveEosReads) {
      // Throw a plain C++ exception: the reader wraps Read() in
      // arrow::Result and drops arrow errors via .ValueOr(0), so an
      // arrow::Status::IOError would be swallowed and the loop would spin on.
      throw std::runtime_error(
          "possible infinite loop: Read() returned 0 for " + std::to_string(kMaxConsecutiveEosReads) +
          " consecutive calls");
    }
    return toRead; // 0 == EOS when payload exhausted
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
    ARROW_ASSIGN_OR_RAISE(int64_t bytesRead, Read(nbytes, buffer->mutable_data()));
    ARROW_RETURN_NOT_OK(buffer->Resize(bytesRead, false));
    buffer->ZeroPadding();
    return std::move(buffer);
  }

 private:
  static constexpr int32_t kMaxConsecutiveEosReads = 100;

  std::vector<uint8_t> payload_;
  int64_t pos_{0};
  bool negativeRead_{false};
  int64_t firstReadLimit_{-1};
  bool firstRead_{true};
  int32_t consecutiveEosReads_{0};
  bool closed_{false};
};

// Append a little-endian POD value to `out` (Presto page header fields are
// machine byte order / little-endian on x86).
template <typename T>
void appendLe(std::vector<uint8_t>& out, T value) {
  T v = value;
  const auto* p = reinterpret_cast<const uint8_t*>(&v);
  out.insert(out.end(), p, p + sizeof(T));
}

// Build a truncated Presto compressed page: a valid 21-byte header declaring
// compressedSize bytes of body, but only `bodyBytes` bytes follow. The
// reader's compressed branch calls source->readBytes(buf, compressedSize);
// when EOS is hit mid-drain, GlutenByteInputStream::readBytes loops to
// next(true) which must VELOX_FAIL instead of spinning.
//
// Header layout (PrestoHeader.cpp): numRows:int32, pageCodecMarker:int8,
// uncompressedSize:int32, compressedSize:int32, checksum:int64 == 21 bytes.
// pageCodecMarker = kCompressedBitMask (1), no checksum bit -> actualCheckSum
// stays 0 and matches header.checksum = 0 (PrestoSerializer.cpp:159).
std::vector<uint8_t> buildTruncatedCompressedPage(int32_t compressedSize, int32_t bodyBytes) {
  std::vector<uint8_t> out;
  out.reserve(21 + bodyBytes);
  appendLe<int32_t>(out, 1); // numRows
  appendLe<int8_t>(out, 1); // pageCodecMarker = kCompressedBitMask, no checksum
  appendLe<int32_t>(out, compressedSize + 64); // uncompressedSize
  appendLe<int32_t>(out, compressedSize);
  appendLe<int64_t>(out, 0); // checksum
  for (int i = 0; i < bodyBytes; ++i) {
    out.push_back(static_cast<uint8_t>(i & 0xFF));
  }
  return out;
}
} // namespace

class VeloxShuffleReaderTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    VeloxBackend::create(AllocationListener::noop(), {});
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  static void TearDownTestSuite() {
    VeloxBackend::get()->tearDown();
  }

  std::shared_ptr<VeloxRssSortShuffleReaderDeserializer> makeDeserializer(
      const std::shared_ptr<arrow::io::InputStream>& in,
      const RowTypePtr& rowType = ROW({"c0"}, {INTEGER()})) {
    int64_t deserializeTime = 0;
    return std::make_shared<VeloxRssSortShuffleReaderDeserializer>(
        std::make_shared<TestStreamReader>(in),
        getDefaultMemoryManager(),
        rowType,
        /*batchSize=*/1024,
        common::CompressionKind_NONE,
        deserializeTime);
  }

  // Serializes `rowVector` into a single uncompressed Presto page
  // (21-byte header + payload), the exact wire format the rss-sort writer
  // produces. With `withChecksum`, a PrestoOutputStreamListener is attached so
  // the writer fills in the checksum bit and CRC (same mechanism as
  // VeloxHashShuffleWriter's complex-type flush). NOTE: must not use
  // gluten::BufferOutputStream here — its write() ignores the listener.
  std::vector<uint8_t> serializePage(const RowVectorPtr& rowVector, bool withChecksum = false) {
    serializer::presto::PrestoVectorSerde::PrestoOptions options;
    options.compressionKind = common::CompressionKind_NONE;
    auto serde = std::make_unique<serializer::presto::PrestoVectorSerde>();
    VectorStreamGroup group(pool(), serde.get());
    group.createStreamTree(asRowType(rowVector->type()), rowVector->size(), &options);
    group.append(rowVector);
    serializer::presto::PrestoOutputStreamListener listener;
    std::stringstream out;
    facebook::velox::OStreamOutputStream os(&out, withChecksum ? &listener : nullptr);
    group.flush(&os);
    const auto str = out.str();
    return std::vector<uint8_t>(str.begin(), str.end());
  }

  // ROW<c0: ARRAY<ROW<a: INTEGER>>> with `numArrays` arrays of
  // `elementsPerArray` dense elements each. The nested struct triggers the
  // Presto serde's pre-scan (tellp -> scan page -> seekp back).
  RowVectorPtr makeNestedArraysRowVector(vector_size_t numArrays, vector_size_t elementsPerArray) {
    const vector_size_t numElements = numArrays * elementsPerArray;
    auto offsets = AlignedBuffer::allocate<vector_size_t>(numArrays, pool());
    auto sizes = AlignedBuffer::allocate<vector_size_t>(numArrays, pool());
    auto* rawOffsets = offsets->asMutable<vector_size_t>();
    auto* rawSizes = sizes->asMutable<vector_size_t>();
    for (vector_size_t i = 0; i < numArrays; ++i) {
      rawOffsets[i] = i * elementsPerArray;
      rawSizes[i] = elementsPerArray;
    }
    auto elements = makeRowVector({makeFlatVector<int32_t>(
        numElements, [](vector_size_t row) { return row % 1024; })});
    auto arrayVector = std::make_shared<ArrayVector>(
        pool(),
        ARRAY(ROW({"a"}, {INTEGER()})),
        BufferPtr(nullptr),
        numArrays,
        offsets,
        sizes,
        elements);
    return makeRowVector({arrayVector});
  }
};

// Empty stream (e.g. an empty Celeborn partition): construction must NOT
// throw; next() returns nullptr (graceful EOS).
TEST_F(VeloxShuffleReaderTest, EmptyStreamGracefulEos) {
  auto deserializer = makeDeserializer(std::make_shared<FakeInputStream>());
  EXPECT_EQ(deserializer->next(), nullptr);
}

// Truncated compressed page: header reads fine and construction succeeds,
// but next() drives PrestoVectorSerde::deserialize's compressed branch ->
// readBytes(compressedSize) -> next(true) on EOS -> VELOX_FAIL. Pre-fix this
// is the documented infinite loop; post-fix it throws. See the file header
// note for how FakeInputStream cuts the pre-fix loop short.
TEST_F(VeloxShuffleReaderTest, EosMidPageTerminates) {
  auto payload = buildTruncatedCompressedPage(/*compressedSize=*/1000, /*bodyBytes=*/8);
  auto deserializer = makeDeserializer(std::make_shared<FakeInputStream>(std::move(payload)));

  VELOX_ASSERT_THROW(
      deserializer->next(), "Reading past end of VeloxRssSortShuffleReaderDeserializer::VeloxInputStream");
}

// A buggy upstream whose Read returns a negative byte count. Without a
// signed-result guard the negative value would implicitly convert to a huge
// uint64_t offset_ and corrupt setRange / loop forever. With the guard it
// fails fast during the probe-read.
TEST_F(VeloxShuffleReaderTest, NegativeReadThrows) {
  auto deserializer = makeDeserializer(
      std::make_shared<FakeInputStream>(std::vector<uint8_t>{}, /*negativeRead=*/true));
  VELOX_ASSERT_THROW(deserializer->next(), "Read returned negative value");
}

// EOS bug regression: uncompressed + nested struct + page spanning multiple
// read windows is the exact combination that reproduced the original failure
// (corrupted data / spurious "Reading past end" EOS). With the fix the page
// deserializes correctly.
TEST_F(VeloxShuffleReaderTest, UncompressedNestedStructPageSpansWindows) {
  constexpr vector_size_t kNumArrays = 20000;
  constexpr vector_size_t kElementsPerArray = 30;

  auto rowVector = makeNestedArraysRowVector(kNumArrays, kElementsPerArray);
  auto payload = serializePage(rowVector);
  // The page must be larger than the reader's read window (~1MB) to exercise
  // the multi-window slow path.
  ASSERT_GT(payload.size(), 1 << 20);

  auto deserializer = makeDeserializer(
      std::make_shared<FakeInputStream>(std::move(payload)),
      asRowType(rowVector->type()));
  auto batch = deserializer->next();
  ASSERT_NE(batch, nullptr);
  auto result = VeloxColumnarBatch::from(pool(), batch)->getRowVector();
  assertEqualVectors(rowVector, result);
  ASSERT_EQ(deserializer->next(), nullptr);
}

// Fast path: a small page that fits in one window is deserialized in-situ
// from the read window (zero copy).
TEST_F(VeloxShuffleReaderTest, SingleWindowPageZeroCopy) {
  constexpr vector_size_t kNumRows = 200;
  auto rowVector = makeRowVector({makeFlatVector<int32_t>(
      kNumRows, [](vector_size_t row) { return static_cast<int32_t>(row * 7); })});

  auto payload = serializePage(rowVector);
  // Small page: header + payload well under 1MB -> zero-copy fast path.
  ASSERT_LT(payload.size(), 1 << 20);

  auto deserializer = makeDeserializer(
      std::make_shared<FakeInputStream>(std::move(payload)),
      asRowType(rowVector->type()));
  auto batch = deserializer->next();
  ASSERT_NE(batch, nullptr);
  auto result = VeloxColumnarBatch::from(pool(), batch)->getRowVector();
  assertEqualVectors(rowVector, result);
  ASSERT_EQ(deserializer->next(), nullptr);
}

// Base-class contract: nextView returns a view truncated at the current
// range's end and advances to the next range; 0 size means end-of-stream.
TEST_F(VeloxShuffleReaderTest, BaseByteStreamNextViewAcrossRanges) {
  uint8_t range1[] = {1, 2, 3};
  uint8_t range2[] = {4, 5, 6, 7};
  GlutenByteInputStream stream(std::vector<ByteRange>{
      ByteRange{range1, 3, 0}, ByteRange{range2, 4, 0}});

  auto view1 = stream.nextView(10);
  EXPECT_EQ(view1.size(), 3);
  EXPECT_EQ(view1.front(), 1);

  auto view2 = stream.nextView(10);
  EXPECT_EQ(view2.size(), 4);
  EXPECT_EQ(view2.front(), 4);

  // All ranges consumed: nextView reports end-of-stream without throwing.
  auto view3 = stream.nextView(10);
  EXPECT_EQ(view3.size(), 0);
}

// Slow path 2: the page header crosses a refill boundary. readPage() copies
// the header out (readBytes refills mid-header) and stitches it with the
// in-situ payload into a contiguous stream before deserializing.
TEST_F(VeloxShuffleReaderTest, HeaderCrossesRefillBoundary) {
  constexpr vector_size_t kArraysA = 1024;
  constexpr vector_size_t kArraysB = 100;
  constexpr vector_size_t kElementsPerArray = 30;

  auto rowVectorA = makeNestedArraysRowVector(kArraysA, kElementsPerArray);
  auto rowVectorB = makeNestedArraysRowVector(kArraysB, kElementsPerArray);
  auto pageA = serializePage(rowVectorA);
  auto pageB = serializePage(rowVectorB);

  std::vector<uint8_t> payload = pageA;
  payload.insert(payload.end(), pageB.begin(), pageB.end());
  // First window holds all of A plus only 10 bytes of B's header.
  const int64_t firstReadLimit = static_cast<int64_t>(pageA.size()) + 10;

  auto deserializer = makeDeserializer(
      std::make_shared<FakeInputStream>(std::move(payload), /*negativeRead=*/false, firstReadLimit),
      asRowType(rowVectorA->type()));

  // Page A fits the window -> zero-copy fast path.
  auto batchA = deserializer->next();
  ASSERT_NE(batchA, nullptr);
  assertEqualVectors(rowVectorA, VeloxColumnarBatch::from(pool(), batchA)->getRowVector());

  // Page B's header crossed the refill boundary -> stitched mid path.
  auto batchB = deserializer->next();
  ASSERT_NE(batchB, nullptr);
  assertEqualVectors(rowVectorB, VeloxColumnarBatch::from(pool(), batchB)->getRowVector());

  ASSERT_EQ(deserializer->next(), nullptr);
}

// Checksummed page: the serde scans the payload via nextView() and seeks
// back to verify the CRC before deserializing.
TEST_F(VeloxShuffleReaderTest, PageDeserializesWithChecksum) {
  auto rowVector = makeRowVector({makeFlatVector<int32_t>(
      200, [](vector_size_t row) { return static_cast<int32_t>(row * 7); })});
  auto payload = serializePage(rowVector, /*withChecksum=*/true);

  auto deserializer = makeDeserializer(
      std::make_shared<FakeInputStream>(std::move(payload)),
      asRowType(rowVector->type()));
  auto batch = deserializer->next();
  ASSERT_NE(batch, nullptr);
  auto result = VeloxColumnarBatch::from(pool(), batch)->getRowVector();
  assertEqualVectors(rowVector, result);
  ASSERT_EQ(deserializer->next(), nullptr);
}

} // namespace gluten

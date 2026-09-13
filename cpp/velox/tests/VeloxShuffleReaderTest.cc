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
//

#include <gtest/gtest.h>

#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/memory_pool.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "compute/VeloxBackend.h"
#include "config/GlutenConfig.h"
#include "memory/VeloxMemoryManager.h"
#include "shuffle/VeloxShuffleReader.h"
#include "tests/utils/TestAllocationListener.h"
#include "tests/utils/TestStreamReader.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/Type.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

namespace gluten {

namespace {
// A minimal arrow::io::InputStream backed by a fixed in-memory payload. Once
// the payload is exhausted, Read returns 0 (EOS). With `negativeRead`, Read
// always returns -1 instead, modeling a buggy upstream that reports EOF as a
// negative byte count.
//
// To keep a possible reader-side infinite loop (readBytes -> next() -> EOS ->
// silently return -> spin) from hanging the test until the CI timeout, Read
// throws after kMaxConsecutiveEosReads consecutive EOS returns. Well-behaved
// readers probe EOS only a couple of times, so the cap never trips for them.
class FakeInputStream final : public arrow::io::InputStream {
 public:
  explicit FakeInputStream(std::vector<uint8_t> payload = {}, bool negativeRead = false)
      : payload_(std::move(payload)), negativeRead_(negativeRead) {}

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
    GLUTEN_ASSIGN_OR_THROW(auto buffer, arrow::AllocateResizableBuffer(nbytes, arrow::default_memory_pool()));
    GLUTEN_ASSIGN_OR_THROW(int64_t bytesRead, Read(nbytes, buffer->mutable_data()));
    GLUTEN_THROW_NOT_OK(buffer->Resize(bytesRead, false));
    buffer->ZeroPadding();
    return std::move(buffer);
  }

 private:
  static constexpr int32_t kMaxConsecutiveEosReads = 100;

  std::vector<uint8_t> payload_;
  int64_t pos_{0};
  bool negativeRead_{false};
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
  appendLe<int32_t>(out, /*numRows=*/1);
  appendLe<int8_t>(out, /*pageCodecMarker=*/1); // kCompressedBitMask, no checksum
  appendLe<int32_t>(out, /*uncompressedSize=*/compressedSize + 64);
  appendLe<int32_t>(out, compressedSize);
  appendLe<int64_t>(out, /*checksum=*/0);
  for (int i = 0; i < bodyBytes; ++i) {
    out.push_back(static_cast<uint8_t>(i & 0xFF));
  }
  return out;
}
} // namespace

class VeloxShuffleReaderTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    if (!isRegisteredNamedVectorSerde("Presto")) {
      serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
    }
    auto listener = std::make_unique<TestAllocationListener>();
    std::unordered_map<std::string, std::string> conf{{kMemoryReservationBlockSize, "1"}, {kDebugModeEnabled, "true"}};
    VeloxBackend::create(std::move(listener), conf);
  }

  static void TearDownTestCase() {
    VeloxBackend::get()->tearDown();
  }

  std::shared_ptr<VeloxRssSortShuffleReaderDeserializer> makeDeserializer(std::shared_ptr<arrow::io::InputStream> in) {
    auto streamReader = std::make_shared<TestStreamReader>(std::move(in));
    return std::make_shared<VeloxRssSortShuffleReaderDeserializer>(
        streamReader,
        getDefaultMemoryManager(),
        ROW({"c0"}, {INTEGER()}),
        /*batchSize=*/1024,
        common::CompressionKind_NONE,
        deserializeTime_);
  }

  int64_t deserializeTime_{0};
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
TEST_F(VeloxShuffleReaderTest, EosMidPageThrows) {
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
  // A buggy upstream whose Read returns a negative byte count
  auto deserializer =
      makeDeserializer(std::make_shared<FakeInputStream>(std::vector<uint8_t>{}, /*negativeRead=*/true));
  VELOX_ASSERT_THROW(deserializer->next(), "Read returned negative value");
}

} // namespace gluten

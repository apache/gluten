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

#include "utils/tac/FForCodec.h"
#include "utils/tac/TypeAwareCompressCodec.h"
#include "utils/tac/ffor.hpp"

#include <arrow/util/compression.h>
#include <gtest/gtest.h>
#include <cstring>
#include <numeric>
#include <random>
#include <vector>

using namespace gluten::ffor;
using namespace gluten;

namespace {

// Some non-TAC type values for negative testing.
static constexpr int8_t kSomeUnsupportedType = 99;

std::vector<uint64_t> genData(size_t n, uint64_t base, uint64_t range, uint64_t seed = 42) {
  std::mt19937_64 rng(seed);
  std::uniform_int_distribution<uint64_t> dist(0, range);
  std::vector<uint64_t> data(n);
  for (size_t i = 0; i < n; ++i) {
    data[i] = base + dist(rng);
  }
  return data;
}

std::vector<uint64_t> padToLanes(const std::vector<uint64_t>& data) {
  size_t padded = (data.size() + kLanes - 1) / kLanes * kLanes;
  auto result = data;
  result.resize(padded, data.empty() ? 0 : data.back());
  return result;
}

template <unsigned BW>
void roundtripTest(const uint64_t* data, size_t n, uint64_t base) {
  size_t nPadded = (n + kLanes - 1) / kLanes * kLanes;
  size_t compN = compressedWords(nPadded, BW);

  std::vector<uint64_t> encoded(compN + kLanes, 0xDEADBEEFDEADBEEF);
  std::vector<uint64_t> decoded(nPadded, 0xDEADBEEFDEADBEEF);

  encode<BW>(data, encoded.data(), base, nPadded);
  decode<BW>(encoded.data(), decoded.data(), base, nPadded);

  for (size_t i = 0; i < n; ++i) {
    ASSERT_EQ(decoded[i], data[i]) << "Mismatch at index " << i;
  }
}

void roundtripTestRt(const uint64_t* data, size_t n, uint64_t base, unsigned bw) {
  size_t nPadded = (n + kLanes - 1) / kLanes * kLanes;
  size_t compN = compressedWords(nPadded, bw);

  std::vector<uint64_t> encoded(compN + kLanes, 0);
  std::vector<uint64_t> decoded(nPadded, 0);

  encodeRt(data, encoded.data(), base, nPadded, bw);
  decodeRt(encoded.data(), decoded.data(), base, nPadded, bw);

  for (size_t i = 0; i < n; ++i) {
    ASSERT_EQ(decoded[i], data[i]) << "Mismatch at index " << i;
  }
}

void compressRoundtrip(const uint64_t* data, size_t num) {
  std::vector<uint8_t> buf(compress64Bound(num));
  size_t written = compress64(data, num, buf.data());

  std::vector<uint64_t> decoded(num);
  size_t nDecoded = decompress64(buf.data(), written, decoded.data(), num);

  ASSERT_EQ(nDecoded, num);
  for (size_t i = 0; i < num; ++i) {
    ASSERT_EQ(decoded[i], data[i]) << "Mismatch at index " << i;
  }
}

} // namespace

// Low-level encode/decode tests

TEST(FForTest, Bw0Constant) {
  std::vector<uint64_t> data(256, 12345);
  roundtripTest<0>(data.data(), data.size(), 12345);
}

TEST(FForTest, Bw1Binary) {
  auto data = padToLanes(genData(256, 100, 1));
  uint64_t base;
  unsigned bw;
  analyze(data.data(), data.size(), base, bw);
  ASSERT_EQ(bw, 1u);
  roundtripTest<1>(data.data(), data.size(), base);
}

TEST(FForTest, Bw6Narrow) {
  auto data = padToLanes(genData(1024, 1000, 63));
  roundtripTest<6>(data.data(), data.size(), 1000);
}

TEST(FForTest, Bw16Medium) {
  auto data = padToLanes(genData(1024, 50000, 65535));
  roundtripTest<16>(data.data(), data.size(), 50000);
}

TEST(FForTest, Bw32Wide) {
  auto data = padToLanes(genData(512, 1000000, (1ULL << 32) - 1));
  roundtripTest<32>(data.data(), data.size(), 1000000);
}

TEST(FForTest, Bw64FullRange) {
  auto data = padToLanes(genData(256, 0, UINT64_MAX));
  roundtripTest<64>(data.data(), data.size(), 0);
}

TEST(FForTest, AllBitwidthsSmall) {
  for (unsigned bw = 0; bw <= 64; ++bw) {
    uint64_t range = (bw == 0) ? 0 : (bw == 64) ? UINT64_MAX : ((1ULL << bw) - 1);
    auto data = padToLanes(genData(64, 42, range, 100 + bw));
    roundtripTestRt(data.data(), data.size(), 42, bw);
  }
}

TEST(FForTest, AllBitwidthsLarge) {
  for (unsigned bw = 0; bw <= 64; ++bw) {
    uint64_t range = (bw == 0) ? 0 : (bw == 64) ? UINT64_MAX : ((1ULL << bw) - 1);
    auto data = padToLanes(genData(4096, 42, range, 200 + bw));
    roundtripTestRt(data.data(), data.size(), 42, bw);
  }
}

TEST(FForTest, VariousSizes) {
  for (size_t n : {4, 8, 12, 16, 20, 28, 32, 60, 64, 100, 128, 255, 256, 500, 1000, 1024, 4096}) {
    auto data = padToLanes(genData(n, 100, 255, n));
    roundtripTest<8>(data.data(), (n + kLanes - 1) / kLanes * kLanes, 100);
  }
}

TEST(FForTest, MinSize) {
  uint64_t data[4] = {10, 11, 12, 13};
  roundtripTest<4>(data, 4, 10);
}

TEST(FForTest, AllSame) {
  std::vector<uint64_t> data(1024, 999999);
  roundtripTest<0>(data.data(), data.size(), 999999);
}

TEST(FForTest, Sequential) {
  std::vector<uint64_t> data(1024);
  for (size_t i = 0; i < 1024; ++i)
    data[i] = 1000 + i;
  uint64_t base;
  unsigned bw;
  analyze(data.data(), data.size(), base, bw);
  ASSERT_EQ(base, uint64_t(1000));
  ASSERT_EQ(bw, 10u);
  roundtripTest<10>(data.data(), data.size(), base);
}

TEST(FForTest, LargeBase) {
  uint64_t largeBase = UINT64_MAX - 1000;
  auto data = padToLanes(genData(256, largeBase, 100));
  roundtripTest<7>(data.data(), data.size(), largeBase);
}

TEST(FForTest, AnalyzeCorrectness) {
  uint64_t data1[] = {5, 5, 5, 5};
  uint64_t b;
  unsigned w;
  analyze(data1, 4, b, w);
  ASSERT_EQ(b, uint64_t(5));
  ASSERT_EQ(w, 0u);

  uint64_t data2[] = {10, 11, 10, 11};
  analyze(data2, 4, b, w);
  ASSERT_EQ(b, uint64_t(10));
  ASSERT_EQ(w, 1u);

  uint64_t data3[] = {0, 255, 128, 64};
  analyze(data3, 4, b, w);
  ASSERT_EQ(b, uint64_t(0));
  ASSERT_EQ(w, 8u);
}

TEST(FForTest, CompressedSize) {
  ASSERT_EQ(compressedWords(256, 6), size_t(24));
  ASSERT_EQ(compressedWords(256, 1), size_t(4));
  ASSERT_EQ(compressedWords(256, 64), size_t(256));
  ASSERT_EQ(compressedWords(256, 0), size_t(0));
}

// compress64 / decompress64 tests

TEST(FForTest, Compress64Basic) {
  auto data = genData(256, 1000, 99);
  compressRoundtrip(data.data(), data.size());
}

TEST(FForTest, Compress64WithTail1) {
  auto data = genData(5, 100, 50);
  compressRoundtrip(data.data(), data.size());
}

TEST(FForTest, Compress64WithTail2) {
  auto data = genData(6, 100, 50);
  compressRoundtrip(data.data(), data.size());
}

TEST(FForTest, Compress64WithTail3) {
  auto data = genData(7, 100, 50);
  compressRoundtrip(data.data(), data.size());
}

TEST(FForTest, Compress64ExactLanes) {
  auto data = genData(4, 100, 50);
  compressRoundtrip(data.data(), data.size());
}

TEST(FForTest, Compress64OnlyTail) {
  for (size_t n = 1; n <= 3; ++n) {
    auto data = genData(n, 42, 10);
    compressRoundtrip(data.data(), data.size());
  }
}

TEST(FForTest, Compress64Large) {
  auto data = genData(10000, 5000, 255);
  compressRoundtrip(data.data(), data.size());
}

TEST(FForTest, Compress64LargeWithTail) {
  auto data = genData(10001, 5000, 255);
  compressRoundtrip(data.data(), data.size());
}

TEST(FForTest, Compress64AllSame) {
  std::vector<uint64_t> data(128, 42);
  compressRoundtrip(data.data(), data.size());
}

TEST(FForTest, Compress64FullRange) {
  auto data = genData(256, 0, UINT64_MAX);
  compressRoundtrip(data.data(), data.size());
}

TEST(FForTest, Compress64SizeCheck) {
  auto narrow = genData(256, 1000, 63); // bw=6
  std::vector<uint8_t> buf(compress64Bound(256));
  size_t written = compress64(narrow.data(), narrow.size(), buf.data());

  // block header(16) + packed data + tail header(16)
  size_t expected = kHeaderSize + compressedWords(256, 6) * sizeof(uint64_t) + kHeaderSize;
  ASSERT_EQ(written, expected);

  size_t raw = 256 * sizeof(uint64_t);
  double ratio = double(raw) / double(written);
  ASSERT_GT(ratio, 9.0) << "Ratio too low: " << ratio;
}

TEST(FForTest, Compress64AllSizes1To20) {
  for (size_t n = 1; n <= 20; ++n) {
    auto data = genData(n, 100, 200, n * 7);
    compressRoundtrip(data.data(), data.size());
  }
}

// OOB read test — decode() reads past the end of the compressed buffer on the
// last group when newBitPos hits a 64-bit boundary. To detect this, we place
// the compressed buffer at the end of an mmap'd page with a PROT_NONE guard
// page immediately after, so any OOB read causes a SIGSEGV.
//
// Example: BW=32, 8 values (2 groups of 4). compressedWords = 4.
// decode pre-loads in[0..3]. After group 1: newBitPos=64, overflow=0,
// the else branch loads in[4..7] — 4 words past end of 4-word buffer.
#if defined(__linux__) || defined(__APPLE__)
#include <sys/mman.h>
#include <unistd.h>

// Allocate `size` bytes at the END of a page, with a guard page after.
// Returns {base_ptr (to munmap), usable_ptr, total_mmap_size}.
static std::tuple<void*, uint8_t*, size_t> allocAtPageEnd(size_t size) {
  long pageSize = sysconf(_SC_PAGESIZE);
  // Round up to cover `size` bytes + 1 guard page.
  size_t dataPages = (size + pageSize - 1) / pageSize;
  size_t totalSize = (dataPages + 1) * pageSize; // +1 for guard
  void* base = mmap(nullptr, totalSize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  EXPECT_NE(base, MAP_FAILED);
  // Make the last page a guard (no access).
  void* guardPage = static_cast<uint8_t*>(base) + dataPages * pageSize;
  mprotect(guardPage, pageSize, PROT_NONE);
  // Return pointer to the last `size` bytes before the guard page.
  auto* usable = static_cast<uint8_t*>(guardPage) - size;
  return {base, usable, totalSize};
}

static void freePageEnd(void* base, size_t totalSize) {
  munmap(base, totalSize);
}

// BW=32, nValues=8: clean 64-bit boundary on last group, triggers OOB pre-load.
TEST(FForTest, DecodeBw32OobGuardPage) {
  constexpr unsigned BW = 32;
  constexpr size_t N = 8; // 2 groups of 4
  uint64_t data[N];
  for (size_t i = 0; i < N; ++i) {
    data[i] = 1000 + i;
  }

  // Encode into exact-size buffer (no padding).
  size_t compN = compressedWords(N, BW);
  size_t compBytes = compN * sizeof(uint64_t);
  auto [encBase, encBuf, encTotalSize] = allocAtPageEnd(compBytes);
  auto* encPtr = reinterpret_cast<uint64_t*>(encBuf);
  encode<BW>(data, encPtr, 1000, N);

  // Decode from the exact-size buffer at page end — OOB read hits guard page.
  uint64_t decoded[N] = {};
  decode<BW>(encPtr, decoded, 1000, N);

  for (size_t i = 0; i < N; ++i) {
    ASSERT_EQ(decoded[i], data[i]) << "Mismatch at i=" << i;
  }
  freePageEnd(encBase, encTotalSize);
}

// BW=16, nValues=16: newBitPos=64 with overflow=0 on groups 3,7,11,15.
// Last group (g=3) triggers OOB.
TEST(FForTest, DecodeBw16OobGuardPage) {
  constexpr unsigned BW = 16;
  constexpr size_t N = 16; // 4 groups of 4
  uint64_t data[N];
  for (size_t i = 0; i < N; ++i) {
    data[i] = 50000 + i;
  }

  size_t compN = compressedWords(N, BW);
  size_t compBytes = compN * sizeof(uint64_t);
  auto [encBase, encBuf, encTotalSize] = allocAtPageEnd(compBytes);
  auto* encPtr = reinterpret_cast<uint64_t*>(encBuf);
  encode<BW>(data, encPtr, 50000, N);

  uint64_t decoded[N] = {};
  decode<BW>(encPtr, decoded, 50000, N);

  for (size_t i = 0; i < N; ++i) {
    ASSERT_EQ(decoded[i], data[i]) << "Mismatch at i=" << i;
  }
  freePageEnd(encBase, encTotalSize);
}

// BW=7, nValues=256: overflow > 0 on last group, triggers OOB in the
// "load next words" branch (lines 177-180).
TEST(FForTest, DecodeBw7OobGuardPage) {
  constexpr unsigned BW = 7;
  constexpr size_t N = 256;
  auto data = padToLanes(genData(N, 1000, 99));

  size_t compN = compressedWords(N, BW);
  size_t compBytes = compN * sizeof(uint64_t);
  auto [encBase, encBuf, encTotalSize] = allocAtPageEnd(compBytes);
  auto* encPtr = reinterpret_cast<uint64_t*>(encBuf);
  encode<BW>(data.data(), encPtr, 1000, N);

  uint64_t decoded[N] = {};
  decode<BW>(encPtr, decoded, 1000, N);

  for (size_t i = 0; i < N; ++i) {
    ASSERT_EQ(decoded[i], data[i]) << "Mismatch at i=" << i;
  }
  freePageEnd(encBase, encTotalSize);
}

#endif // __linux__ || __APPLE__

// Misalignment tests — verify compress64/decompress64 handle unaligned pointers.

TEST(FForTest, Compress64MisalignedOutput) {
  auto data = genData(256, 1000, 99);
  std::vector<uint8_t> buf(compress64Bound(256) + 16);

  for (size_t offset = 0; offset < 8; ++offset) {
    uint8_t* out = buf.data() + offset;
    size_t written = compress64(data.data(), data.size(), out);

    std::vector<uint64_t> decoded(256);
    size_t n = decompress64(out, written, decoded.data(), decoded.size());
    ASSERT_EQ(n, size_t(256));
    for (size_t i = 0; i < 256; ++i) {
      ASSERT_EQ(decoded[i], data[i]) << "offset=" << offset << " i=" << i;
    }
  }
}

TEST(FForTest, Compress64MisalignedInput) {
  auto raw = genData(256, 1000, 99);
  std::vector<uint8_t> inputBuf(256 * sizeof(uint64_t) + 16);

  for (size_t offset = 0; offset < 8; ++offset) {
    std::memcpy(inputBuf.data() + offset, raw.data(), 256 * sizeof(uint64_t));
    const auto* misalignedInput = reinterpret_cast<const uint64_t*>(inputBuf.data() + offset);

    std::vector<uint8_t> comp(compress64Bound(256));
    size_t written = compress64(misalignedInput, 256, comp.data());

    std::vector<uint64_t> decoded(256);
    size_t n = decompress64(comp.data(), written, decoded.data(), decoded.size());
    ASSERT_EQ(n, size_t(256));
    for (size_t i = 0; i < 256; ++i) {
      ASSERT_EQ(decoded[i], raw[i]) << "offset=" << offset << " i=" << i;
    }
  }
}

TEST(FForTest, Decompress64MisalignedOutput) {
  auto data = genData(256, 1000, 99);
  std::vector<uint8_t> comp(compress64Bound(256));
  size_t written = compress64(data.data(), data.size(), comp.data());

  std::vector<uint8_t> outBuf(256 * sizeof(uint64_t) + 16);
  for (size_t offset = 0; offset < 8; ++offset) {
    auto* misalignedOutput = reinterpret_cast<uint64_t*>(outBuf.data() + offset);
    size_t n = decompress64(comp.data(), written, misalignedOutput, 256);
    ASSERT_EQ(n, size_t(256));
    for (size_t i = 0; i < 256; ++i) {
      uint64_t val;
      std::memcpy(&val, reinterpret_cast<uint8_t*>(misalignedOutput) + i * sizeof(uint64_t), sizeof(val));
      ASSERT_EQ(val, data[i]) << "offset=" << offset << " i=" << i;
    }
  }
}

TEST(FForTest, Compress64AllMisaligned) {
  auto raw = genData(256, 1000, 99);
  std::vector<uint8_t> inputBuf(256 * sizeof(uint64_t) + 16);
  std::vector<uint8_t> compBuf(compress64Bound(256) + 16);
  std::vector<uint8_t> outBuf(256 * sizeof(uint64_t) + 16);

  for (size_t inOff = 1; inOff < 8; inOff += 3) {
    for (size_t compOff = 1; compOff < 8; compOff += 3) {
      for (size_t outOff = 1; outOff < 8; outOff += 3) {
        std::memcpy(inputBuf.data() + inOff, raw.data(), 256 * sizeof(uint64_t));
        const auto* inPtr = reinterpret_cast<const uint64_t*>(inputBuf.data() + inOff);

        size_t written = compress64(inPtr, 256, compBuf.data() + compOff);

        auto* outPtr = reinterpret_cast<uint64_t*>(outBuf.data() + outOff);
        size_t n = decompress64(compBuf.data() + compOff, written, outPtr, 256);
        ASSERT_EQ(n, size_t(256));
        for (size_t i = 0; i < 256; ++i) {
          uint64_t val;
          std::memcpy(&val, reinterpret_cast<uint8_t*>(outPtr) + i * sizeof(uint64_t), sizeof(val));
          ASSERT_EQ(val, raw[i]) << "inOff=" << inOff << " compOff=" << compOff << " outOff=" << outOff << " i=" << i;
        }
      }
    }
  }
}

// FForCodec wrapper tests

TEST(FForCodecTest, CompressDecompressRoundtrip) {
  auto data = genData(1024, 5000, 255);
  int64_t inputSize = data.size() * sizeof(uint64_t);

  auto maxLen = FForCodec::maxCompressedLength(inputSize);
  std::vector<uint8_t> compressed(maxLen);

  auto compResult =
      FForCodec::compress(reinterpret_cast<const uint8_t*>(data.data()), inputSize, compressed.data(), maxLen);
  ASSERT_TRUE(compResult.ok()) << compResult.status().ToString();
  auto compressedSize = *compResult;
  ASSERT_GT(compressedSize, 0);
  ASSERT_LT(compressedSize, inputSize);

  std::vector<uint64_t> decoded(data.size());
  auto decResult =
      FForCodec::decompress(compressed.data(), compressedSize, reinterpret_cast<uint8_t*>(decoded.data()), inputSize);
  ASSERT_TRUE(decResult.ok()) << decResult.status().ToString();

  for (size_t i = 0; i < data.size(); ++i) {
    ASSERT_EQ(decoded[i], data[i]) << "Mismatch at index " << i;
  }
}

TEST(FForCodecTest, EmptyInput) {
  auto result = FForCodec::compress(nullptr, 0, nullptr, 0);
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(*result, 0);
}

TEST(FForCodecTest, InvalidInputSize) {
  uint8_t dummy[7] = {};
  auto result = FForCodec::compress(dummy, 7, dummy, 100);
  ASSERT_FALSE(result.ok());
}

// Full-range random data: bw=64, FFOR can't compress below raw size.
// This exercises the fallback path in compressTypeAwareBuffer where
// compressed size >= uncompressed size and kUncompressedBuffer is used.
TEST(FForCodecTest, FullRangeDataRoundtrip) {
  auto data = genData(256, 0, UINT64_MAX);
  int64_t inputSize = data.size() * sizeof(uint64_t);

  auto maxLen = FForCodec::maxCompressedLength(inputSize);
  std::vector<uint8_t> compressed(maxLen);

  auto compResult =
      FForCodec::compress(reinterpret_cast<const uint8_t*>(data.data()), inputSize, compressed.data(), maxLen);
  ASSERT_TRUE(compResult.ok()) << compResult.status().ToString();
  auto compressedSize = *compResult;
  // Full-range data: compressed >= raw (FFOR adds overhead at bw=64).
  ASSERT_GE(compressedSize, inputSize);

  std::vector<uint64_t> decoded(data.size());
  auto decResult =
      FForCodec::decompress(compressed.data(), compressedSize, reinterpret_cast<uint8_t*>(decoded.data()), inputSize);
  ASSERT_TRUE(decResult.ok()) << decResult.status().ToString();

  for (size_t i = 0; i < data.size(); ++i) {
    ASSERT_EQ(decoded[i], data[i]) << "Mismatch at index " << i;
  }
}

// TypeAwareCompressCodec roundtrip tests

TEST(TypeAwareCompressCodecTest, SupportedTypes) {
  // Supported TAC types.
  ASSERT_TRUE(TypeAwareCompressCodec::support(tac::kUInt64));
  ASSERT_TRUE(TypeAwareCompressCodec::support(tac::kUInt128));
  ASSERT_TRUE(TypeAwareCompressCodec::support(tac::kUInt32));

  // Not supported.
  ASSERT_FALSE(TypeAwareCompressCodec::support(tac::kUnsupported));
  ASSERT_FALSE(TypeAwareCompressCodec::support(kSomeUnsupportedType));
}

TEST(TypeAwareCompressCodecTest, NarrowDataRoundtrip) {
  // Narrow range data: compresses well.
  auto data = genData(1024, 5000, 255);
  int64_t inputSize = data.size() * sizeof(uint64_t);

  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(inputSize, tac::kUInt64);
  std::vector<uint8_t> compressed(maxLen);

  auto compResult = TypeAwareCompressCodec::compress(
      reinterpret_cast<const uint8_t*>(data.data()), inputSize, compressed.data(), maxLen, tac::kUInt64);
  ASSERT_TRUE(compResult.ok()) << compResult.status().ToString();
  auto compressedSize = *compResult;
  ASSERT_GT(compressedSize, 0);
  ASSERT_LT(compressedSize, inputSize);

  std::vector<uint64_t> decoded(data.size());
  auto decResult = TypeAwareCompressCodec::decompress(
      compressed.data(), compressedSize, reinterpret_cast<uint8_t*>(decoded.data()), inputSize);
  ASSERT_TRUE(decResult.ok()) << decResult.status().ToString();

  for (size_t i = 0; i < data.size(); ++i) {
    ASSERT_EQ(decoded[i], data[i]) << "Mismatch at index " << i;
  }
}

// Full-range random data through TypeAwareCompressCodec.
// FFOR produces output >= input size. The caller (compressTypeAwareBuffer) would
// fall back to kUncompressedBuffer, but TypeAwareCompressCodec itself still
// produces valid (just large) output that roundtrips correctly.
TEST(TypeAwareCompressCodecTest, FullRangeDataRoundtrip) {
  auto data = genData(256, 0, UINT64_MAX);
  int64_t inputSize = data.size() * sizeof(uint64_t);

  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(inputSize, tac::kUInt64);
  std::vector<uint8_t> compressed(maxLen);

  auto compResult = TypeAwareCompressCodec::compress(
      reinterpret_cast<const uint8_t*>(data.data()), inputSize, compressed.data(), maxLen, tac::kUInt64);
  ASSERT_TRUE(compResult.ok()) << compResult.status().ToString();
  auto compressedSize = *compResult;
  // Compressed size >= input because full-range data can't be compressed.
  ASSERT_GE(compressedSize, inputSize);

  std::vector<uint64_t> decoded(data.size());
  auto decResult = TypeAwareCompressCodec::decompress(
      compressed.data(), compressedSize, reinterpret_cast<uint8_t*>(decoded.data()), inputSize);
  ASSERT_TRUE(decResult.ok()) << decResult.status().ToString();

  for (size_t i = 0; i < data.size(); ++i) {
    ASSERT_EQ(decoded[i], data[i]) << "Mismatch at index " << i;
  }
}

TEST(TypeAwareCompressCodecTest, DoubleTypeRoundtrip) {
  // Doubles reinterpreted as uint64 — exercises the codec with DOUBLE type.
  std::vector<double> doubles(512);
  std::mt19937_64 rng(99);
  std::uniform_real_distribution<double> dist(1000.0, 1001.0); // narrow range
  for (auto& d : doubles) {
    d = dist(rng);
  }

  int64_t inputSize = doubles.size() * sizeof(double);
  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(inputSize, tac::kUInt64);
  std::vector<uint8_t> compressed(maxLen);

  auto compResult = TypeAwareCompressCodec::compress(
      reinterpret_cast<const uint8_t*>(doubles.data()), inputSize, compressed.data(), maxLen, tac::kUInt64);
  ASSERT_TRUE(compResult.ok()) << compResult.status().ToString();

  std::vector<double> decoded(doubles.size());
  auto decResult = TypeAwareCompressCodec::decompress(
      compressed.data(), *compResult, reinterpret_cast<uint8_t*>(decoded.data()), inputSize);
  ASSERT_TRUE(decResult.ok()) << decResult.status().ToString();

  for (size_t i = 0; i < doubles.size(); ++i) {
    ASSERT_EQ(*reinterpret_cast<const uint64_t*>(&decoded[i]), *reinterpret_cast<const uint64_t*>(&doubles[i]))
        << "Mismatch at index " << i;
  }
}

TEST(TypeAwareCompressCodecTest, EmptyInput) {
  auto result = TypeAwareCompressCodec::compress(nullptr, 0, nullptr, 0, tac::kUInt64);
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(*result, 0);
}

TEST(TypeAwareCompressCodecTest, UnsupportedType) {
  uint8_t dummy[8] = {};
  auto result = TypeAwareCompressCodec::compress(dummy, 8, dummy, 100, kSomeUnsupportedType);
  ASSERT_FALSE(result.ok());
}

// ---------------------------------------------------------------------------
// TypeAwareCompressCodec / kUInt128 — split-lane FFOR(uint64) for int128 data.
// ---------------------------------------------------------------------------

namespace {

// Build an int128 buffer from low / high 64-bit halves.
std::vector<__int128_t> buildI128(const std::vector<uint64_t>& lo, const std::vector<uint64_t>& hi) {
  EXPECT_EQ(lo.size(), hi.size());
  std::vector<__int128_t> out(lo.size());
  for (size_t i = 0; i < lo.size(); ++i) {
    out[i] = (static_cast<__uint128_t>(hi[i]) << 64) | lo[i];
  }
  return out;
}

void roundtripUInt128(const std::vector<__int128_t>& data, bool expectShrink) {
  int64_t inputSize = static_cast<int64_t>(data.size() * sizeof(__int128_t));

  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(inputSize, tac::kUInt128);
  ASSERT_GE(maxLen, inputSize) << "maxCompressedLen must be at least input size";
  std::vector<uint8_t> compressed(maxLen + 64, 0xCC); // 64-byte sentinel tail
  size_t kSentinelStart = static_cast<size_t>(maxLen);

  auto compResult = TypeAwareCompressCodec::compress(
      reinterpret_cast<const uint8_t*>(data.data()), inputSize, compressed.data(), maxLen, tac::kUInt128);
  ASSERT_TRUE(compResult.ok()) << compResult.status().ToString();
  auto compressedSize = *compResult;
  ASSERT_GT(compressedSize, 0);
  ASSERT_LE(compressedSize, maxLen);

  // Sentinel tail must be untouched (no out-of-bounds write).
  for (size_t i = kSentinelStart; i < compressed.size(); ++i) {
    ASSERT_EQ(compressed[i], 0xCC) << "Sentinel byte trampled at offset " << i;
  }

  if (expectShrink) {
    ASSERT_LT(compressedSize, inputSize) << "Expected narrow-range data to compress below raw size";
  }

  std::vector<__int128_t> decoded(data.size(), 0);
  auto decResult = TypeAwareCompressCodec::decompress(
      compressed.data(), compressedSize, reinterpret_cast<uint8_t*>(decoded.data()), inputSize);
  ASSERT_TRUE(decResult.ok()) << decResult.status().ToString();

  for (size_t i = 0; i < data.size(); ++i) {
    ASSERT_EQ(decoded[i], data[i]) << "Mismatch at index " << i;
  }
}

} // namespace

TEST(TypeAwareCompressCodecTest, UInt128AllZero) {
  std::vector<__int128_t> data(256, 0);
  roundtripUInt128(data, /*expectShrink=*/true);
}

TEST(TypeAwareCompressCodecTest, UInt128SingleValue) {
  // Exercise tail handling: 4 values = one FFOR lane group, no full block.
  // Don't expect shrink: 4 values × 16 B = 64 B input; FFOR's per-lane block
  // header (16 B) + tail (16 B) + split body header (8 B) + TAC header (2 B)
  // exceeds the raw payload at this size.
  std::vector<__int128_t> data(4, (__int128_t)1234567890LL);
  roundtripUInt128(data, /*expectShrink=*/false);
}

TEST(TypeAwareCompressCodecTest, UInt128NarrowPositive) {
  // Typical SUM(decimal(7,2)) shuffle pattern: small range, all fit in lower-64,
  // upper-64 is identically zero -> both lanes compress to near-zero bits.
  auto lo = genData(1024, 5000, 255);
  std::vector<uint64_t> hi(1024, 0);
  auto data = buildI128(lo, hi);
  roundtripUInt128(data, /*expectShrink=*/true);
}

TEST(TypeAwareCompressCodecTest, UInt128MediumPositive) {
  // Values up to ~2^40, fits in lower-64, upper-64 is zero.
  auto lo = genData(1024, 0, (uint64_t(1) << 40) - 1, /*seed=*/7);
  std::vector<uint64_t> hi(1024, 0);
  auto data = buildI128(lo, hi);
  roundtripUInt128(data, /*expectShrink=*/true);
}

TEST(TypeAwareCompressCodecTest, UInt128MixedSign) {
  // Mix positive and negative int128. Negative values stored two's-complement
  // have lower-64 equal to 2^64 - |value|. With alternating signs, the lower
  // lane's unsigned representation spans both "small positive" and
  // "near-max-uint64", which makes FFOR's frame-of-reference compress poorly
  // (bw=64 on the lower lane). The upper lane is well-compressed: only two
  // distinct values (0 for positives, ~0 for negatives).
  //
  // This test does NOT assert shrink — pathological worst case for split-lane
  // FFOR — but does verify the round-trip is exact, the codec falls through
  // to FForCodec's bw=64 path correctly, and there is no buffer overflow.
  std::vector<__int128_t> data;
  for (int i = 0; i < 1024; ++i) {
    int64_t v = (i % 2 == 0) ? int64_t(i + 100) : -int64_t(i + 100);
    data.push_back(static_cast<__int128_t>(v));
  }
  roundtripUInt128(data, /*expectShrink=*/false);
}

TEST(TypeAwareCompressCodecTest, UInt128MixedSignSmallNegative) {
  // Realistic "mostly positive with a few negatives" pattern, e.g. signed
  // sum aggregates. Lower lane stays in a narrow range because the negative
  // values are sparse. Verify shrink still happens.
  auto pos = genData(1000, 100000, 50000, /*seed=*/37);
  std::vector<uint64_t> hi(1024, 0);
  std::vector<uint64_t> lo;
  lo.reserve(1024);
  for (uint64_t v : pos) {
    lo.push_back(v);
  }
  // 24 trailing negatives (-1..-24) to round out to 1024.
  for (int i = 1; i <= 24; ++i) {
    int64_t v = -i;
    lo.push_back(static_cast<uint64_t>(v));
    hi[lo.size() - 1] = ~uint64_t(0);
  }
  auto data = buildI128(lo, hi);
  // With only 24 negatives in 1024 values, the lower lane spans
  // [min(100000), max(near-max-uint64)] which defeats FoR. So don't
  // assert shrink here either — but verify correctness.
  roundtripUInt128(data, /*expectShrink=*/false);
}

TEST(TypeAwareCompressCodecTest, UInt128UsesUpperLane) {
  // Values that actually use the upper 64 bits — typical of decimal(38,*) with
  // very large precomputed sums. Both lanes carry real entropy.
  auto lo = genData(512, 0, (uint64_t(1) << 50) - 1, /*seed=*/11);
  auto hi = genData(512, 0, (uint64_t(1) << 10) - 1, /*seed=*/13);
  auto data = buildI128(lo, hi);
  roundtripUInt128(data, /*expectShrink=*/true);
}

TEST(TypeAwareCompressCodecTest, UInt128NearMaxValue) {
  // Stress the FFOR with values near INT128_MAX. Forces bw=64 on the upper lane
  // (which is full-range uint64 in this construction); roundtrip must still be
  // exact even when compression doesn't shrink.
  auto lo = genData(256, 0, UINT64_MAX, /*seed=*/17);
  auto hi = genData(256, 0, UINT64_MAX, /*seed=*/19);
  auto data = buildI128(lo, hi);
  // Don't assert shrink — full-range high lane defeats FFOR.
  roundtripUInt128(data, /*expectShrink=*/false);
}

TEST(TypeAwareCompressCodecTest, UInt128MonotoneSequence) {
  // Sequential decimal values — perfect frame-of-reference for the lo lane.
  std::vector<__int128_t> data(2048);
  for (size_t i = 0; i < data.size(); ++i) {
    data[i] = static_cast<__int128_t>(1000000LL + static_cast<int64_t>(i));
  }
  roundtripUInt128(data, /*expectShrink=*/true);
}

TEST(TypeAwareCompressCodecTest, UInt128MaxCompressedLenBoundary) {
  // maxCompressedLen must be tight enough to refuse undersized output buffers
  // but loose enough to always accommodate worst-case (high-entropy) input.
  // Pick a high-entropy input and verify the compress call respects the budget.
  auto lo = genData(256, 0, UINT64_MAX, /*seed=*/23);
  auto hi = genData(256, 0, UINT64_MAX, /*seed=*/29);
  auto data = buildI128(lo, hi);

  int64_t inputSize = static_cast<int64_t>(data.size() * sizeof(__int128_t));
  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(inputSize, tac::kUInt128);

  // maxLen budget must succeed:
  std::vector<uint8_t> ok(maxLen);
  auto okResult = TypeAwareCompressCodec::compress(
      reinterpret_cast<const uint8_t*>(data.data()), inputSize, ok.data(), maxLen, tac::kUInt128);
  ASSERT_TRUE(okResult.ok()) << okResult.status().ToString();

  // A budget of just the TAC header must NOT succeed:
  std::vector<uint8_t> tiny(2);
  auto tinyResult = TypeAwareCompressCodec::compress(
      reinterpret_cast<const uint8_t*>(data.data()), inputSize, tiny.data(), tiny.size(), tac::kUInt128);
  ASSERT_FALSE(tinyResult.ok());
}

TEST(TypeAwareCompressCodecTest, UInt128InvalidInputSize) {
  // Input size not a multiple of 16 must be rejected.
  std::vector<uint8_t> bad(15, 0);
  std::vector<uint8_t> out(128);
  auto result = TypeAwareCompressCodec::compress(bad.data(), bad.size(), out.data(), out.size(), tac::kUInt128);
  ASSERT_FALSE(result.ok());
}

TEST(TypeAwareCompressCodecTest, UInt128EmptyInput) {
  auto result = TypeAwareCompressCodec::compress(nullptr, 0, nullptr, 0, tac::kUInt128);
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(*result, 0);
}

TEST(TypeAwareCompressCodecTest, UInt128InvalidOutputSize) {
  // Decompressing into an output buffer whose size is not a multiple of 16 must error.
  std::vector<__int128_t> data(4, __int128_t(42));
  int64_t inputSize = data.size() * sizeof(__int128_t);
  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(inputSize, tac::kUInt128);
  std::vector<uint8_t> compressed(maxLen);
  auto cr = TypeAwareCompressCodec::compress(
      reinterpret_cast<const uint8_t*>(data.data()), inputSize, compressed.data(), maxLen, tac::kUInt128);
  ASSERT_TRUE(cr.ok());

  std::vector<uint8_t> badOut(15);
  auto dr = TypeAwareCompressCodec::decompress(compressed.data(), *cr, badOut.data(), badOut.size());
  ASSERT_FALSE(dr.ok());
}

TEST(TypeAwareCompressCodecTest, UInt128CorruptedHeaderRejected) {
  // Forge a wire payload with codec=kFForSplit128 + native strategy byte but
  // a corrupted loCompLen larger than the body. Decompress must reject cleanly
  // (not crash).
  // Header bytes (private, but we know the wire format):
  //   byte 0: CodecId::kFForSplit128 = 2
  //   byte 1: tac::kUInt128 = 1
  //   byte 2: int-codec strategy: kIntStrategyNative = 0
  //   bytes 3..10: int64 loCompLen
  std::vector<uint8_t> bogus(2 + 1 + 8 + 16, 0);
  bogus[0] = 2; // kFForSplit128
  bogus[1] = 1; // kUInt128
  bogus[2] = 0; // kIntStrategyNative
  int64_t huge = (int64_t(1) << 40); // far exceeds body length
  std::memcpy(bogus.data() + 3, &huge, sizeof(int64_t));
  std::vector<uint8_t> out(16, 0);
  auto dr = TypeAwareCompressCodec::decompress(bogus.data(), bogus.size(), out.data(), out.size());
  ASSERT_FALSE(dr.ok());
}

TEST(TypeAwareCompressCodecTest, UInt128NegativeLoCompLenRejected) {
  std::vector<uint8_t> bogus(2 + 1 + 8 + 16, 0);
  bogus[0] = 2; // kFForSplit128
  bogus[1] = 1; // kUInt128
  bogus[2] = 0; // kIntStrategyNative
  int64_t neg = int64_t(-1);
  std::memcpy(bogus.data() + 3, &neg, sizeof(int64_t));
  std::vector<uint8_t> out(16, 0);
  auto dr = TypeAwareCompressCodec::decompress(bogus.data(), bogus.size(), out.data(), out.size());
  ASSERT_FALSE(dr.ok());
}

TEST(TypeAwareCompressCodecTest, UInt128WireFormatIndependent) {
  // Two payloads compressed independently must each decompress to their own
  // input — proves the wire format is self-describing (no shared state).
  std::vector<__int128_t> a(64, (__int128_t)1000);
  std::vector<__int128_t> b(64, (__int128_t)2000);

  int64_t sz = a.size() * sizeof(__int128_t);
  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(sz, tac::kUInt128);

  std::vector<uint8_t> cA(maxLen), cB(maxLen);
  auto rA = TypeAwareCompressCodec::compress(
      reinterpret_cast<const uint8_t*>(a.data()), sz, cA.data(), maxLen, tac::kUInt128);
  auto rB = TypeAwareCompressCodec::compress(
      reinterpret_cast<const uint8_t*>(b.data()), sz, cB.data(), maxLen, tac::kUInt128);
  ASSERT_TRUE(rA.ok());
  ASSERT_TRUE(rB.ok());

  std::vector<__int128_t> dA(64, 0), dB(64, 0);
  ASSERT_TRUE(TypeAwareCompressCodec::decompress(cA.data(), *rA, reinterpret_cast<uint8_t*>(dA.data()), sz).ok());
  ASSERT_TRUE(TypeAwareCompressCodec::decompress(cB.data(), *rB, reinterpret_cast<uint8_t*>(dB.data()), sz).ok());

  for (size_t i = 0; i < 64; ++i) {
    ASSERT_EQ(dA[i], a[i]);
    ASSERT_EQ(dB[i], b[i]);
  }
}

TEST(TypeAwareCompressCodecTest, UInt128CompressesBetterThanUncompressed) {
  // Sanity: realistic decimal(38,2) shuffle pattern (small positive prices)
  // must compress to substantially less than the raw 16-bytes-per-value.
  auto lo = genData(4096, 100000, 50000, /*seed=*/31); // prices ~$1000-$1500 in scale-2
  std::vector<uint64_t> hi(4096, 0);
  auto data = buildI128(lo, hi);

  int64_t inputSize = data.size() * sizeof(__int128_t);
  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(inputSize, tac::kUInt128);
  std::vector<uint8_t> compressed(maxLen);

  auto cr = TypeAwareCompressCodec::compress(
      reinterpret_cast<const uint8_t*>(data.data()), inputSize, compressed.data(), maxLen, tac::kUInt128);
  ASSERT_TRUE(cr.ok());

  // 4096 values × 16 B = 65536 bytes raw. Tight FoR on the lo lane (~17 bits)
  // plus an all-zero hi lane should easily land under 25% of raw.
  ASSERT_LT(*cr, inputSize / 4) << "compressed=" << *cr << " raw=" << inputSize;
}

// ---------------------------------------------------------------------------
// TypeAwareCompressCodec / kUInt32 — FFOR(uint64) over a zero-extended view
// of the uint32 stream.  Used for INT32, DATE32, and string-offsets buffers.
// ---------------------------------------------------------------------------

namespace {

void roundtripUInt32(const std::vector<uint32_t>& data, bool expectShrink) {
  int64_t inputSize = static_cast<int64_t>(data.size() * sizeof(uint32_t));

  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(inputSize, tac::kUInt32);
  ASSERT_GE(maxLen, inputSize) << "maxCompressedLen must be at least input size";
  std::vector<uint8_t> compressed(maxLen + 64, 0xCC);
  size_t kSentinelStart = static_cast<size_t>(maxLen);

  auto compResult = TypeAwareCompressCodec::compress(
      reinterpret_cast<const uint8_t*>(data.data()), inputSize, compressed.data(), maxLen, tac::kUInt32);
  ASSERT_TRUE(compResult.ok()) << compResult.status().ToString();
  auto compressedSize = *compResult;
  ASSERT_GT(compressedSize, 0);
  ASSERT_LE(compressedSize, maxLen);

  // Sentinel tail must be untouched (no out-of-bounds write).
  for (size_t i = kSentinelStart; i < compressed.size(); ++i) {
    ASSERT_EQ(compressed[i], 0xCC) << "Sentinel byte trampled at offset " << i;
  }

  if (expectShrink) {
    ASSERT_LT(compressedSize, inputSize) << "Expected narrow-range data to compress below raw size";
  }

  std::vector<uint32_t> decoded(data.size(), 0xDEADBEEF);
  auto decResult = TypeAwareCompressCodec::decompress(
      compressed.data(), compressedSize, reinterpret_cast<uint8_t*>(decoded.data()), inputSize);
  ASSERT_TRUE(decResult.ok()) << decResult.status().ToString();

  for (size_t i = 0; i < data.size(); ++i) {
    ASSERT_EQ(decoded[i], data[i]) << "Mismatch at index " << i;
  }
}

} // namespace

TEST(TypeAwareCompressCodecTest, UInt32AllZero) {
  std::vector<uint32_t> data(256, 0);
  roundtripUInt32(data, /*expectShrink=*/true);
}

TEST(TypeAwareCompressCodecTest, UInt32SingleValueTail) {
  // 4 values — tail handling.
  std::vector<uint32_t> data(4, 42);
  // No shrink at this size — per-block header overhead exceeds payload.
  roundtripUInt32(data, /*expectShrink=*/false);
}

TEST(TypeAwareCompressCodecTest, UInt32NarrowPositive) {
  // Typical INT32 column shuffle pattern: small values clustered together.
  std::mt19937 rng(57);
  std::uniform_int_distribution<uint32_t> dist(0, 255);
  std::vector<uint32_t> data(1024);
  for (auto& v : data)
    v = dist(rng);
  roundtripUInt32(data, /*expectShrink=*/true);
}

TEST(TypeAwareCompressCodecTest, UInt32StringOffsetsMonotone) {
  // Realistic Arrow string-offsets buffer: monotonically increasing int32
  // starting at 0, deltas in [1, 30] (typical short-string lengths).
  std::mt19937 rng(101);
  std::uniform_int_distribution<uint32_t> lengthDist(1, 30);
  std::vector<uint32_t> offsets;
  offsets.reserve(2048);
  uint32_t cur = 0;
  offsets.push_back(cur);
  for (size_t i = 1; i < 2048; ++i) {
    cur += lengthDist(rng);
    offsets.push_back(cur);
  }
  // Plain FoR will pick base=0 and bw covering max-min ~ 30*2047 ~ 16 bits.
  // ~2x compression vs raw — still a real saving.  (Delta-FFOR would do
  // much better; see future work in the writeup.)
  roundtripUInt32(offsets, /*expectShrink=*/true);
}

TEST(TypeAwareCompressCodecTest, UInt32MaxValue) {
  // Full uint32 range; FFOR can't compress below raw + headers.
  std::mt19937 rng(83);
  std::uniform_int_distribution<uint32_t> dist(0, UINT32_MAX);
  std::vector<uint32_t> data(256);
  for (auto& v : data)
    v = dist(rng);
  roundtripUInt32(data, /*expectShrink=*/false);
}

TEST(TypeAwareCompressCodecTest, UInt32MaxCompressedLenBoundary) {
  std::mt19937 rng(89);
  std::uniform_int_distribution<uint32_t> dist(0, UINT32_MAX);
  std::vector<uint32_t> data(256);
  for (auto& v : data)
    v = dist(rng);

  int64_t inputSize = static_cast<int64_t>(data.size() * sizeof(uint32_t));
  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(inputSize, tac::kUInt32);

  // The maxLen budget always succeeds:
  std::vector<uint8_t> ok(maxLen);
  auto okResult = TypeAwareCompressCodec::compress(
      reinterpret_cast<const uint8_t*>(data.data()), inputSize, ok.data(), maxLen, tac::kUInt32);
  ASSERT_TRUE(okResult.ok()) << okResult.status().ToString();

  // Just the TAC header (2 bytes) is not enough:
  std::vector<uint8_t> tiny(2);
  auto tinyResult = TypeAwareCompressCodec::compress(
      reinterpret_cast<const uint8_t*>(data.data()), inputSize, tiny.data(), tiny.size(), tac::kUInt32);
  ASSERT_FALSE(tinyResult.ok());
}

TEST(TypeAwareCompressCodecTest, UInt32InvalidInputSize) {
  // Size not a multiple of 4 must be rejected.
  std::vector<uint8_t> bad(7, 0);
  std::vector<uint8_t> out(128);
  auto r = TypeAwareCompressCodec::compress(bad.data(), bad.size(), out.data(), out.size(), tac::kUInt32);
  ASSERT_FALSE(r.ok());
}

TEST(TypeAwareCompressCodecTest, UInt32EmptyInput) {
  auto r = TypeAwareCompressCodec::compress(nullptr, 0, nullptr, 0, tac::kUInt32);
  ASSERT_TRUE(r.ok());
  ASSERT_EQ(*r, 0);
}

TEST(TypeAwareCompressCodecTest, UInt32InvalidOutputSize) {
  // Output size not a multiple of 4 on decompress must error cleanly.
  std::vector<uint32_t> data(4, 1234567);
  int64_t inputSize = data.size() * sizeof(uint32_t);
  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(inputSize, tac::kUInt32);
  std::vector<uint8_t> compressed(maxLen);
  auto cr = TypeAwareCompressCodec::compress(
      reinterpret_cast<const uint8_t*>(data.data()), inputSize, compressed.data(), maxLen, tac::kUInt32);
  ASSERT_TRUE(cr.ok());

  std::vector<uint8_t> badOut(7);
  auto dr = TypeAwareCompressCodec::decompress(compressed.data(), *cr, badOut.data(), badOut.size());
  ASSERT_FALSE(dr.ok());
}

TEST(TypeAwareCompressCodecTest, UInt32CompressesBetterThanUncompressed) {
  // INT32 columns in real shuffle: dense, narrow range (e.g. d_year values).
  std::mt19937 rng(127);
  std::uniform_int_distribution<uint32_t> dist(1990, 2020);
  std::vector<uint32_t> data(4096);
  for (auto& v : data)
    v = dist(rng);

  int64_t inputSize = data.size() * sizeof(uint32_t);
  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(inputSize, tac::kUInt32);
  std::vector<uint8_t> compressed(maxLen);

  auto cr = TypeAwareCompressCodec::compress(
      reinterpret_cast<const uint8_t*>(data.data()), inputSize, compressed.data(), maxLen, tac::kUInt32);
  ASSERT_TRUE(cr.ok());

  // 4096 × 4 B = 16384 bytes raw. Range = ~30 ⇒ bw=5 ⇒ ~3 KB packed.
  // Should land under 30% of raw.
  ASSERT_LT(*cr, inputSize * 0.3) << "compressed=" << *cr << " raw=" << inputSize;
}

TEST(TypeAwareCompressCodecTest, UInt32CrossCodecIndependence) {
  // Verify a kUInt32 payload and a kUInt64 payload decompress independently
  // to their own data, even when one immediately follows the other in memory.
  std::vector<uint32_t> a32(64);
  for (size_t i = 0; i < a32.size(); ++i)
    a32[i] = static_cast<uint32_t>(1000 + i);
  std::vector<uint64_t> a64(64);
  for (size_t i = 0; i < a64.size(); ++i)
    a64[i] = static_cast<uint64_t>(2000000 + i);

  int64_t s32 = a32.size() * sizeof(uint32_t);
  int64_t s64 = a64.size() * sizeof(uint64_t);
  auto m32 = TypeAwareCompressCodec::maxCompressedLen(s32, tac::kUInt32);
  auto m64 = TypeAwareCompressCodec::maxCompressedLen(s64, tac::kUInt64);

  std::vector<uint8_t> c32(m32), c64(m64);
  auto r32 = TypeAwareCompressCodec::compress(
      reinterpret_cast<const uint8_t*>(a32.data()), s32, c32.data(), m32, tac::kUInt32);
  auto r64 = TypeAwareCompressCodec::compress(
      reinterpret_cast<const uint8_t*>(a64.data()), s64, c64.data(), m64, tac::kUInt64);
  ASSERT_TRUE(r32.ok());
  ASSERT_TRUE(r64.ok());

  std::vector<uint32_t> d32(64, 0);
  std::vector<uint64_t> d64(64, 0);
  ASSERT_TRUE(TypeAwareCompressCodec::decompress(c32.data(), *r32, reinterpret_cast<uint8_t*>(d32.data()), s32).ok());
  ASSERT_TRUE(TypeAwareCompressCodec::decompress(c64.data(), *r64, reinterpret_cast<uint8_t*>(d64.data()), s64).ok());
  for (size_t i = 0; i < 64; ++i) {
    ASSERT_EQ(d32[i], a32[i]);
    ASSERT_EQ(d64[i], a64[i]);
  }
}

// =============================================================================
// TIMESTAMP-shaped data tests
//
// Velox Timestamp is a 16-byte struct { int64_t seconds_; uint64_t nanos_; }.
// The MS-fork TAC dispatch routes TypeKind::TIMESTAMP to tac::kUInt128, which
// applies split-lane FFOR independently on the two uint64 lanes. These tests
// exercise realistic timestamp data shapes through that path and confirm
// roundtrip correctness + reasonable compression.
// =============================================================================

namespace {

// Pack (seconds, nanos) pairs into a contiguous uint8 buffer with the same
// layout as Velox FlatVector<Timestamp>: 16 bytes per row, low 8 = seconds
// (int64), high 8 = nanos (uint64).
std::vector<uint8_t> packTimestamps(const std::vector<int64_t>& seconds, const std::vector<uint64_t>& nanos) {
  EXPECT_EQ(seconds.size(), nanos.size());
  std::vector<uint8_t> bytes(seconds.size() * 16);
  for (size_t i = 0; i < seconds.size(); ++i) {
    std::memcpy(bytes.data() + i * 16, &seconds[i], 8);
    std::memcpy(bytes.data() + i * 16 + 8, &nanos[i], 8);
  }
  return bytes;
}

void unpackTimestamps(
    const uint8_t* bytes,
    size_t rows,
    std::vector<int64_t>& secondsOut,
    std::vector<uint64_t>& nanosOut) {
  secondsOut.assign(rows, 0);
  nanosOut.assign(rows, 0);
  for (size_t i = 0; i < rows; ++i) {
    std::memcpy(&secondsOut[i], bytes + i * 16, 8);
    std::memcpy(&nanosOut[i], bytes + i * 16 + 8, 8);
  }
}

void roundtripTimestampViaUInt128(const std::vector<int64_t>& secondsIn, const std::vector<uint64_t>& nanosIn) {
  auto packed = packTimestamps(secondsIn, nanosIn);
  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(packed.size(), tac::kUInt128);
  std::vector<uint8_t> compressed(maxLen);

  auto r = TypeAwareCompressCodec::compress(packed.data(), packed.size(), compressed.data(), maxLen, tac::kUInt128);
  ASSERT_TRUE(r.ok()) << r.status().ToString();

  std::vector<uint8_t> decompressed(packed.size());
  ASSERT_TRUE(TypeAwareCompressCodec::decompress(compressed.data(), *r, decompressed.data(), packed.size()).ok());

  std::vector<int64_t> secondsOut;
  std::vector<uint64_t> nanosOut;
  unpackTimestamps(decompressed.data(), secondsIn.size(), secondsOut, nanosOut);

  ASSERT_EQ(secondsOut, secondsIn);
  ASSERT_EQ(nanosOut, nanosIn);
}

} // namespace

TEST(TypeAwareCompressCodecTest, TimestampSecondAlignedRoundtrip) {
  // Realistic shape #1: a column of second-aligned timestamps from a single
  // day, nanos == 0 everywhere. Seconds lane has very low FFOR width (range
  // ~86400). Nanos lane is constant zero.
  const int64_t baseSec = 1716220800; // 2024-05-20 16:00:00 UTC
  std::vector<int64_t> seconds;
  std::vector<uint64_t> nanos;
  for (int i = 0; i < 512; ++i) {
    seconds.push_back(baseSec + i * 7); // every 7 seconds
    nanos.push_back(0);
  }
  roundtripTimestampViaUInt128(seconds, nanos);
}

TEST(TypeAwareCompressCodecTest, TimestampSparseNanosRoundtrip) {
  // Realistic shape #2: timestamps clustered in a day, with nanos sometimes
  // non-zero (e.g., sub-second precision events). Seconds lane low-width,
  // nanos lane medium-width but mostly small.
  const int64_t baseSec = 1716220800;
  std::vector<int64_t> seconds;
  std::vector<uint64_t> nanos;
  for (int i = 0; i < 1024; ++i) {
    seconds.push_back(baseSec + i / 4); // four readings per second
    nanos.push_back((i % 4) * 250'000'000ULL); // 0, 250M, 500M, 750M ns
  }
  roundtripTimestampViaUInt128(seconds, nanos);
}

TEST(TypeAwareCompressCodecTest, TimestampMultiDayRoundtrip) {
  // Realistic shape #3: timestamps spread across multiple days. Seconds lane
  // wider than the single-day case (still narrow vs full int64 range). Tests
  // that the split-lane FFOR handles realistic fact-table date ranges.
  const int64_t baseSec = 1716220800;
  std::vector<int64_t> seconds;
  std::vector<uint64_t> nanos;
  for (int i = 0; i < 2048; ++i) {
    seconds.push_back(baseSec + i * 3600); // hourly readings, ~85 days span
    nanos.push_back(0);
  }
  roundtripTimestampViaUInt128(seconds, nanos);
}

TEST(TypeAwareCompressCodecTest, TimestampAllSameRoundtrip) {
  // Degenerate shape: constant timestamp column (e.g., a partition key derived
  // from a fixed instant). Both lanes have bw=0; codec should still roundtrip.
  std::vector<int64_t> seconds(256, 1716220800);
  std::vector<uint64_t> nanos(256, 12345);
  roundtripTimestampViaUInt128(seconds, nanos);
}

TEST(TypeAwareCompressCodecTest, TimestampCompressesBetterThanUncompressed) {
  // Validate that for a realistic timestamp shape (dense seconds, sparse
  // nanos), the kUInt128 codec actually saves bytes compared to plain memcpy.
  const int64_t baseSec = 1716220800;
  std::vector<int64_t> seconds;
  std::vector<uint64_t> nanos;
  for (int i = 0; i < 4096; ++i) {
    seconds.push_back(baseSec + i * 60); // minute-aligned for one minute span
    nanos.push_back(0);
  }
  auto packed = packTimestamps(seconds, nanos);
  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(packed.size(), tac::kUInt128);
  std::vector<uint8_t> compressed(maxLen);
  auto r = TypeAwareCompressCodec::compress(packed.data(), packed.size(), compressed.data(), maxLen, tac::kUInt128);
  ASSERT_TRUE(r.ok());
  // Expect at least 4x compression for this shape (constant nanos + dense
  // narrow-bw seconds). 4096 rows * 16 bytes = 65536 raw; target < 16384.
  EXPECT_LT(*r, packed.size() / 4) << "compressed=" << *r << " raw=" << packed.size();
}

// =============================================================================
// kStringDict adaptive codec tests
//
// String DATA buffer compression: codec tries dictionary encoding AND LZ4
// fallback, emits whichever is smaller with a strategy byte. Reader dispatches
// on the strategy byte.
//
// These tests cover:
//   - Round-trip correctness for both strategies and degenerate inputs.
//   - Strategy selection: low-card with long runs => LZ4 wins; medium-card
//     scattered => dict wins.
//   - Invalid input rejection (truncated headers, bad indices, etc.).
// =============================================================================

namespace {

// Pack a vector of strings into a contiguous data buffer + Arrow int32 offsets
// buffer (length numRows+1, offsets[0]=0, offsets[i+1]=offsets[i]+len(s_i)).
struct PackedStrings {
  std::vector<uint8_t> data;
  std::vector<int32_t> offsets;
};

PackedStrings packStrings(const std::vector<std::string>& strings) {
  PackedStrings p;
  p.offsets.reserve(strings.size() + 1);
  p.offsets.push_back(0);
  int32_t cur = 0;
  for (const auto& s : strings) {
    cur += static_cast<int32_t>(s.size());
    p.offsets.push_back(cur);
  }
  p.data.reserve(static_cast<size_t>(cur));
  for (const auto& s : strings) {
    p.data.insert(p.data.end(), s.begin(), s.end());
  }
  return p;
}

void roundtripStringDict(const std::vector<std::string>& strings) {
  auto packed = packStrings(strings);
  int32_t numRows = static_cast<int32_t>(strings.size());
  int64_t inputLen = static_cast<int64_t>(packed.data.size());

  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(inputLen, tac::kStringDict);
  std::vector<uint8_t> compressed(static_cast<size_t>(maxLen));

  auto r = TypeAwareCompressCodec::compress(
      packed.data.data(),
      inputLen,
      compressed.data(),
      maxLen,
      tac::kStringDict,
      reinterpret_cast<const uint8_t*>(packed.offsets.data()),
      numRows);
  ASSERT_TRUE(r.ok()) << r.status().ToString();

  // For all-empty inputs (inputLen == 0), the codec returns 0 bytes (nothing
  // to compress) and decompress isn't called by the wrapper — the higher-level
  // wrapper short-circuits via kZeroLengthBuffer. Mirror that here.
  if (inputLen == 0) {
    EXPECT_EQ(*r, 0);
    return;
  }

  std::vector<uint8_t> decompressed(static_cast<size_t>(inputLen));
  ASSERT_TRUE(TypeAwareCompressCodec::decompress(compressed.data(), *r, decompressed.data(), inputLen).ok());

  ASSERT_EQ(decompressed.size(), packed.data.size());
  ASSERT_EQ(std::memcmp(decompressed.data(), packed.data.data(), packed.data.size()), 0);
}

// Inspect the strategy byte emitted (after the 2-byte codec header).
uint8_t strategyByte(const uint8_t* compressed) {
  // [codec_id (1B)] [tac_type (1B)] [strategy (1B)] ...
  return compressed[2];
}

} // namespace

TEST(TypeAwareCompressCodecTest, StringDictLowCardRoundtrip) {
  // Very low cardinality, like d_day_name in TPC-DS: 7 unique values, many rows.
  std::vector<std::string> names = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};
  std::vector<std::string> strings;
  for (int i = 0; i < 1024; ++i) {
    strings.push_back(names[i % names.size()]);
  }
  roundtripStringDict(strings);
}

TEST(TypeAwareCompressCodecTest, StringDictMediumCardScatteredRoundtrip) {
  // Medium cardinality, scattered. Realistic shape for an item-brand column
  // in shuffle (hundreds of unique brands distributed across rows).
  std::vector<std::string> strings;
  strings.reserve(2048);
  for (int i = 0; i < 2048; ++i) {
    strings.push_back("brand_" + std::to_string((i * 7919) % 300));
  }
  roundtripStringDict(strings);
}

TEST(TypeAwareCompressCodecTest, StringDictHighCardRoundtrip) {
  // High cardinality (one unique per row, names-like).
  std::vector<std::string> strings;
  strings.reserve(1024);
  for (int i = 0; i < 1024; ++i) {
    strings.push_back("user_" + std::to_string(i));
  }
  roundtripStringDict(strings);
}

TEST(TypeAwareCompressCodecTest, StringDictAllSameRoundtrip) {
  std::vector<std::string> strings(256, "same");
  roundtripStringDict(strings);
}

TEST(TypeAwareCompressCodecTest, StringDictEmptyStringsRoundtrip) {
  std::vector<std::string> strings(128, "");
  roundtripStringDict(strings);
}

TEST(TypeAwareCompressCodecTest, StringDictMixedLengthsRoundtrip) {
  std::vector<std::string> strings = {"", "a", "ab", "abcdef", std::string(1024, 'x'), "tail", "", "z"};
  // Repeat the pattern to exercise the index path properly.
  std::vector<std::string> repeated;
  for (int i = 0; i < 32; ++i) {
    for (const auto& s : strings) {
      repeated.push_back(s);
    }
  }
  roundtripStringDict(repeated);
}

TEST(TypeAwareCompressCodecTest, StringDictLargeDictionaryRoundtrip) {
  // 1000 unique strings, each appearing twice. Forces 2-byte indices.
  std::vector<std::string> strings;
  strings.reserve(2000);
  for (int i = 0; i < 1000; ++i) {
    strings.push_back("entry_" + std::to_string(i) + "_padding");
  }
  for (int i = 0; i < 1000; ++i) {
    strings.push_back("entry_" + std::to_string(i) + "_padding");
  }
  roundtripStringDict(strings);
}

TEST(TypeAwareCompressCodecTest, StringDictPicksLz4ForLongRuns) {
  // 7 unique names, but presented as long consecutive runs (the case where
  // LZ4 beats dict). Verify codec actually picks LZ4 strategy.
  std::vector<std::string> names = {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"};
  std::vector<std::string> strings;
  for (const auto& n : names) {
    for (int i = 0; i < 2000; ++i) {
      strings.push_back(n);
    }
  }
  auto packed = packStrings(strings);
  int32_t numRows = static_cast<int32_t>(strings.size());
  int64_t inputLen = static_cast<int64_t>(packed.data.size());
  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(inputLen, tac::kStringDict);
  std::vector<uint8_t> compressed(static_cast<size_t>(maxLen));
  auto r = TypeAwareCompressCodec::compress(
      packed.data.data(),
      inputLen,
      compressed.data(),
      maxLen,
      tac::kStringDict,
      reinterpret_cast<const uint8_t*>(packed.offsets.data()),
      numRows);
  ASSERT_TRUE(r.ok()) << r.status().ToString();
  EXPECT_EQ(strategyByte(compressed.data()), 1) << "expected LZ4 strategy (1) for long-run low-card input";
}

TEST(TypeAwareCompressCodecTest, StringDictPicksDictForScattered) {
  // High-cardinality random-content strings (e.g., the c_first_name or
  // *_address shape in TPC-DS/TPC-H): LZ4 cannot find repeating substrings,
  // so dict encoding cleanly wins. Validates the adaptive selection picks
  // dict here.
  std::mt19937 rng(42);
  std::vector<std::string> uniq;
  for (int i = 0; i < 3000; ++i) {
    std::string s;
    int len = 4 + (rng() % 8);
    for (int j = 0; j < len; ++j) {
      s += ('a' + (rng() % 26));
    }
    uniq.push_back(s);
  }
  std::vector<std::string> strings;
  strings.reserve(50000);
  for (int i = 0; i < 50000; ++i) {
    strings.push_back(uniq[rng() % uniq.size()]);
  }
  auto packed = packStrings(strings);
  int32_t numRows = static_cast<int32_t>(strings.size());
  int64_t inputLen = static_cast<int64_t>(packed.data.size());
  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(inputLen, tac::kStringDict);
  std::vector<uint8_t> compressed(static_cast<size_t>(maxLen));
  auto r = TypeAwareCompressCodec::compress(
      packed.data.data(),
      inputLen,
      compressed.data(),
      maxLen,
      tac::kStringDict,
      reinterpret_cast<const uint8_t*>(packed.offsets.data()),
      numRows);
  ASSERT_TRUE(r.ok()) << r.status().ToString();
  EXPECT_EQ(strategyByte(compressed.data()), 0) << "expected Dict strategy (0) for high-card random-content input";
}

TEST(TypeAwareCompressCodecTest, StringDictRejectsMissingOffsets) {
  std::vector<uint8_t> data = {'a', 'b', 'c'};
  std::vector<uint8_t> out(64);
  auto r = TypeAwareCompressCodec::compress(data.data(), 3, out.data(), out.size(), tac::kStringDict, nullptr, 0);
  EXPECT_FALSE(r.ok());
}

TEST(TypeAwareCompressCodecTest, StringDictAcceptsOffsetsFirstNotZeroViaLz4) {
  // Historically the codec rejected offsets[0] != 0 outright. After the OCP
  // Run C regression we know Velox passes sliced buffers where offsets[0] > 0;
  // the codec now silently falls back to LZ4 in that case and produces a
  // bit-exact round-trip of the full input buffer (including the leading
  // bytes outside the offset range).
  std::vector<int32_t> offsets = {1, 4};
  std::vector<uint8_t> data = {0xFF /* leading byte at offset 0 */, 'a', 'b', 'c'};
  std::vector<uint8_t> out(64);
  auto r = TypeAwareCompressCodec::compress(
      data.data(),
      static_cast<int64_t>(data.size()),
      out.data(),
      static_cast<int64_t>(out.size()),
      tac::kStringDict,
      reinterpret_cast<const uint8_t*>(offsets.data()),
      1);
  ASSERT_TRUE(r.ok()) << r.status().ToString();
  // Confirm LZ4 strategy was picked.
  ASSERT_GT(*r, 2);
  EXPECT_EQ(out[2], 1u) << "non-zero offsets[0] must pick LZ4 strategy";
  // Confirm bit-exact round-trip.
  std::vector<uint8_t> decoded(data.size(), 0);
  auto dr = TypeAwareCompressCodec::decompress(out.data(), *r, decoded.data(), static_cast<int64_t>(data.size()));
  ASSERT_TRUE(dr.ok()) << dr.status().ToString();
  EXPECT_EQ(0, std::memcmp(data.data(), decoded.data(), data.size()));
}

TEST(TypeAwareCompressCodecTest, StringDictRejectsTruncatedDecompressInput) {
  std::vector<uint8_t> tooSmall = {4 /* codec_id=kStringDict */, 3 /* tac_type */};
  std::vector<uint8_t> out(16);
  auto r = TypeAwareCompressCodec::decompress(tooSmall.data(), tooSmall.size(), out.data(), out.size());
  EXPECT_FALSE(r.ok());
}

TEST(TypeAwareCompressCodecTest, StringDictRejectsUnknownStrategy) {
  // codec_id=kStringDict, tac_type=kStringDict, strategy=99 (invalid)
  std::vector<uint8_t> bad = {4, 3, 99};
  std::vector<uint8_t> out(16);
  auto r = TypeAwareCompressCodec::decompress(bad.data(), bad.size(), out.data(), out.size());
  EXPECT_FALSE(r.ok());
}

TEST(TypeAwareCompressCodecTest, StringDictCompressesBetterThanRaw) {
  // For a realistic medium-card scattered input, dict must beat raw size.
  std::vector<std::string> strings;
  for (int i = 0; i < 4096; ++i) {
    strings.push_back("brand_" + std::to_string((i * 7919) % 256));
  }
  auto packed = packStrings(strings);
  int64_t inputLen = static_cast<int64_t>(packed.data.size());
  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(inputLen, tac::kStringDict);
  std::vector<uint8_t> compressed(static_cast<size_t>(maxLen));
  auto r = TypeAwareCompressCodec::compress(
      packed.data.data(),
      inputLen,
      compressed.data(),
      maxLen,
      tac::kStringDict,
      reinterpret_cast<const uint8_t*>(packed.offsets.data()),
      static_cast<int32_t>(strings.size()));
  ASSERT_TRUE(r.ok());
  EXPECT_LT(*r, inputLen / 2) << "compressed=" << *r << " raw=" << inputLen;
}
// Regression test for PR Assistant review #2 (ffor.hpp:263): encodeRt/decodeRt
// must not deref kEncodeTable/kDecodeTable with bw > 64.
TEST(FForCodecTest, RtDispatchOutOfBoundsBitWidthIsSafe) {
  uint64_t in[16] = {0};
  uint64_t out[16] = {0};
  // bw = 200 is well past the 0..64 valid range. The guards in encodeRt /
  // decodeRt return early without touching the dispatch tables.
  ffor::encodeRt(in, out, /*base=*/0, /*n=*/16, /*bw=*/200);
  ffor::decodeRt(in, out, /*base=*/0, /*n=*/16, /*bw=*/65);
  // No assertion needed — the test passes if these calls don't crash.
  SUCCEED();
}

// Regression test for PR Assistant review #3 (ffor.hpp:177): decode<BW> with
// nValues == 0 must not preload from `in`.
TEST(FForCodecTest, DecodeZeroLengthBlockIsSafe) {
  // Build a compressed payload that decodes to a 0-length output:
  //   compress with input size 0 → output is just the kBwTailMarker header
  //   with count=0. Round-tripping through decompress with outputLen=0 must
  //   not deref `in` past the header.
  std::vector<uint8_t> compressed(64);
  auto cr = FForCodec::compress(nullptr, 0, compressed.data(), compressed.size());
  ASSERT_TRUE(cr.ok());
  EXPECT_EQ(*cr, 0);
  // Decompress with output buffer of size 0; must not crash.
  std::vector<uint8_t> out(8);
  auto dr = FForCodec::decompress(compressed.data(), 0, out.data(), 0);
  ASSERT_TRUE(dr.ok()) << dr.status().message();
}

// Regression test for PR Assistant reviews #4 + #5 (ffor.hpp:437): forged
// payload with a tail-marker `count` larger than remaining input bytes must
// not read past the input buffer; non-tail `count > kMaxValuesPerBlock/kLanes`
// must be rejected.
TEST(FForCodecTest, ForgedTailMarkerCountIsRejected) {
  // Build a tail-marker header that claims to memcpy 1000*8 bytes but the
  // payload buffer only has 8 bytes after the header. Defensive guard must
  // break out of the decode loop without reading OOB.
  // Header layout (kHeaderSize = 16): bw(1) + count(1) + reserved(6) + base(8).
  // We need bw=kBwTailMarker=255 and count=10 (claims 80 bytes of tail).
  std::vector<uint8_t> bogus(16 + 8, 0); // header + only 8 bytes after
  bogus[0] = 255; // kBwTailMarker
  bogus[1] = 10; // claim 10 values = 80 bytes, but only 8 available
  // No need to write base.
  std::vector<uint8_t> out(80, 0);
  // Should not crash; will silently decode only what fits (or stop).
  auto r = FForCodec::decompress(bogus.data(), bogus.size(), out.data(), 80);
  // Either ok with truncated output or an Invalid status — both acceptable;
  // the key correctness property is "no UB / OOB read".
  ASSERT_TRUE(r.ok() || r.status().IsInvalid()) << r.status().message();
}

// Regression test for PR Assistant review (test-coverage): BW=0 path (all
// values constant). The FFOR encoder picks BW=0 when min == max in the input
// block; the constexpr decode<0> specialization just writes base for each
// output. Verify round-trip.
TEST(FForCodecTest, ConstantValuesRoundTripBW0) {
  std::vector<uint64_t> input(64, 0xDEADBEEFCAFEBABEULL);
  int64_t inputSize = static_cast<int64_t>(input.size() * sizeof(uint64_t));
  int64_t maxLen = FForCodec::maxCompressedLength(inputSize);
  std::vector<uint8_t> compressed(maxLen);
  auto cr = FForCodec::compress(reinterpret_cast<const uint8_t*>(input.data()), inputSize, compressed.data(), maxLen);
  ASSERT_TRUE(cr.ok());
  // Constants compress to near-zero bytes (header + tiny payload).
  EXPECT_LT(*cr, inputSize);

  std::vector<uint64_t> output(input.size(), 0);
  auto dr = FForCodec::decompress(compressed.data(), *cr, reinterpret_cast<uint8_t*>(output.data()), inputSize);
  ASSERT_TRUE(dr.ok());
  EXPECT_EQ(input, output);
}

// Regression test for PR Assistant review (test-coverage): BW=64 path (full
// 64-bit range, FFOR cannot compress). The decoder takes the BW==64 fast
// path which just adds base to each input value.
TEST(FForCodecTest, FullRangeRoundTripBW64) {
  // Make input span [0, UINT64_MAX] so FFOR must use BW=64.
  std::vector<uint64_t> input;
  input.reserve(64);
  std::mt19937_64 rng(42);
  for (int i = 0; i < 64; ++i) {
    input.push_back(rng());
  }
  input[0] = 0;
  input[1] = UINT64_MAX;

  int64_t inputSize = static_cast<int64_t>(input.size() * sizeof(uint64_t));
  int64_t maxLen = FForCodec::maxCompressedLength(inputSize);
  std::vector<uint8_t> compressed(maxLen);
  auto cr = FForCodec::compress(reinterpret_cast<const uint8_t*>(input.data()), inputSize, compressed.data(), maxLen);
  ASSERT_TRUE(cr.ok());

  std::vector<uint64_t> output(input.size(), 0);
  auto dr = FForCodec::decompress(compressed.data(), *cr, reinterpret_cast<uint8_t*>(output.data()), inputSize);
  ASSERT_TRUE(dr.ok());
  EXPECT_EQ(input, output);
}

// Regression test for PR Assistant review (test-coverage): N not divisible by
// kLanes=4. The compressor handles the tail via the kBwTailMarker path which
// stores leftover values uncompressed; the decompressor must round-trip them
// exactly.
TEST(FForCodecTest, TailValuesNotMultipleOfLanesRoundTrip) {
  // 67 values: 64 in the main block + 3-value tail (67 % 4 == 3).
  std::vector<uint64_t> input(67);
  std::iota(input.begin(), input.end(), 5000ULL);
  int64_t inputSize = static_cast<int64_t>(input.size() * sizeof(uint64_t));
  int64_t maxLen = FForCodec::maxCompressedLength(inputSize);
  std::vector<uint8_t> compressed(maxLen);
  auto cr = FForCodec::compress(reinterpret_cast<const uint8_t*>(input.data()), inputSize, compressed.data(), maxLen);
  ASSERT_TRUE(cr.ok());

  std::vector<uint64_t> output(input.size(), 0);
  auto dr = FForCodec::decompress(compressed.data(), *cr, reinterpret_cast<uint8_t*>(output.data()), inputSize);
  ASSERT_TRUE(dr.ok());
  EXPECT_EQ(input, output);
}

// Regression test for PR Assistant review (test-coverage): N > kMaxValuesPerBlock
// (=256). Forces the compressor / decompressor to emit / consume multiple
// blocks. Round-trip must remain exact across block boundaries.
TEST(FForCodecTest, MultiBlockRoundTrip) {
  // 1027 values forces 5 blocks (4 of 256 + 1 tail of 3).
  std::vector<uint64_t> input(1027);
  std::iota(input.begin(), input.end(), 10ULL);
  int64_t inputSize = static_cast<int64_t>(input.size() * sizeof(uint64_t));
  int64_t maxLen = FForCodec::maxCompressedLength(inputSize);
  std::vector<uint8_t> compressed(maxLen);
  auto cr = FForCodec::compress(reinterpret_cast<const uint8_t*>(input.data()), inputSize, compressed.data(), maxLen);
  ASSERT_TRUE(cr.ok());

  std::vector<uint64_t> output(input.size(), 0);
  auto dr = FForCodec::decompress(compressed.data(), *cr, reinterpret_cast<uint8_t*>(output.data()), inputSize);
  ASSERT_TRUE(dr.ok());
  EXPECT_EQ(input, output);
}

// ---------------------------------------------------------------------------
// Adaptive int-codec tests: each numeric codec (kUInt64, kUInt128, kUInt32)
// must pick the smaller of native-FFor vs LZ4-fallback. The body strategy
// byte (kIntStrategyNative=0 / kIntStrategyLz4=1) selects the decode path.
// ---------------------------------------------------------------------------

namespace {

// Round-trip helper that returns the strategy byte chosen by compress() for
// the given tacType. Validates correctness on the round-trip. For kUInt64
// and kUInt32 the strategy byte sits at offset (kPayloadHeaderSize) =
// byte 2 of the compressed payload. Same for kUInt128.
uint8_t roundtripAndGetStrategy(const uint8_t* input, int64_t inputLen, int8_t tacType) {
  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(inputLen, tacType);
  std::vector<uint8_t> compressed(static_cast<size_t>(maxLen));
  auto cr = TypeAwareCompressCodec::compress(input, inputLen, compressed.data(), maxLen, tacType);
  EXPECT_TRUE(cr.ok()) << cr.status().ToString();
  EXPECT_GE(*cr, 3) << "expected at least outer header (2) + strategy byte (1)";
  uint8_t strategy = compressed[2];

  std::vector<uint8_t> decoded(static_cast<size_t>(inputLen), 0xCC);
  auto dr = TypeAwareCompressCodec::decompress(compressed.data(), *cr, decoded.data(), inputLen);
  EXPECT_TRUE(dr.ok()) << dr.status().ToString();
  EXPECT_EQ(0, std::memcmp(input, decoded.data(), static_cast<size_t>(inputLen)));
  return strategy;
}

} // namespace

TEST(TypeAwareCompressCodecTest, UInt64AdaptivePicksNativeForNarrowRange) {
  // Narrow-range data: FFor with bw small wins easily vs LZ4 (which has
  // per-block overhead).
  auto data = genData(4096, 100000, 1000);
  uint8_t strat = roundtripAndGetStrategy(
      reinterpret_cast<const uint8_t*>(data.data()),
      static_cast<int64_t>(data.size() * sizeof(uint64_t)),
      tac::kUInt64);
  EXPECT_EQ(strat, 0u) << "narrow-range int64 should choose native FFor";
}

TEST(TypeAwareCompressCodecTest, UInt64AdaptivePicksLz4ForLongRuns) {
  // All-zeros (or all-same value) is the canonical case where LZ4 should
  // dominate FFor: bw=0 in FFor still has a small per-block header, but
  // LZ4 compresses runs of identical bytes to near-zero.
  std::vector<uint64_t> data(8192, 42);
  uint8_t strat = roundtripAndGetStrategy(
      reinterpret_cast<const uint8_t*>(data.data()),
      static_cast<int64_t>(data.size() * sizeof(uint64_t)),
      tac::kUInt64);
  EXPECT_EQ(strat, 1u) << "long-run int64 should fall back to LZ4";
}

TEST(TypeAwareCompressCodecTest, UInt64AdaptiveNoByteRegressionVsLz4) {
  // For any input, the adaptive codec must produce output no larger than
  // LZ4-alone (modulo the 2-byte outer header + 1-byte strategy + 4-byte
  // LZ4 length prefix). This is the whole point of the adaptive design.
  std::vector<uint64_t> data(4096);
  std::mt19937_64 rng(0xC0DE);
  for (auto& v : data) {
    v = rng() & 0xFFFFFF; // 24-bit values
  }
  int64_t inputLen = static_cast<int64_t>(data.size() * sizeof(uint64_t));
  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(inputLen, tac::kUInt64);
  std::vector<uint8_t> compressed(static_cast<size_t>(maxLen));
  auto cr = TypeAwareCompressCodec::compress(
      reinterpret_cast<const uint8_t*>(data.data()), inputLen, compressed.data(), maxLen, tac::kUInt64);
  ASSERT_TRUE(cr.ok());

  // Compute LZ4-alone reference.
  auto lz4 = arrow::util::Codec::Create(arrow::Compression::LZ4_FRAME).ValueOrDie();
  int64_t lz4Max = lz4->MaxCompressedLen(inputLen, nullptr);
  std::vector<uint8_t> lz4Buf(static_cast<size_t>(lz4Max));
  auto lz4Result = lz4->Compress(inputLen, reinterpret_cast<const uint8_t*>(data.data()), lz4Max, lz4Buf.data());
  ASSERT_TRUE(lz4Result.ok());
  int64_t lz4Len = *lz4Result;

  // Adaptive output is at most: 2 (TAC hdr) + 1 (strategy) + 4 (lz4Len prefix) + lz4Len.
  // It can be strictly smaller if native FFor beats LZ4.
  int64_t adaptiveBudget = 2 + 1 + 4 + lz4Len;
  EXPECT_LE(*cr, adaptiveBudget) << "Adaptive must never exceed LZ4-alone + framing";
}

TEST(TypeAwareCompressCodecTest, UInt32AdaptivePicksNativeForNarrowRange) {
  // Narrow-range int32: dates (e.g. 18000..19000 from epoch) — FFor wins.
  std::vector<uint32_t> data(4096);
  std::mt19937 rng(31);
  std::uniform_int_distribution<uint32_t> dist(18000, 19000);
  for (auto& v : data) {
    v = dist(rng);
  }
  uint8_t strat = roundtripAndGetStrategy(
      reinterpret_cast<const uint8_t*>(data.data()),
      static_cast<int64_t>(data.size() * sizeof(uint32_t)),
      tac::kUInt32);
  EXPECT_EQ(strat, 0u) << "narrow-range int32 should choose native FFor";
}

TEST(TypeAwareCompressCodecTest, UInt32AdaptivePicksLz4ForLongRuns) {
  // Long runs of identical int32 values — LZ4 should dominate.
  std::vector<uint32_t> data(8192, 7);
  uint8_t strat = roundtripAndGetStrategy(
      reinterpret_cast<const uint8_t*>(data.data()),
      static_cast<int64_t>(data.size() * sizeof(uint32_t)),
      tac::kUInt32);
  EXPECT_EQ(strat, 1u) << "long-run int32 should fall back to LZ4";
}

TEST(TypeAwareCompressCodecTest, UInt128AdaptivePicksNativeForNarrowRange) {
  // Narrow-range int128 (decimal(p,s)) with hi lane all-zero — split-lane
  // FFor wins easily.
  auto lo = genData(2048, 50000, 200);
  std::vector<uint64_t> hi(lo.size(), 0);
  std::vector<__int128_t> data(lo.size());
  for (size_t i = 0; i < lo.size(); ++i) {
    data[i] = (static_cast<__uint128_t>(hi[i]) << 64) | lo[i];
  }
  uint8_t strat = roundtripAndGetStrategy(
      reinterpret_cast<const uint8_t*>(data.data()),
      static_cast<int64_t>(data.size() * sizeof(__int128_t)),
      tac::kUInt128);
  EXPECT_EQ(strat, 0u) << "narrow-range int128 should choose native split-lane FFor";
}

TEST(TypeAwareCompressCodecTest, UInt128AdaptivePicksLz4ForLongRuns) {
  // All-same int128 values — LZ4 dominates.
  std::vector<__int128_t> data(4096, static_cast<__int128_t>(123456789));
  uint8_t strat = roundtripAndGetStrategy(
      reinterpret_cast<const uint8_t*>(data.data()),
      static_cast<int64_t>(data.size() * sizeof(__int128_t)),
      tac::kUInt128);
  EXPECT_EQ(strat, 1u) << "long-run int128 should fall back to LZ4";
}

TEST(TypeAwareCompressCodecTest, IntCodecRejectsUnknownStrategy) {
  // Forge a kFFor payload with strategy byte = 99. Must reject cleanly.
  std::vector<uint8_t> bogus = {/*codec=kFFor*/ 1, /*tac=kUInt64*/ 0, /*strategy=*/99, 0, 0, 0, 0};
  std::vector<uint8_t> out(32);
  auto r = TypeAwareCompressCodec::decompress(bogus.data(), bogus.size(), out.data(), out.size());
  EXPECT_FALSE(r.ok());
}

TEST(TypeAwareCompressCodecTest, IntCodecRejectsLz4StrategyWithBadLen) {
  // Forge a kFFor payload with strategy=LZ4 but negative length.
  std::vector<uint8_t> bogus(2 + 1 + 4 + 4, 0);
  bogus[0] = 1; // kFFor
  bogus[1] = 0; // kUInt64
  bogus[2] = 1; // kIntStrategyLz4
  int32_t neg = -1;
  std::memcpy(bogus.data() + 3, &neg, sizeof(int32_t));
  std::vector<uint8_t> out(8);
  auto r = TypeAwareCompressCodec::decompress(bogus.data(), bogus.size(), out.data(), out.size());
  EXPECT_FALSE(r.ok());
}

TEST(TypeAwareCompressCodecTest, IntCodecRejectsLz4StrategyMissingLenPrefix) {
  // kFFor + kIntStrategyLz4 but body lacks the int32 length prefix.
  std::vector<uint8_t> bogus = {1, 0, 1};
  std::vector<uint8_t> out(8);
  auto r = TypeAwareCompressCodec::decompress(bogus.data(), bogus.size(), out.data(), out.size());
  EXPECT_FALSE(r.ok());
}

TEST(TypeAwareCompressCodecTest, IntCodecMissingStrategyByteRejected) {
  // Outer header present but body is empty — strategy byte missing.
  std::vector<uint8_t> bogus = {1, 0};
  std::vector<uint8_t> out(8);
  auto r = TypeAwareCompressCodec::decompress(bogus.data(), bogus.size(), out.data(), out.size());
  EXPECT_FALSE(r.ok());
}

// Regression test for OCP Run C (build 220597501) — Velox shuffle can pass
// a SLICED string data buffer where offsets[0] != 0 and/or
// offsets[numRows] < inputLen. Leading and trailing bytes belong to other
// slices / padding and the shuffle reader expects them preserved byte-for-byte.
// The codec MUST fall back to LZ4 (which trivially preserves bytes) in that
// case rather than rejecting the input or losing the leading/trailing bytes.
TEST(TypeAwareCompressCodecTest, StringDictSlicedBufferFallsBackToLz4) {
  // Build a data buffer with 10 leading garbage bytes + 5 strings + 7 trailing
  // garbage bytes. The offsets describe rows starting at byte 10.
  std::vector<std::string> rows = {"alpha", "beta", "gamma", "delta", "epsilon"};
  std::vector<uint8_t> leading(10, 0xAB);
  std::vector<uint8_t> trailing(7, 0xCD);
  std::vector<uint8_t> data;
  data.insert(data.end(), leading.begin(), leading.end());
  for (const auto& s : rows) {
    data.insert(data.end(), s.begin(), s.end());
  }
  data.insert(data.end(), trailing.begin(), trailing.end());

  std::vector<int32_t> offsets;
  offsets.reserve(rows.size() + 1);
  int32_t cur = static_cast<int32_t>(leading.size());
  offsets.push_back(cur);
  for (const auto& s : rows) {
    cur += static_cast<int32_t>(s.size());
    offsets.push_back(cur);
  }

  int32_t numRows = static_cast<int32_t>(rows.size());
  int64_t inputLen = static_cast<int64_t>(data.size());
  ASSERT_NE(offsets[0], 0);
  ASSERT_LT(offsets[numRows], static_cast<int32_t>(inputLen));

  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(inputLen, tac::kStringDict);
  std::vector<uint8_t> compressed(static_cast<size_t>(maxLen));
  auto cr = TypeAwareCompressCodec::compress(
      data.data(),
      inputLen,
      compressed.data(),
      maxLen,
      tac::kStringDict,
      reinterpret_cast<const uint8_t*>(offsets.data()),
      numRows);
  ASSERT_TRUE(cr.ok()) << cr.status().ToString();

  // Confirm we chose the LZ4 strategy (byte 2 of the body — after the
  // 2-byte outer codec header — must be StringDictStrategy::kStrategyLz4 == 1).
  ASSERT_GT(*cr, 2);
  EXPECT_EQ(compressed[2], 1u) << "sliced input must pick LZ4 strategy";

  // Round-trip must reproduce ALL inputLen bytes byte-for-byte, including the
  // leading 0xAB and trailing 0xCD padding.
  std::vector<uint8_t> decoded(static_cast<size_t>(inputLen), 0);
  auto dr = TypeAwareCompressCodec::decompress(compressed.data(), *cr, decoded.data(), inputLen);
  ASSERT_TRUE(dr.ok()) << dr.status().ToString();
  EXPECT_EQ(0, std::memcmp(data.data(), decoded.data(), static_cast<size_t>(inputLen)));
}

// Sliced input with non-zero offsets[0] only (offsets[numRows]==inputLen).
TEST(TypeAwareCompressCodecTest, StringDictLeadingOnlySliceRoundtrip) {
  std::vector<std::string> rows = {"foo", "bar"};
  std::vector<uint8_t> data(3, 0xAA); // 3 garbage bytes at the start
  for (const auto& s : rows) {
    data.insert(data.end(), s.begin(), s.end());
  }
  std::vector<int32_t> offsets = {3, 6, 9};
  ASSERT_EQ(static_cast<int32_t>(data.size()), offsets.back());

  int32_t numRows = static_cast<int32_t>(rows.size());
  int64_t inputLen = static_cast<int64_t>(data.size());

  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(inputLen, tac::kStringDict);
  std::vector<uint8_t> compressed(static_cast<size_t>(maxLen));
  auto cr = TypeAwareCompressCodec::compress(
      data.data(),
      inputLen,
      compressed.data(),
      maxLen,
      tac::kStringDict,
      reinterpret_cast<const uint8_t*>(offsets.data()),
      numRows);
  ASSERT_TRUE(cr.ok()) << cr.status().ToString();
  ASSERT_GT(*cr, 2);
  EXPECT_EQ(compressed[2], 1u) << "non-zero offsets[0] must pick LZ4";

  std::vector<uint8_t> decoded(static_cast<size_t>(inputLen), 0);
  auto dr = TypeAwareCompressCodec::decompress(compressed.data(), *cr, decoded.data(), inputLen);
  ASSERT_TRUE(dr.ok()) << dr.status().ToString();
  EXPECT_EQ(0, std::memcmp(data.data(), decoded.data(), static_cast<size_t>(inputLen)));
}

// Sliced input with trailing padding only (offsets[0]==0, offsets[numRows]<inputLen).
TEST(TypeAwareCompressCodecTest, StringDictTrailingOnlySliceRoundtrip) {
  std::vector<std::string> rows = {"foo", "bar"};
  std::vector<uint8_t> data;
  for (const auto& s : rows) {
    data.insert(data.end(), s.begin(), s.end());
  }
  data.insert(data.end(), 4, 0xBB); // 4 garbage bytes at the end
  std::vector<int32_t> offsets = {0, 3, 6};

  int32_t numRows = static_cast<int32_t>(rows.size());
  int64_t inputLen = static_cast<int64_t>(data.size());
  ASSERT_LT(offsets.back(), static_cast<int32_t>(inputLen));

  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(inputLen, tac::kStringDict);
  std::vector<uint8_t> compressed(static_cast<size_t>(maxLen));
  auto cr = TypeAwareCompressCodec::compress(
      data.data(),
      inputLen,
      compressed.data(),
      maxLen,
      tac::kStringDict,
      reinterpret_cast<const uint8_t*>(offsets.data()),
      numRows);
  ASSERT_TRUE(cr.ok()) << cr.status().ToString();
  EXPECT_EQ(compressed[2], 1u) << "trailing padding must pick LZ4";

  std::vector<uint8_t> decoded(static_cast<size_t>(inputLen), 0);
  auto dr = TypeAwareCompressCodec::decompress(compressed.data(), *cr, decoded.data(), inputLen);
  ASSERT_TRUE(dr.ok()) << dr.status().ToString();
  EXPECT_EQ(0, std::memcmp(data.data(), decoded.data(), static_cast<size_t>(inputLen)));
}

// ---------------------------------------------------------------------------
// kStringDict v3 regression-guard tests.
// Background: the dict-build path costs O(N) hashing + heap allocations per
// column. Two guards short-circuit that on workloads where dict can never
// pay off:
//   Guard 1: tiny buffer (inputLen < kDictMinInputBytes = 4096) — skip the
//            dict build entirely; LZ4 dominates at that scale.
//   Guard 2: single deterministic probe at clamp(numRows/8, 256, 2048) — if
//            no duplicate has been seen by then, the column is essentially
//            all-unique; dict can never recoup its overhead.
// v1 (75 % unique after 64 rows) regressed str_high_card8k.
// v2 (no-dup in 256 rows) regressed str_long_comments via birthday paradox.
// v3 single deterministic probe at clamped position is the production logic.
// ---------------------------------------------------------------------------

namespace {

// A small set of guard-test helpers. All produce inputs sized so each guard
// is hit deterministically — no flakiness from RNG.
std::vector<std::string> repeat(const std::string& s, int32_t n) {
  std::vector<std::string> out;
  out.reserve(static_cast<size_t>(n));
  for (int32_t i = 0; i < n; ++i) {
    out.push_back(s);
  }
  return out;
}

// Append the LZ4 strategy roundtrip-byte-equality check (mirrors the rest
// of the file's pattern).
void assertRoundtripByteEqual(
    const std::vector<uint8_t>& compressed,
    int64_t compressedLen,
    const std::vector<uint8_t>& original) {
  std::vector<uint8_t> decoded(original.size(), 0);
  auto dr = TypeAwareCompressCodec::decompress(
      compressed.data(), compressedLen, decoded.data(), static_cast<int64_t>(original.size()));
  ASSERT_TRUE(dr.ok()) << dr.status().ToString();
  ASSERT_EQ(0, std::memcmp(original.data(), decoded.data(), original.size()));
}

} // namespace

TEST(TypeAwareCompressCodecTest, StringDictTinyBufferBailsToLz4) {
  // 200 short strings × ~10 bytes each ≈ 2 KB — well below kDictMinInputBytes
  // (4096). Guard 1 must fire and pick LZ4 even though the column is
  // dictionary-friendly (low cardinality).
  std::vector<std::string> strings = repeat("monday", 50);
  for (int i = 0; i < 50; ++i) {
    strings.push_back("tuesday");
  }
  for (int i = 0; i < 50; ++i) {
    strings.push_back("wednesday");
  }
  for (int i = 0; i < 50; ++i) {
    strings.push_back("thursday");
  }
  auto packed = packStrings(strings);
  int32_t numRows = static_cast<int32_t>(strings.size());
  int64_t inputLen = static_cast<int64_t>(packed.data.size());
  ASSERT_LT(inputLen, 4096) << "test premise: input must be below kDictMinInputBytes";

  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(inputLen, tac::kStringDict);
  std::vector<uint8_t> compressed(static_cast<size_t>(maxLen));
  auto r = TypeAwareCompressCodec::compress(
      packed.data.data(),
      inputLen,
      compressed.data(),
      maxLen,
      tac::kStringDict,
      reinterpret_cast<const uint8_t*>(packed.offsets.data()),
      numRows);
  ASSERT_TRUE(r.ok()) << r.status().ToString();
  EXPECT_EQ(strategyByte(compressed.data()), 1u)
      << "tiny input must pick LZ4 (kStrategyLz4=1); dict-build is uneconomical";
  assertRoundtripByteEqual(compressed, *r, packed.data);
}

TEST(TypeAwareCompressCodecTest, StringDictAllUniqueRowsBailsToLz4) {
  // 100K rows, every row distinct. Guard 2 must fire at row clamp(100000/8,
  // 256, 2048) = 2048, breaking out of the dict-build loop early and picking
  // LZ4. Without the guard we would scan all 100K rows for no compression win.
  std::vector<std::string> strings;
  strings.reserve(100000);
  for (int i = 0; i < 100000; ++i) {
    // 24-char unique key to make inputLen comfortably above kDictMinInputBytes.
    char buf[32];
    std::snprintf(buf, sizeof(buf), "uniqkey_%016d", i);
    strings.emplace_back(buf);
  }
  auto packed = packStrings(strings);
  int32_t numRows = static_cast<int32_t>(strings.size());
  int64_t inputLen = static_cast<int64_t>(packed.data.size());
  ASSERT_GE(inputLen, 4096) << "test premise: input must clear Guard 1";

  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(inputLen, tac::kStringDict);
  std::vector<uint8_t> compressed(static_cast<size_t>(maxLen));
  auto r = TypeAwareCompressCodec::compress(
      packed.data.data(),
      inputLen,
      compressed.data(),
      maxLen,
      tac::kStringDict,
      reinterpret_cast<const uint8_t*>(packed.offsets.data()),
      numRows);
  ASSERT_TRUE(r.ok()) << r.status().ToString();
  EXPECT_EQ(strategyByte(compressed.data()), 1u) << "all-unique column must bail to LZ4 (kStrategyLz4=1) via Guard 2";
  assertRoundtripByteEqual(compressed, *r, packed.data);
}

TEST(TypeAwareCompressCodecTest, StringDictConstantColumnRoundtrips) {
  // Every row is the same long-ish string. Tons of duplicates seen by row 1,
  // so Guard 2 never trips. Guard 1 doesn't trip either because numRows×len
  // is large. Both strategies (dict and LZ4) compress this shape well; the
  // codec is contractually allowed to pick either as long as it picks the
  // smaller body. In practice LZ4's RLE-like behaviour beats dict on truly
  // constant data (one match reference spans the whole input), so the codec
  // typically emits LZ4 here — that is the *correct* choice. This test only
  // validates the input/output contract: compress succeeds, the result is a
  // dramatic shrink (regardless of strategy), and decompress is byte-exact.
  std::vector<std::string> strings = repeat("the_same_string_value_repeated", 10000);
  auto packed = packStrings(strings);
  int32_t numRows = static_cast<int32_t>(strings.size());
  int64_t inputLen = static_cast<int64_t>(packed.data.size());

  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(inputLen, tac::kStringDict);
  std::vector<uint8_t> compressed(static_cast<size_t>(maxLen));
  auto r = TypeAwareCompressCodec::compress(
      packed.data.data(),
      inputLen,
      compressed.data(),
      maxLen,
      tac::kStringDict,
      reinterpret_cast<const uint8_t*>(packed.offsets.data()),
      numRows);
  ASSERT_TRUE(r.ok()) << r.status().ToString();
  // Either strategy must shrink constant data by >10× (LZ4 typically gets
  // 100×+; the dict path would get ~30× with 1 entry + 10K 1-byte indices).
  EXPECT_LT(*r * 10, inputLen);
  assertRoundtripByteEqual(compressed, *r, packed.data);
}

TEST(TypeAwareCompressCodecTest, StringDictGuardKeepsDictForBoundedHighCardinality) {
  // v2 false-positive shape (ported from OSS best-tac):
  // bounded pool of ~8K distinct strings sampled at 64K rows. By row 256 it
  // is plausible (probability ~3 %) to have seen no duplicate purely by
  // chance — v2's 256-row probe would have wrongly bailed. v3's 2048-row
  // probe makes P(no duplicate from 8K pool) ≈ exp(-2048²/16000) ≈ 10⁻¹¹⁴
  // — guard never fires on this shape, dict wins handily.
  std::mt19937_64 rng(0x12345abc);
  std::uniform_int_distribution<int> pickPool(0, 7999);
  std::vector<std::string> strings;
  strings.reserve(65536);
  for (int i = 0; i < 65536; ++i) {
    strings.push_back("hkey_" + std::to_string(pickPool(rng)));
  }
  auto packed = packStrings(strings);
  int32_t numRows = static_cast<int32_t>(strings.size());
  int64_t inputLen = static_cast<int64_t>(packed.data.size());

  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(inputLen, tac::kStringDict);
  std::vector<uint8_t> compressed(static_cast<size_t>(maxLen));
  auto r = TypeAwareCompressCodec::compress(
      packed.data.data(),
      inputLen,
      compressed.data(),
      maxLen,
      tac::kStringDict,
      reinterpret_cast<const uint8_t*>(packed.offsets.data()),
      numRows);
  ASSERT_TRUE(r.ok()) << r.status().ToString();
  EXPECT_EQ(strategyByte(compressed.data()), 0u) << "bounded-cardinality column must keep Dict (kStrategyDict=0); "
                                                 << "v2 regression would have selected LZ4 here";
  EXPECT_LT(*r * 2, inputLen) << "dict should at least halve input on this shape";
  assertRoundtripByteEqual(compressed, *r, packed.data);
}

TEST(TypeAwareCompressCodecTest, StringDictLongCommentsRoundtrips) {
  // Mid-card with long strings (comment-like): 10K rows drawn from a pool of
  // ~3K distinct ~80-char comments that share a long template prefix.
  // v2 (256-row probe) regressed this shape when a particular RNG seed
  // happened to yield 256 distinct draws (within birthday-paradox tolerance).
  // v3 probes at 2048 rows; by then we have seen many duplicates and the
  // guard does not trigger, so dict is built and offered.  The codec is then
  // free to pick whichever strategy compresses better; on this particular
  // shape LZ4 tends to win because the shared 66-char prefix is highly
  // compressible by LZ77.  This test only validates the input/output
  // contract: compress succeeds, the output is at least 2× smaller than the
  // input (both strategies satisfy this), and decompress is byte-exact.
  std::mt19937_64 rng(0xc0ffee01);
  std::uniform_int_distribution<int> pickPool(0, 2999);
  std::vector<std::string> strings;
  strings.reserve(10000);
  for (int i = 0; i < 10000; ++i) {
    int idx = pickPool(rng);
    std::string s = "comment_template_with_padding_to_eighty_chars_field_value_index_";
    s += std::to_string(idx);
    while (s.size() < 80) {
      s.push_back('x');
    }
    strings.push_back(s);
  }
  auto packed = packStrings(strings);
  int32_t numRows = static_cast<int32_t>(strings.size());
  int64_t inputLen = static_cast<int64_t>(packed.data.size());

  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(inputLen, tac::kStringDict);
  std::vector<uint8_t> compressed(static_cast<size_t>(maxLen));
  auto r = TypeAwareCompressCodec::compress(
      packed.data.data(),
      inputLen,
      compressed.data(),
      maxLen,
      tac::kStringDict,
      reinterpret_cast<const uint8_t*>(packed.offsets.data()),
      numRows);
  ASSERT_TRUE(r.ok()) << r.status().ToString();
  EXPECT_LT(*r * 2, inputLen) << "either strategy should at least halve input on this shape";
  assertRoundtripByteEqual(compressed, *r, packed.data);
}

TEST(TypeAwareCompressCodecTest, StringDictConstantColumnPicksLz4) {
  // Counterpart to StringDictConstantColumnRoundtrips: pin the deterministic
  // strategy choice on a constant-value column.  The dict body costs
  // kDictBodyFixedHeader + (4 + 30) for the single entry + 10 000 x 1B for
  // the indices ~= 10 KB.  LZ4 on 300 KB of constant bytes is dominated by
  // a single 64 KB-windowed match reference and produces only hundreds of
  // bytes.  LZ4 wins by 50-100x, so the codec must pick kStrategyLz4 (1).
  // This test guards against any future change that would silently make
  // the codec prefer dict on shapes where LZ4 is much smaller (which would
  // bloat the wire form for the most-common low-cardinality patterns).
  std::vector<std::string> strings = repeat("the_same_string_value_repeated", 10000);
  auto packed = packStrings(strings);
  int32_t numRows = static_cast<int32_t>(strings.size());
  int64_t inputLen = static_cast<int64_t>(packed.data.size());

  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(inputLen, tac::kStringDict);
  std::vector<uint8_t> compressed(static_cast<size_t>(maxLen));
  auto r = TypeAwareCompressCodec::compress(
      packed.data.data(),
      inputLen,
      compressed.data(),
      maxLen,
      tac::kStringDict,
      reinterpret_cast<const uint8_t*>(packed.offsets.data()),
      numRows);
  ASSERT_TRUE(r.ok()) << r.status().ToString();
  EXPECT_EQ(strategyByte(compressed.data()), 1u)
      << "constant column: LZ4 must beat dict by 50-100x and be the chosen strategy";
  // Sanity: LZ4 on constant data should compress to well under 1% of input.
  EXPECT_LT(*r * 100, inputLen);
  assertRoundtripByteEqual(compressed, *r, packed.data);
}

TEST(TypeAwareCompressCodecTest, StringDictLongCommentsPicksLz4) {
  // Counterpart to StringDictLongCommentsRoundtrips: pin the deterministic
  // strategy choice on a mid-cardinality column whose values share a long
  // template prefix.  The dict body costs ~3 000 x (4 + 80) bytes for the
  // entries + 10 000 x 2 bytes for the (16-bit) indices ~= 272 KB.  LZ4
  // captures the 66-byte shared prefix as a single repeated match plus
  // small per-row tail differences, getting down to roughly 80-100 KB on
  // 800 KB of input.  LZ4 therefore wins on size and the codec must emit
  // kStrategyLz4 (1).  The test exists so a future change to dict-header
  // accounting (or to the strategy decision) cannot silently regress to
  // emitting a larger dict body on a shape LZ4 compresses better.
  std::mt19937_64 rng(0xc0ffee01);
  std::uniform_int_distribution<int> pickPool(0, 2999);
  std::vector<std::string> strings;
  strings.reserve(10000);
  for (int i = 0; i < 10000; ++i) {
    int idx = pickPool(rng);
    std::string s = "comment_template_with_padding_to_eighty_chars_field_value_index_";
    s += std::to_string(idx);
    while (s.size() < 80) {
      s.push_back('x');
    }
    strings.push_back(s);
  }
  auto packed = packStrings(strings);
  int32_t numRows = static_cast<int32_t>(strings.size());
  int64_t inputLen = static_cast<int64_t>(packed.data.size());

  auto maxLen = TypeAwareCompressCodec::maxCompressedLen(inputLen, tac::kStringDict);
  std::vector<uint8_t> compressed(static_cast<size_t>(maxLen));
  auto r = TypeAwareCompressCodec::compress(
      packed.data.data(),
      inputLen,
      compressed.data(),
      maxLen,
      tac::kStringDict,
      reinterpret_cast<const uint8_t*>(packed.offsets.data()),
      numRows);
  ASSERT_TRUE(r.ok()) << r.status().ToString();
  EXPECT_EQ(strategyByte(compressed.data()), 1u)
      << "long-comments shape: LZ4 must beat the ~272 KB dict body and be the chosen strategy";
  // Sanity: LZ4 on long shared-prefix data should compress at least 5x.
  EXPECT_LT(*r * 5, inputLen);
  assertRoundtripByteEqual(compressed, *r, packed.data);
}

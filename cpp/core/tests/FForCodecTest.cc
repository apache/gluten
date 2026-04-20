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

#include <gtest/gtest.h>
#include <cstring>
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
  size_t nDecoded = decompress64(buf.data(), written, decoded.data());

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
    size_t n = decompress64(out, written, decoded.data());
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
    size_t n = decompress64(comp.data(), written, decoded.data());
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
    size_t n = decompress64(comp.data(), written, misalignedOutput);
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
        size_t n = decompress64(compBuf.data() + compOff, written, outPtr);
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

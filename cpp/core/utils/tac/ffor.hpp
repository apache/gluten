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

/*
 * MIT License
 *
 * Copyright (c) 2024 Azim Afroozeh, CWI Database Architectures Group
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 * ---
 *
 * Modifications Copyright (c) 2026 Wangyang Guo, licensed under the
 * Apache License, Version 2.0.
 */

// 4-lane FFOR (Frame-of-Reference + Bit-Packing) codec for uint64_t.
// Uses a 4-lane transposed layout for auto-vectorization.
// Reference: https://www.vldb.org/pvldb/vol16/p2132-afroozeh.pdf

#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

namespace gluten {
namespace ffor {

// Byte order (applies to the 128-bit codec below): this codec round-trips
// data as native uint64 reads/writes against the lo (offset 0) and hi
// (offset 8) halves of each 128-bit value (DECIMAL128's in-memory layout in
// Velox).  Producer and consumer share byte order by virtue of running in
// the same Spark cluster -- LZ4 and any other shuffle codec carry the same
// implicit assumption -- so no explicit endian guard is needed here.

static constexpr unsigned kLanes = 4;

// Compile-time mask for a given bit width.
template <unsigned BW>
static constexpr uint64_t bitmask() {
  if constexpr (BW == 0) {
    return 0;
  } else if constexpr (BW >= 64) {
    return ~uint64_t(0);
  } else {
    return (uint64_t(1) << BW) - 1;
  }
}

// Returns number of uint64_t words needed for the compressed output.
inline constexpr size_t compressedWords(size_t nValues, unsigned bw) {
  if (bw == 0) {
    return 0;
  }
  const size_t valsPerLane = nValues / kLanes;
  const size_t wordsPerLane = (valsPerLane * bw + 63) / 64;
  return wordsPerLane * kLanes;
}

// FFOR encode: bit-pack nValues uint64_t values with a given base and bit width.
// nValues must be a multiple of kLanes.
template <unsigned BW>
#if defined(__clang__)
__attribute__((noinline))
#elif defined(__GNUC__)
__attribute__((optimize("O3,tree-vectorize"), noinline))
#endif
void encode(const uint64_t* __restrict in, uint64_t* __restrict out, uint64_t base, size_t nValues) {
  static_assert(BW <= 64, "BW must be <= 64");

  if constexpr (BW == 0) {
    return;
  } else if constexpr (BW == 64) {
    for (size_t i = 0; i < nValues; ++i) {
      out[i] = in[i] - base;
    }
    return;
  } else {
    constexpr uint64_t kMask = bitmask<BW>();
    const size_t nGroups = nValues / kLanes;

    uint64_t tmp[kLanes] = {};
    size_t outOffset = 0;
    unsigned bitPos = 0;

    for (size_t g = 0; g < nGroups; ++g) {
#if defined(__clang__)
#pragma clang loop vectorize(enable) interleave(enable)
#elif defined(__GNUC__)
#pragma GCC ivdep
#endif
      for (unsigned lane = 0; lane < kLanes; ++lane) {
        uint64_t val = (in[g * kLanes + lane] - base) & kMask;
        tmp[lane] |= val << bitPos;
      }

      unsigned newBitPos = bitPos + BW;

      if (newBitPos >= 64) {
        for (unsigned lane = 0; lane < kLanes; ++lane) {
          out[outOffset + lane] = tmp[lane];
        }
        outOffset += kLanes;

        unsigned overflow = newBitPos - 64;
        if (overflow > 0) {
          for (unsigned lane = 0; lane < kLanes; ++lane) {
            uint64_t val = (in[g * kLanes + lane] - base) & kMask;
            tmp[lane] = val >> (BW - overflow);
          }
        } else {
          for (unsigned lane = 0; lane < kLanes; ++lane) {
            tmp[lane] = 0;
          }
        }
        bitPos = overflow;
      } else {
        bitPos = newBitPos;
      }
    }

    if (bitPos > 0) {
      for (unsigned lane = 0; lane < kLanes; ++lane) {
        out[outOffset + lane] = tmp[lane];
      }
    }
  }
}

// FFOR decode: unpack nValues uint64_t values with a given base and bit width.
// nValues must be a multiple of kLanes.
template <unsigned BW>
#if defined(__clang__)
__attribute__((noinline))
#elif defined(__GNUC__)
__attribute__((optimize("O3,tree-vectorize"), noinline))
#endif
void decode(const uint64_t* __restrict in, uint64_t* __restrict out, uint64_t base, size_t nValues) {
  static_assert(BW <= 64, "BW must be <= 64");

  if constexpr (BW == 0) {
    for (size_t i = 0; i < nValues; ++i) {
      out[i] = base;
    }
    return;
  } else if constexpr (BW == 64) {
    for (size_t i = 0; i < nValues; ++i) {
      out[i] = in[i] + base;
    }
    return;
  } else {
    constexpr uint64_t kMask = bitmask<BW>();
    const size_t nGroups = nValues / kLanes;

    uint64_t cur[kLanes];
    size_t inOffset = 0;
    unsigned bitPos = 0;

    for (unsigned lane = 0; lane < kLanes; ++lane) {
      cur[lane] = in[inOffset + lane];
    }
    inOffset += kLanes;

    for (size_t g = 0; g < nGroups; ++g) {
#if defined(__clang__)
#pragma clang loop vectorize(enable) interleave(enable)
#elif defined(__GNUC__)
#pragma GCC ivdep
#endif
      for (unsigned lane = 0; lane < kLanes; ++lane) {
        uint64_t val = (cur[lane] >> bitPos) & kMask;
        out[g * kLanes + lane] = val + base;
      }

      unsigned newBitPos = bitPos + BW;

      if (newBitPos >= 64) {
        unsigned overflow = newBitPos - 64;

        if (overflow > 0) {
          // Straddled values: need bits from the next words.
          // Safe even on last group — encoder wrote these partial words.
          for (unsigned lane = 0; lane < kLanes; ++lane) {
            cur[lane] = in[inOffset + lane];
          }
          inOffset += kLanes;

          for (unsigned lane = 0; lane < kLanes; ++lane) {
            uint64_t prevPart = (in[inOffset - 2 * kLanes + lane] >> bitPos);
            uint64_t nextPart = cur[lane] << (BW - overflow);
            out[g * kLanes + lane] = ((prevPart | nextPart) & kMask) + base;
          }
        } else if (g + 1 < nGroups) {
          // Clean 64-bit boundary, more groups to follow — pre-load next words.
          for (unsigned lane = 0; lane < kLanes; ++lane) {
            cur[lane] = in[inOffset + lane];
          }
          inOffset += kLanes;
        }
        // else: clean boundary on last group — values fully decoded, no load needed.
        bitPos = overflow;
      } else {
        bitPos = newBitPos;
      }
    }
  }
}

// Runtime BW dispatch via compile-time generated jump table.
namespace detail {

template <unsigned BW>
void encodeDispatch(const uint64_t* in, uint64_t* out, uint64_t base, size_t n) {
  encode<BW>(in, out, base, n);
}

template <unsigned BW>
void decodeDispatch(const uint64_t* in, uint64_t* out, uint64_t base, size_t n) {
  decode<BW>(in, out, base, n);
}

using DispatchFn = void (*)(const uint64_t*, uint64_t*, uint64_t, size_t);

template <size_t... Is>
constexpr auto makeEncodeTable(std::index_sequence<Is...>) {
  return std::array<DispatchFn, sizeof...(Is)>{&encodeDispatch<Is>...};
}

template <size_t... Is>
constexpr auto makeDecodeTable(std::index_sequence<Is...>) {
  return std::array<DispatchFn, sizeof...(Is)>{&decodeDispatch<Is>...};
}

inline const auto kEncodeTable = makeEncodeTable(std::make_index_sequence<65>{});
inline const auto kDecodeTable = makeDecodeTable(std::make_index_sequence<65>{});

} // namespace detail

// Runtime-dispatched encode (when BW is not known at compile time).
inline void encodeRt(const uint64_t* in, uint64_t* out, uint64_t base, size_t n, unsigned bw) {
  detail::kEncodeTable[bw](in, out, base, n);
}

// Runtime-dispatched decode.
inline void decodeRt(const uint64_t* in, uint64_t* out, uint64_t base, size_t n, unsigned bw) {
  detail::kDecodeTable[bw](in, out, base, n);
}

// Compute base (min) and bitwidth for a vector of values.
inline void analyze(const uint64_t* data, size_t n, uint64_t& base, unsigned& bw) {
  if (n == 0) {
    base = 0;
    bw = 0;
    return;
  }
  uint64_t mn = data[0];
  uint64_t mx = data[0];
  for (size_t i = 1; i < n; ++i) {
    if (data[i] < mn) {
      mn = data[i];
    }
    if (data[i] > mx) {
      mx = data[i];
    }
  }
  base = mn;
  uint64_t range = mx - mn;
  bw = 0;
  while (range > 0) {
    bw++;
    range >>= 1;
  }
}

// All headers are 16 bytes (64-bit aligned), so packed data that follows
// is always naturally aligned — no memcpy needed for encode/decode.
//
// Block header (16 bytes, aligned):
//   | bw (1B) | count (1B) | reserved (6B) | base (8B) |
//   bw = 0..64:  compressed block, count = LANES-groups.
//   bw = 255:    tail marker, count = number of raw values (0..3).
//                base field is unused (zeroed) in tail marker.
static constexpr uint8_t kBwTailMarker = 255;
static constexpr size_t kHeaderSize = 16; // 8-byte aligned
static constexpr size_t kMaxValuesPerBlock = 256;

// Write a 16-byte block header: | bw(1) | count(1) | reserved(6) | base(8) |
inline void writeHeader(uint8_t* p, uint8_t bw, uint8_t count, uint64_t base) {
  p[0] = bw;
  p[1] = count;
  std::memset(p + 2, 0, 6);
  std::memcpy(p + 8, &base, sizeof(base));
}

// Read a 16-byte block header.
inline void readHeader(const uint8_t* p, uint8_t& bw, uint8_t& count, uint64_t& base) {
  bw = p[0];
  count = p[1];
  std::memcpy(&base, p + 8, sizeof(base));
}

// Worst-case compressed buffer size for num values.
inline constexpr size_t compress64Bound(size_t num) {
  size_t nBlocks = (num + kMaxValuesPerBlock - 1) / kMaxValuesPerBlock;
  if (nBlocks == 0) {
    nBlocks = 1;
  }
  // block headers + data + tail header(16) + tail data
  return (nBlocks + 1) * kHeaderSize + num * sizeof(uint64_t);
}

// Encode one block: write header + bit-packed payload into `out`.
// Returns the number of bytes written.  `out` must be 8-byte aligned;
// callers whose output buffer is unaligned stage through a local scratch.
// Shared by the 64-bit and 128-bit codecs.
inline size_t encodeBlock(const uint64_t* src, size_t blockVals, uint64_t* out) {
  uint64_t base;
  unsigned bw;
  analyze(src, blockVals, base, bw);
  writeHeader(
      reinterpret_cast<uint8_t*>(out), static_cast<uint8_t>(bw), static_cast<uint8_t>(blockVals / kLanes), base);
  const size_t compWords = compressedWords(blockVals, bw);
  encodeRt(src, out + 2, base, blockVals, bw);
  return (2 + compWords) * sizeof(uint64_t);
}

// Decode one block of `blockVals` values from `in` into `dst`.
// Returns the number of bytes consumed, or 0 on failure (corrupt header or
// payload that would read past inEnd).  `in` must be 8-byte aligned;
// callers whose input buffer is unaligned stage through a local scratch.
// Shared by the 64-bit and 128-bit codecs.
inline size_t decodeBlock(const uint64_t* in, size_t inBytes, size_t blockVals, uint64_t* dst) {
  if (inBytes < kHeaderSize) {
    return 0;
  }
  uint8_t bw;
  uint8_t count;
  uint64_t base;
  readHeader(reinterpret_cast<const uint8_t*>(in), bw, count, base);
  if (bw > 64 || bw == kBwTailMarker || static_cast<size_t>(count) * kLanes != blockVals) {
    return 0;
  }
  const size_t compWords = compressedWords(blockVals, bw);
  const size_t totalBytes = (2 + compWords) * sizeof(uint64_t);
  if (totalBytes > inBytes) {
    return 0;
  }
  decodeRt(in + 2, dst, base, blockVals, bw);
  return totalBytes;
}

// Template-based compress/decompress with alignment dispatch.
// InAligned:  true if input  (const uint64_t*) is 8-byte aligned.
// OutAligned: true if output (uint8_t*) is 8-byte aligned.
// encodeBlock requires aligned uint64_t* output; when the caller's output
// buffer is unaligned, we stage through tmpOut and memcpy back.
template <bool InAligned, bool OutAligned>
inline size_t compress64Impl(const uint64_t* input, size_t num, uint8_t* output) {
  alignas(64) uint64_t tmpIn[kMaxValuesPerBlock];
  alignas(64) uint64_t tmpOut[kMaxValuesPerBlock + 2]; // header(2 words) + payload

  uint8_t* outPtr = output;
  size_t remaining = num;
  const uint64_t* inPtr = input;

  while (remaining >= kLanes) {
    size_t blockVals = remaining - (remaining % kLanes);
    if (blockVals > kMaxValuesPerBlock) {
      blockVals = kMaxValuesPerBlock;
    }

    const uint64_t* src;
    if constexpr (InAligned) {
      src = inPtr;
    } else {
      std::memcpy(tmpIn, inPtr, blockVals * sizeof(uint64_t));
      src = tmpIn;
    }

    if constexpr (OutAligned) {
      outPtr += encodeBlock(src, blockVals, reinterpret_cast<uint64_t*>(outPtr));
    } else {
      const size_t produced = encodeBlock(src, blockVals, tmpOut);
      std::memcpy(outPtr, tmpOut, produced);
      outPtr += produced;
    }

    inPtr += blockVals;
    remaining -= blockVals;
  }

  // Tail.
  writeHeader(outPtr, kBwTailMarker, static_cast<uint8_t>(remaining), 0);
  outPtr += kHeaderSize;

  if (remaining > 0) {
    std::memcpy(outPtr, inPtr, remaining * sizeof(uint64_t));
    outPtr += remaining * sizeof(uint64_t);
  }

  return static_cast<size_t>(outPtr - output);
}

// Runtime dispatch — check alignment once, pick the right template.
inline size_t compress64(const uint64_t* input, size_t num, uint8_t* output) {
  bool inOk = (reinterpret_cast<uintptr_t>(input) % alignof(uint64_t) == 0);
  bool outOk = (reinterpret_cast<uintptr_t>(output) % alignof(uint64_t) == 0);
  if (inOk && outOk) {
    return compress64Impl<true, true>(input, num, output);
  }
  if (inOk && !outOk) {
    return compress64Impl<true, false>(input, num, output);
  }
  if (!inOk && outOk) {
    return compress64Impl<false, true>(input, num, output);
  }
  return compress64Impl<false, false>(input, num, output);
}

// Template-based decompress with alignment dispatch.
// decodeBlock requires aligned uint64_t* input; when the caller's input
// buffer is unaligned, we stage through tmpIn and memcpy in.
template <bool InAligned, bool OutAligned>
inline size_t decompress64Impl(const uint8_t* input, size_t inputSize, uint64_t* output, size_t outputSize) {
  alignas(64) uint64_t tmpIn[kMaxValuesPerBlock + 2];
  alignas(64) uint64_t tmpOut[kMaxValuesPerBlock];

  const uint8_t* inPtr = input;
  const uint8_t* inEnd = input + inputSize;
  const size_t outValuesMax = outputSize / sizeof(uint64_t);
  size_t nDecoded = 0;

  while (inPtr + kHeaderSize <= inEnd) {
    if (inPtr[0] == kBwTailMarker) {
      const uint8_t count = inPtr[1];
      inPtr += kHeaderSize;
      const size_t tailBytes = static_cast<size_t>(count) * sizeof(uint64_t);
      if (count > 0 && inPtr + tailBytes <= inEnd && count <= outValuesMax - nDecoded) {
        std::memcpy(reinterpret_cast<uint8_t*>(output) + nDecoded * sizeof(uint64_t), inPtr, tailBytes);
        nDecoded += count;
      }
      break;
    }
    const size_t blockVals = static_cast<size_t>(inPtr[1]) * kLanes;
    if (blockVals == 0 || blockVals > kMaxValuesPerBlock || blockVals > outValuesMax - nDecoded) {
      break;
    }
    const size_t remaining = static_cast<size_t>(inEnd - inPtr);
    uint64_t* decDst = OutAligned ? output + nDecoded : tmpOut;

    size_t consumed;
    if constexpr (InAligned) {
      consumed = decodeBlock(reinterpret_cast<const uint64_t*>(inPtr), remaining, blockVals, decDst);
    } else {
      const size_t n = std::min(remaining, sizeof(tmpIn));
      std::memcpy(tmpIn, inPtr, n);
      consumed = decodeBlock(tmpIn, n, blockVals, decDst);
    }
    if (consumed == 0) {
      break;
    }
    inPtr += consumed;

    if constexpr (!OutAligned) {
      std::memcpy(
          reinterpret_cast<uint8_t*>(output) + nDecoded * sizeof(uint64_t), tmpOut, blockVals * sizeof(uint64_t));
    }
    nDecoded += blockVals;
  }

  return nDecoded;
}

// Runtime dispatch.
inline size_t decompress64(const uint8_t* input, size_t inputSize, uint64_t* output, size_t outputSize) {
  bool inOk = (reinterpret_cast<uintptr_t>(input) % alignof(uint64_t) == 0);
  bool outOk = (reinterpret_cast<uintptr_t>(output) % alignof(uint64_t) == 0);
  if (inOk && outOk) {
    return decompress64Impl<true, true>(input, inputSize, output, outputSize);
  }
  if (inOk && !outOk) {
    return decompress64Impl<true, false>(input, inputSize, output, outputSize);
  }
  if (!inOk && outOk) {
    return decompress64Impl<false, true>(input, inputSize, output, outputSize);
  }
  return decompress64Impl<false, false>(input, inputSize, output, outputSize);
}

// =============================================================================
// 128-bit codec.
//
// Each 128-bit value occupies a 16B slot (lo at offset 0, hi at offset 8 --
// the DECIMAL128 / __int128_t layout used by Velox).  Per block, the lo and
// hi halves are gathered into two stack scratches and each is fed through
// the 64-bit FFOR encoder.  Reads/writes go through native uint64, so the
// codec is byte-order agnostic as long as producer and consumer agree.
//
// Wire format per block:  [hdr][lo payload][hdr][hi payload]
// followed by one tail block (kBwTailMarker) carrying the remaining 16B
// values raw.
// =============================================================================

inline constexpr size_t compress128Bound(size_t numValues) {
  // Two 64-bit streams (lo + hi), worst case each = compress64Bound(numValues).
  return 2 * compress64Bound(numValues);
}

// 128-bit compress.  See encodeBlock for InAligned/OutAligned semantics.
template <bool InAligned, bool OutAligned>
inline size_t compress128Impl(const uint8_t* input, size_t numValues, uint8_t* output) {
  alignas(64) uint64_t loBuffer[kMaxValuesPerBlock];
  alignas(64) uint64_t hiBuffer[kMaxValuesPerBlock];
  alignas(64) uint64_t tmpIn[kMaxValuesPerBlock * 2];
  alignas(64) uint64_t tmpOut[kMaxValuesPerBlock + 2]; // header(2 words) + payload

  uint8_t* outPtr = output;
  size_t remaining = numValues;
  const uint8_t* inPtr = input;

  while (remaining >= kLanes) {
    size_t blockVals = remaining - (remaining % kLanes);
    if (blockVals > kMaxValuesPerBlock) {
      blockVals = kMaxValuesPerBlock;
    }

    const uint64_t* in64;
    if constexpr (InAligned) {
      in64 = reinterpret_cast<const uint64_t*>(inPtr);
    } else {
      std::memcpy(tmpIn, inPtr, blockVals * sizeof(__int128_t));
      in64 = tmpIn;
    }
    for (size_t j = 0; j < blockVals; ++j) {
      loBuffer[j] = in64[j * 2];
      hiBuffer[j] = in64[j * 2 + 1];
    }

    if constexpr (OutAligned) {
      outPtr += encodeBlock(loBuffer, blockVals, reinterpret_cast<uint64_t*>(outPtr));
      outPtr += encodeBlock(hiBuffer, blockVals, reinterpret_cast<uint64_t*>(outPtr));
    } else {
      size_t n = encodeBlock(loBuffer, blockVals, tmpOut);
      std::memcpy(outPtr, tmpOut, n);
      outPtr += n;
      n = encodeBlock(hiBuffer, blockVals, tmpOut);
      std::memcpy(outPtr, tmpOut, n);
      outPtr += n;
    }

    inPtr += blockVals * sizeof(__int128_t);
    remaining -= blockVals;
  }

  // Tail: one header + remaining values copied raw.
  writeHeader(outPtr, kBwTailMarker, static_cast<uint8_t>(remaining), 0);
  outPtr += kHeaderSize;
  if (remaining > 0) {
    std::memcpy(outPtr, inPtr, remaining * sizeof(__int128_t));
    outPtr += remaining * sizeof(__int128_t);
  }
  return static_cast<size_t>(outPtr - output);
}

// Runtime dispatch — check alignment once, pick the right template.
inline size_t compress128(const uint8_t* input, size_t numValues, uint8_t* output) {
  const bool inOk = (reinterpret_cast<uintptr_t>(input) % alignof(uint64_t) == 0);
  const bool outOk = (reinterpret_cast<uintptr_t>(output) % alignof(uint64_t) == 0);
  if (inOk && outOk) {
    return compress128Impl<true, true>(input, numValues, output);
  }
  if (inOk && !outOk) {
    return compress128Impl<true, false>(input, numValues, output);
  }
  if (!inOk && outOk) {
    return compress128Impl<false, true>(input, numValues, output);
  }
  return compress128Impl<false, false>(input, numValues, output);
}

// 128-bit decompress.  decodeBlock requires aligned uint64_t* input; when
// the caller's input buffer is unaligned, we stage through tmpIn.
template <bool InAligned, bool OutAligned>
inline size_t decompress128Impl(const uint8_t* input, size_t inputSize, uint8_t* output, size_t outputSize) {
  alignas(64) uint64_t loBuffer[kMaxValuesPerBlock];
  alignas(64) uint64_t hiBuffer[kMaxValuesPerBlock];
  alignas(64) uint64_t tmpIn[kMaxValuesPerBlock + 2];
  alignas(64) uint64_t tmpOut[kMaxValuesPerBlock * 2];

  const uint8_t* inPtr = input;
  const uint8_t* inEnd = input + inputSize;
  const size_t outValuesMax = outputSize / sizeof(__int128_t);
  size_t nDecoded = 0;

  while (inPtr + kHeaderSize <= inEnd) {
    if (inPtr[0] == kBwTailMarker) {
      const uint8_t count = inPtr[1];
      inPtr += kHeaderSize;
      const size_t tailBytes = static_cast<size_t>(count) * sizeof(__int128_t);
      if (inPtr + tailBytes > inEnd || count > outValuesMax - nDecoded) {
        break;
      }
      std::memcpy(output + nDecoded * sizeof(__int128_t), inPtr, tailBytes);
      nDecoded += count;
      break;
    }
    const size_t blockVals = static_cast<size_t>(inPtr[1]) * kLanes;
    if (blockVals == 0 || blockVals > kMaxValuesPerBlock || blockVals > outValuesMax - nDecoded) {
      break;
    }

    auto decodeOne = [&](uint64_t* dst) -> bool {
      const size_t remaining = static_cast<size_t>(inEnd - inPtr);
      size_t consumed;
      if constexpr (InAligned) {
        consumed = decodeBlock(reinterpret_cast<const uint64_t*>(inPtr), remaining, blockVals, dst);
      } else {
        const size_t n = std::min(remaining, sizeof(tmpIn));
        std::memcpy(tmpIn, inPtr, n);
        consumed = decodeBlock(tmpIn, n, blockVals, dst);
      }
      if (consumed == 0) {
        return false;
      }
      inPtr += consumed;
      return true;
    };
    if (!decodeOne(loBuffer) || !decodeOne(hiBuffer)) {
      break;
    }

    uint64_t* dst64 = OutAligned ? reinterpret_cast<uint64_t*>(output + nDecoded * sizeof(__int128_t)) : tmpOut;
    for (size_t j = 0; j < blockVals; ++j) {
      dst64[j * 2] = loBuffer[j];
      dst64[j * 2 + 1] = hiBuffer[j];
    }
    if constexpr (!OutAligned) {
      std::memcpy(output + nDecoded * sizeof(__int128_t), tmpOut, blockVals * sizeof(__int128_t));
    }
    nDecoded += blockVals;
  }

  return nDecoded;
}

// Runtime dispatch — check alignment once, pick the right template.
inline size_t decompress128(const uint8_t* input, size_t inputSize, uint8_t* output, size_t outputSize) {
  const bool inOk = (reinterpret_cast<uintptr_t>(input) % alignof(uint64_t) == 0);
  const bool outOk = (reinterpret_cast<uintptr_t>(output) % alignof(uint64_t) == 0);
  if (inOk && outOk) {
    return decompress128Impl<true, true>(input, inputSize, output, outputSize);
  }
  if (inOk && !outOk) {
    return decompress128Impl<true, false>(input, inputSize, output, outputSize);
  }
  if (!inOk && outOk) {
    return decompress128Impl<false, true>(input, inputSize, output, outputSize);
  }
  return decompress128Impl<false, false>(input, inputSize, output, outputSize);
}

} // namespace ffor
} // namespace gluten

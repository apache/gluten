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

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

namespace gluten {
namespace ffor {

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

// Template-based compress/decompress with alignment dispatch.
// InAligned:  true if input  (const uint64_t*) is 8-byte aligned.
// OutAligned: true if output (uint8_t*) is 8-byte aligned.
// When aligned, encode_rt/decode_rt work on pointers directly.
// When not aligned, a per-block aligned temp buffer is used.
template <bool InAligned, bool OutAligned>
inline size_t compress64Impl(const uint64_t* input, size_t num, uint8_t* output) {
  alignas(64) uint64_t tmpIn[kMaxValuesPerBlock];
  alignas(64) uint64_t tmpOut[kMaxValuesPerBlock];

  uint8_t* outPtr = output;
  size_t remaining = num;
  const uint64_t* inPtr = input;

  while (remaining >= kLanes) {
    size_t blockVals = remaining - (remaining % kLanes);
    if (blockVals > kMaxValuesPerBlock) {
      blockVals = kMaxValuesPerBlock;
    }

    // Analyze — read input via memcpy if unaligned.
    const uint64_t* analyzeSrc;
    if constexpr (InAligned) {
      analyzeSrc = inPtr;
    } else {
      std::memcpy(tmpIn, inPtr, blockVals * sizeof(uint64_t));
      analyzeSrc = tmpIn;
    }

    uint64_t base;
    unsigned bw;
    analyze(analyzeSrc, blockVals, base, bw);

    writeHeader(outPtr, static_cast<uint8_t>(bw), static_cast<uint8_t>(blockVals / kLanes), base);
    outPtr += kHeaderSize;

    size_t compN = compressedWords(blockVals, bw);
    size_t compBytes = compN * sizeof(uint64_t);

    // Encode: pick aligned src/dst.
    const uint64_t* encIn = InAligned ? inPtr : tmpIn;
    uint64_t* encOut = OutAligned ? reinterpret_cast<uint64_t*>(outPtr) : tmpOut;

    encodeRt(encIn, encOut, base, blockVals, bw);

    if constexpr (!OutAligned) {
      std::memcpy(outPtr, tmpOut, compBytes);
    }
    outPtr += compBytes;

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
template <bool InAligned, bool OutAligned>
inline size_t decompress64Impl(const uint8_t* input, size_t inputSize, uint64_t* output) {
  alignas(64) uint64_t tmpIn[kMaxValuesPerBlock];
  alignas(64) uint64_t tmpOut[kMaxValuesPerBlock];

  const uint8_t* inPtr = input;
  const uint8_t* inEnd = input + inputSize;
  size_t nDecoded = 0;

  while (inPtr + kHeaderSize <= inEnd) {
    uint8_t bw;
    uint8_t count;
    uint64_t base;
    readHeader(inPtr, bw, count, base);
    inPtr += kHeaderSize;

    if (bw == kBwTailMarker) {
      if (count > 0) {
        // memcpy handles any alignment, no special case needed.
        std::memcpy(
            reinterpret_cast<uint8_t*>(output) + nDecoded * sizeof(uint64_t), inPtr, count * sizeof(uint64_t));
        nDecoded += count;
      }
      break;
    }

    size_t blockVals = static_cast<size_t>(count) * kLanes;
    size_t compBytes = compressedWords(blockVals, bw) * sizeof(uint64_t);

    if (inPtr + compBytes > inEnd) {
      break;
    }

    // Decode: pick aligned src/dst.
    const uint64_t* decIn;
    if constexpr (InAligned) {
      decIn = reinterpret_cast<const uint64_t*>(inPtr);
    } else {
      std::memcpy(tmpIn, inPtr, compBytes);
      decIn = tmpIn;
    }

    uint64_t* decOut;
    if constexpr (OutAligned) {
      decOut = output + nDecoded;
    } else {
      decOut = tmpOut;
    }

    decodeRt(decIn, decOut, base, blockVals, bw);

    if constexpr (!OutAligned) {
      std::memcpy(
          reinterpret_cast<uint8_t*>(output) + nDecoded * sizeof(uint64_t), tmpOut, blockVals * sizeof(uint64_t));
    }

    inPtr += compBytes;
    nDecoded += blockVals;
  }

  return nDecoded;
}

// Runtime dispatch.
inline size_t decompress64(const uint8_t* input, size_t inputSize, uint64_t* output) {
  bool inOk = (reinterpret_cast<uintptr_t>(input) % alignof(uint64_t) == 0);
  bool outOk = (reinterpret_cast<uintptr_t>(output) % alignof(uint64_t) == 0);
  if (inOk && outOk) {
    return decompress64Impl<true, true>(input, inputSize, output);
  }
  if (inOk && !outOk) {
    return decompress64Impl<true, false>(input, inputSize, output);
  }
  if (!inOk && outOk) {
    return decompress64Impl<false, true>(input, inputSize, output);
  }
  return decompress64Impl<false, false>(input, inputSize, output);
}

} // namespace ffor
} // namespace gluten

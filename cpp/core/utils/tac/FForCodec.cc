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
#include "utils/Exception.h"
#include "utils/tac/ffor.hpp"

namespace gluten {
namespace ffor {
namespace detail {

// AVX2 has no unsigned 64-bit min/max (vpminuq/vpmaxuq need AVX-512), so GCC's AVX2 vectorization of
// this loop emulates them with a bias, a signed compare and a blend on the loop-carried chain.  The
// source is a plain scalar loop; target_clones adds an x86-64-v4 version and an ifunc that picks
// one at load time, and the result is identical.
// Defined here so the clones and the resolver are emitted once, not once per including TU.
// GCC 11 rejects the attribute (no runtime x86-64-v4 level check) and clang's support is untested,
// so those builds, non-ELF/non-x86-64 targets and GLUTEN_ENABLE_MULTITARGET_CODE=0 get a plain function.
#if defined(__GNUC__) && !defined(__clang__) && __GNUC__ >= 12 && defined(__x86_64__) && defined(__ELF__) && \
    (!defined(GLUTEN_ENABLE_MULTITARGET_CODE) || GLUTEN_ENABLE_MULTITARGET_CODE)
__attribute__((target_clones("arch=x86-64-v4", "default")))
#endif
void minMaxScan(const uint64_t* data, size_t n, uint64_t& mn, uint64_t& mx) {
  GLUTEN_DCHECK(n > 0, "minMaxScan requires at least one value");
  uint64_t low = data[0];
  uint64_t high = data[0];
  for (size_t i = 1; i < n; ++i) {
    if (data[i] < low) {
      low = data[i];
    }
    if (data[i] > high) {
      high = data[i];
    }
  }
  mn = low;
  mx = high;
}

} // namespace detail
} // namespace ffor

int64_t FForCodec::maxCompressedLength(int64_t inputSize) {
  size_t numValues = inputSize / sizeof(uint64_t);
  return static_cast<int64_t>(ffor::compress64Bound(numValues));
}

arrow::Result<int64_t>
FForCodec::compress(const uint8_t* input, int64_t inputSize, uint8_t* output, int64_t outputSize) {
  if (inputSize == 0) {
    return 0;
  }
  if (inputSize % sizeof(uint64_t) != 0) {
    return arrow::Status::Invalid("FForCodec: input size ", inputSize, " is not a multiple of 8.");
  }

  size_t numValues = inputSize / sizeof(uint64_t);
  auto maxLen = static_cast<int64_t>(ffor::compress64Bound(numValues));
  if (outputSize < maxLen) {
    return arrow::Status::Invalid(
        "FForCodec: output buffer too small (need ", maxLen, " bytes, got ", outputSize, ").");
  }

  auto written = ffor::compress64(reinterpret_cast<const uint64_t*>(input), numValues, output);
  return static_cast<int64_t>(written);
}

arrow::Result<int64_t>
FForCodec::decompress(const uint8_t* input, int64_t inputSize, uint8_t* output, int64_t outputSize) {
  if (outputSize == 0) {
    return 0;
  }
  if (outputSize % sizeof(uint64_t) != 0) {
    return arrow::Status::Invalid("FForCodec: output size ", outputSize, " is not a multiple of 8.");
  }

  auto nDecoded =
      ffor::decompress64(input, inputSize, reinterpret_cast<uint64_t*>(output), static_cast<size_t>(outputSize));
  return static_cast<int64_t>(nDecoded);
}

int64_t FForCodec::maxCompressedLength128(int64_t inputSize) {
  if (inputSize % sizeof(__int128_t) != 0) {
    return 0;
  }
  size_t numValues = inputSize / sizeof(__int128_t);
  return static_cast<int64_t>(ffor::compress128Bound(numValues));
}

arrow::Result<int64_t>
FForCodec::compress128(const uint8_t* input, int64_t inputSize, uint8_t* output, int64_t outputSize) {
  if (inputSize == 0) {
    return 0;
  }
  if (inputSize % sizeof(__int128_t) != 0) {
    return arrow::Status::Invalid(
        "FForCodec: input size ", inputSize, " is not a multiple of ", sizeof(__int128_t), ".");
  }

  size_t numValues = inputSize / sizeof(__int128_t);
  auto maxLen = static_cast<int64_t>(ffor::compress128Bound(numValues));
  if (outputSize < maxLen) {
    return arrow::Status::Invalid(
        "FForCodec: output buffer too small for 128-bit compression (need ", maxLen, " bytes, got ", outputSize, ").");
  }

  auto written = ffor::compress128(input, numValues, output);
  return static_cast<int64_t>(written);
}

arrow::Result<int64_t>
FForCodec::decompress128(const uint8_t* input, int64_t inputSize, uint8_t* output, int64_t outputSize) {
  if (outputSize == 0) {
    return 0;
  }
  if (outputSize % sizeof(__int128_t) != 0) {
    return arrow::Status::Invalid(
        "FForCodec: output size ", outputSize, " is not a multiple of ", sizeof(__int128_t), ".");
  }

  auto nDecoded = ffor::decompress128(input, inputSize, output, static_cast<size_t>(outputSize));
  return static_cast<int64_t>(nDecoded);
}

} // namespace gluten

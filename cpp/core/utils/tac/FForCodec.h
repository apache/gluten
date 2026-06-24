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

#pragma once

#include <arrow/result.h>
#include <arrow/status.h>
#include <cstdint>

namespace gluten {

// FFOR (Frame-of-Reference) codec for uint64_t / 128-bit data using 4-lane layout.
// Used for INT64/UINT64 and INT128/HUGEINT (DECIMAL) columns in shuffle.
class FForCodec {
 public:
  // Returns the maximum compressed size in bytes for the given input size.
  // Input is treated as a stream of uint64 values; size must be a multiple of 8.
  static int64_t maxCompressedLength(int64_t inputSize);

  // Compress uint64_t data.
  // inputSize must be a multiple of 8 (sizeof(uint64_t)).
  // Returns the number of compressed bytes written to output.
  static arrow::Result<int64_t> compress(const uint8_t* input, int64_t inputSize, uint8_t* output, int64_t outputSize);

  // Decompress data compressed by compress().
  // outputSize must be a multiple of 8 (sizeof(uint64_t)).
  // Returns the number of uint64_t values decoded.
  static arrow::Result<int64_t>
  decompress(const uint8_t* input, int64_t inputSize, uint8_t* output, int64_t outputSize);

  // Worst-case compressed size for a stream of 128-bit values.
  // inputSize must be a multiple of sizeof(__int128_t); returns 0 otherwise.
  static int64_t maxCompressedLength128(int64_t inputSize);

  // Compress a stream of 128-bit values whose in-memory layout consists of
  // two 64-bit halves: the low 8B at offset 0 and the high 8B at offset 8
  // (DECIMAL128 in Velox). Internally splits each value into hi/lo uint64
  // sub-streams per block and runs the 64-bit FFOR encoder on each.
  // inputSize must be a multiple of sizeof(__int128_t); returns the number
  // of compressed bytes written to output.
  static arrow::Result<int64_t>
  compress128(const uint8_t* input, int64_t inputSize, uint8_t* output, int64_t outputSize);

  // Decompress data produced by compress128().  outputSize must be a multiple
  // of sizeof(__int128_t); returns the number of 128-bit values decoded.
  static arrow::Result<int64_t>
  decompress128(const uint8_t* input, int64_t inputSize, uint8_t* output, int64_t outputSize);
};

} // namespace gluten

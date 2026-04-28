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
namespace tac {

/// Type identifiers for type-aware compression.
/// Independent of any external type system (Arrow, Velox, etc.).
/// Backend-specific code converts to/from these types.
enum TacDataType : int8_t {
  kUnsupported = -1, // Not compressible by TAC.
  kUInt64 = 0, // 8-byte unsigned integer (also used for int64, double, date64).
};

} // namespace tac

/// TypeAwareCompressCodec provides type-aware compression that selects the best
/// compression algorithm based on the data type of the buffer.
///
/// Currently supported:
///   kUInt64 -> FFor (Frame-of-Reference + Bit-Packing) for uint64_t streams.
///
/// The compressed wire format is self-describing: decompress() does not need
/// a type hint because codec ID and element width are embedded in the header.
class TypeAwareCompressCodec {
 public:
  /// Check if type-aware compression is supported for the given TAC type.
  static bool support(int8_t tacType);

  /// Estimate the maximum compressed output size.
  static int64_t maxCompressedLen(int64_t inputLen, int8_t tacType);

  /// Compress a buffer with a type hint. Returns bytes written to output.
  static arrow::Result<int64_t>
  compress(const uint8_t* input, int64_t inputLen, uint8_t* output, int64_t outputLen, int8_t tacType);

  /// Decompress without a type hint. Self-describing from the payload header.
  static arrow::Result<int64_t> decompress(const uint8_t* input, int64_t inputLen, uint8_t* output, int64_t outputLen);

 private:
  enum CodecId : uint8_t {
    kFFor = 1,
  };

  static constexpr int64_t kPayloadHeaderSize = sizeof(uint8_t) + sizeof(uint8_t);
};

} // namespace gluten

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
#include <vector>

namespace gluten {
namespace tac {

/// Type identifiers for type-aware compression.
/// Independent of any external type system (Arrow, Velox, etc.).
/// Backend-specific code converts to/from these types.
enum TacDataType : int8_t {
  kUnsupported = -1, // Not compressible by TAC.
  kUInt64 = 0, // 8-byte unsigned integer (also used for int64, double, date64).
  kUInt128 = 1, // 16-byte unsigned integer (used for int128 / HugeInt / decimal(p > 18, s)).
  kUInt32 = 2, // 4-byte unsigned integer (also used for int32, date32, string-offsets buffer).
  kStringDict =
      3, // Variable-length string DATA buffer. Adaptive: emits dictionary or LZ4 payload based on which is smaller.
};

} // namespace tac

/// TypeAwareCompressCodec provides type-aware compression that selects the best
/// compression algorithm based on the data type of the buffer.
///
/// All numeric codecs (kUInt64, kUInt128, kUInt32) are adaptive: each computes
/// both its native specialized encoding AND an LZ4 baseline over the raw input,
/// emitting whichever is smaller along with a 1-byte body strategy header.
/// This guarantees we never produce more compressed bytes than LZ4 alone would.
///
/// Currently supported:
///   kUInt64    -> FFor (Frame-of-Reference + Bit-Packing) for uint64_t streams.
///                 LZ4 fallback wins for low-cardinality data with long runs.
///   kUInt128   -> FFor split-lane: split each int128 into two uint64 lanes
///                 (low / high), FFor-encode each lane independently. Reuses
///                 the existing FFor(uint64) machinery. LZ4 fallback wins for
///                 long runs or columns where neither lane has narrow range.
///   kUInt32    -> FFor over a zero-extended uint64 view of the uint32 stream.
///                 Reuses the FFor(uint64) machinery; decompress truncates
///                 back to uint32. Suited for INT32, DATE32, and string-offsets
///                 buffers (where the dense int32 range and locality both fit
///                 FFor's frame-of-reference well). LZ4 fallback handles the
///                 low-cardinality or highly-repetitive edge cases.
///   kStringDict-> Variable-length string DATA buffer. Requires the column's
///                 OFFSETS buffer (passed via compress() overload) to delimit
///                 individual strings. Computes both a dictionary encoding
///                 and an LZ4-compressed payload; emits whichever is smaller
///                 along with a strategy byte for the reader to dispatch on.
///                 LZ4 wins for low-cardinality data with long consecutive
///                 runs (LZ4 finds the long-distance repetitions); dictionary
///                 wins for medium-cardinality scattered data.
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
  ///
  /// For kStringDict, the offsetsBuffer + numRows arguments are required and
  /// must describe the buffer's row boundaries (Arrow int32 offsets array of
  /// length numRows+1). For all other TAC types, these arguments are ignored
  /// and should be left at their defaults.
  static arrow::Result<int64_t> compress(
      const uint8_t* input,
      int64_t inputLen,
      uint8_t* output,
      int64_t outputLen,
      int8_t tacType,
      const uint8_t* offsetsBuffer = nullptr,
      int32_t numRows = 0);

  /// Decompress without a type hint. Self-describing from the payload header.
  static arrow::Result<int64_t> decompress(const uint8_t* input, int64_t inputLen, uint8_t* output, int64_t outputLen);

 private:
  enum CodecId : uint8_t {
    kFFor = 1,
    kFForSplit128 = 2,
    kFForWidened32 = 3,
    kStringDict = 4,
  };

  /// Strategy byte for kStringDict payloads. Tells the reader whether the
  /// payload was dictionary-encoded or LZ4-fallback-compressed.
  enum StringDictStrategy : uint8_t {
    kStrategyDict = 0,
    kStrategyLz4 = 1,
  };

  /// Strategy byte for kFFor / kFForSplit128 / kFForWidened32 bodies. Each of
  /// those codecs adaptively chooses between its native FFor-based encoding
  /// and an LZ4 fallback over the raw input, depending on which produces a
  /// smaller payload. The first byte of the body identifies which path was
  /// taken so decompress() can dispatch accordingly.
  enum IntCodecStrategy : uint8_t {
    kIntStrategyNative = 0,
    kIntStrategyLz4 = 1,
  };

  static constexpr int64_t kPayloadHeaderSize = sizeof(uint8_t) + sizeof(uint8_t);
  static constexpr int64_t kIntStrategyHeaderSize = sizeof(uint8_t);
  static constexpr int64_t kIntLz4BodyHeaderSize = sizeof(uint8_t) + sizeof(int32_t);
  static constexpr int64_t kSplit128BodyHeaderSize = sizeof(int64_t);

  static arrow::Result<int64_t>
  compressSplit128(const uint8_t* input, int64_t inputLen, uint8_t* output, int64_t outputLen);

  static arrow::Result<int64_t>
  decompressSplit128(const uint8_t* input, int64_t inputLen, uint8_t* output, int64_t outputLen);

  static arrow::Result<int64_t>
  compressWidened32(const uint8_t* input, int64_t inputLen, uint8_t* output, int64_t outputLen);

  static arrow::Result<int64_t>
  decompressWidened32(const uint8_t* input, int64_t inputLen, uint8_t* output, int64_t outputLen);

  static arrow::Result<int64_t> compressStringDict(
      const uint8_t* input,
      int64_t inputLen,
      const uint8_t* offsetsBuffer,
      int32_t numRows,
      uint8_t* output,
      int64_t outputLen);

  static arrow::Result<int64_t>
  decompressStringDict(const uint8_t* input, int64_t inputLen, uint8_t* output, int64_t outputLen);

  /// Compress `input` with LZ4 into `out`. Returns the raw lz4 byte count.
  static arrow::Result<int64_t> runLz4Fallback(const uint8_t* input, int64_t inputLen, std::vector<uint8_t>& out);

  /// Emit an int-codec LZ4 fallback body: strategy byte + int32 length + lz4 bytes.
  static arrow::Result<int64_t>
  writeLz4Body(const uint8_t* lz4Bytes, int64_t lz4Len, uint8_t* output, int64_t outputLen);

  /// Decode an int-codec LZ4 fallback body (strategy byte already consumed).
  static arrow::Result<int64_t>
  decompressLz4Body(const uint8_t* input, int64_t inputLen, uint8_t* output, int64_t outputLen);
};

} // namespace gluten

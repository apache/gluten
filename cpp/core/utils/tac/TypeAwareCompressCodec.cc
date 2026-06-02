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

#include "utils/tac/TypeAwareCompressCodec.h"
#include "utils/tac/FForCodec.h"

#include <arrow/util/compression.h>
#include <algorithm>
#include <cstring>
#include <limits>
#include <unordered_map>
#include <vector>

namespace gluten {

namespace {

// Light wrapper over arrow's LZ4_FRAME codec, lazily created and shared per
// process. Used both by kStringDict and by the adaptive LZ4 fallback added
// to the numeric codecs.
arrow::util::Codec* lz4CodecShared() {
  static auto codec = arrow::util::Codec::Create(arrow::Compression::LZ4_FRAME).ValueOrDie();
  return codec.get();
}

} // namespace

bool TypeAwareCompressCodec::support(int8_t tacType) {
  return tacType == tac::kUInt64 || tacType == tac::kUInt128 || tacType == tac::kUInt32 || tacType == tac::kStringDict;
}

int64_t TypeAwareCompressCodec::maxCompressedLen(int64_t inputLen, int8_t tacType) {
  if (!support(tacType)) {
    return 0;
  }
  // Numeric codecs are adaptive: pick max(nativeUpperBound, lz4UpperBound)
  // then add the body strategy byte. The outer TAC header (2 B) is always
  // added at the very end.
  auto* lz4 = lz4CodecShared();
  int64_t lz4Max = lz4->MaxCompressedLen(inputLen, nullptr) + kIntLz4BodyHeaderSize;
  if (tacType == tac::kUInt128) {
    // Two uint64 lanes of (inputLen / 2) bytes each + 8-byte body header.
    int64_t bytesPerLane = inputLen / 2;
    int64_t maxLane = FForCodec::maxCompressedLength(bytesPerLane);
    int64_t nativeMax = kIntStrategyHeaderSize + kSplit128BodyHeaderSize + 2 * maxLane;
    return kPayloadHeaderSize + std::max(nativeMax, lz4Max);
  }
  if (tacType == tac::kUInt32) {
    // Widen each uint32 -> uint64 then FFor.  Worst case is 2x the input
    // bytes through the uint64 codec.
    int64_t widenedBytes = 2 * inputLen;
    int64_t nativeMax = kIntStrategyHeaderSize + FForCodec::maxCompressedLength(widenedBytes);
    return kPayloadHeaderSize + std::max(nativeMax, lz4Max);
  }
  if (tacType == tac::kStringDict) {
    // Dictionary payload worst case (all unique values) is essentially the
    // original data plus per-row offsets (4B) plus per-entry length prefix
    // (4B) plus indices (worst case 4B per row).  LZ4 worst case has its
    // own MaxCompressedLen.  Be generous: use the larger of the two upper
    // bounds + a small fixed-overhead allowance (codec hdr + strategy +
    // payload hdr + reserved counts).  120 bytes covers all headers.
    int64_t lz4Bound = lz4->MaxCompressedLen(inputLen, nullptr);
    int64_t dictMax = inputLen + 4 * 8 /* widest indices */ + 120;
    return kPayloadHeaderSize + 120 + std::max(lz4Bound, dictMax);
  }
  // kUInt64
  int64_t nativeMax = kIntStrategyHeaderSize + FForCodec::maxCompressedLength(inputLen);
  return kPayloadHeaderSize + std::max(nativeMax, lz4Max);
}

arrow::Result<int64_t>
TypeAwareCompressCodec::runLz4Fallback(const uint8_t* input, int64_t inputLen, std::vector<uint8_t>& out) {
  auto* codec = lz4CodecShared();
  int64_t lz4Max = codec->MaxCompressedLen(inputLen, input);
  out.resize(static_cast<size_t>(lz4Max));
  ARROW_ASSIGN_OR_RAISE(int64_t lz4Len, codec->Compress(inputLen, input, lz4Max, out.data()));
  return lz4Len;
}

arrow::Result<int64_t>
TypeAwareCompressCodec::writeLz4Body(const uint8_t* lz4Bytes, int64_t lz4Len, uint8_t* output, int64_t outputLen) {
  int64_t bodySize = kIntLz4BodyHeaderSize + lz4Len;
  if (outputLen < bodySize) {
    return arrow::Status::Invalid("Int codec LZ4 body: output too small (", outputLen, " < ", bodySize, ")");
  }
  uint8_t* out = output;
  *out++ = static_cast<uint8_t>(kIntStrategyLz4);
  int32_t lz4Len32 = static_cast<int32_t>(lz4Len);
  std::memcpy(out, &lz4Len32, sizeof(int32_t));
  out += sizeof(int32_t);
  std::memcpy(out, lz4Bytes, static_cast<size_t>(lz4Len));
  out += lz4Len;
  return out - output;
}

arrow::Result<int64_t>
TypeAwareCompressCodec::decompressLz4Body(const uint8_t* input, int64_t inputLen, uint8_t* output, int64_t outputLen) {
  if (inputLen < static_cast<int64_t>(sizeof(int32_t))) {
    return arrow::Status::Invalid("Int codec LZ4 body: missing length prefix.");
  }
  int32_t lz4Len = 0;
  std::memcpy(&lz4Len, input, sizeof(int32_t));
  const uint8_t* lz4Bytes = input + sizeof(int32_t);
  int64_t remaining = inputLen - sizeof(int32_t);
  if (lz4Len < 0 || lz4Len > remaining) {
    return arrow::Status::Invalid("Int codec LZ4 body: invalid length ", lz4Len);
  }
  auto* codec = lz4CodecShared();
  ARROW_ASSIGN_OR_RAISE(int64_t nDecoded, codec->Decompress(lz4Len, lz4Bytes, outputLen, output));
  if (nDecoded != outputLen) {
    return arrow::Status::Invalid("Int codec LZ4 body: decompress produced ", nDecoded, " bytes, expected ", outputLen);
  }
  return outputLen;
}

arrow::Result<int64_t> TypeAwareCompressCodec::compress(
    const uint8_t* input,
    int64_t inputLen,
    uint8_t* output,
    int64_t outputLen,
    int8_t tacType,
    const uint8_t* offsetsBuffer,
    int32_t numRows) {
  if (!support(tacType)) {
    return arrow::Status::Invalid("Type-aware compression not supported for tac type: ", static_cast<int>(tacType));
  }
  if (inputLen == 0) {
    return 0;
  }
  if (outputLen < kPayloadHeaderSize) {
    return arrow::Status::Invalid("Output buffer too small for type-aware compression.");
  }

  auto* out = output;

  // kStringDict has its own internal adaptive logic (dict vs LZ4) — pass through.
  if (tacType == tac::kStringDict) {
    if (offsetsBuffer == nullptr || numRows <= 0) {
      return arrow::Status::Invalid(
          "Type-aware compression (string dict): offsets buffer and positive numRows are required.");
    }
    *out++ = static_cast<uint8_t>(CodecId::kStringDict);
    *out++ = static_cast<uint8_t>(tacType);
    ARROW_ASSIGN_OR_RAISE(
        auto bodyLen, compressStringDict(input, inputLen, offsetsBuffer, numRows, out, outputLen - kPayloadHeaderSize));
    return kPayloadHeaderSize + bodyLen;
  }

  // Numeric codecs: build native AND LZ4 outputs, pick the smaller.
  CodecId nativeCodecId;
  switch (tacType) {
    case tac::kUInt64:
      nativeCodecId = CodecId::kFFor;
      break;
    case tac::kUInt128:
      nativeCodecId = CodecId::kFForSplit128;
      break;
    case tac::kUInt32:
      nativeCodecId = CodecId::kFForWidened32;
      break;
    default:
      // Unreachable: support() would have rejected.
      return arrow::Status::Invalid("Unhandled tac type: ", static_cast<int>(tacType));
  }

  // Produce the native body into a scratch buffer so we can compare its size
  // against LZ4 before committing to one.
  int64_t nativeUpperBound = 0;
  if (nativeCodecId == CodecId::kFFor) {
    nativeUpperBound = FForCodec::maxCompressedLength(inputLen);
  } else if (nativeCodecId == CodecId::kFForSplit128) {
    int64_t bytesPerLane = inputLen / 2;
    nativeUpperBound = kSplit128BodyHeaderSize + 2 * FForCodec::maxCompressedLength(bytesPerLane);
  } else {
    // kFForWidened32
    nativeUpperBound = FForCodec::maxCompressedLength(2 * inputLen);
  }
  std::vector<uint8_t> nativeBuf(static_cast<size_t>(nativeUpperBound));
  int64_t nativeLen = 0;
  if (nativeCodecId == CodecId::kFFor) {
    ARROW_ASSIGN_OR_RAISE(nativeLen, FForCodec::compress(input, inputLen, nativeBuf.data(), nativeUpperBound));
  } else if (nativeCodecId == CodecId::kFForSplit128) {
    ARROW_ASSIGN_OR_RAISE(nativeLen, compressSplit128(input, inputLen, nativeBuf.data(), nativeUpperBound));
  } else {
    ARROW_ASSIGN_OR_RAISE(nativeLen, compressWidened32(input, inputLen, nativeBuf.data(), nativeUpperBound));
  }

  // Produce LZ4 body into a scratch buffer.
  std::vector<uint8_t> lz4Buf;
  ARROW_ASSIGN_OR_RAISE(int64_t lz4Len, runLz4Fallback(input, inputLen, lz4Buf));

  int64_t nativeBodySize = kIntStrategyHeaderSize + nativeLen;
  int64_t lz4BodySize = kIntLz4BodyHeaderSize + lz4Len;

  // Tie goes to native (decompression is cheaper and avoids LZ4 lookup overhead).
  bool useNative = nativeBodySize <= lz4BodySize;
  int64_t bodySize = useNative ? nativeBodySize : lz4BodySize;
  if (outputLen - kPayloadHeaderSize < bodySize) {
    return arrow::Status::Invalid(
        "Adaptive int codec: output buffer too small (", outputLen - kPayloadHeaderSize, " < ", bodySize, ")");
  }

  *out++ = static_cast<uint8_t>(nativeCodecId);
  *out++ = static_cast<uint8_t>(tacType);

  if (useNative) {
    *out++ = static_cast<uint8_t>(kIntStrategyNative);
    std::memcpy(out, nativeBuf.data(), static_cast<size_t>(nativeLen));
    out += nativeLen;
  } else {
    ARROW_ASSIGN_OR_RAISE(auto written, writeLz4Body(lz4Buf.data(), lz4Len, out, outputLen - kPayloadHeaderSize));
    out += written;
  }

  return out - output;
}

arrow::Result<int64_t>
TypeAwareCompressCodec::decompress(const uint8_t* input, int64_t inputLen, uint8_t* output, int64_t outputLen) {
  if (inputLen < kPayloadHeaderSize) {
    return arrow::Status::Invalid("Input too small for type-aware decompress header.");
  }

  auto* in = input;
  auto codecId = static_cast<CodecId>(*in++);
  [[maybe_unused]] auto tacType = *in++;
  auto dataLen = inputLen - kPayloadHeaderSize;

  // String-dict codec keeps its own internal strategy byte; pass body through.
  if (codecId == CodecId::kStringDict) {
    ARROW_RETURN_NOT_OK(decompressStringDict(in, dataLen, output, outputLen));
    return inputLen;
  }

  // All numeric codecs share the body strategy byte (native vs LZ4 fallback).
  if (codecId != CodecId::kFFor && codecId != CodecId::kFForSplit128 && codecId != CodecId::kFForWidened32) {
    return arrow::Status::Invalid("Unknown type-aware codec ID: ", static_cast<int>(codecId));
  }
  if (dataLen < kIntStrategyHeaderSize) {
    return arrow::Status::Invalid("Int codec body: missing strategy byte.");
  }
  auto strategy = static_cast<IntCodecStrategy>(*in++);
  dataLen -= kIntStrategyHeaderSize;

  if (strategy == kIntStrategyLz4) {
    ARROW_RETURN_NOT_OK(decompressLz4Body(in, dataLen, output, outputLen));
    return inputLen;
  }
  if (strategy != kIntStrategyNative) {
    return arrow::Status::Invalid("Int codec body: unknown strategy ", static_cast<int>(strategy));
  }

  // Native FFor path — dispatch by codec id.
  switch (codecId) {
    case CodecId::kFFor: {
      ARROW_ASSIGN_OR_RAISE(auto nDecoded, FForCodec::decompress(in, dataLen, output, outputLen));
      (void)nDecoded;
      return inputLen;
    }
    case CodecId::kFForSplit128: {
      ARROW_RETURN_NOT_OK(decompressSplit128(in, dataLen, output, outputLen));
      return inputLen;
    }
    case CodecId::kFForWidened32: {
      ARROW_RETURN_NOT_OK(decompressWidened32(in, dataLen, output, outputLen));
      return inputLen;
    }
    default:
      return arrow::Status::Invalid("Unreachable: codecId already validated.");
  }
}

arrow::Result<int64_t>
TypeAwareCompressCodec::compressSplit128(const uint8_t* input, int64_t inputLen, uint8_t* output, int64_t outputLen) {
  if (inputLen % 16 != 0) {
    return arrow::Status::Invalid(
        "Type-aware compression (split128): input size ", inputLen, " is not a multiple of 16.");
  }
  if (outputLen < kSplit128BodyHeaderSize) {
    return arrow::Status::Invalid("Output buffer too small for split128 body header.");
  }

  const size_t nValues = static_cast<size_t>(inputLen / 16);

  // Split the int128 stream into two parallel uint64 streams.
  // On little-endian (x86-64 / aarch64), __int128 layout is [lo_8B][hi_8B]
  // — matches Velox HugeInt::lower/upper. Use memcpy to stay alignment-safe.
  std::vector<uint64_t> lo(nValues);
  std::vector<uint64_t> hi(nValues);
  const uint8_t* src = input;
  for (size_t i = 0; i < nValues; ++i) {
    std::memcpy(&lo[i], src, sizeof(uint64_t));
    std::memcpy(&hi[i], src + sizeof(uint64_t), sizeof(uint64_t));
    src += 16;
  }

  // Body layout: | loCompLen (int64) | loPayload | hiPayload |
  uint8_t* loDst = output + kSplit128BodyHeaderSize;
  int64_t loBudget = outputLen - kSplit128BodyHeaderSize;
  if (loBudget < 0) {
    return arrow::Status::Invalid("Output buffer too small for split128 lo payload.");
  }
  ARROW_ASSIGN_OR_RAISE(
      int64_t loLen,
      FForCodec::compress(
          reinterpret_cast<const uint8_t*>(lo.data()),
          static_cast<int64_t>(nValues * sizeof(uint64_t)),
          loDst,
          loBudget));

  uint8_t* hiDst = loDst + loLen;
  int64_t hiBudget = loBudget - loLen;
  if (hiBudget < 0) {
    return arrow::Status::Invalid("Output buffer too small for split128 hi payload.");
  }
  ARROW_ASSIGN_OR_RAISE(
      int64_t hiLen,
      FForCodec::compress(
          reinterpret_cast<const uint8_t*>(hi.data()),
          static_cast<int64_t>(nValues * sizeof(uint64_t)),
          hiDst,
          hiBudget));

  std::memcpy(output, &loLen, sizeof(int64_t));

  return kSplit128BodyHeaderSize + loLen + hiLen;
}

arrow::Result<int64_t>
TypeAwareCompressCodec::decompressSplit128(const uint8_t* input, int64_t inputLen, uint8_t* output, int64_t outputLen) {
  if (inputLen < kSplit128BodyHeaderSize) {
    return arrow::Status::Invalid("Input too small for split128 body header.");
  }
  if (outputLen % 16 != 0) {
    return arrow::Status::Invalid(
        "Type-aware decompression (split128): output size ", outputLen, " is not a multiple of 16.");
  }

  int64_t loLen;
  std::memcpy(&loLen, input, sizeof(int64_t));
  if (loLen < 0 || loLen > inputLen - kSplit128BodyHeaderSize) {
    return arrow::Status::Invalid("Type-aware decompression (split128): invalid loCompLen ", loLen);
  }
  int64_t hiLen = inputLen - kSplit128BodyHeaderSize - loLen;

  const size_t nValues = static_cast<size_t>(outputLen / 16);
  const int64_t laneBytes = static_cast<int64_t>(nValues * sizeof(uint64_t));

  std::vector<uint64_t> lo(nValues);
  std::vector<uint64_t> hi(nValues);

  ARROW_ASSIGN_OR_RAISE(
      auto nLo,
      FForCodec::decompress(input + kSplit128BodyHeaderSize, loLen, reinterpret_cast<uint8_t*>(lo.data()), laneBytes));
  ARROW_ASSIGN_OR_RAISE(
      auto nHi,
      FForCodec::decompress(
          input + kSplit128BodyHeaderSize + loLen, hiLen, reinterpret_cast<uint8_t*>(hi.data()), laneBytes));
  // Defense against truncated/corrupted streams: FForCodec::decompress reports
  // how many bytes it produced.  Both lanes must produce exactly `laneBytes`
  // worth of decoded data; anything else means the stream was mis-framed and
  // we would be reading from a partially-uninitialized lane vector below.
  if (nLo != laneBytes || nHi != laneBytes) {
    return arrow::Status::Invalid(
        "Split128 decompress: lane size mismatch (lo=", nLo, ", hi=", nHi, ", expected=", laneBytes, ")");
  }

  uint8_t* dst = output;
  for (size_t i = 0; i < nValues; ++i) {
    std::memcpy(dst, &lo[i], sizeof(uint64_t));
    std::memcpy(dst + sizeof(uint64_t), &hi[i], sizeof(uint64_t));
    dst += 16;
  }

  return outputLen;
}

arrow::Result<int64_t>
TypeAwareCompressCodec::compressWidened32(const uint8_t* input, int64_t inputLen, uint8_t* output, int64_t outputLen) {
  if (inputLen % 4 != 0) {
    return arrow::Status::Invalid(
        "Type-aware compression (widened32): input size ", inputLen, " is not a multiple of 4.");
  }

  const size_t nValues = static_cast<size_t>(inputLen / 4);

  // Widen uint32 -> uint64 so the existing FFor(uint64) machinery applies.
  // memcpy keeps it alignment-safe and matches little-endian on x86-64 / aarch64.
  std::vector<uint64_t> widened(nValues);
  const uint8_t* src = input;
  for (size_t i = 0; i < nValues; ++i) {
    uint32_t v;
    std::memcpy(&v, src, sizeof(uint32_t));
    widened[i] = static_cast<uint64_t>(v);
    src += sizeof(uint32_t);
  }

  ARROW_ASSIGN_OR_RAISE(
      int64_t writtenLen,
      FForCodec::compress(
          reinterpret_cast<const uint8_t*>(widened.data()),
          static_cast<int64_t>(nValues * sizeof(uint64_t)),
          output,
          outputLen));
  return writtenLen;
}

arrow::Result<int64_t> TypeAwareCompressCodec::decompressWidened32(
    const uint8_t* input,
    int64_t inputLen,
    uint8_t* output,
    int64_t outputLen) {
  if (outputLen % 4 != 0) {
    return arrow::Status::Invalid(
        "Type-aware decompression (widened32): output size ", outputLen, " is not a multiple of 4.");
  }

  const size_t nValues = static_cast<size_t>(outputLen / 4);
  std::vector<uint64_t> widened(nValues);

  const int64_t widenedBytes = static_cast<int64_t>(nValues * sizeof(uint64_t));
  ARROW_ASSIGN_OR_RAISE(
      auto nDecoded, FForCodec::decompress(input, inputLen, reinterpret_cast<uint8_t*>(widened.data()), widenedBytes));
  // Defense against truncated/corrupted streams: the widened lane must be
  // fully populated before we truncate it back down to 4-byte values, else
  // we would be writing uninitialized memory into the caller's output.
  if (nDecoded != widenedBytes) {
    return arrow::Status::Invalid(
        "Widened32 decompress: widened lane produced ", nDecoded, " bytes, expected ", widenedBytes);
  }

  // Truncate uint64 -> uint32 back into the output buffer.
  uint8_t* dst = output;
  for (size_t i = 0; i < nValues; ++i) {
    uint32_t v = static_cast<uint32_t>(widened[i]);
    std::memcpy(dst, &v, sizeof(uint32_t));
    dst += sizeof(uint32_t);
  }

  return outputLen;
}

// String dictionary codec body format (after CodecId+tacType header):
//   strategy (uint8): 0 = dict, 1 = LZ4 fallback
//
// Dict strategy body:
//   indexWidth (uint8): 1, 2, or 4 bytes per index
//   numRows    (int32)
//   dictCount  (int32)
//   dictBytes  (int32)  // total bytes of the dict section
//   dict section: dictCount entries of (lengthDelta varint? no — fixed: int32 len, bytes)
//   index section: numRows * indexWidth bytes
//
// LZ4 strategy body:
//   compressedLen (int32)
//   lz4 compressed bytes
//
// Both strategies are recoverable from the wire alone — decompressStringDict
// dispatches on the strategy byte.
namespace {

constexpr int64_t kDictBodyFixedHeader = sizeof(uint8_t) + // strategy
    sizeof(uint8_t) + // indexWidth
    sizeof(int32_t) + // numRows
    sizeof(int32_t) + // dictCount
    sizeof(int32_t); // dictBytes

constexpr int64_t kLz4BodyFixedHeader = sizeof(uint8_t) + // strategy
    sizeof(int32_t); // compressedLen

// Regression guards for the dict-build path. See compressStringDict for the
// detailed CPU/ratio rationale and the v1/v2 false-positive history.
//
// v1 (heuristic: bail after 64 rows if >75% unique) regressed str_high_card8k.
// v2 (single check at row 256) regressed str_long_comments via the birthday
// paradox on a sampled 10K-element string pool.
// v3 raises the probe row to clamp(numRows/8, 256, 4096): at the cap (4096)
// the probability of seeing no duplicate is vanishingly small for any
// cardinality C ≤ 64K (~exp(-4096²/(2C)), ~10⁻⁵⁷ at C=64K), and remains
// ~10⁻⁴ at C=1M — only essentially-all-unique columns ever bail.
constexpr int64_t kDictMinInputBytes = 4096;
constexpr int32_t kDictBailoutMinCheckRows = 256;
constexpr int32_t kDictBailoutMaxCheckRows = 4096;
constexpr int32_t kDictBailoutCheckDivisor = 8;

uint8_t indexWidthFor(int32_t dictCount) {
  // 0/1 entries → width 1 (still need a valid value). 2..256 → 1B. 257..65536 → 2B. else 4B.
  if (dictCount <= 256) {
    return 1;
  }
  if (dictCount <= 65536) {
    return 2;
  }
  return 4;
}

} // namespace

arrow::Result<int64_t> TypeAwareCompressCodec::compressStringDict(
    const uint8_t* input,
    int64_t inputLen,
    const uint8_t* offsetsBuffer,
    int32_t numRows,
    uint8_t* output,
    int64_t outputLen) {
  const int32_t* offsets = reinterpret_cast<const int32_t*>(offsetsBuffer);

  // Velox shuffle can hand us a *sliced* string-data buffer: the buffer
  // contains numRows worth of variable-length payload, but the row content
  // does not necessarily start at byte 0 of the buffer (leading bytes can
  // belong to a previous slice or padding), and the row content may not
  // extend to byte (inputLen-1) (trailing bytes likewise).
  //
  // The shuffle reader reconstructs the data buffer byte-for-byte from the
  // compressed payload, so any leading/trailing bytes must be preserved
  // exactly. The dictionary path cannot do that without extra bookkeeping
  // (and the win from dict encoding is small on a sliced buffer anyway —
  // sliced inputs are typically small fragments). The safe and simple fix
  // is to fall back to LZ4-only when the input is sliced; LZ4 over the raw
  // buffer trivially preserves bytes.
  const bool sliced = (numRows >= 1) && (offsets[0] != 0 || offsets[numRows] != static_cast<int32_t>(inputLen));

  // Try LZ4 over the raw input buffer regardless of slicing — LZ4 always
  // round-trips inputLen bytes exactly.
  auto* codec = lz4CodecShared();
  int64_t lz4Max = codec->MaxCompressedLen(inputLen, input);
  std::vector<uint8_t> lz4Buf(static_cast<size_t>(lz4Max));
  ARROW_ASSIGN_OR_RAISE(int64_t lz4Len, codec->Compress(inputLen, input, lz4Max, lz4Buf.data()));
  int64_t lz4BodySize = kLz4BodyFixedHeader + lz4Len;

  // Compute the dict body size only when the input is not sliced AND the
  // input is large enough that dict-build CPU can pay off (Guard 1: tiny
  // buffer skip). We still need to materialize the dict + indices even if
  // we end up choosing it, so do the scan first; if it turns out larger
  // than LZ4 (or the input is sliced / too small), we emit LZ4 instead.
  //
  // For production diagnosability: any decision to skip the dict path is
  // observable via the wire-level strategy byte (kStrategyLz4 = 1) — no
  // separate logline needed at this hot path.
  int64_t dictBodySize = std::numeric_limits<int64_t>::max();
  std::unordered_map<std::string_view, int32_t> dict;
  std::vector<int32_t> indices;
  std::vector<std::string_view> dictEntries;
  int32_t dictCount = 0;
  uint8_t indexWidth = 1;
  int64_t dictBytes = 0;
  bool dictBailoutFired = false;

  const bool buildDict = !sliced && inputLen >= kDictMinInputBytes;
  if (buildDict) {
    dict.reserve(static_cast<size_t>(numRows));
    indices.assign(static_cast<size_t>(numRows), 0);
    dictEntries.reserve(static_cast<size_t>(numRows));

    // Guard 2: single deterministic "no-duplicates-after-N-rows" probe.
    // Clamp to [kDictBailoutMinCheckRows, kDictBailoutMaxCheckRows] so we
    // (a) always scan enough rows that birthday-paradox false positives are
    // negligible for any reasonable cardinality, and (b) cap wasted CPU on
    // true-unique columns. See namespace-level comment for full rationale.
    const int32_t bailoutCheckAt =
        std::clamp(numRows / kDictBailoutCheckDivisor, kDictBailoutMinCheckRows, kDictBailoutMaxCheckRows);
    bool seenAnyDuplicate = false;

    for (int32_t i = 0; i < numRows; ++i) {
      int32_t start = offsets[i];
      int32_t len = offsets[i + 1] - start;
      if (len < 0 || start + len > inputLen) {
        return arrow::Status::Invalid("String dict codec: invalid offsets at row ", i);
      }
      std::string_view sv(reinterpret_cast<const char*>(input + start), static_cast<size_t>(len));
      auto [it, inserted] = dict.try_emplace(sv, static_cast<int32_t>(dictEntries.size()));
      if (inserted) {
        dictEntries.push_back(sv);
        dictBytes += sizeof(int32_t) + len;
      } else {
        seenAnyDuplicate = true;
      }
      indices[i] = it->second;

      // Single probe: if we have scanned bailoutCheckAt rows without
      // observing a single duplicate, the column is essentially all-unique
      // and the dict can never recoup its overhead. Bail to LZ4.
      if (!seenAnyDuplicate && (i + 1) == bailoutCheckAt) {
        dictBailoutFired = true;
        break;
      }
    }

    if (!dictBailoutFired) {
      dictCount = static_cast<int32_t>(dictEntries.size());
      indexWidth = indexWidthFor(dictCount);
      int64_t indicesBytes = static_cast<int64_t>(numRows) * indexWidth;
      dictBodySize = kDictBodyFixedHeader + dictBytes + indicesBytes;
    }
  }

  // Pick the smaller strategy. Tie goes to dict (cheaper to decompress).
  // sliced inputs always pick LZ4 because dictBodySize is INT64_MAX.
  bool useDict = dictBodySize <= lz4BodySize;
  int64_t bodySize = useDict ? dictBodySize : lz4BodySize;
  if (outputLen < bodySize) {
    return arrow::Status::Invalid("String dict codec: output buffer too small (", outputLen, " < ", bodySize, ")");
  }

  uint8_t* out = output;
  if (useDict) {
    *out++ = static_cast<uint8_t>(kStrategyDict);
    *out++ = indexWidth;
    std::memcpy(out, &numRows, sizeof(int32_t));
    out += sizeof(int32_t);
    std::memcpy(out, &dictCount, sizeof(int32_t));
    out += sizeof(int32_t);
    int32_t dictBytes32 = static_cast<int32_t>(dictBytes);
    std::memcpy(out, &dictBytes32, sizeof(int32_t));
    out += sizeof(int32_t);

    for (const auto& sv : dictEntries) {
      int32_t len = static_cast<int32_t>(sv.size());
      std::memcpy(out, &len, sizeof(int32_t));
      out += sizeof(int32_t);
      std::memcpy(out, sv.data(), sv.size());
      out += sv.size();
    }

    for (int32_t i = 0; i < numRows; ++i) {
      int32_t idx = indices[i];
      if (indexWidth == 1) {
        *out++ = static_cast<uint8_t>(idx);
      } else if (indexWidth == 2) {
        uint16_t v = static_cast<uint16_t>(idx);
        std::memcpy(out, &v, sizeof(uint16_t));
        out += sizeof(uint16_t);
      } else {
        std::memcpy(out, &idx, sizeof(int32_t));
        out += sizeof(int32_t);
      }
    }
  } else {
    *out++ = static_cast<uint8_t>(kStrategyLz4);
    int32_t lz4Len32 = static_cast<int32_t>(lz4Len);
    std::memcpy(out, &lz4Len32, sizeof(int32_t));
    out += sizeof(int32_t);
    std::memcpy(out, lz4Buf.data(), static_cast<size_t>(lz4Len));
    out += lz4Len;
  }

  return out - output;
}

arrow::Result<int64_t> TypeAwareCompressCodec::decompressStringDict(
    const uint8_t* input,
    int64_t inputLen,
    uint8_t* output,
    int64_t outputLen) {
  if (inputLen < 1) {
    return arrow::Status::Invalid("String dict codec: input too small for strategy byte.");
  }
  StringDictStrategy strategy = static_cast<StringDictStrategy>(input[0]);
  const uint8_t* in = input + 1;
  int64_t remaining = inputLen - 1;

  if (strategy == kStrategyLz4) {
    if (remaining < static_cast<int64_t>(sizeof(int32_t))) {
      return arrow::Status::Invalid("String dict codec: LZ4 strategy missing length header.");
    }
    int32_t lz4Len = 0;
    std::memcpy(&lz4Len, in, sizeof(int32_t));
    in += sizeof(int32_t);
    remaining -= sizeof(int32_t);
    if (lz4Len < 0 || lz4Len > remaining) {
      return arrow::Status::Invalid("String dict codec: invalid LZ4 length ", lz4Len);
    }
    auto* codec = lz4CodecShared();
    ARROW_ASSIGN_OR_RAISE(int64_t nDecoded, codec->Decompress(lz4Len, in, outputLen, output));
    if (nDecoded != outputLen) {
      return arrow::Status::Invalid(
          "String dict codec: LZ4 decode produced ", nDecoded, " bytes, expected ", outputLen);
    }
    return outputLen;
  }

  if (strategy != kStrategyDict) {
    return arrow::Status::Invalid("String dict codec: unknown strategy ", static_cast<int>(strategy));
  }

  if (remaining < static_cast<int64_t>(sizeof(uint8_t) + 3 * sizeof(int32_t))) {
    return arrow::Status::Invalid("String dict codec: dict strategy header truncated.");
  }
  uint8_t indexWidth = *in++;
  int32_t numRows = 0;
  std::memcpy(&numRows, in, sizeof(int32_t));
  in += sizeof(int32_t);
  int32_t dictCount = 0;
  std::memcpy(&dictCount, in, sizeof(int32_t));
  in += sizeof(int32_t);
  int32_t dictBytes = 0;
  std::memcpy(&dictBytes, in, sizeof(int32_t));
  in += sizeof(int32_t);
  remaining -= sizeof(uint8_t) + 3 * sizeof(int32_t);

  if (indexWidth != 1 && indexWidth != 2 && indexWidth != 4) {
    return arrow::Status::Invalid("String dict codec: invalid indexWidth ", static_cast<int>(indexWidth));
  }
  if (numRows < 0 || dictCount < 0 || dictBytes < 0) {
    return arrow::Status::Invalid("String dict codec: negative counts in header.");
  }

  // Materialize dict entries as (ptr, len) into the input buffer (no copy).
  std::vector<std::pair<const uint8_t*, int32_t>> dictEntries;
  dictEntries.reserve(static_cast<size_t>(dictCount));
  const uint8_t* dictEnd = in + dictBytes;
  if (dictEnd > input + inputLen) {
    return arrow::Status::Invalid("String dict codec: dict section overruns input.");
  }
  for (int32_t i = 0; i < dictCount; ++i) {
    if (in + sizeof(int32_t) > dictEnd) {
      return arrow::Status::Invalid("String dict codec: dict entry header truncated at index ", i);
    }
    int32_t len = 0;
    std::memcpy(&len, in, sizeof(int32_t));
    in += sizeof(int32_t);
    if (len < 0 || in + len > dictEnd) {
      return arrow::Status::Invalid("String dict codec: dict entry ", i, " has invalid length ", len);
    }
    dictEntries.emplace_back(in, len);
    in += len;
  }

  int64_t indicesBytes = static_cast<int64_t>(numRows) * indexWidth;
  if (in + indicesBytes > input + inputLen) {
    return arrow::Status::Invalid("String dict codec: indices section overruns input.");
  }

  uint8_t* dst = output;
  uint8_t* dstEnd = output + outputLen;
  for (int32_t i = 0; i < numRows; ++i) {
    int32_t idx = 0;
    if (indexWidth == 1) {
      idx = *in;
      in += 1;
    } else if (indexWidth == 2) {
      uint16_t v = 0;
      std::memcpy(&v, in, sizeof(uint16_t));
      in += sizeof(uint16_t);
      idx = v;
    } else {
      std::memcpy(&idx, in, sizeof(int32_t));
      in += sizeof(int32_t);
    }
    if (idx < 0 || idx >= dictCount) {
      return arrow::Status::Invalid("String dict codec: index ", idx, " out of range [0, ", dictCount, ")");
    }
    const auto& entry = dictEntries[static_cast<size_t>(idx)];
    if (dst + entry.second > dstEnd) {
      return arrow::Status::Invalid("String dict codec: output overrun at row ", i);
    }
    std::memcpy(dst, entry.first, static_cast<size_t>(entry.second));
    dst += entry.second;
  }

  // The caller's `decompress` wrapper ignores the byte-count return value and
  // assumes the whole `outputLen` window was written.  Reject mismatches here
  // so that any stream describing fewer (or — by way of an earlier overrun
  // check — more) bytes than expected fails loudly instead of leaving the
  // tail of the caller's buffer uninitialised.
  if (dst != dstEnd) {
    return arrow::Status::Invalid("String dict codec: decompressed ", dst - output, " bytes, expected ", outputLen);
  }
  return outputLen;
}

} // namespace gluten

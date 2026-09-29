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

#include "VeloxColumnarBatchSerializer.h"

#include <arrow/buffer.h>

#include <cmath>
#include <cstring>
#include <limits>
#include <optional>
#include <sstream>

#include "memory/ArrowMemory.h"
#include "memory/VeloxColumnarBatch.h"
#include "velox/common/compression/Compression.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/LazyVector.h"
#include "velox/vector/arrow/Bridge.h"

#include <iostream>

using namespace facebook::velox;

namespace gluten {
namespace {

std::unique_ptr<ByteInputStream> toByteStream(uint8_t* data, int32_t size) {
  std::vector<ByteRange> byteRanges;
  byteRanges.push_back(ByteRange{data, size, 0});
  auto byteStream = std::make_unique<BufferInputStream>(byteRanges);
  return byteStream;
}

} // namespace

VeloxColumnarBatchSerializer::VeloxColumnarBatchSerializer(
    arrow::MemoryPool* arrowPool,
    std::shared_ptr<memory::MemoryPool> veloxPool,
    struct ArrowSchema* cSchema,
    const std::string& compressionKind)
    : ColumnarBatchSerializer(arrowPool), veloxPool_(std::move(veloxPool)) {
  // serializeColumnarBatches don't need rowType_
  if (cSchema != nullptr) {
    rowType_ = asRowType(importFromArrow(*cSchema));
    ArrowSchemaRelease(cSchema); // otherwise the c schema leaks memory
  }
  arena_ = std::make_unique<StreamArena>(veloxPool_.get());
  serde_ = std::make_unique<serializer::presto::PrestoVectorSerde>();
  options_.useLosslessTimestamp = true;
  options_.compressionKind = facebook::velox::common::stringToCompressionKind(compressionKind);
  options_.nullsFirst = false;
}

void VeloxColumnarBatchSerializer::append(const std::shared_ptr<ColumnarBatch>& batch) {
  auto rowVector = VeloxColumnarBatch::from(veloxPool_.get(), batch)->getRowVector();
  if (serializer_ == nullptr) {
    // Using first batch's schema to create the Velox serializer. This logic was introduced in
    // https://github.com/apache/gluten/pull/1568. It's a bit suboptimal because the schemas
    // across different batches may vary.
    auto numRows = rowVector->size();
    auto rowType = asRowType(rowVector->type());
    serializer_ = serde_->createIterativeSerializer(rowType, numRows, arena_.get(), &options_);
  }
  const IndexRange allRows{0, rowVector->size()};
  serializer_->append(rowVector, folly::Range(&allRows, 1));
}

int64_t VeloxColumnarBatchSerializer::maxSerializedSize() {
  VELOX_DCHECK(serializer_ != nullptr, "Should serialize at least 1 vector");
  return serializer_->maxSerializedSize();
}

void VeloxColumnarBatchSerializer::serializeTo(uint8_t* address, int64_t size) {
  VELOX_DCHECK(serializer_ != nullptr, "Should serialize at least 1 vector");
  auto sizeNeeded = serializer_->maxSerializedSize();
  GLUTEN_CHECK(
      size >= sizeNeeded,
      "The target buffer size is insufficient: " + std::to_string(size) + " vs." + std::to_string(sizeNeeded));
  std::shared_ptr<arrow::MutableBuffer> valueBuffer = std::make_shared<arrow::MutableBuffer>(address, size);
  auto output = std::make_shared<arrow::io::FixedSizeBufferWriter>(valueBuffer);
  serializer::presto::PrestoOutputStreamListener listener;
  ArrowFixedSizeBufferOutputStream out(output, &listener);
  serializer_->flush(&out);
}

std::shared_ptr<ColumnarBatch> VeloxColumnarBatchSerializer::deserialize(uint8_t* data, int32_t size) {
  RowVectorPtr result;
  auto byteStream = toByteStream(data, size);
  serde_->deserialize(byteStream.get(), veloxPool_.get(), rowType_, &result, &options_);
  return std::make_shared<VeloxColumnarBatch>(result);
}

namespace {

// Per-type FlatVector min/max scan. Returns true (column may carry min/max bounds); the caller
// gates on `seen` to decide whether any non-null value was actually observed.
//
// Floating-point NaN handling -- mirror vanilla Spark Float/DoubleColumnStats.gatherValueStats:
// vanilla updates bounds with `if (value > upper)` / `if (value < lower)`, and since every NaN
// comparison is false, NaN never widens min/max -- vanilla silently SKIPS NaN yet still emits
// the finite min/max. We must do the SAME. Poisoning the whole column to unsupported (the prior
// behavior) instead emits NULL bounds; the vanilla SimpleMetricsCachedBatchSerializer.buildFilter
// then turns `col = l` into `lowerBound <= l && l <= upperBound`, which evaluates to null on null
// bounds -> coerced to false -> `!eval` -> the batch is PRUNED. That silently drops finite rows
// that genuinely match when a NaN happens to share the batch (data loss, and a regression vs
// vanilla). Skipping NaN keeps the finite bounds and matches vanilla exactly. NaN-literal
// predicates (`k = NaN`, `k > huge`) inherit vanilla's existing finite-bound behavior -- that is
// parity with Spark, not a new Gluten divergence.
//
// NaN still counts as a non-null value, so it is NOT added to nullCnt -- only true nulls are,
// keeping Spark IsNull/IsNotNull pruning (`nullCount > 0`, `count - nullCount > 0`) correct.
//
// Floating-point edge cases that produce normal finite/ordered bounds (never skipped):
// - +/-Infinity: ordered (-Inf < x < +Inf for finite x); participate in min/max normally.
// - +0 and -0: IEEE 754 declares them equal under <, ==; min/max bound is correct either way.
// - subnormal (denormal) values: ordered like normal floats; no special handling needed.
template <typename T>
bool scanMinMax(const facebook::velox::FlatVector<T>* flat, T& tLo, T& tHi, int64_t& nullCnt, bool& seen) {
  static_assert(!std::is_same_v<T, bool>, "BOOLEAN must use scanBoolMinMax (FlatVector<bool>::rawValues unsupported)");
  const auto size = flat->size();
  const uint64_t* nulls = flat->rawNulls();
  const T* values = flat->rawValues();
  for (vector_size_t i = 0; i < size; ++i) {
    if (nulls != nullptr && bits::isBitNull(nulls, i)) {
      ++nullCnt;
      continue;
    }
    T v = values[i];
    if constexpr (std::is_floating_point_v<T>) {
      if (std::isnan(v)) {
        // Skip NaN (matches vanilla); it neither widens min/max nor counts as null.
        continue;
      }
    }
    if (!seen) {
      tLo = v;
      tHi = v;
      seen = true;
    } else {
      if (v < tLo)
        tLo = v;
      if (v > tHi)
        tHi = v;
    }
  }
  return true;
}

// BOOLEAN-specific scan: FlatVector<bool>::rawValues() is unsupported in Velox (bit-packed
// storage, no bool* accessor) and throws VeloxRuntimeError. Use valueAt(i)/isNullAt(i)
// instead. Semantics match scanMinMax<T>: track lo/hi (false<true), accrue nullCnt.
inline bool
scanBoolMinMax(const facebook::velox::FlatVector<bool>* flat, bool& tLo, bool& tHi, int64_t& nullCnt, bool& seen) {
  const auto size = flat->size();
  for (vector_size_t i = 0; i < size; ++i) {
    if (flat->isNullAt(i)) {
      ++nullCnt;
      continue;
    }
    bool v = flat->valueAt(i);
    if (!seen) {
      tLo = v;
      tHi = v;
      seen = true;
    } else {
      if (v < tLo)
        tLo = v;
      if (v > tHi)
        tHi = v;
    }
  }
  return true;
}

// Null counter for non-flat encoding (Dictionary / Constant / Complex). Their rawValues path
// is undefined for stats compute; we still must report a real nullCount so JVM-side IsNull
// pruning (`statsFor(a).nullCount > 0`) does not incorrectly skip null-bearing partitions.
inline int64_t countNullsAny(const facebook::velox::BaseVector* vec) {
  const auto size = vec->size();
  int64_t nullCnt = 0;
  for (vector_size_t i = 0; i < size; ++i) {
    if (vec->isNullAt(i)) {
      ++nullCnt;
    }
  }
  return nullCnt;
}

} // namespace

std::vector<ColumnStats> VeloxColumnarBatchSerializer::computeStats(RowVectorPtr rowVector) {
  std::vector<ColumnStats> result;
  const auto numCols = rowVector->childrenSize();
  result.resize(numCols);
  for (column_index_t col = 0; col < numCols; ++col) {
    auto& stats = result[col];
    auto child = rowVector->childAt(col);
    if (child == nullptr) {
      continue;
    }
    if (!child->isFlatEncoding()) {
      // Non-flat (Dictionary / Constant / Complex): min/max stays unsupported, but we MUST
      // still report a real nullCount. Spark IsNull pruning reads `statsFor(a).nullCount > 0`
      // and a default-0 on a null-bearing dict-encoded column would incorrectly prune.
      stats.nullCount = countNullsAny(child.get());
      continue;
    }
    bool seen = false;
    int64_t nullCnt = 0;
    bool supported = false;
    switch (child->typeKind()) {
      case TypeKind::BIGINT: {
        auto* flat = child->asFlatVector<int64_t>();
        int64_t lo = 0, hi = 0;
        supported = scanMinMax<int64_t>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          stats.lowerBound = variant(lo);
          stats.upperBound = variant(hi);
        }
        break;
      }
      case TypeKind::INTEGER: {
        auto* flat = child->asFlatVector<int32_t>();
        int32_t lo = 0, hi = 0;
        supported = scanMinMax<int32_t>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          stats.lowerBound = variant(lo);
          stats.upperBound = variant(hi);
        }
        break;
      }
      case TypeKind::SMALLINT: {
        auto* flat = child->asFlatVector<int16_t>();
        int16_t lo = 0, hi = 0;
        supported = scanMinMax<int16_t>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          stats.lowerBound = variant(lo);
          stats.upperBound = variant(hi);
        }
        break;
      }
      case TypeKind::TINYINT: {
        auto* flat = child->asFlatVector<int8_t>();
        int8_t lo = 0, hi = 0;
        supported = scanMinMax<int8_t>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          stats.lowerBound = variant(lo);
          stats.upperBound = variant(hi);
        }
        break;
      }
      case TypeKind::REAL: {
        auto* flat = child->asFlatVector<float>();
        float lo = 0.f, hi = 0.f;
        supported = scanMinMax<float>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          stats.lowerBound = variant(lo);
          stats.upperBound = variant(hi);
        }
        break;
      }
      case TypeKind::DOUBLE: {
        auto* flat = child->asFlatVector<double>();
        double lo = 0.0, hi = 0.0;
        supported = scanMinMax<double>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          stats.lowerBound = variant(lo);
          stats.upperBound = variant(hi);
        }
        break;
      }
      case TypeKind::BOOLEAN: {
        auto* flat = child->asFlatVector<bool>();
        bool lo = false, hi = false;
        supported = scanBoolMinMax(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          stats.lowerBound = variant(lo);
          stats.upperBound = variant(hi);
        }
        break;
      }
      case TypeKind::HUGEINT: {
        // long-Decimal (precision > 18); marshaled as 16B LE int128 downstream.
        auto* flat = child->asFlatVector<int128_t>();
        int128_t lo = 0, hi = 0;
        supported = scanMinMax<int128_t>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          stats.lowerBound = variant(lo);
          stats.upperBound = variant(hi);
        }
        break;
      }
      case TypeKind::TIMESTAMP: {
        // Velox Timestamp has defaulted operator<=> (Timestamp.h) so scanMinMax compiles via the
        // existing template. Wire emit converts via toMicros() to int64; Spark TimestampType /
        // TimestampNTZType physical = Long microseconds.
        auto* flat = child->asFlatVector<Timestamp>();
        Timestamp lo, hi;
        supported = scanMinMax<Timestamp>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          stats.lowerBound = variant(lo);
          stats.upperBound = variant(hi);
        }
        break;
      }
      case TypeKind::VARCHAR: {
        // StringView::operator<=> uses memcmp -> unsigned byte ordering, matching Spark
        // ByteArray.compareBinary. variant(std::string{sv}) heap-copies so post-computeStats
        // lifetime is decoupled from the RowVector buffer.
        //
        // Truncate to 256B at the source so the JVM never sees > 256B (single source of truth).
        // Lower bound: prefix is byte-wise <= original. Upper bound: prefix +1 carry on the
        // rightmost byte to ensure encoded >= original; carry overflow on an all-0xFF prefix
        // demotes supported=0. Mirrors the JVM-side encodeStringBounds.
        constexpr size_t kStatsStringTruncateLen = 256;
        auto* flat = child->asFlatVector<StringView>();
        StringView lo, hi;
        supported = scanMinMax<StringView>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          const size_t loLen = std::min(static_cast<size_t>(lo.size()), kStatsStringTruncateLen);
          std::string loBytes(lo.data(), loLen);
          const size_t hiSrcLen = static_cast<size_t>(hi.size());
          std::string hiBytes(hi.data(), std::min(hiSrcLen, kStatsStringTruncateLen));
          bool hiOk = true;
          if (hiSrcLen > kStatsStringTruncateLen) {
            bool carryDone = false;
            for (int i = static_cast<int>(hiBytes.size()) - 1; i >= 0; --i) {
              uint8_t b = static_cast<uint8_t>(hiBytes[i]) + 1;
              if (b != 0) {
                hiBytes[i] = static_cast<char>(b);
                carryDone = true;
                break;
              }
              hiBytes[i] = 0;
            }
            hiOk = carryDone;
          }
          if (hiOk) {
            stats.hasLowerBound = true;
            stats.hasUpperBound = true;
            stats.lowerBound = variant(std::move(loBytes));
            stats.upperBound = variant(std::move(hiBytes));
          } else {
            supported = false;
          }
        }
        break;
      }
      default:
        // Mirror non-flat path: real nullCount needed for JVM IsNull pruning.
        nullCnt = countNullsAny(child.get());
        // Unsupported type -> hasLowerBound=hasUpperBound=false -> JVM buildFilter pass-through.
        break;
    }
    stats.nullCount = nullCnt;
  }
  return result;
}

std::vector<uint8_t> VeloxColumnarBatchSerializer::framedSerializeWithStats(
    const std::shared_ptr<ColumnarBatch>& batch) {
  // Compute stats over the inbound rowVector BEFORE delegating to the append path (which may
  // consume / mutate iterator state on subsequent calls).
  auto rowVector = VeloxColumnarBatch::from(veloxPool_.get(), batch)->getRowVector();
  const uint32_t numRows = static_cast<uint32_t>(rowVector->size());
  std::vector<ColumnStats> perCol = computeStats(rowVector);
  const uint32_t numCols = static_cast<uint32_t>(perCol.size());

  // Marshal statsBlob (LE primitives via lambdas).
  std::vector<uint8_t> statsBlob;
  auto pushU8 = [&](uint8_t v) { statsBlob.push_back(v); };
  auto pushU32 = [&](uint32_t v) {
    statsBlob.push_back(static_cast<uint8_t>(v & 0xFF));
    statsBlob.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    statsBlob.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    statsBlob.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
  };
  auto pushU64 = [&](uint64_t v) {
    for (int i = 0; i < 8; ++i) {
      statsBlob.push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xFF));
    }
  };
  auto pushI64LE = [&](int64_t v) { pushU64(static_cast<uint64_t>(v)); };
  auto pushU16LE = [&](uint16_t v) {
    statsBlob.push_back(static_cast<uint8_t>(v & 0xFF));
    statsBlob.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
  };

  pushU32(numCols);
  for (const auto& s : perCol) {
    auto kind = s.lowerBound.kind();
    bool emitSupported = s.hasLowerBound && s.hasUpperBound && s.lowerBound.kind() == s.upperBound.kind() &&
        (kind == facebook::velox::TypeKind::BIGINT || kind == facebook::velox::TypeKind::INTEGER ||
         kind == facebook::velox::TypeKind::SMALLINT || kind == facebook::velox::TypeKind::TINYINT ||
         kind == facebook::velox::TypeKind::HUGEINT || kind == facebook::velox::TypeKind::REAL ||
         kind == facebook::velox::TypeKind::DOUBLE || kind == facebook::velox::TypeKind::BOOLEAN ||
         kind == facebook::velox::TypeKind::TIMESTAMP || kind == facebook::velox::TypeKind::VARCHAR);
    pushU8(emitSupported ? 1 : 0);
    pushU32(static_cast<uint32_t>(s.nullCount));
    // PartitionStatistics.count = numRows (vanilla gatherNullStats increments count for null
    // rows too; subtracting nullCount inverts the IsNotNull predicate).
    pushU32(numRows);
    pushU64(0); // sizeInBytes placeholder
    if (emitSupported) {
      switch (kind) {
        case facebook::velox::TypeKind::BIGINT:
          pushU32(8);
          pushI64LE(s.lowerBound.value<int64_t>());
          pushU32(8);
          pushI64LE(s.upperBound.value<int64_t>());
          break;
        case facebook::velox::TypeKind::INTEGER:
          pushU32(4);
          pushU32(static_cast<uint32_t>(s.lowerBound.value<int32_t>()));
          pushU32(4);
          pushU32(static_cast<uint32_t>(s.upperBound.value<int32_t>()));
          break;
        case facebook::velox::TypeKind::SMALLINT:
          pushU32(2);
          pushU16LE(static_cast<uint16_t>(s.lowerBound.value<int16_t>()));
          pushU32(2);
          pushU16LE(static_cast<uint16_t>(s.upperBound.value<int16_t>()));
          break;
        case facebook::velox::TypeKind::TINYINT:
          pushU32(1);
          pushU8(static_cast<uint8_t>(s.lowerBound.value<int8_t>()));
          pushU32(1);
          pushU8(static_cast<uint8_t>(s.upperBound.value<int8_t>()));
          break;
        case facebook::velox::TypeKind::HUGEINT: {
          // 16 LE bytes: int128 split into low/high uint64 halves, low first. JVM reconstructs
          // via BigInteger from signed two's-complement big-endian byte array (reverse on read).
          auto pushI128LE = [&](int128_t v) {
            pushU64(static_cast<uint64_t>(v));
            pushU64(static_cast<uint64_t>(v >> 64));
          };
          pushU32(16);
          pushI128LE(s.lowerBound.value<int128_t>());
          pushU32(16);
          pushI128LE(s.upperBound.value<int128_t>());
          break;
        }
        case facebook::velox::TypeKind::REAL: {
          uint32_t loBits, hiBits;
          float lo = s.lowerBound.value<float>();
          float hi = s.upperBound.value<float>();
          std::memcpy(&loBits, &lo, sizeof(uint32_t));
          std::memcpy(&hiBits, &hi, sizeof(uint32_t));
          pushU32(4);
          pushU32(loBits);
          pushU32(4);
          pushU32(hiBits);
          break;
        }
        case facebook::velox::TypeKind::DOUBLE: {
          uint64_t loBits, hiBits;
          double lo = s.lowerBound.value<double>();
          double hi = s.upperBound.value<double>();
          std::memcpy(&loBits, &lo, sizeof(uint64_t));
          std::memcpy(&hiBits, &hi, sizeof(uint64_t));
          pushU32(8);
          pushU64(loBits);
          pushU32(8);
          pushU64(hiBits);
          break;
        }
        case facebook::velox::TypeKind::BOOLEAN:
          pushU32(1);
          pushU8(s.lowerBound.value<bool>() ? 1 : 0);
          pushU32(1);
          pushU8(s.upperBound.value<bool>() ? 1 : 0);
          break;
        case facebook::velox::TypeKind::TIMESTAMP: {
          // Spark Timestamp / TimestampNTZ physical = Long microseconds; share the JVM LongType
          // 8B wire arm. Velox Timestamp::toMicros() floors toward -infinity. Floor on lo widens
          // the prune interval downward (conservative) but floor on hi can shrink it and
          // false-negative drop rows whose true ts has nanos % 1000 != 0. Fix: ceil hi by +1us
          // when there is any sub-microsecond residue.
          const auto& loTs = s.lowerBound.value<facebook::velox::Timestamp>();
          const auto& hiTs = s.upperBound.value<facebook::velox::Timestamp>();
          int64_t loMicros = loTs.toMicros();
          int64_t hiMicros = hiTs.toMicros();
          // Ceil hi by +1us on sub-microsecond residue so the upper bound stays a superset.
          // Guard int64 overflow at the max representable instant: a +1 at INT64_MAX would wrap
          // negative and wrongly prune. INT64_MAX micros already bounds any Spark
          // (micros-resolution) value, so no ceil is needed there.
          if (hiTs.getNanos() % 1000 != 0 && hiMicros != std::numeric_limits<int64_t>::max()) {
            hiMicros += 1;
          }
          pushU32(8);
          pushI64LE(loMicros);
          pushU32(8);
          pushI64LE(hiMicros);
          break;
        }
        case facebook::velox::TypeKind::VARCHAR: {
          // Truncation already applied by computeStats (256B + carry); emit raw bytes with
          // u32 LE length prefix. variant.value<VARCHAR>() returns owned std::string&.
          const auto& loStr = s.lowerBound.value<facebook::velox::TypeKind::VARCHAR>();
          const auto& hiStr = s.upperBound.value<facebook::velox::TypeKind::VARCHAR>();
          pushU32(static_cast<uint32_t>(loStr.size()));
          for (auto c : loStr) {
            pushU8(static_cast<uint8_t>(c));
          }
          pushU32(static_cast<uint32_t>(hiStr.size()));
          for (auto c : hiStr) {
            pushU8(static_cast<uint8_t>(c));
          }
          break;
        }
        default:
          break;
      }
    }
  }
  const uint32_t statsLen = static_cast<uint32_t>(statsBlob.size());

  // Produce bytesBlob via the existing serializer path.
  append(batch);
  const int64_t bytesLen = maxSerializedSize();
  std::vector<uint8_t> bytesBlob(bytesLen);
  serializeTo(bytesBlob.data(), bytesLen);

  // Assemble: [magic(4) | statsLen(u32 LE) | statsBlob | bytesLen(u32 LE) | bytesBlob].
  std::vector<uint8_t> framed;
  framed.reserve(4 + 4 + statsLen + 4 + bytesLen);
  framed.push_back(0xFE);
  framed.push_back(0xCA);
  framed.push_back(0x53);
  framed.push_back(0x02);
  framed.push_back(static_cast<uint8_t>(statsLen & 0xFF));
  framed.push_back(static_cast<uint8_t>((statsLen >> 8) & 0xFF));
  framed.push_back(static_cast<uint8_t>((statsLen >> 16) & 0xFF));
  framed.push_back(static_cast<uint8_t>((statsLen >> 24) & 0xFF));
  framed.insert(framed.end(), statsBlob.begin(), statsBlob.end());
  // Wire framing encodes bytesLen as u32 LE, so reject any single-batch payload that would
  // overflow that field (>4GB). Pathological in practice (very wide schemas / huge string
  // columns); fail fast here rather than silently truncate and corrupt the JVM-side parser.
  GLUTEN_CHECK(
      bytesLen >= 0 && bytesLen <= static_cast<int64_t>(std::numeric_limits<uint32_t>::max()),
      "Serialized batch size (" + std::to_string(bytesLen) + ") exceeds u32 framing limit");
  const uint32_t bytesLen32 = static_cast<uint32_t>(bytesLen);
  framed.push_back(static_cast<uint8_t>(bytesLen32 & 0xFF));
  framed.push_back(static_cast<uint8_t>((bytesLen32 >> 8) & 0xFF));
  framed.push_back(static_cast<uint8_t>((bytesLen32 >> 16) & 0xFF));
  framed.push_back(static_cast<uint8_t>((bytesLen32 >> 24) & 0xFF));
  framed.insert(framed.end(), bytesBlob.begin(), bytesBlob.end());
  return framed;
}

// ─── V3: per-column serialization ────────────────────────────────────────────

namespace {

// Lazy column loader backed by a per-column Presto-format byte slice.
// Implements VectorLoader::loadInternal by calling deserializeSingleColumn.
// Lifecycle: colBytes_ is cleared after the first load to release memory (avoid double-buffer).
class CachedColumnLoader : public facebook::velox::VectorLoader {
 public:
  CachedColumnLoader(
      std::vector<uint8_t> colBytes,
      facebook::velox::TypePtr type,
      facebook::velox::vector_size_t expectedRows,
      std::shared_ptr<facebook::velox::memory::MemoryPool> pool,
      const facebook::velox::serializer::presto::PrestoVectorSerde::PrestoOptions& options)
      : colBytes_(std::move(colBytes)),
        type_(std::move(type)),
        expectedRows_(expectedRows),
        pool_(std::move(pool)),
        options_(options) {
    GLUTEN_CHECK(!colBytes_.empty(), "CachedColumnLoader: colBytes must not be empty");
    GLUTEN_CHECK(
        colBytes_.size() <= static_cast<size_t>(std::numeric_limits<int32_t>::max()),
        "CachedColumnLoader: colBytes size exceeds int32 ByteRange limit");
  }

 protected:
  void loadInternal(
      facebook::velox::RowSet /* rows */,
      facebook::velox::ValueHook* /* hook */,
      facebook::velox::vector_size_t /* resultSize */,
      facebook::velox::VectorPtr* result) override {
    // Guard against double-invocation (colBytes_ cleared after first load).
    GLUTEN_CHECK(!colBytes_.empty(), "CachedColumnLoader::loadInternal: called after bytes already consumed");
    std::vector<facebook::velox::ByteRange> ranges;
    ranges.push_back({const_cast<uint8_t*>(colBytes_.data()), static_cast<int32_t>(colBytes_.size()), 0});
    auto stream = std::make_unique<facebook::velox::BufferInputStream>(ranges);
    facebook::velox::serializer::presto::PrestoVectorSerde serde;
    serde.deserializeSingleColumn(stream.get(), pool_.get(), type_, result, &options_);
    GLUTEN_CHECK(
        result != nullptr && *result != nullptr,
        "CachedColumnLoader::loadInternal: deserializeSingleColumn returned null result");
    GLUTEN_CHECK(
        (*result)->size() == expectedRows_,
        "CachedColumnLoader::loadInternal: decoded column size=" + std::to_string((*result)->size()) +
            " != V3 frame numRows=" + std::to_string(expectedRows_));
    // Free raw bytes after decode to avoid holding two copies simultaneously.
    std::vector<uint8_t>().swap(colBytes_);
  }

 private:
  std::vector<uint8_t> colBytes_;
  facebook::velox::TypePtr type_;
  facebook::velox::vector_size_t expectedRows_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  facebook::velox::serializer::presto::PrestoVectorSerde::PrestoOptions options_;
};

} // namespace

std::vector<uint8_t> VeloxColumnarBatchSerializer::buildStatsBlob(
    const std::vector<ColumnStats>& perCol,
    uint32_t numRows,
    uint32_t numCols) {
  std::vector<uint8_t> statsBlob;
  auto pushU8 = [&](uint8_t v) { statsBlob.push_back(v); };
  auto pushU32 = [&](uint32_t v) {
    statsBlob.push_back(static_cast<uint8_t>(v & 0xFF));
    statsBlob.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    statsBlob.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    statsBlob.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
  };
  auto pushU64 = [&](uint64_t v) {
    for (int i = 0; i < 8; ++i) {
      statsBlob.push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xFF));
    }
  };
  auto pushI64LE = [&](int64_t v) { pushU64(static_cast<uint64_t>(v)); };
  auto pushU16LE = [&](uint16_t v) {
    statsBlob.push_back(static_cast<uint8_t>(v & 0xFF));
    statsBlob.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
  };

  pushU32(numCols);
  for (const auto& s : perCol) {
    auto kind = s.lowerBound.kind();
    bool emitSupported = s.hasLowerBound && s.hasUpperBound && s.lowerBound.kind() == s.upperBound.kind() &&
        (kind == facebook::velox::TypeKind::BIGINT || kind == facebook::velox::TypeKind::INTEGER ||
         kind == facebook::velox::TypeKind::SMALLINT || kind == facebook::velox::TypeKind::TINYINT ||
         kind == facebook::velox::TypeKind::HUGEINT || kind == facebook::velox::TypeKind::REAL ||
         kind == facebook::velox::TypeKind::DOUBLE || kind == facebook::velox::TypeKind::BOOLEAN ||
         kind == facebook::velox::TypeKind::TIMESTAMP || kind == facebook::velox::TypeKind::VARCHAR);
    pushU8(emitSupported ? 1 : 0);
    pushU32(static_cast<uint32_t>(s.nullCount));
    pushU32(numRows);
    pushU64(0); // sizeInBytes placeholder
    if (emitSupported) {
      switch (kind) {
        case facebook::velox::TypeKind::BIGINT:
          pushU32(8);
          pushI64LE(s.lowerBound.value<int64_t>());
          pushU32(8);
          pushI64LE(s.upperBound.value<int64_t>());
          break;
        case facebook::velox::TypeKind::INTEGER:
          pushU32(4);
          pushU32(static_cast<uint32_t>(s.lowerBound.value<int32_t>()));
          pushU32(4);
          pushU32(static_cast<uint32_t>(s.upperBound.value<int32_t>()));
          break;
        case facebook::velox::TypeKind::SMALLINT:
          pushU32(2);
          pushU16LE(static_cast<uint16_t>(s.lowerBound.value<int16_t>()));
          pushU32(2);
          pushU16LE(static_cast<uint16_t>(s.upperBound.value<int16_t>()));
          break;
        case facebook::velox::TypeKind::TINYINT:
          pushU32(1);
          pushU8(static_cast<uint8_t>(s.lowerBound.value<int8_t>()));
          pushU32(1);
          pushU8(static_cast<uint8_t>(s.upperBound.value<int8_t>()));
          break;
        case facebook::velox::TypeKind::HUGEINT: {
          auto pushI128LE = [&](int128_t v) {
            pushU64(static_cast<uint64_t>(v));
            pushU64(static_cast<uint64_t>(v >> 64));
          };
          pushU32(16);
          pushI128LE(s.lowerBound.value<int128_t>());
          pushU32(16);
          pushI128LE(s.upperBound.value<int128_t>());
          break;
        }
        case facebook::velox::TypeKind::REAL: {
          uint32_t loBits, hiBits;
          float lo = s.lowerBound.value<float>(), hi = s.upperBound.value<float>();
          std::memcpy(&loBits, &lo, sizeof(uint32_t));
          std::memcpy(&hiBits, &hi, sizeof(uint32_t));
          pushU32(4);
          pushU32(loBits);
          pushU32(4);
          pushU32(hiBits);
          break;
        }
        case facebook::velox::TypeKind::DOUBLE: {
          uint64_t loBits, hiBits;
          double lo = s.lowerBound.value<double>(), hi = s.upperBound.value<double>();
          std::memcpy(&loBits, &lo, sizeof(uint64_t));
          std::memcpy(&hiBits, &hi, sizeof(uint64_t));
          pushU32(8);
          pushU64(loBits);
          pushU32(8);
          pushU64(hiBits);
          break;
        }
        case facebook::velox::TypeKind::BOOLEAN:
          pushU32(1);
          pushU8(s.lowerBound.value<bool>() ? 1 : 0);
          pushU32(1);
          pushU8(s.upperBound.value<bool>() ? 1 : 0);
          break;
        case facebook::velox::TypeKind::TIMESTAMP: {
          const auto& loTs = s.lowerBound.value<facebook::velox::Timestamp>();
          const auto& hiTs = s.upperBound.value<facebook::velox::Timestamp>();
          int64_t loMicros = loTs.toMicros();
          int64_t hiMicros = hiTs.toMicros();
          // See framedSerializeWithStats: guard the +1us ceil against int64 overflow at INT64_MAX.
          if (hiTs.getNanos() % 1000 != 0 && hiMicros != std::numeric_limits<int64_t>::max())
            hiMicros += 1;
          pushU32(8);
          pushI64LE(loMicros);
          pushU32(8);
          pushI64LE(hiMicros);
          break;
        }
        case facebook::velox::TypeKind::VARCHAR: {
          const auto& loStr = s.lowerBound.value<facebook::velox::TypeKind::VARCHAR>();
          const auto& hiStr = s.upperBound.value<facebook::velox::TypeKind::VARCHAR>();
          pushU32(static_cast<uint32_t>(loStr.size()));
          for (auto c : loStr)
            pushU8(static_cast<uint8_t>(c));
          pushU32(static_cast<uint32_t>(hiStr.size()));
          for (auto c : hiStr)
            pushU8(static_cast<uint8_t>(c));
          break;
        }
        default:
          break;
      }
    }
  }
  return statsBlob;
}

std::vector<uint8_t> VeloxColumnarBatchSerializer::framedSerializeV3(const std::shared_ptr<ColumnarBatch>& batch) {
  return framedSerializeV3Impl(batch, false);
}

std::vector<uint8_t> VeloxColumnarBatchSerializer::framedSerializeWithStatsV3(
    const std::shared_ptr<ColumnarBatch>& batch) {
  return framedSerializeV3Impl(batch, true);
}

std::vector<uint8_t> VeloxColumnarBatchSerializer::framedSerializeV3Impl(
    const std::shared_ptr<ColumnarBatch>& batch,
    bool includeStats) {
  // Use getFlattenedRowVector() to force-load lazy children and flatten
  // DictionaryVector / ConstantVector encodings before serializeSingleColumn.
  // This can increase V3 cache bytes for dictionary-heavy inputs, but keeps the
  // per-column payload readable by PrestoVectorSerde.
  auto vb = VeloxColumnarBatch::from(veloxPool_.get(), batch);
  auto rowVector = vb->getFlattenedRowVector();
  GLUTEN_CHECK(
      rowVector->size() >= 0 && static_cast<uint64_t>(rowVector->size()) <= std::numeric_limits<uint32_t>::max(),
      "V3 row count exceeds u32 frame limit: " + std::to_string(rowVector->size()));
  GLUTEN_CHECK(
      rowVector->childrenSize() <= std::numeric_limits<uint32_t>::max(),
      "V3 column count exceeds u32 frame limit: " + std::to_string(rowVector->childrenSize()));
  const uint32_t numRows = static_cast<uint32_t>(rowVector->size());
  const uint32_t numCols = static_cast<uint32_t>(rowVector->childrenSize());

  // 1. Optionally compute stats. Lazy materialization itself is a base V3
  // capability; partition stats are an optional payload used only for pruning.
  std::vector<uint8_t> statsBlob;
  if (includeStats) {
    std::vector<ColumnStats> perCol = computeStats(rowVector);
    statsBlob = buildStatsBlob(perCol, numRows, numCols);
    GLUTEN_CHECK(
        statsBlob.size() <= static_cast<size_t>(std::numeric_limits<int32_t>::max()),
        "V3 stats blob size exceeds Java byte array limit");
  }

  // 2. Serialize each column independently using serializeSingleColumn.
  // options_ must have compressionKind=NONE and nullsFirst=false (checked by Velox).
  facebook::velox::serializer::presto::PrestoVectorSerde localSerde;
  auto pushU32LE = [](std::vector<uint8_t>& buf, uint32_t v) {
    buf.push_back(static_cast<uint8_t>(v & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
  };

  std::vector<std::vector<uint8_t>> colBytesList(numCols);
  for (uint32_t col = 0; col < numCols; ++col) {
    std::ostringstream colStream;
    localSerde.serializeSingleColumn(rowVector->childAt(col), &options_, veloxPool_.get(), &colStream);
    const std::string colStr = colStream.str();
    GLUTEN_CHECK(
        colStr.size() <= static_cast<size_t>(std::numeric_limits<uint32_t>::max()),
        "V3 column " + std::to_string(col) + " size exceeds u32 limit");
    GLUTEN_CHECK(
        colStr.size() <= static_cast<size_t>(std::numeric_limits<int32_t>::max()),
        "V3 column " + std::to_string(col) + " size exceeds int32 ByteRange limit");
    colBytesList[col] = std::vector<uint8_t>(colStr.begin(), colStr.end());
  }

  // 3. Assemble V3 framed format.
  // [magic=0x03(4B)][statsLen(4B)][statsBlob][numRows(4B)][numCols(4B)][per-col: colLen+colBytes]
  std::vector<uint8_t> framed;
  const uint32_t statsLen = static_cast<uint32_t>(statsBlob.size());
  // Capacity hint only (does not affect emitted bytes). Compute in size_t so the
  // per-column length-prefix term cannot wrap for pathological column counts.
  framed.reserve(static_cast<size_t>(4) + 4 + statsLen + 4 + 4 + static_cast<size_t>(numCols) * 4);
  // V3 magic
  framed.push_back(0xFE);
  framed.push_back(0xCA);
  framed.push_back(0x53);
  framed.push_back(0x03);
  pushU32LE(framed, statsLen);
  framed.insert(framed.end(), statsBlob.begin(), statsBlob.end());
  pushU32LE(framed, numRows);
  pushU32LE(framed, numCols);
  for (uint32_t col = 0; col < numCols; ++col) {
    const auto& cb = colBytesList[col];
    pushU32LE(framed, static_cast<uint32_t>(cb.size()));
    framed.insert(framed.end(), cb.begin(), cb.end());
  }
  GLUTEN_CHECK(
      framed.size() <= static_cast<size_t>(std::numeric_limits<int32_t>::max()),
      "V3 framed payload size exceeds Java byte array limit");
  return framed;
}

std::shared_ptr<ColumnarBatch> VeloxColumnarBatchSerializer::deserializeV3(
    uint8_t* data,
    int32_t size,
    const std::optional<std::vector<int32_t>>& requestedColumns) {
  // Local helpers.
  auto readU32LE = [](const uint8_t*& p) -> uint32_t {
    uint32_t v = static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
        (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
    p += 4;
    return v;
  };

  GLUTEN_CHECK(size >= 0, "V3 frame size must be non-negative");
  const uint8_t* p = data;
  const uint8_t* end = data + size;
  auto remaining = [&]() -> size_t { return static_cast<size_t>(end - p); };
  auto requireRemaining = [&](size_t n, const std::string& message) { GLUTEN_CHECK(remaining() >= n, message); };

  // 1. Validate V3 magic.
  GLUTEN_CHECK(size >= 4, "V3 frame too short for magic");
  GLUTEN_CHECK(
      p[0] == 0xFE && p[1] == 0xCA && p[2] == 0x53 && p[3] == 0x03,
      "deserializeV3: magic mismatch (expected V3=0x03, got 0x" + std::to_string(p[3] & 0xFF) + ")");
  p += 4;

  // 2. Skip statsBlob (parsed by JVM side).
  requireRemaining(4, "V3 frame truncated before statsLen");
  uint32_t statsLen = readU32LE(p);
  requireRemaining(statsLen, "V3 frame statsBlob truncated");
  p += statsLen;

  // 3. Read numRows and numCols.
  requireRemaining(8, "V3 frame truncated before numRows/numCols");
  uint32_t numRows = readU32LE(p);
  uint32_t numCols = readU32LE(p);
  GLUTEN_CHECK(
      numRows <= static_cast<uint32_t>(std::numeric_limits<facebook::velox::vector_size_t>::max()),
      "V3 frame numRows exceeds Velox vector_size_t max: " + std::to_string(numRows));
  const auto expectedRows = static_cast<facebook::velox::vector_size_t>(numRows);

  GLUTEN_CHECK(rowType_ != nullptr, "deserializeV3: rowType_ not initialized");
  GLUTEN_CHECK(
      rowType_->size() == numCols,
      "V3 frame numCols=" + std::to_string(numCols) + " != schema size=" + std::to_string(rowType_->size()));

  // 4. Read per-column byte ranges; requested columns are copied before loaders are built.
  struct ColRange {
    const uint8_t* start;
    uint32_t len;
  };
  std::vector<ColRange> colRanges(numCols);
  for (uint32_t col = 0; col < numCols; ++col) {
    requireRemaining(4, "V3 frame truncated at colLen col=" + std::to_string(col));
    uint32_t colLen = readU32LE(p);
    requireRemaining(colLen, "V3 frame colBytes truncated at col=" + std::to_string(col));
    colRanges[col] = {p, colLen};
    p += colLen;
  }
  GLUTEN_CHECK(
      p == end,
      "V3 frame has trailing bytes after column payloads: trailing=" + std::to_string(static_cast<size_t>(end - p)));

  // 5. Determine requested columns.
  const bool loadAll = !requestedColumns.has_value();
  // Use value (not reference) to avoid binding a const-ref to a temporary (C++ UB when loadAll=true).
  const std::vector<int32_t> reqVec = loadAll ? std::vector<int32_t>{} : requestedColumns.value();
  const uint32_t M = loadAll ? numCols : static_cast<uint32_t>(reqVec.size());

  // 6. Build M-column subset RowVector with LazyVector children.
  std::vector<std::string> subNames(M);
  std::vector<facebook::velox::TypePtr> subTypes(M);
  std::vector<facebook::velox::VectorPtr> subChildren(M);

  const uint32_t i_limit = M;
  for (uint32_t i = 0; i < i_limit; ++i) {
    const int32_t col = loadAll ? static_cast<int32_t>(i) : reqVec[i];
    GLUTEN_CHECK(
        col >= 0 && static_cast<uint32_t>(col) < numCols,
        "deserializeV3: requestedColumn " + std::to_string(col) + " out of range [0," + std::to_string(numCols) + ")");

    const auto& range = colRanges[col];
    auto colType = rowType_->childAt(col);

    if (range.len == 0 && numRows == 0) {
      // Truly empty batch (0 rows): any column type is safely represented as null constant.
      subChildren[i] = facebook::velox::BaseVector::createNullConstant(colType, 0, veloxPool_.get());
    } else if (range.len == 0) {
      // numRows > 0 but colLen == 0: this is malformed — serializeSingleColumn always emits at
      // least a column header. Treat as a serialization bug; surface clearly rather than silently
      // substituting wrong-type null data.
      GLUTEN_CHECK(
          false,
          "V3 deserializeV3: col=" + std::to_string(col) + " colLen=0 but numRows=" + std::to_string(numRows) +
              " (malformed V3 frame; serializeSingleColumn never emits 0 bytes for non-empty batch)");
    } else {
      // Copy bytes while JNI pin is still valid.
      std::vector<uint8_t> colBytes(range.start, range.start + range.len);
      auto loader =
          std::make_unique<CachedColumnLoader>(std::move(colBytes), colType, expectedRows, veloxPool_, options_);
      subChildren[i] =
          std::make_shared<facebook::velox::LazyVector>(veloxPool_.get(), colType, expectedRows, std::move(loader));
    }
    subTypes[i] = colType;
    subNames[i] = rowType_->nameOf(col);
  }

  // 7. Construct M-column RowVector.
  auto subRowType = facebook::velox::ROW(std::move(subNames), std::move(subTypes));
  auto result = std::make_shared<facebook::velox::RowVector>(
      veloxPool_.get(), subRowType, facebook::velox::BufferPtr(nullptr), expectedRows, std::move(subChildren));

  return std::make_shared<VeloxColumnarBatch>(result);
}

} // namespace gluten

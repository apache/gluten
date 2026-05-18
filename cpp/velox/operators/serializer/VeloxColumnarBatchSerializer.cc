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

#include "memory/ArrowMemory.h"
#include "memory/VeloxColumnarBatch.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/FlatVector.h"
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
    struct ArrowSchema* cSchema)
    : ColumnarBatchSerializer(arrowPool), veloxPool_(std::move(veloxPool)) {
  // serializeColumnarBatches don't need rowType_
  if (cSchema != nullptr) {
    rowType_ = asRowType(importFromArrow(*cSchema));
    ArrowSchemaRelease(cSchema); // otherwise the c schema leaks memory
  }
  arena_ = std::make_unique<StreamArena>(veloxPool_.get());
  serde_ = std::make_unique<serializer::presto::PrestoVectorSerde>();
  options_.useLosslessTimestamp = true;
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

// Per-type FlatVector min/max scan + NaN guard. Returns false when the column must be marked
// unsupported (any NaN observed for floating-point types -- Spark equality NaN != NaN means
// min/max-based pruning would silently drop matching rows). On NaN, scan still completes the
// loop to accrue real nullCnt -- framed stats serialize nullCount even when emitSupported=0,
// and Spark IsNull pruning reads `statsFor(a).nullCount > 0`; an under-counted nullCount on a
// `[NaN, null]` partition would let `col IS NULL` predicates incorrectly prune matching rows.
//
// Floating-point edge cases that DO NOT poison the column:
// - +/-Infinity: ordered (-Inf < x < +Inf for finite x); participate in min/max normally.
// - +0 and -0: IEEE 754 declares them equal under <, ==; min/max bound is correct either way.
// - subnormal (denormal) values: ordered like normal floats; no special handling needed.
template <typename T>
bool scanMinMax(const facebook::velox::FlatVector<T>* flat, T& tLo, T& tHi, int64_t& nullCnt, bool& seen) {
  static_assert(!std::is_same_v<T, bool>, "BOOLEAN must use scanBoolMinMax (FlatVector<bool>::rawValues unsupported)");
  const auto size = flat->size();
  const uint64_t* nulls = flat->rawNulls();
  const T* values = flat->rawValues();
  bool floatingUnsupported = false;
  for (vector_size_t i = 0; i < size; ++i) {
    if (nulls != nullptr && bits::isBitNull(nulls, i)) {
      ++nullCnt;
      continue;
    }
    T v = values[i];
    if constexpr (std::is_floating_point_v<T>) {
      if (std::isnan(v)) {
        floatingUnsupported = true;
        // Continue scanning to accrue real nullCnt -- do NOT early-return.
        continue;
      }
    }
    if (floatingUnsupported) {
      // NaN already poisoned min/max; skip bound updates but keep counting (nulls handled above).
      continue;
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
  return !floatingUnsupported;
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
          if (hiTs.getNanos() % 1000 != 0) {
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

} // namespace gluten

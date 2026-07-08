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

#include <gtest/gtest.h>

#include "../utils/VeloxArrowUtils.h"
#include "compute/VeloxBackend.h"
#include "memory/ArrowMemoryPool.h"
#include "memory/VeloxColumnarBatch.h"
#include "operators/serializer/VeloxColumnarBatchSerializer.h"

#include "velox/vector/LazyVector.h"
#include "velox/vector/arrow/Bridge.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <arrow/buffer.h>

#include <limits>

using namespace facebook::velox;

namespace gluten {

class VeloxColumnarBatchSerializerTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    VeloxBackend::create(AllocationListener::noop(), {});
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  static void TearDownTestSuite() {
    VeloxBackend::get()->tearDown();
  }

  std::shared_ptr<VeloxColumnarBatchSerializer> makeV3Deserializer(arrow::MemoryPool* arrowPool) {
    auto schemaVector = makeRowVector(
        {"a", "b", "c"},
        {makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
         makeFlatVector<int64_t>({10, 20, 30, 40, 50}),
         makeFlatVector<StringView>({"a", "bb", "ccc", "dddd", "eeeee"})});
    ArrowSchema cSchema;
    exportToArrow(schemaVector, cSchema, ArrowUtils::getBridgeOptions());
    return std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, &cSchema);
  }
};

TEST_F(VeloxColumnarBatchSerializerTest, serialize) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();

  std::vector<VectorPtr> children = {
      makeNullableFlatVector<int8_t>({1, 2, 3, std::nullopt, 4}),
      makeNullableFlatVector<int8_t>({1, -1, std::nullopt, std::nullopt, -2}),
      makeNullableFlatVector<int32_t>({1, 2, 3, 4, std::nullopt}),
      makeNullableFlatVector<int64_t>({std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt}),
      makeNullableFlatVector<float>({-0.1234567, std::nullopt, 0.1234567, std::nullopt, -0.142857}),
      makeNullableFlatVector<bool>({std::nullopt, true, false, std::nullopt, true}),
      makeFlatVector<StringView>({"alice0", "bob1", "alice2", "bob3", "Alice4uuudeuhdhfudhfudhfudhbvudubvudfvu"}),
      makeNullableFlatVector<StringView>({"alice", "bob", std::nullopt, std::nullopt, "Alice"}),
      makeNullableFlatVector<int64_t>({34567235, 4567, 222, 34567, 333}, DECIMAL(12, 4)),
      makeNullableFlatVector<int128_t>({34567235, 4567, 222, 34567, 333}, DECIMAL(20, 4)),
  };
  auto vector = makeRowVector(children);
  auto batch = std::make_shared<VeloxColumnarBatch>(vector);
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);
  serializer->append(batch);
  std::shared_ptr<arrow::Buffer> buffer;
  GLUTEN_ASSIGN_OR_THROW(buffer, arrow::AllocateResizableBuffer(serializer->maxSerializedSize(), arrowPool));
  serializer->serializeTo(reinterpret_cast<uint8_t*>(buffer->mutable_address()), buffer->size());

  ArrowSchema cSchema;
  exportToArrow(vector, cSchema, ArrowUtils::getBridgeOptions());
  auto deserializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, &cSchema);
  auto deserialized = deserializer->deserialize(const_cast<uint8_t*>(buffer->data()), buffer->size());
  auto deserializedVector = std::dynamic_pointer_cast<VeloxColumnarBatch>(deserialized)->getRowVector();
  test::assertEqualVectors(vector, deserializedVector);
}

// BIGINT FlatVector min/max scan: values [42, 7, 99, -3, 50] -> lo=-3, hi=99.
TEST_F(VeloxColumnarBatchSerializerTest, computeStatsBigintFlatVector) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();

  std::vector<VectorPtr> children = {
      makeFlatVector<int64_t>({42, 7, 99, -3, 50}),
  };
  auto vector = makeRowVector(children);
  auto batch = std::make_shared<VeloxColumnarBatch>(vector);
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  auto stats = serializer->computeStats(vector);

  ASSERT_EQ(stats.size(), 1u);
  EXPECT_TRUE(stats[0].hasLowerBound);
  EXPECT_TRUE(stats[0].hasUpperBound);
  EXPECT_EQ(stats[0].lowerBound.value<int64_t>(), -3);
  EXPECT_EQ(stats[0].upperBound.value<int64_t>(), 99);
  EXPECT_EQ(stats[0].nullCount, 0);
}

// REAL FlatVector: a no-NaN partition is supported; a partition that mixes NaN with finite
// values stays supported and emits the FINITE min/max (NaN skipped) -- matching vanilla Spark
// Float/DoubleColumnStats, which ignores NaN and keeps finite bounds. Poisoning to
// hasLowerBound=hasUpperBound=false here would emit null bounds and let the vanilla buildFilter
// silently prune finite matching rows (data loss). (Spark NaN != NaN, would silently
// drop matching rows under min/max pruning).
TEST_F(VeloxColumnarBatchSerializerTest, computeStatsNaNRealSkippedFiniteBoundsEmitted) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  // (a) REAL FlatVector NO NaN -- min/max well-defined, should be supported.
  {
    std::vector<VectorPtr> children = {
        makeFlatVector<float>({1.5f, 2.5f, 0.5f, 3.5f}),
    };
    auto vector = makeRowVector(children);
    auto stats = serializer->computeStats(vector);
    ASSERT_EQ(stats.size(), 1u);
    EXPECT_TRUE(stats[0].hasLowerBound) << "REAL FlatVector w/o NaN must be supported";
    EXPECT_TRUE(stats[0].hasUpperBound);
    EXPECT_EQ(stats[0].lowerBound.value<float>(), 0.5f);
    EXPECT_EQ(stats[0].upperBound.value<float>(), 3.5f);
  }

  // (b) REAL FlatVector mixing NaN with finite values -- NaN is skipped (matches vanilla Spark),
  // the column stays supported, and the FINITE [1.5, 3.5] bounds are emitted. Emitting null
  // bounds here would let the vanilla buildFilter prune the batch and drop the finite rows.
  {
    const float nan = std::numeric_limits<float>::quiet_NaN();
    std::vector<VectorPtr> children = {
        makeFlatVector<float>({1.5f, nan, 3.5f}),
    };
    auto vector = makeRowVector(children);
    auto stats = serializer->computeStats(vector);
    ASSERT_EQ(stats.size(), 1u);
    EXPECT_TRUE(stats[0].hasLowerBound) << "NaN must be skipped, finite bounds still emitted (no poison)";
    EXPECT_TRUE(stats[0].hasUpperBound);
    EXPECT_EQ(stats[0].lowerBound.value<float>(), 1.5f);
    EXPECT_EQ(stats[0].upperBound.value<float>(), 3.5f);
  }

  // (c) REAL FlatVector that is ALL NaN -- no finite value observed, so no bounds are emitted
  // (hasLowerBound=false). With null bounds the JVM stats row stays unsupported for this column;
  // there are no finite values that could be wrongly pruned.
  {
    const float nan = std::numeric_limits<float>::quiet_NaN();
    std::vector<VectorPtr> children = {
        makeFlatVector<float>({nan, nan, nan}),
    };
    auto vector = makeRowVector(children);
    auto stats = serializer->computeStats(vector);
    ASSERT_EQ(stats.size(), 1u);
    EXPECT_FALSE(stats[0].hasLowerBound) << "all-NaN column observes no finite value -> no bounds";
    EXPECT_FALSE(stats[0].hasUpperBound);
  }
}

// Float / double boundary values (+/-Inf, +/-0, subnormal) must NOT poison the column the way
// NaN does -- they are ordered under IEEE 754 < and so produce well-defined min/max bounds.
TEST_F(VeloxColumnarBatchSerializerTest, computeStatsFloatBoundaryValuesNotPoisoned) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  const float negInf = -std::numeric_limits<float>::infinity();
  const float posInf = std::numeric_limits<float>::infinity();
  const float subnormal = std::numeric_limits<float>::denorm_min();

  // (a) REAL: -Inf / +Inf / +0 / -0 / subnormal mixed -- must be supported with [-Inf, +Inf].
  {
    std::vector<VectorPtr> children = {
        makeFlatVector<float>({negInf, +0.0f, -0.0f, subnormal, posInf}),
    };
    auto vector = makeRowVector(children);
    auto stats = serializer->computeStats(vector);
    ASSERT_EQ(stats.size(), 1u);
    EXPECT_TRUE(stats[0].hasLowerBound) << "boundary-value REAL column must be supported";
    EXPECT_TRUE(stats[0].hasUpperBound);
    EXPECT_EQ(stats[0].lowerBound.value<float>(), negInf);
    EXPECT_EQ(stats[0].upperBound.value<float>(), posInf);
  }

  // (b) DOUBLE: same set, exercise the 8-byte path.
  {
    const double dNegInf = -std::numeric_limits<double>::infinity();
    const double dPosInf = std::numeric_limits<double>::infinity();
    const double dSubnormal = std::numeric_limits<double>::denorm_min();
    std::vector<VectorPtr> children = {
        makeFlatVector<double>({dNegInf, +0.0, -0.0, dSubnormal, dPosInf}),
    };
    auto vector = makeRowVector(children);
    auto stats = serializer->computeStats(vector);
    ASSERT_EQ(stats.size(), 1u);
    EXPECT_TRUE(stats[0].hasLowerBound) << "boundary-value DOUBLE column must be supported";
    EXPECT_TRUE(stats[0].hasUpperBound);
    EXPECT_EQ(stats[0].lowerBound.value<double>(), dNegInf);
    EXPECT_EQ(stats[0].upperBound.value<double>(), dPosInf);
  }
}

// HUGEINT (int128) FlatVector for LongDecimal(20, 4): values [100, -50, 9999]
// -> lo=-50, hi=9999.
TEST_F(VeloxColumnarBatchSerializerTest, computeStatsHugeintDecimalFlatVector) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  std::vector<VectorPtr> children = {
      makeFlatVector<int128_t>(
          {static_cast<int128_t>(100), static_cast<int128_t>(-50), static_cast<int128_t>(9999)}, DECIMAL(20, 4)),
  };
  auto vector = makeRowVector(children);
  auto stats = serializer->computeStats(vector);
  ASSERT_EQ(stats.size(), 1u);
  EXPECT_TRUE(stats[0].hasLowerBound) << "HUGEINT (long Decimal P>=19) supported via 16B LE marshal";
  EXPECT_TRUE(stats[0].hasUpperBound);
  EXPECT_EQ(stats[0].lowerBound.value<int128_t>(), static_cast<int128_t>(-50));
  EXPECT_EQ(stats[0].upperBound.value<int128_t>(), static_cast<int128_t>(9999));
}

// framedSerializeWithStats top-level layout:
//   [ magic(4) = 0xFE 0xCA 0x53 0x02 | statsLen(u32 LE) | statsBlob | bytesLen(u32 LE) | bytesBlob ]
TEST_F(VeloxColumnarBatchSerializerTest, framedSerializeWithStatsTopLayout) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  std::vector<VectorPtr> children = {
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
  };
  auto vector = makeRowVector(children);
  auto batch = std::make_shared<VeloxColumnarBatch>(vector);

  std::vector<uint8_t> framed = serializer->framedSerializeWithStats(batch);

  // Top-level layout: at least magic(4) + statsLen(4) + bytesLen(4) = 12 bytes header.
  ASSERT_GE(framed.size(), 12u);

  // Magic bytes 0xFE 0xCA 0x53 0x02.
  EXPECT_EQ(framed[0], static_cast<uint8_t>(0xFE));
  EXPECT_EQ(framed[1], static_cast<uint8_t>(0xCA));
  EXPECT_EQ(framed[2], static_cast<uint8_t>(0x53));
  EXPECT_EQ(framed[3], static_cast<uint8_t>(0x02));

  // statsLen LE int32 -- non-zero now that statsBlob is populated. The framing-only
  // the layout shape (offsets / framing), not the numeric statsLen value.
  uint32_t statsLen = static_cast<uint32_t>(framed[4]) | (static_cast<uint32_t>(framed[5]) << 8) |
      (static_cast<uint32_t>(framed[6]) << 16) | (static_cast<uint32_t>(framed[7]) << 24);

  // bytesLen LE int32 immediately after empty statsBlob.
  size_t bytesLenOffset = 8u + statsLen;
  ASSERT_GE(framed.size(), bytesLenOffset + 4u);
  uint32_t bytesLen = static_cast<uint32_t>(framed[bytesLenOffset]) |
      (static_cast<uint32_t>(framed[bytesLenOffset + 1]) << 8) |
      (static_cast<uint32_t>(framed[bytesLenOffset + 2]) << 16) |
      (static_cast<uint32_t>(framed[bytesLenOffset + 3]) << 24);
  EXPECT_GT(bytesLen, 0u) << "bytesBlob must contain serialized batch payload";
  EXPECT_EQ(framed.size(), bytesLenOffset + 4u + bytesLen)
      << "framed total size must match magic + statsLen + statsBlob + bytesLen + bytesBlob";
}

// statsBlob layout (BIGINT 1-col):
//   [ numCols u32 LE ] per col [ supported u8 | nullCount u32 | count u32 |
//     sizeInBytes u64 | lowerLen u32 | lower bytes | upperLen u32 | upper bytes ]
TEST_F(VeloxColumnarBatchSerializerTest, statsBlobBigintLayout) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  std::vector<VectorPtr> children = {
      makeFlatVector<int64_t>({42, 7, 99, -3, 50}),
  };
  auto vector = makeRowVector(children);
  auto batch = std::make_shared<VeloxColumnarBatch>(vector);

  std::vector<uint8_t> framed = serializer->framedSerializeWithStats(batch);
  ASSERT_GE(framed.size(), 12u);

  // Skip magic(4) header; read statsLen LE.
  uint32_t statsLen = static_cast<uint32_t>(framed[4]) | (static_cast<uint32_t>(framed[5]) << 8) |
      (static_cast<uint32_t>(framed[6]) << 16) | (static_cast<uint32_t>(framed[7]) << 24);
  ASSERT_GE(statsLen, 4u) << "statsBlob must contain at least numCols(uint32)";

  // statsBlob starts at offset 8.
  auto readU32LE = [&](size_t off) {
    return static_cast<uint32_t>(framed[off]) | (static_cast<uint32_t>(framed[off + 1]) << 8) |
        (static_cast<uint32_t>(framed[off + 2]) << 16) | (static_cast<uint32_t>(framed[off + 3]) << 24);
  };
  auto readU64LE = [&](size_t off) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
      v |= static_cast<uint64_t>(framed[off + i]) << (8 * i);
    }
    return v;
  };
  auto readI64LE = [&](size_t off) { return static_cast<int64_t>(readU64LE(off)); };

  size_t off = 8;
  uint32_t numCols = readU32LE(off);
  off += 4;
  EXPECT_EQ(numCols, 1u);

  // Per-col header.
  uint8_t supported = framed[off];
  off += 1;
  EXPECT_EQ(supported, 1u) << "BIGINT no-null col must be supported";

  uint32_t nullCount = readU32LE(off);
  off += 4;
  EXPECT_EQ(nullCount, 0u);

  uint32_t count = readU32LE(off);
  off += 4;
  EXPECT_EQ(count, 5u) << "count = rowVector->size()";

  uint64_t sizeInBytes = readU64LE(off);
  off += 8;
  EXPECT_EQ(sizeInBytes, 0u) << "sizeInBytes is a 0 placeholder";

  // lowerBound: BIGINT -> 8 bytes int64 LE = -3.
  uint32_t lowerLen = readU32LE(off);
  off += 4;
  EXPECT_EQ(lowerLen, 8u);
  EXPECT_EQ(readI64LE(off), -3);
  off += lowerLen;

  // upperBound: 8 bytes int64 LE = 99.
  uint32_t upperLen = readU32LE(off);
  off += 4;
  EXPECT_EQ(upperLen, 8u);
  EXPECT_EQ(readI64LE(off), 99);
  off += upperLen;

  // statsBlob ends exactly at off (no trailing bytes within statsBlob).
  EXPECT_EQ(off, 8u + statsLen) << "statsBlob content must exactly match statsLen";
}

// INTEGER (int32) FlatVector min/max scan; covers Spark IntegerType / DateType /
// YearMonthIntervalType which all map to Velox TypeKind::INTEGER.
TEST_F(VeloxColumnarBatchSerializerTest, computeStatsIntegerFlatVector) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  std::vector<VectorPtr> children = {
      makeFlatVector<int32_t>({100, -2147483, 2147483, 0, 42}),
  };
  auto vector = makeRowVector(children);
  auto stats = serializer->computeStats(vector);
  ASSERT_EQ(stats.size(), 1u);
  EXPECT_TRUE(stats[0].hasLowerBound) << "INTEGER FlatVector must be supported";
  EXPECT_TRUE(stats[0].hasUpperBound);
  EXPECT_EQ(stats[0].lowerBound.value<int32_t>(), -2147483);
  EXPECT_EQ(stats[0].upperBound.value<int32_t>(), 2147483);
  EXPECT_EQ(stats[0].nullCount, 0);
}

// SMALLINT (int16) FlatVector min/max scan.
// Spark ShortType -> Velox TypeKind::SMALLINT.
TEST_F(VeloxColumnarBatchSerializerTest, computeStatsSmallintFlatVector) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  std::vector<VectorPtr> children = {
      makeFlatVector<int16_t>({static_cast<int16_t>(-12345), static_cast<int16_t>(0), static_cast<int16_t>(12345)}),
  };
  auto vector = makeRowVector(children);
  auto stats = serializer->computeStats(vector);
  ASSERT_EQ(stats.size(), 1u);
  EXPECT_TRUE(stats[0].hasLowerBound) << "SMALLINT FlatVector must be supported";
  EXPECT_TRUE(stats[0].hasUpperBound);
  EXPECT_EQ(stats[0].lowerBound.value<int16_t>(), static_cast<int16_t>(-12345));
  EXPECT_EQ(stats[0].upperBound.value<int16_t>(), static_cast<int16_t>(12345));
}

// TINYINT (int8) FlatVector min/max scan.
// Spark ByteType -> Velox TypeKind::TINYINT.
TEST_F(VeloxColumnarBatchSerializerTest, computeStatsTinyintFlatVector) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  std::vector<VectorPtr> children = {
      makeFlatVector<int8_t>({static_cast<int8_t>(-128), static_cast<int8_t>(0), static_cast<int8_t>(127)}),
  };
  auto vector = makeRowVector(children);
  auto stats = serializer->computeStats(vector);
  ASSERT_EQ(stats.size(), 1u);
  EXPECT_TRUE(stats[0].hasLowerBound) << "TINYINT FlatVector must be supported";
  EXPECT_TRUE(stats[0].hasUpperBound);
  EXPECT_EQ(stats[0].lowerBound.value<int8_t>(), static_cast<int8_t>(-128));
  EXPECT_EQ(stats[0].upperBound.value<int8_t>(), static_cast<int8_t>(127));
}

// TIMESTAMP FlatVector min/max scan; wire emit via Timestamp::toMicros() so it
// shares the JVM LongType 8B arm (Spark Timestamp / TimestampNTZ physical = Long us).
TEST_F(VeloxColumnarBatchSerializerTest, computeStatsTimestampFlatVector) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  // Three distinct timestamps in ascending order.
  Timestamp t1(946684800, 0);
  Timestamp t2(1704067200, 0);
  Timestamp t3(1778198400, 0);
  std::vector<VectorPtr> children = {makeFlatVector<Timestamp>({t2, t1, t3})};
  auto vector = makeRowVector(children);
  auto stats = serializer->computeStats(vector);
  ASSERT_EQ(stats.size(), 1u);
  EXPECT_TRUE(stats[0].hasLowerBound) << "TIMESTAMP FlatVector must be supported";
  EXPECT_TRUE(stats[0].hasUpperBound);
  EXPECT_EQ(stats[0].lowerBound.value<Timestamp>(), t1);
  EXPECT_EQ(stats[0].upperBound.value<Timestamp>(), t3);
  EXPECT_EQ(stats[0].nullCount, 0);
}

// TIMESTAMP wire: lowerLen=8 + LE int64 microseconds (JVM LongType arm).
TEST_F(VeloxColumnarBatchSerializerTest, framedSerializeWithStatsTimestamp) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  Timestamp t1(946684800, 0); // 2000-01-01
  Timestamp t3(1778198400, 0); // 2026-05-13
  std::vector<VectorPtr> children = {makeFlatVector<Timestamp>({t1, t3})};
  auto rowVector = makeRowVector(children);
  auto batch = std::make_shared<VeloxColumnarBatch>(rowVector);
  auto framed = serializer->framedSerializeWithStats(batch);

  // statsBlob starts at offset 8 (after magic + statsLen).
  ASSERT_GE(framed.size(), 8u);
  const uint8_t* p = framed.data() + 8;
  // numCols
  EXPECT_EQ(p[0], 1u);
  EXPECT_EQ(p[1], 0u);
  EXPECT_EQ(p[2], 0u);
  EXPECT_EQ(p[3], 0u);
  p += 4;
  // supported
  EXPECT_EQ(p[0], 1u) << "TIMESTAMP column should emit supported=1";
  p += 1;
  // nullCount + count + sizeInBytes
  p += 4 + 4 + 8;
  // lowerLen must be 8 (microseconds Long), matching JVM LongType path.
  uint32_t lowerLen = static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
      (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
  EXPECT_EQ(lowerLen, 8u) << "TIMESTAMP wire lowerLen must be 8B (microseconds)";
  p += 4;
  // Read int64 LE microseconds; expect t1.toMicros().
  int64_t loMicros = 0;
  for (int i = 0; i < 8; ++i) {
    loMicros |= (static_cast<int64_t>(p[i]) << (8 * i));
  }
  EXPECT_EQ(loMicros, t1.toMicros()) << "TIMESTAMP wire lowerBound microseconds mismatch";
}

// VARCHAR FlatVector min/max via StringView. memcmp byte-order matches Spark
// ByteArray.compareBinary; variant(std::string) owns the bytes after the scan.
TEST_F(VeloxColumnarBatchSerializerTest, computeStatsVarcharFlatVector) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  // Test inputs include high-bit byte (0xC2) to confirm unsigned comparison
  // (signed cmp would put 0xC2 < 'a' = 0x61, wrong).
  std::vector<VectorPtr> children = {makeFlatVector<StringView>(
      {StringView("apple"),
       StringView("banana"),
       StringView("\xc2\xa9"
                  "copy")})};
  auto vector = makeRowVector(children);
  auto stats = serializer->computeStats(vector);
  ASSERT_EQ(stats.size(), 1u);
  EXPECT_TRUE(stats[0].hasLowerBound) << "VARCHAR FlatVector must be supported";
  EXPECT_TRUE(stats[0].hasUpperBound);
  // Unsigned byte order: "apple"(0x61) < "banana"(0x62) < "\xc2\xa9copy"(0xc2).
  EXPECT_EQ(stats[0].lowerBound.value<TypeKind::VARCHAR>(), std::string("apple"));
  EXPECT_EQ(
      stats[0].upperBound.value<TypeKind::VARCHAR>(),
      std::string("\xc2\xa9"
                  "copy"));
  EXPECT_EQ(stats[0].nullCount, 0);
}

// VARCHAR wire: lowerLen u32 LE + raw UTF-8 bytes.
TEST_F(VeloxColumnarBatchSerializerTest, framedSerializeWithStatsVarchar) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  std::vector<VectorPtr> children = {makeFlatVector<StringView>({StringView("apple"), StringView("banana")})};
  auto rowVector = makeRowVector(children);
  auto batch = std::make_shared<VeloxColumnarBatch>(rowVector);
  auto framed = serializer->framedSerializeWithStats(batch);

  // Skip MAGIC (4) + statsLen (4); statsBlob starts at offset 8.
  ASSERT_GE(framed.size(), 8u);
  const uint8_t* p = framed.data() + 8;
  // numCols = 1
  EXPECT_EQ(p[0], 1u);
  p += 4;
  // supported = 1
  EXPECT_EQ(p[0], 1u) << "VARCHAR column should emit supported=1";
  p += 1;
  // skip nullCount + count + sizeInBytes (4 + 4 + 8)
  p += 4 + 4 + 8;
  // lowerLen = 5 (apple)
  uint32_t lowerLen = static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
      (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
  EXPECT_EQ(lowerLen, 5u) << "VARCHAR lowerLen should be 5 for 'apple'";
  p += 4;
  std::string lower(reinterpret_cast<const char*>(p), 5);
  EXPECT_EQ(lower, "apple") << "VARCHAR lowerBound bytes mismatch";
  p += 5;
  // upperLen = 6 (banana)
  uint32_t upperLen = static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
      (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
  EXPECT_EQ(upperLen, 6u) << "VARCHAR upperLen should be 6 for 'banana'";
  p += 4;
  std::string upper(reinterpret_cast<const char*>(p), 6);
  EXPECT_EQ(upper, "banana") << "VARCHAR upperBound bytes mismatch";
}

// TIMESTAMP with sub-us nanos must conservatively widen: floor(lo), ceil(hi).
// Naive toMicros() floors both, which would shrink the interval and false-negative
// drop rows whose true ts has nanos % 1000 != 0.
TEST_F(VeloxColumnarBatchSerializerTest, timestampNanosCeilUpperFloorLower) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  // lo nanos=500 -> floor lower = s*1e6.
  // hi nanos=999'500 -> ceil upper = s*1e6 + 1000 (raw toMicros gives 999 < true).
  const int64_t s = 1704067200; // 2024-01-01T00:00:00Z
  Timestamp lo(s, 500);
  Timestamp hi(s, 999'500);
  std::vector<VectorPtr> children = {makeFlatVector<Timestamp>({lo, hi})};
  auto rowVector = makeRowVector(children);
  auto batch = std::make_shared<VeloxColumnarBatch>(rowVector);
  auto framed = serializer->framedSerializeWithStats(batch);

  ASSERT_GE(framed.size(), 8u);
  const uint8_t* p = framed.data() + 8; // skip MAGIC + statsLen
  EXPECT_EQ(p[0], 1u); // numCols=1
  p += 4;
  EXPECT_EQ(p[0], 1u) << "TIMESTAMP nanos column should still emit supported=1";
  p += 1 + 4 + 4 + 8; // skip supported + nullCount + count + sizeInBytes

  // lower: lowerLen=8, payload = floor lo = s*1e6
  uint32_t lowerLen = static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
      (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
  EXPECT_EQ(lowerLen, 8u);
  p += 4;
  int64_t loMicros = 0;
  for (int i = 0; i < 8; ++i) {
    loMicros |= (static_cast<int64_t>(p[i]) << (8 * i));
  }
  const int64_t expectedLo = s * 1'000'000LL;
  EXPECT_EQ(loMicros, expectedLo) << "lower must be floor(lo). Got " << loMicros << ", want " << expectedLo;
  p += 8;

  // upper: upperLen=8, payload = ceil hi = s*1e6 + 1000 (NOT s*1e6+999)
  uint32_t upperLen = static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
      (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
  EXPECT_EQ(upperLen, 8u);
  p += 4;
  int64_t hiMicros = 0;
  for (int i = 0; i < 8; ++i) {
    hiMicros |= (static_cast<int64_t>(p[i]) << (8 * i));
  }
  const int64_t expectedHi = s * 1'000'000LL + 1000; // ceil to next us
  EXPECT_EQ(hiMicros, expectedHi) << "upper must be ceil(hi) when nanos%1000 != 0. Got " << hiMicros << ", want "
                                  << expectedHi;
}

// When nanos % 1000 == 0 (exact us), no ceil adjustment needed.
TEST_F(VeloxColumnarBatchSerializerTest, timestampExactMicrosNoCeil) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  const int64_t s = 1704067200;
  Timestamp lo(s, 0);
  Timestamp hi(s, 1'000); // exactly 1 us
  std::vector<VectorPtr> children = {makeFlatVector<Timestamp>({lo, hi})};
  auto rowVector = makeRowVector(children);
  auto batch = std::make_shared<VeloxColumnarBatch>(rowVector);
  auto framed = serializer->framedSerializeWithStats(batch);

  const uint8_t* p = framed.data() + 8;
  p += 4 + 1 + 4 + 4 + 8 + 4; // numCols + supported + n/c/sz + lowerLen
  int64_t loMicros = 0;
  for (int i = 0; i < 8; ++i)
    loMicros |= (static_cast<int64_t>(p[i]) << (8 * i));
  EXPECT_EQ(loMicros, s * 1'000'000LL);
  p += 8 + 4; // lower bytes + upperLen
  int64_t hiMicros = 0;
  for (int i = 0; i < 8; ++i)
    hiMicros |= (static_cast<int64_t>(p[i]) << (8 * i));
  EXPECT_EQ(hiMicros, s * 1'000'000LL + 1) << "upper exact us should NOT ceil-adjust beyond toMicros";
}

// VARCHAR cpp truncates to 256B at source (single source of truth).
TEST_F(VeloxColumnarBatchSerializerTest, varcharTruncatesAt256Bytes) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  // 300-byte 'a' (lo) + 300-byte 'm' (hi); after truncate+carry expect
  //   lo = 256B 'a' (prefix <= original)
  //   hi = 255B 'm' + 1B 'n' (+1 carry on last byte)
  std::string longA(300, 'a');
  std::string longM(300, 'm');
  // makeFlatVector<StringView> with two rows so one becomes lo, the other hi.
  std::vector<VectorPtr> children = {
      makeFlatVector<StringView>({StringView(longA.data(), longA.size()), StringView(longM.data(), longM.size())})};
  auto rowVector = makeRowVector(children);
  auto batch = std::make_shared<VeloxColumnarBatch>(rowVector);
  auto framed = serializer->framedSerializeWithStats(batch);

  ASSERT_GE(framed.size(), 8u);
  const uint8_t* p = framed.data() + 8;
  EXPECT_EQ(p[0], 1u); // numCols
  p += 4;
  EXPECT_EQ(p[0], 1u) << "column should still be supported=1 after cpp truncate";
  p += 1 + 4 + 4 + 8; // skip supported + nullCount + count + sizeInBytes

  // lower: 256B prefix of 'a'
  uint32_t lowerLen = static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
      (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
  EXPECT_EQ(lowerLen, 256u) << "cpp must truncate lower to 256B";
  p += 4;
  for (uint32_t i = 0; i < 256u; ++i) {
    if (p[i] != 'a') {
      ADD_FAILURE() << "lower byte " << i << " = " << static_cast<int>(p[i]) << ", expected 'a'";
      break;
    }
  }
  p += 256;

  // upper: 255B 'm' + 1B 'n' (carry)
  uint32_t upperLen = static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
      (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
  EXPECT_EQ(upperLen, 256u) << "cpp must truncate upper to 256B";
  p += 4;
  for (uint32_t i = 0; i < 255u; ++i) {
    if (p[i] != 'm') {
      ADD_FAILURE() << "upper byte " << i << " = " << static_cast<int>(p[i]) << ", expected 'm'";
      break;
    }
  }
  EXPECT_EQ(p[255], 'n') << "upper last byte should be 'm'+1='n' (carry), got " << static_cast<int>(p[255]);
}

// All-0xFF upper prefix: carry overflows -> demote supported=0.
TEST_F(VeloxColumnarBatchSerializerTest, varcharCarryOverflowDemotesUnsupported) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  std::string longLo(10, '\xff');
  std::string longHi(300, '\xff');
  std::vector<VectorPtr> children = {
      makeFlatVector<StringView>({StringView(longLo.data(), longLo.size()), StringView(longHi.data(), longHi.size())})};
  auto rowVector = makeRowVector(children);
  auto batch = std::make_shared<VeloxColumnarBatch>(rowVector);
  auto framed = serializer->framedSerializeWithStats(batch);

  const uint8_t* p = framed.data() + 8;
  p += 4; // numCols
  EXPECT_EQ(p[0], 0u) << "300x 0xFF upper must demote to supported=0 (carry overflow)";
}

// Regression: short string (<= 256B) must round-trip unchanged.
TEST_F(VeloxColumnarBatchSerializerTest, varcharShortStringRoundTripsIntact) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  std::vector<VectorPtr> children = {makeFlatVector<StringView>({StringView("apple"), StringView("banana")})};
  auto rowVector = makeRowVector(children);
  auto batch = std::make_shared<VeloxColumnarBatch>(rowVector);
  auto framed = serializer->framedSerializeWithStats(batch);

  const uint8_t* p = framed.data() + 8;
  p += 4 + 1 + 4 + 4 + 8; // numCols + supported + n/c/sz
  uint32_t lowerLen = static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
      (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
  EXPECT_EQ(lowerLen, 5u) << "short lower bytes intact (no truncate)";
  p += 4;
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(p), 5), "apple");
  p += 5;
  uint32_t upperLen = static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
      (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
  EXPECT_EQ(upperLen, 6u) << "short upper bytes intact (no truncate, no carry)";
  p += 4;
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(p), 6), "banana");
}

// BOOLEAN FlatVector min/max scan must use valueAt(i)/isNullAt(i):
// FlatVector<bool>::rawValues() throws VeloxRuntimeError (bit-packed
// storage), so without scanBoolMinMax materializing a cached
// BooleanType column would hard-fail. Verify nullable bool batch
// produces (lo=false, hi=true, nullCount=2) instead of throwing.
TEST_F(VeloxColumnarBatchSerializerTest, computeStatsBooleanFlatVectorWithNulls) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  std::vector<VectorPtr> children = {
      makeNullableFlatVector<bool>({true, std::nullopt, false, std::nullopt, true}),
  };
  auto vector = makeRowVector(children);

  std::vector<ColumnStats> stats;
  ASSERT_NO_THROW(stats = serializer->computeStats(vector))
      << "Boolean scan must NOT throw (Velox FlatVector<bool>::rawValues unsupported)";
  ASSERT_EQ(stats.size(), 1u);
  EXPECT_TRUE(stats[0].hasLowerBound) << "Bool with non-null values must be supported";
  EXPECT_TRUE(stats[0].hasUpperBound);
  EXPECT_EQ(stats[0].lowerBound.value<bool>(), false);
  EXPECT_EQ(stats[0].upperBound.value<bool>(), true);
  EXPECT_EQ(stats[0].nullCount, 2);
}

// A float column mixing NaN with nulls and finite values: NaN is skipped (matches vanilla),
// finite bounds are still emitted, and nullCount counts only true nulls (NaN is non-null).
// Under-counting nullCount would let `col IS NULL` pruning (`nullCount > 0`) wrongly drop rows.
TEST_F(VeloxColumnarBatchSerializerTest, computeStatsNaNFloatStillCountsNulls) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  const float nan = std::numeric_limits<float>::quiet_NaN();
  std::vector<VectorPtr> children = {
      // [1.0, null, NaN, null, 2.0] -- 2 nulls, 1 NaN (skipped), finite bounds [1.0, 2.0].
      makeNullableFlatVector<float>({1.0f, std::nullopt, nan, std::nullopt, 2.0f}),
  };
  auto vector = makeRowVector(children);
  auto stats = serializer->computeStats(vector);
  ASSERT_EQ(stats.size(), 1u);
  EXPECT_TRUE(stats[0].hasLowerBound) << "NaN skipped, finite bounds still emitted (matches vanilla)";
  EXPECT_TRUE(stats[0].hasUpperBound);
  EXPECT_EQ(stats[0].lowerBound.value<float>(), 1.0f);
  EXPECT_EQ(stats[0].upperBound.value<float>(), 2.0f);
  EXPECT_EQ(stats[0].nullCount, 2) << "NaN is non-null; only true nulls counted (IsNull prune correctness)";
}

// Non-flat encoding (Dictionary / Constant / Complex) must still
// report a real nullCount. A null-bearing dict-encoded column
// reporting nullCount=0 would be advertised as having no nulls and
// incorrectly pruned by `col IS NULL`.
TEST_F(VeloxColumnarBatchSerializerTest, computeStatsDictEncodedNullCountReported) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  // Base flat: [10, 20, 30]. Dictionary indices [0, 1, 0, 2, 1] but with nulls at
  // positions 1 and 3 of the wrapping vector. Result has 5 rows, 2 nulls.
  auto base = makeFlatVector<int32_t>({10, 20, 30});
  vector_size_t outSize = 5;
  BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(outSize, pool_.get());
  auto* rawIndices = indices->asMutable<vector_size_t>();
  rawIndices[0] = 0;
  rawIndices[1] = 1;
  rawIndices[2] = 0;
  rawIndices[3] = 2;
  rawIndices[4] = 1;

  BufferPtr nulls = AlignedBuffer::allocate<bool>(outSize, pool_.get());
  auto* rawNulls = nulls->asMutable<uint64_t>();
  bits::fillBits(rawNulls, 0, outSize, true);
  bits::setNull(rawNulls, 1, true);
  bits::setNull(rawNulls, 3, true);

  auto dictVec = BaseVector::wrapInDictionary(nulls, indices, outSize, base);
  std::vector<VectorPtr> children = {dictVec};
  auto vector = makeRowVector(children);

  auto stats = serializer->computeStats(vector);
  ASSERT_EQ(stats.size(), 1u);
  EXPECT_FALSE(stats[0].hasLowerBound) << "Dictionary encoding -> min/max unsupported";
  EXPECT_FALSE(stats[0].hasUpperBound);
  EXPECT_EQ(stats[0].nullCount, 2)
      << "Dict-encoded vector with 2 nulls must report nullCount=2 (IsNull prune correctness)";
}

// Cross-language golden-frame pin.
//
// Asserts framedSerializeWithStats() emits a byte-identical frame for a fixed
// deterministic 4-col / 100-row input.
// Paired with backends-velox/src/test/scala/.../ColumnarCachedBatchFramedBytesSuite
// which feeds the SAME byte literal back through parseFramedBytes -- together
// they pin the JVM<->cpp wire contract end-to-end. A Velox PrestoSerde wire
// change OR a gluten stats-blob layout change will fire this test loudly.
//
// Input schema:
//   col a INTEGER  -- 100 rows, value = i (i in [0,99])
//   col b BIGINT   -- 100 rows, value = i - 50 (in [-50, 49])
//   col c VARCHAR  -- 100 rows, cycling 4 fixed strings ("apple","banana","cherry","date")
//   col d BIGINT   -- 100 rows, ALL NULL
//
// Update protocol: if Velox/gluten makes an intentional wire change, regenerate
// kGoldenFrame by capturing a fresh printf dump of the actual frame bytes, and
// call out the wire bump explicitly in the PR description.
TEST_F(VeloxColumnarBatchSerializerTest, framedSerializeWithStatsGolden) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  constexpr int kRows = 100;
  std::vector<int32_t> aVals(kRows);
  std::vector<int64_t> bVals(kRows);
  std::vector<StringView> cVals(kRows);
  const StringView kStrings[4] = {StringView("apple"), StringView("banana"), StringView("cherry"), StringView("date")};
  for (int i = 0; i < kRows; ++i) {
    aVals[i] = i;
    bVals[i] = static_cast<int64_t>(i) - 50;
    cVals[i] = kStrings[i % 4];
  }
  std::vector<VectorPtr> children = {
      makeFlatVector<int32_t>(aVals),
      makeFlatVector<int64_t>(bVals),
      makeFlatVector<StringView>(cVals),
      makeFlatVector<int64_t>(std::vector<int64_t>(kRows, 0)),
  };
  auto rowVector = makeRowVector({"a", "b", "c", "d"}, children);
  auto batch = std::make_shared<VeloxColumnarBatch>(rowVector);

  std::vector<uint8_t> actual = serializer->framedSerializeWithStats(batch);

  // 3198-byte golden frame captured from apache/incubator-gluten main at
  // ce6e16fe8f. Determinism verified by 3 reruns md5-identical
  // (0f9c54179325b10f965b0dabe52f8749). Regenerate by capturing a fresh
  // printf dump if an intentional wire bump occurs.
  static const std::vector<uint8_t> kGoldenFrame = {
      0xFE, 0xCA, 0x53, 0x02, 0x99, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x64, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
      0x00, 0x00, 0x00, 0x63, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0xCE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x08, 0x00,
      0x00, 0x00, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x61, 0x70, 0x70, 0x6C, 0x65, 0x04, 0x00,
      0x00, 0x00, 0x64, 0x61, 0x74, 0x65, 0x01, 0x00, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xD9, 0x0B, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0x04, 0xC4,
      0x0B, 0x00, 0x00, 0xC4, 0x0B, 0x00, 0x00, 0xF1, 0x94, 0x47, 0xAA, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
      0x09, 0x00, 0x00, 0x00, 0x49, 0x4E, 0x54, 0x5F, 0x41, 0x52, 0x52, 0x41, 0x59, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
      0x05, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00,
      0x00, 0x0A, 0x00, 0x00, 0x00, 0x0B, 0x00, 0x00, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x0D, 0x00, 0x00, 0x00, 0x0E, 0x00,
      0x00, 0x00, 0x0F, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x12, 0x00, 0x00, 0x00, 0x13,
      0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x15, 0x00, 0x00, 0x00, 0x16, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00, 0x00,
      0x18, 0x00, 0x00, 0x00, 0x19, 0x00, 0x00, 0x00, 0x1A, 0x00, 0x00, 0x00, 0x1B, 0x00, 0x00, 0x00, 0x1C, 0x00, 0x00,
      0x00, 0x1D, 0x00, 0x00, 0x00, 0x1E, 0x00, 0x00, 0x00, 0x1F, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x21, 0x00,
      0x00, 0x00, 0x22, 0x00, 0x00, 0x00, 0x23, 0x00, 0x00, 0x00, 0x24, 0x00, 0x00, 0x00, 0x25, 0x00, 0x00, 0x00, 0x26,
      0x00, 0x00, 0x00, 0x27, 0x00, 0x00, 0x00, 0x28, 0x00, 0x00, 0x00, 0x29, 0x00, 0x00, 0x00, 0x2A, 0x00, 0x00, 0x00,
      0x2B, 0x00, 0x00, 0x00, 0x2C, 0x00, 0x00, 0x00, 0x2D, 0x00, 0x00, 0x00, 0x2E, 0x00, 0x00, 0x00, 0x2F, 0x00, 0x00,
      0x00, 0x30, 0x00, 0x00, 0x00, 0x31, 0x00, 0x00, 0x00, 0x32, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00, 0x00, 0x34, 0x00,
      0x00, 0x00, 0x35, 0x00, 0x00, 0x00, 0x36, 0x00, 0x00, 0x00, 0x37, 0x00, 0x00, 0x00, 0x38, 0x00, 0x00, 0x00, 0x39,
      0x00, 0x00, 0x00, 0x3A, 0x00, 0x00, 0x00, 0x3B, 0x00, 0x00, 0x00, 0x3C, 0x00, 0x00, 0x00, 0x3D, 0x00, 0x00, 0x00,
      0x3E, 0x00, 0x00, 0x00, 0x3F, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x41, 0x00, 0x00, 0x00, 0x42, 0x00, 0x00,
      0x00, 0x43, 0x00, 0x00, 0x00, 0x44, 0x00, 0x00, 0x00, 0x45, 0x00, 0x00, 0x00, 0x46, 0x00, 0x00, 0x00, 0x47, 0x00,
      0x00, 0x00, 0x48, 0x00, 0x00, 0x00, 0x49, 0x00, 0x00, 0x00, 0x4A, 0x00, 0x00, 0x00, 0x4B, 0x00, 0x00, 0x00, 0x4C,
      0x00, 0x00, 0x00, 0x4D, 0x00, 0x00, 0x00, 0x4E, 0x00, 0x00, 0x00, 0x4F, 0x00, 0x00, 0x00, 0x50, 0x00, 0x00, 0x00,
      0x51, 0x00, 0x00, 0x00, 0x52, 0x00, 0x00, 0x00, 0x53, 0x00, 0x00, 0x00, 0x54, 0x00, 0x00, 0x00, 0x55, 0x00, 0x00,
      0x00, 0x56, 0x00, 0x00, 0x00, 0x57, 0x00, 0x00, 0x00, 0x58, 0x00, 0x00, 0x00, 0x59, 0x00, 0x00, 0x00, 0x5A, 0x00,
      0x00, 0x00, 0x5B, 0x00, 0x00, 0x00, 0x5C, 0x00, 0x00, 0x00, 0x5D, 0x00, 0x00, 0x00, 0x5E, 0x00, 0x00, 0x00, 0x5F,
      0x00, 0x00, 0x00, 0x60, 0x00, 0x00, 0x00, 0x61, 0x00, 0x00, 0x00, 0x62, 0x00, 0x00, 0x00, 0x63, 0x00, 0x00, 0x00,
      0x0A, 0x00, 0x00, 0x00, 0x4C, 0x4F, 0x4E, 0x47, 0x5F, 0x41, 0x52, 0x52, 0x41, 0x59, 0x64, 0x00, 0x00, 0x00, 0x00,
      0xCE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xCF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xD0, 0xFF, 0xFF,
      0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xD1, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xD2, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
      0xFF, 0xFF, 0xD3, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xD4, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xD5,
      0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xD6, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xD7, 0xFF, 0xFF, 0xFF,
      0xFF, 0xFF, 0xFF, 0xFF, 0xD8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xD9, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
      0xFF, 0xDA, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xDB, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xDC, 0xFF,
      0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xDD, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xDE, 0xFF, 0xFF, 0xFF, 0xFF,
      0xFF, 0xFF, 0xFF, 0xDF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xE0, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
      0xE1, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xE2, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xE3, 0xFF, 0xFF,
      0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xE4, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xE5, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
      0xFF, 0xFF, 0xE6, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xE7, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xE8,
      0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xE9, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xEA, 0xFF, 0xFF, 0xFF,
      0xFF, 0xFF, 0xFF, 0xFF, 0xEB, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xEC, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
      0xFF, 0xED, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xEE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xEF, 0xFF,
      0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xF0, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xF1, 0xFF, 0xFF, 0xFF, 0xFF,
      0xFF, 0xFF, 0xFF, 0xF2, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xF3, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
      0xF4, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xF5, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xF6, 0xFF, 0xFF,
      0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xF7, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xF8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
      0xFF, 0xFF, 0xF9, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFA, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFB,
      0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFC, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFD, 0xFF, 0xFF, 0xFF,
      0xFF, 0xFF, 0xFF, 0xFF, 0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
      0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0B, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0D, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0E,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x13, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x15, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x16, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x19, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x1A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1B, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1C, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x1D, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1E, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x1F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x21,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x23, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x24, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x25, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x26, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x27, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x28, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x29, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x2B, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2C, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x2D, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2E, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2F, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x0E, 0x00, 0x00, 0x00, 0x56, 0x41, 0x52, 0x49, 0x41, 0x42, 0x4C, 0x45, 0x5F, 0x57, 0x49, 0x44, 0x54,
      0x48, 0x64, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x0B, 0x00, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x15, 0x00,
      0x00, 0x00, 0x1A, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x26, 0x00, 0x00, 0x00, 0x2A, 0x00, 0x00, 0x00, 0x2F,
      0x00, 0x00, 0x00, 0x35, 0x00, 0x00, 0x00, 0x3B, 0x00, 0x00, 0x00, 0x3F, 0x00, 0x00, 0x00, 0x44, 0x00, 0x00, 0x00,
      0x4A, 0x00, 0x00, 0x00, 0x50, 0x00, 0x00, 0x00, 0x54, 0x00, 0x00, 0x00, 0x59, 0x00, 0x00, 0x00, 0x5F, 0x00, 0x00,
      0x00, 0x65, 0x00, 0x00, 0x00, 0x69, 0x00, 0x00, 0x00, 0x6E, 0x00, 0x00, 0x00, 0x74, 0x00, 0x00, 0x00, 0x7A, 0x00,
      0x00, 0x00, 0x7E, 0x00, 0x00, 0x00, 0x83, 0x00, 0x00, 0x00, 0x89, 0x00, 0x00, 0x00, 0x8F, 0x00, 0x00, 0x00, 0x93,
      0x00, 0x00, 0x00, 0x98, 0x00, 0x00, 0x00, 0x9E, 0x00, 0x00, 0x00, 0xA4, 0x00, 0x00, 0x00, 0xA8, 0x00, 0x00, 0x00,
      0xAD, 0x00, 0x00, 0x00, 0xB3, 0x00, 0x00, 0x00, 0xB9, 0x00, 0x00, 0x00, 0xBD, 0x00, 0x00, 0x00, 0xC2, 0x00, 0x00,
      0x00, 0xC8, 0x00, 0x00, 0x00, 0xCE, 0x00, 0x00, 0x00, 0xD2, 0x00, 0x00, 0x00, 0xD7, 0x00, 0x00, 0x00, 0xDD, 0x00,
      0x00, 0x00, 0xE3, 0x00, 0x00, 0x00, 0xE7, 0x00, 0x00, 0x00, 0xEC, 0x00, 0x00, 0x00, 0xF2, 0x00, 0x00, 0x00, 0xF8,
      0x00, 0x00, 0x00, 0xFC, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x07, 0x01, 0x00, 0x00, 0x0D, 0x01, 0x00, 0x00,
      0x11, 0x01, 0x00, 0x00, 0x16, 0x01, 0x00, 0x00, 0x1C, 0x01, 0x00, 0x00, 0x22, 0x01, 0x00, 0x00, 0x26, 0x01, 0x00,
      0x00, 0x2B, 0x01, 0x00, 0x00, 0x31, 0x01, 0x00, 0x00, 0x37, 0x01, 0x00, 0x00, 0x3B, 0x01, 0x00, 0x00, 0x40, 0x01,
      0x00, 0x00, 0x46, 0x01, 0x00, 0x00, 0x4C, 0x01, 0x00, 0x00, 0x50, 0x01, 0x00, 0x00, 0x55, 0x01, 0x00, 0x00, 0x5B,
      0x01, 0x00, 0x00, 0x61, 0x01, 0x00, 0x00, 0x65, 0x01, 0x00, 0x00, 0x6A, 0x01, 0x00, 0x00, 0x70, 0x01, 0x00, 0x00,
      0x76, 0x01, 0x00, 0x00, 0x7A, 0x01, 0x00, 0x00, 0x7F, 0x01, 0x00, 0x00, 0x85, 0x01, 0x00, 0x00, 0x8B, 0x01, 0x00,
      0x00, 0x8F, 0x01, 0x00, 0x00, 0x94, 0x01, 0x00, 0x00, 0x9A, 0x01, 0x00, 0x00, 0xA0, 0x01, 0x00, 0x00, 0xA4, 0x01,
      0x00, 0x00, 0xA9, 0x01, 0x00, 0x00, 0xAF, 0x01, 0x00, 0x00, 0xB5, 0x01, 0x00, 0x00, 0xB9, 0x01, 0x00, 0x00, 0xBE,
      0x01, 0x00, 0x00, 0xC4, 0x01, 0x00, 0x00, 0xCA, 0x01, 0x00, 0x00, 0xCE, 0x01, 0x00, 0x00, 0xD3, 0x01, 0x00, 0x00,
      0xD9, 0x01, 0x00, 0x00, 0xDF, 0x01, 0x00, 0x00, 0xE3, 0x01, 0x00, 0x00, 0xE8, 0x01, 0x00, 0x00, 0xEE, 0x01, 0x00,
      0x00, 0xF4, 0x01, 0x00, 0x00, 0xF8, 0x01, 0x00, 0x00, 0xFD, 0x01, 0x00, 0x00, 0x03, 0x02, 0x00, 0x00, 0x09, 0x02,
      0x00, 0x00, 0x0D, 0x02, 0x00, 0x00, 0x00, 0x0D, 0x02, 0x00, 0x00, 0x61, 0x70, 0x70, 0x6C, 0x65, 0x62, 0x61, 0x6E,
      0x61, 0x6E, 0x61, 0x63, 0x68, 0x65, 0x72, 0x72, 0x79, 0x64, 0x61, 0x74, 0x65, 0x61, 0x70, 0x70, 0x6C, 0x65, 0x62,
      0x61, 0x6E, 0x61, 0x6E, 0x61, 0x63, 0x68, 0x65, 0x72, 0x72, 0x79, 0x64, 0x61, 0x74, 0x65, 0x61, 0x70, 0x70, 0x6C,
      0x65, 0x62, 0x61, 0x6E, 0x61, 0x6E, 0x61, 0x63, 0x68, 0x65, 0x72, 0x72, 0x79, 0x64, 0x61, 0x74, 0x65, 0x61, 0x70,
      0x70, 0x6C, 0x65, 0x62, 0x61, 0x6E, 0x61, 0x6E, 0x61, 0x63, 0x68, 0x65, 0x72, 0x72, 0x79, 0x64, 0x61, 0x74, 0x65,
      0x61, 0x70, 0x70, 0x6C, 0x65, 0x62, 0x61, 0x6E, 0x61, 0x6E, 0x61, 0x63, 0x68, 0x65, 0x72, 0x72, 0x79, 0x64, 0x61,
      0x74, 0x65, 0x61, 0x70, 0x70, 0x6C, 0x65, 0x62, 0x61, 0x6E, 0x61, 0x6E, 0x61, 0x63, 0x68, 0x65, 0x72, 0x72, 0x79,
      0x64, 0x61, 0x74, 0x65, 0x61, 0x70, 0x70, 0x6C, 0x65, 0x62, 0x61, 0x6E, 0x61, 0x6E, 0x61, 0x63, 0x68, 0x65, 0x72,
      0x72, 0x79, 0x64, 0x61, 0x74, 0x65, 0x61, 0x70, 0x70, 0x6C, 0x65, 0x62, 0x61, 0x6E, 0x61, 0x6E, 0x61, 0x63, 0x68,
      0x65, 0x72, 0x72, 0x79, 0x64, 0x61, 0x74, 0x65, 0x61, 0x70, 0x70, 0x6C, 0x65, 0x62, 0x61, 0x6E, 0x61, 0x6E, 0x61,
      0x63, 0x68, 0x65, 0x72, 0x72, 0x79, 0x64, 0x61, 0x74, 0x65, 0x61, 0x70, 0x70, 0x6C, 0x65, 0x62, 0x61, 0x6E, 0x61,
      0x6E, 0x61, 0x63, 0x68, 0x65, 0x72, 0x72, 0x79, 0x64, 0x61, 0x74, 0x65, 0x61, 0x70, 0x70, 0x6C, 0x65, 0x62, 0x61,
      0x6E, 0x61, 0x6E, 0x61, 0x63, 0x68, 0x65, 0x72, 0x72, 0x79, 0x64, 0x61, 0x74, 0x65, 0x61, 0x70, 0x70, 0x6C, 0x65,
      0x62, 0x61, 0x6E, 0x61, 0x6E, 0x61, 0x63, 0x68, 0x65, 0x72, 0x72, 0x79, 0x64, 0x61, 0x74, 0x65, 0x61, 0x70, 0x70,
      0x6C, 0x65, 0x62, 0x61, 0x6E, 0x61, 0x6E, 0x61, 0x63, 0x68, 0x65, 0x72, 0x72, 0x79, 0x64, 0x61, 0x74, 0x65, 0x61,
      0x70, 0x70, 0x6C, 0x65, 0x62, 0x61, 0x6E, 0x61, 0x6E, 0x61, 0x63, 0x68, 0x65, 0x72, 0x72, 0x79, 0x64, 0x61, 0x74,
      0x65, 0x61, 0x70, 0x70, 0x6C, 0x65, 0x62, 0x61, 0x6E, 0x61, 0x6E, 0x61, 0x63, 0x68, 0x65, 0x72, 0x72, 0x79, 0x64,
      0x61, 0x74, 0x65, 0x61, 0x70, 0x70, 0x6C, 0x65, 0x62, 0x61, 0x6E, 0x61, 0x6E, 0x61, 0x63, 0x68, 0x65, 0x72, 0x72,
      0x79, 0x64, 0x61, 0x74, 0x65, 0x61, 0x70, 0x70, 0x6C, 0x65, 0x62, 0x61, 0x6E, 0x61, 0x6E, 0x61, 0x63, 0x68, 0x65,
      0x72, 0x72, 0x79, 0x64, 0x61, 0x74, 0x65, 0x61, 0x70, 0x70, 0x6C, 0x65, 0x62, 0x61, 0x6E, 0x61, 0x6E, 0x61, 0x63,
      0x68, 0x65, 0x72, 0x72, 0x79, 0x64, 0x61, 0x74, 0x65, 0x61, 0x70, 0x70, 0x6C, 0x65, 0x62, 0x61, 0x6E, 0x61, 0x6E,
      0x61, 0x63, 0x68, 0x65, 0x72, 0x72, 0x79, 0x64, 0x61, 0x74, 0x65, 0x61, 0x70, 0x70, 0x6C, 0x65, 0x62, 0x61, 0x6E,
      0x61, 0x6E, 0x61, 0x63, 0x68, 0x65, 0x72, 0x72, 0x79, 0x64, 0x61, 0x74, 0x65, 0x61, 0x70, 0x70, 0x6C, 0x65, 0x62,
      0x61, 0x6E, 0x61, 0x6E, 0x61, 0x63, 0x68, 0x65, 0x72, 0x72, 0x79, 0x64, 0x61, 0x74, 0x65, 0x61, 0x70, 0x70, 0x6C,
      0x65, 0x62, 0x61, 0x6E, 0x61, 0x6E, 0x61, 0x63, 0x68, 0x65, 0x72, 0x72, 0x79, 0x64, 0x61, 0x74, 0x65, 0x61, 0x70,
      0x70, 0x6C, 0x65, 0x62, 0x61, 0x6E, 0x61, 0x6E, 0x61, 0x63, 0x68, 0x65, 0x72, 0x72, 0x79, 0x64, 0x61, 0x74, 0x65,
      0x61, 0x70, 0x70, 0x6C, 0x65, 0x62, 0x61, 0x6E, 0x61, 0x6E, 0x61, 0x63, 0x68, 0x65, 0x72, 0x72, 0x79, 0x64, 0x61,
      0x74, 0x65, 0x61, 0x70, 0x70, 0x6C, 0x65, 0x62, 0x61, 0x6E, 0x61, 0x6E, 0x61, 0x63, 0x68, 0x65, 0x72, 0x72, 0x79,
      0x64, 0x61, 0x74, 0x65, 0x0A, 0x00, 0x00, 0x00, 0x4C, 0x4F, 0x4E, 0x47, 0x5F, 0x41, 0x52, 0x52, 0x41, 0x59, 0x64,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

  ASSERT_EQ(actual.size(), kGoldenFrame.size()) << "framedSerializeWithStats output size diverged from golden frame. "
                                                << "If this is an intentional wire change, regenerate kGoldenFrame "
                                                << "(see comment block above the test).";
  EXPECT_EQ(actual, kGoldenFrame) << "framedSerializeWithStats output bytes diverged from golden frame.";
}

// Sibling no-bounds branch coverage (emitSupported=0 path).
//
// All-null BIGINT col MUST hit the no-bounds branch (computeStats ->
// hasLowerBound=hasUpperBound=false -> marshalStatsInto emits supported=0, no
// lower/upper bytes).
//
// This is intentionally a FIELD-LEVEL test, NOT a byte-equal golden: Velox
// PrestoSerde dumps the FlatVector's raw `values` buffer regardless of null
// bitmap, and an all-null FlatVector has UNINITIALIZED values buffer bytes.
// Reader uses the null bitmap to skip them, but the wire output is
// non-deterministic across reruns (3 runs -> 3 distinct md5 hashes; offset
// ~166-170 + tail byte varied).
//
// So the emitSupported=0 coverage lives here as a field-level test, not in
// the byte-equal golden above.
TEST_F(VeloxColumnarBatchSerializerTest, framedSerializeWithStatsAllNullColNoBounds) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  constexpr int kRows = 5;
  std::vector<VectorPtr> children = {
      makeNullableFlatVector<int64_t>(std::vector<std::optional<int64_t>>(kRows, std::nullopt)),
  };
  auto rowVector = makeRowVector({"d"}, children);
  auto batch = std::make_shared<VeloxColumnarBatch>(rowVector);

  std::vector<uint8_t> framed = serializer->framedSerializeWithStats(batch);

  // Skip magic(4) + statsLen(4); statsBlob starts at offset 8.
  ASSERT_GE(framed.size(), 8u);
  const uint8_t* p = framed.data() + 8;
  // numCols = 1 (u32 LE).
  EXPECT_EQ(p[0], 1u);
  EXPECT_EQ(p[1], 0u);
  EXPECT_EQ(p[2], 0u);
  EXPECT_EQ(p[3], 0u);
  p += 4;
  // Per-col header: supported(u8) + nullCount(u32) + count(u32) + sizeInBytes(u64).
  EXPECT_EQ(p[0], 0u) << "all-null col must emit supported=0 (no-bounds branch)";
  p += 1;
  uint32_t nullCount = static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
      (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
  EXPECT_EQ(nullCount, static_cast<uint32_t>(kRows))
      << "nullCount must equal row count for all-null col (JVM IsNull pruning correctness)";
  p += 4;
  uint32_t count = static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
      (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
  EXPECT_EQ(count, static_cast<uint32_t>(kRows));
  p += 4;
  // sizeInBytes placeholder = 0 (8 bytes).
  for (int k = 0; k < 8; ++k) {
    EXPECT_EQ(p[k], 0u) << "sizeInBytes placeholder byte " << k << " must be 0";
  }
  // No lowerLen / upperLen / bounds bytes follow when supported=0.
}

// Cross-language V3 golden-frame pin.
//
// Empty 0-row / 0-col input has no per-column PrestoSerde payload, so this
// byte-equal golden pins the V3 top-level frame contract:
//   [ magic=0x03 ][ statsLen ][ statsBlob(numCols=0) ][ numRows=0 ][ numCols=0 ]
// Paired with ColumnarCachedBatchFramedBytesSuite's kGoldenFrameV3Empty parser
// round-trip on the JVM side.
TEST_F(VeloxColumnarBatchSerializerTest, framedSerializeWithStatsV3EmptyGolden) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  auto rowVector = makeRowVector(ROW({}, {}), 0);
  auto batch = std::make_shared<VeloxColumnarBatch>(rowVector);

  std::vector<uint8_t> actual = serializer->framedSerializeWithStatsV3(batch);

  static const std::vector<uint8_t> kGoldenFrame = {0xFE, 0xCA, 0x53, 0x03, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00,
                                                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  EXPECT_EQ(actual, kGoldenFrame) << "framedSerializeWithStatsV3 empty frame diverged.";
}

TEST_F(VeloxColumnarBatchSerializerTest, framedSerializeV3NoStatsEmptyGolden) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  auto rowVector = makeRowVector(ROW({}, {}), 0);
  auto batch = std::make_shared<VeloxColumnarBatch>(rowVector);

  std::vector<uint8_t> actual = serializer->framedSerializeV3(batch);

  static const std::vector<uint8_t> kGoldenFrame = {
      0xFE, 0xCA, 0x53, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  EXPECT_EQ(actual, kGoldenFrame) << "framedSerializeV3 no-stats empty frame diverged.";
}

namespace {

uint32_t readU32LE(const std::vector<uint8_t>& bytes, size_t& offset) {
  GLUTEN_CHECK(offset + 4 <= bytes.size(), "readU32LE offset out of range");
  uint32_t value = static_cast<uint32_t>(bytes[offset]) | (static_cast<uint32_t>(bytes[offset + 1]) << 8) |
      (static_cast<uint32_t>(bytes[offset + 2]) << 16) | (static_cast<uint32_t>(bytes[offset + 3]) << 24);
  offset += 4;
  return value;
}

void appendU32LE(std::vector<uint8_t>& bytes, uint32_t value) {
  bytes.push_back(static_cast<uint8_t>(value & 0xFF));
  bytes.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
  bytes.push_back(static_cast<uint8_t>((value >> 16) & 0xFF));
  bytes.push_back(static_cast<uint8_t>((value >> 24) & 0xFF));
}

void appendU64LE(std::vector<uint8_t>& bytes, uint64_t value) {
  for (int i = 0; i < 8; ++i) {
    bytes.push_back(static_cast<uint8_t>((value >> (8 * i)) & 0xFF));
  }
}

void assertV3WriterFrameLayout(
    const std::vector<uint8_t>& framed,
    uint32_t expectedRows,
    uint32_t expectedColumns,
    bool expectStats) {
  ASSERT_GE(framed.size(), 16u);
  ASSERT_EQ(framed[0], 0xFE);
  ASSERT_EQ(framed[1], 0xCA);
  ASSERT_EQ(framed[2], 0x53);
  ASSERT_EQ(framed[3], 0x03);

  size_t offset = 4;
  const uint32_t statsLen = readU32LE(framed, offset);
  if (expectStats) {
    ASSERT_GT(statsLen, 0u);
    ASSERT_GE(framed.size() - offset, statsLen);
    size_t statsOffset = offset;
    ASSERT_EQ(readU32LE(framed, statsOffset), expectedColumns);
  } else {
    ASSERT_EQ(statsLen, 0u);
  }
  offset += statsLen;

  ASSERT_EQ(readU32LE(framed, offset), expectedRows);
  ASSERT_EQ(readU32LE(framed, offset), expectedColumns);
  for (uint32_t col = 0; col < expectedColumns; ++col) {
    const uint32_t colLen = readU32LE(framed, offset);
    ASSERT_GT(colLen, 0u) << "non-empty writer frame must include serialized bytes for col " << col;
    ASSERT_GE(framed.size() - offset, colLen);
    offset += colLen;
  }
  ASSERT_EQ(offset, framed.size()) << "writer frame must not contain trailing bytes";
}

std::vector<uint8_t> v3NoStatsNonEmptyFrameFixture() {
  return {0xFE, 0xCA, 0x53, 0x03, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01,
          0x00, 0x00, 0x00, 0xaa, 0x02, 0x00, 0x00, 0x00, 0xbb, 0xcc, 0x03, 0x00, 0x00, 0x00, 0xdd, 0xee, 0xff};
}

std::vector<uint8_t> v3WithStatsNonEmptyFrameFixture() {
  std::vector<uint8_t> stats;
  appendU32LE(stats, 3);
  for (uint32_t col = 0; col < 3; ++col) {
    stats.push_back(0); // unsupported: no lower/upper bound payload follows.
    appendU32LE(stats, 0); // nullCount
    appendU32LE(stats, 5); // count
    appendU64LE(stats, 0); // sizeInBytes placeholder
  }

  std::vector<uint8_t> framed = {0xFE, 0xCA, 0x53, 0x03};
  appendU32LE(framed, static_cast<uint32_t>(stats.size()));
  framed.insert(framed.end(), stats.begin(), stats.end());
  appendU32LE(framed, 5);
  appendU32LE(framed, 3);
  framed.insert(
      framed.end(),
      {0x01, 0x00, 0x00, 0x00, 0xaa, 0x02, 0x00, 0x00, 0x00, 0xbb, 0xcc, 0x03, 0x00, 0x00, 0x00, 0xdd, 0xee, 0xff});
  return framed;
}

} // namespace

TEST_F(VeloxColumnarBatchSerializerTest, framedSerializeV3NonEmptyWriterLayout) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);
  std::vector<VectorPtr> children = {
      makeNullableFlatVector<int32_t>({1, 2, std::nullopt, 4, 5}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50}),
      makeFlatVector<StringView>({"a", "bb", "ccc", "dddd", "eeeee"}),
  };
  auto vector = makeRowVector({"a", "b", "c"}, children);
  auto batch = std::make_shared<VeloxColumnarBatch>(vector);

  auto framed = serializer->framedSerializeV3(batch);

  assertV3WriterFrameLayout(framed, 5, 3, false);

  std::vector<int32_t> requestedColumns = {0, 2};
  auto deserializer = makeV3Deserializer(arrowPool);
  auto projectedBatch = deserializer->deserializeV3(
      framed.data(), static_cast<int32_t>(framed.size()), std::optional<std::vector<int32_t>>(requestedColumns));
  auto projectedVector = std::dynamic_pointer_cast<VeloxColumnarBatch>(projectedBatch)->getRowVector();
  auto expectedProjected = makeRowVector({"a", "c"}, {children[0], children[2]});
  test::assertEqualVectors(expectedProjected, projectedVector);
}

TEST_F(VeloxColumnarBatchSerializerTest, framedSerializeWithStatsV3NonEmptyWriterLayout) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);
  std::vector<VectorPtr> children = {
      makeNullableFlatVector<int32_t>({1, 2, std::nullopt, 4, 5}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50}),
      makeFlatVector<StringView>({"a", "bb", "ccc", "dddd", "eeeee"}),
  };
  auto vector = makeRowVector({"a", "b", "c"}, children);
  auto batch = std::make_shared<VeloxColumnarBatch>(vector);

  auto framed = serializer->framedSerializeWithStatsV3(batch);

  assertV3WriterFrameLayout(framed, 5, 3, true);

  std::vector<int32_t> requestedColumns = {0, 2};
  auto deserializer = makeV3Deserializer(arrowPool);
  auto projectedBatch = deserializer->deserializeV3(
      framed.data(), static_cast<int32_t>(framed.size()), std::optional<std::vector<int32_t>>(requestedColumns));
  auto projectedVector = std::dynamic_pointer_cast<VeloxColumnarBatch>(projectedBatch)->getRowVector();
  auto expectedProjected = makeRowVector({"a", "c"}, {children[0], children[2]});
  test::assertEqualVectors(expectedProjected, projectedVector);
}

TEST_F(VeloxColumnarBatchSerializerTest, deserializeV3ZeroProjectionNonEmptyNoStatsFrameFixture) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto deserializer = makeV3Deserializer(arrowPool);
  auto framed = v3NoStatsNonEmptyFrameFixture();
  std::vector<int32_t> requestedColumns;

  assertV3WriterFrameLayout(framed, 5, 3, false);
  auto projectedBatch = deserializer->deserializeV3(
      framed.data(), static_cast<int32_t>(framed.size()), std::optional<std::vector<int32_t>>(requestedColumns));
  auto projectedVector = std::dynamic_pointer_cast<VeloxColumnarBatch>(projectedBatch)->getRowVector();

  ASSERT_EQ(projectedVector->size(), 5);
  ASSERT_EQ(projectedVector->childrenSize(), 0u);
}

TEST_F(VeloxColumnarBatchSerializerTest, deserializeV3ZeroProjectionNonEmptyWithStatsFrameFixture) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto deserializer = makeV3Deserializer(arrowPool);
  auto framed = v3WithStatsNonEmptyFrameFixture();
  std::vector<int32_t> requestedColumns;

  assertV3WriterFrameLayout(framed, 5, 3, true);
  auto projectedBatch = deserializer->deserializeV3(
      framed.data(), static_cast<int32_t>(framed.size()), std::optional<std::vector<int32_t>>(requestedColumns));
  auto projectedVector = std::dynamic_pointer_cast<VeloxColumnarBatch>(projectedBatch)->getRowVector();

  ASSERT_EQ(projectedVector->size(), 5);
  ASSERT_EQ(projectedVector->childrenSize(), 0u);
}

TEST_F(VeloxColumnarBatchSerializerTest, deserializeV3RejectsTrailingBytes) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto deserializer = makeV3Deserializer(arrowPool);
  auto framed = v3NoStatsNonEmptyFrameFixture();
  framed.push_back(0x42);
  std::vector<int32_t> requestedColumns;

  EXPECT_THROW(
      (void)deserializer->deserializeV3(
          framed.data(), static_cast<int32_t>(framed.size()), std::optional<std::vector<int32_t>>(requestedColumns)),
      GlutenException);
}

TEST_F(VeloxColumnarBatchSerializerTest, deserializeV3RejectsTruncatedColumnBytes) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto deserializer = makeV3Deserializer(arrowPool);
  auto framed = v3NoStatsNonEmptyFrameFixture();
  framed.pop_back();
  std::vector<int32_t> requestedColumns;

  EXPECT_THROW(
      (void)deserializer->deserializeV3(
          framed.data(), static_cast<int32_t>(framed.size()), std::optional<std::vector<int32_t>>(requestedColumns)),
      GlutenException);
}

TEST_F(VeloxColumnarBatchSerializerTest, deserializeV3RejectsSchemaNumColsMismatch) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto deserializer = makeV3Deserializer(arrowPool);
  auto framed = v3NoStatsNonEmptyFrameFixture();
  framed[12] = 0x04;
  std::vector<int32_t> requestedColumns;

  EXPECT_THROW(
      (void)deserializer->deserializeV3(
          framed.data(), static_cast<int32_t>(framed.size()), std::optional<std::vector<int32_t>>(requestedColumns)),
      GlutenException);
}

TEST_F(VeloxColumnarBatchSerializerTest, deserializeV3RejectsRequestedColumnOutOfRange) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto deserializer = makeV3Deserializer(arrowPool);
  auto framed = v3NoStatsNonEmptyFrameFixture();
  std::vector<int32_t> requestedColumns = {3};

  EXPECT_THROW(
      (void)deserializer->deserializeV3(
          framed.data(), static_cast<int32_t>(framed.size()), std::optional<std::vector<int32_t>>(requestedColumns)),
      GlutenException);
}

TEST_F(VeloxColumnarBatchSerializerTest, deserializeV3RejectsDecodedColumnRowCountMismatch) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();

  std::vector<VectorPtr> children = {
      makeNullableFlatVector<int32_t>({1, 2, std::nullopt, 4, 5}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50}),
      makeFlatVector<StringView>({"a", "bb", "ccc", "dddd", "eeeee"}),
  };
  auto vector = makeRowVector({"a", "b", "c"}, children);
  auto batch = std::make_shared<VeloxColumnarBatch>(vector);

  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);
  std::vector<uint8_t> framed = serializer->framedSerializeV3(batch);

  // No-stats V3 layout puts numRows at offset 8. Keep the 5-row column payloads intact, but
  // declare 4 rows in the frame header so the lazy column loader must reject the mismatch.
  ASSERT_GE(framed.size(), 12u);
  framed[8] = 0x04;
  framed[9] = 0x00;
  framed[10] = 0x00;
  framed[11] = 0x00;

  ArrowSchema cSchema;
  exportToArrow(vector, cSchema, ArrowUtils::getBridgeOptions());
  auto deserializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, &cSchema);
  std::vector<int32_t> requestedColumns = {0};

  auto projectedBatch = deserializer->deserializeV3(
      framed.data(), static_cast<int32_t>(framed.size()), std::optional<std::vector<int32_t>>(requestedColumns));
  auto projectedVector = std::dynamic_pointer_cast<VeloxColumnarBatch>(projectedBatch)->getRowVector();

  ASSERT_EQ(projectedVector->size(), 4);
  ASSERT_TRUE(isLazyNotLoaded(*projectedVector->childAt(0)));
  EXPECT_THROW((void)projectedVector->childAt(0)->loadedVector(), GlutenException);
}

TEST_F(VeloxColumnarBatchSerializerTest, deserializeV3ProjectsNonEmptyFrameAsLazyColumns) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();

  std::vector<VectorPtr> children = {
      makeNullableFlatVector<int32_t>({1, 2, std::nullopt, 4, 5}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50}),
      makeFlatVector<StringView>({"a", "bb", "ccc", "dddd", "eeeee"}),
  };
  auto vector = makeRowVector({"a", "b", "c"}, children);
  auto batch = std::make_shared<VeloxColumnarBatch>(vector);

  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);
  std::vector<uint8_t> framed = serializer->framedSerializeV3(batch);

  ArrowSchema cSchema;
  exportToArrow(vector, cSchema, ArrowUtils::getBridgeOptions());
  auto deserializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, &cSchema);
  std::vector<int32_t> requestedColumns = {0, 2};

  auto projectedBatch = deserializer->deserializeV3(
      framed.data(), static_cast<int32_t>(framed.size()), std::optional<std::vector<int32_t>>(requestedColumns));
  auto projectedVector = std::dynamic_pointer_cast<VeloxColumnarBatch>(projectedBatch)->getRowVector();

  ASSERT_EQ(projectedVector->size(), vector->size());
  ASSERT_EQ(projectedVector->childrenSize(), 2u);
  ASSERT_TRUE(isLazyNotLoaded(*projectedVector->childAt(0)));
  ASSERT_TRUE(isLazyNotLoaded(*projectedVector->childAt(1)));

  auto expected = makeRowVector({"a", "c"}, {children[0], children[2]});
  test::assertEqualVectors(expected, projectedVector);
}

} // namespace gluten

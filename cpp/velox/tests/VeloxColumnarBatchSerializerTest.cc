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

// REAL FlatVector: no-NaN partition becomes supported; any NaN row poisons the
// column to hasLowerBound=hasUpperBound=false (Spark NaN != NaN, would silently
// drop matching rows under min/max pruning).
TEST_F(VeloxColumnarBatchSerializerTest, computeStatsNaNRealPartitionPoisons) {
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
  }

  // (b) REAL FlatVector WITH NaN -- must fall back to unsupported.
  {
    const float nan = std::numeric_limits<float>::quiet_NaN();
    std::vector<VectorPtr> children = {
        makeFlatVector<float>({1.5f, nan, 3.5f}),
    };
    auto vector = makeRowVector(children);
    auto stats = serializer->computeStats(vector);
    ASSERT_EQ(stats.size(), 1u);
    EXPECT_FALSE(stats[0].hasLowerBound) << "NaN-poisoned REAL column must emit hasLowerBound=false (NB4)";
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

// NaN-poisoned float column must STILL accrue real nullCount (not
// early-return). framed stats serialize nullCount even when
// emitSupported=0; under-counting on `[NaN, null]` would let
// `col IS NULL` predicates incorrectly prune matching rows under
// Spark IsNull pruning.
TEST_F(VeloxColumnarBatchSerializerTest, computeStatsNaNFloatStillCountsNulls) {
  auto* arrowPool = getDefaultMemoryManager()->defaultArrowMemoryPool();
  auto serializer = std::make_shared<VeloxColumnarBatchSerializer>(arrowPool, pool_, nullptr);

  const float nan = std::numeric_limits<float>::quiet_NaN();
  std::vector<VectorPtr> children = {
      // [1.0, null, NaN, null, 2.0] -- 2 nulls, 1 NaN poisons min/max.
      makeNullableFlatVector<float>({1.0f, std::nullopt, nan, std::nullopt, 2.0f}),
  };
  auto vector = makeRowVector(children);
  auto stats = serializer->computeStats(vector);
  ASSERT_EQ(stats.size(), 1u);
  EXPECT_FALSE(stats[0].hasLowerBound) << "NaN poisons min/max -> unsupported";
  EXPECT_FALSE(stats[0].hasUpperBound);
  EXPECT_EQ(stats[0].nullCount, 2) << "NaN scan must continue and count both nulls (IsNull prune correctness)";
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

} // namespace gluten

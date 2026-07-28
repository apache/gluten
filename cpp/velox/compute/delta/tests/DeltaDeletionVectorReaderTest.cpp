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
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "compute/delta/DeltaDeletionVectorReader.h"
#include "compute/delta/RoaringBitmapArray.h"
#include "velox/common/base/tests/GTestUtils.h"

#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace gluten::delta;

class DeltaDeletionVectorReaderTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool();
  }

  std::string createSerializedPayload(const std::vector<uint64_t>& deletedRows) {
    RoaringBitmapArray bitmap;
    for (auto row : deletedRows) {
      bitmap.addSafe(row);
    }

    const auto serializedSize = bitmap.serializedSizeInBytes();
    auto buffer = AlignedBuffer::allocate<char>(serializedSize, pool_.get());
    bitmap.serialize(buffer->asMutable<char>());
    return std::string(buffer->as<char>(), serializedSize);
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(DeltaDeletionVectorReaderTest, LoadSerializedPayload) {
  auto payload = createSerializedPayload({2, 7, 12});

  DeltaDeletionVectorReader reader;
  reader.loadSerializedDeletionVector(payload, 3);

  EXPECT_TRUE(reader.isRowDeleted(2));
  EXPECT_TRUE(reader.isRowDeleted(7));
  EXPECT_TRUE(reader.isRowDeleted(12));
  EXPECT_FALSE(reader.isRowDeleted(0));
  EXPECT_FALSE(reader.isRowDeleted(3));
  EXPECT_FALSE(reader.isRowDeleted(20));
}

TEST_F(DeltaDeletionVectorReaderTest, LoadPortablePayload) {
  // Captured from a Delta 3.3.2 table after `DELETE WHERE id < 10`.
  const std::vector<uint8_t> payloadBytes = {0xd1, 0xd3, 0x39, 0x64, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                             0x00, 0x00, 0x00, 0x00, 0x00, 0x3b, 0x30, 0x00, 0x00, 0x01, 0x00,
                                             0x00, 0x09, 0x00, 0x01, 0x00, 0x00, 0x00, 0x09, 0x00};

  DeltaDeletionVectorReader reader;
  reader.loadSerializedDeletionVector(
      std::string_view(reinterpret_cast<const char*>(payloadBytes.data()), payloadBytes.size()), 10);

  for (uint64_t deleted = 0; deleted < 10; ++deleted) {
    EXPECT_TRUE(reader.isRowDeleted(deleted));
  }
  EXPECT_FALSE(reader.isRowDeleted(10));
  EXPECT_FALSE(reader.isRowDeleted(100));
}

TEST_F(DeltaDeletionVectorReaderTest, ApplyDeletionFilter) {
  auto payload = createSerializedPayload({2, 5, 8});

  DeltaDeletionVectorReader reader;
  reader.loadSerializedDeletionVector(payload);

  auto deleteBitmap = AlignedBuffer::allocate<uint64_t>(bits::nwords(10), pool_.get());
  reader.applyDeletionFilter(0, 10, deleteBitmap);

  auto* rawBitmap = deleteBitmap->as<uint64_t>();
  EXPECT_TRUE(bits::isBitSet(rawBitmap, 2));
  EXPECT_TRUE(bits::isBitSet(rawBitmap, 5));
  EXPECT_TRUE(bits::isBitSet(rawBitmap, 8));
  EXPECT_FALSE(bits::isBitSet(rawBitmap, 1));
  EXPECT_FALSE(bits::isBitSet(rawBitmap, 7));
}

TEST_F(DeltaDeletionVectorReaderTest, ApplyDeletionFilterWithOffset) {
  auto payload = createSerializedPayload({10, 15, 20});

  DeltaDeletionVectorReader reader;
  reader.loadSerializedDeletionVector(payload);

  auto deleteBitmap = AlignedBuffer::allocate<uint64_t>(bits::nwords(15), pool_.get());
  reader.applyDeletionFilter(10, 15, deleteBitmap);

  auto* rawBitmap = deleteBitmap->as<uint64_t>();
  EXPECT_TRUE(bits::isBitSet(rawBitmap, 0));
  EXPECT_TRUE(bits::isBitSet(rawBitmap, 5));
  EXPECT_TRUE(bits::isBitSet(rawBitmap, 10));
  EXPECT_FALSE(bits::isBitSet(rawBitmap, 1));
  EXPECT_FALSE(bits::isBitSet(rawBitmap, 14));
}

TEST_F(DeltaDeletionVectorReaderTest, EmptyReader) {
  DeltaDeletionVectorReader reader;

  EXPECT_TRUE(reader.empty());
  EXPECT_FALSE(reader.isRowDeleted(0));

  auto deleteBitmap = AlignedBuffer::allocate<uint64_t>(bits::nwords(10), pool_.get());
  reader.applyDeletionFilter(0, 10, deleteBitmap);

  auto* rawBitmap = deleteBitmap->as<uint64_t>();
  for (int i = 0; i < 10; ++i) {
    EXPECT_FALSE(bits::isBitSet(rawBitmap, i));
  }
}

TEST_F(DeltaDeletionVectorReaderTest, MultipleLoadsOverwrite) {
  DeltaDeletionVectorReader reader;
  reader.loadSerializedDeletionVector(createSerializedPayload({1, 2, 3}));
  EXPECT_TRUE(reader.isRowDeleted(1));
  EXPECT_FALSE(reader.isRowDeleted(10));

  reader.loadSerializedDeletionVector(createSerializedPayload({10, 20, 30}));
  EXPECT_FALSE(reader.isRowDeleted(1));
  EXPECT_TRUE(reader.isRowDeleted(10));
}

TEST_F(DeltaDeletionVectorReaderTest, EmptyPayloadThrows) {
  DeltaDeletionVectorReader reader;
  VELOX_ASSERT_THROW(reader.loadSerializedDeletionVector(std::string_view()), "Serialized deletion vector is empty");
}

TEST_F(DeltaDeletionVectorReaderTest, CorruptedMagicNumberThrows) {
  const uint32_t wrongMagic = 12345678;
  const std::string payload(reinterpret_cast<const char*>(&wrongMagic), sizeof(wrongMagic));

  DeltaDeletionVectorReader reader;
  VELOX_ASSERT_THROW(reader.loadSerializedDeletionVector(payload), "Unexpected Delta bitmap array magic number");
}

TEST_F(DeltaDeletionVectorReaderTest, CardinalityValidationSuccess) {
  auto payload = createSerializedPayload({1, 2, 3, 4, 5});

  DeltaDeletionVectorReader reader;
  EXPECT_NO_THROW(reader.loadSerializedDeletionVector(payload, 5));
  EXPECT_EQ(reader.estimatedDeletedRowCount(), 5);
}

TEST_F(DeltaDeletionVectorReaderTest, CardinalityValidationMismatchThrows) {
  auto payload = createSerializedPayload({1, 2, 3, 4, 5});

  DeltaDeletionVectorReader reader;
  EXPECT_THROW(reader.loadSerializedDeletionVector(payload, 3), VeloxUserError);
}

TEST_F(DeltaDeletionVectorReaderTest, LargeCardinalityValidation) {
  std::vector<uint64_t> deletedRows;
  for (uint64_t i = 0; i < 10000; i += 10) {
    deletedRows.push_back(i);
  }

  DeltaDeletionVectorReader reader;
  reader.loadSerializedDeletionVector(createSerializedPayload(deletedRows), 1000);
  EXPECT_EQ(reader.estimatedDeletedRowCount(), 1000);
}

TEST_F(DeltaDeletionVectorReaderTest, BatchFilteringPartialOverlap) {
  std::vector<uint64_t> deletedRows;
  for (uint64_t i = 45; i <= 55; ++i) {
    deletedRows.push_back(i);
  }

  DeltaDeletionVectorReader reader;
  reader.loadSerializedDeletionVector(createSerializedPayload(deletedRows));

  auto deleteBitmap = AlignedBuffer::allocate<uint64_t>(bits::nwords(20), pool_.get());
  reader.applyDeletionFilter(40, 20, deleteBitmap);

  auto* rawBitmap = deleteBitmap->as<uint64_t>();
  for (int i = 0; i < 5; ++i) {
    EXPECT_FALSE(bits::isBitSet(rawBitmap, i));
  }
  for (int i = 5; i <= 15; ++i) {
    EXPECT_TRUE(bits::isBitSet(rawBitmap, i));
  }
  for (int i = 16; i < 20; ++i) {
    EXPECT_FALSE(bits::isBitSet(rawBitmap, i));
  }
}

// --- Corner-case tests for iterator-based applyDeletionFilter ---

TEST_F(DeltaDeletionVectorReaderTest, ApplyDeletionFilterAllRowsDeleted) {
  // Every row in the batch is deleted.
  std::vector<uint64_t> deletedRows;
  for (uint64_t i = 0; i < 100; ++i) {
    deletedRows.push_back(i);
  }

  DeltaDeletionVectorReader reader;
  reader.loadSerializedDeletionVector(createSerializedPayload(deletedRows));

  auto deleteBitmap = AlignedBuffer::allocate<uint64_t>(bits::nwords(100), pool_.get());
  reader.applyDeletionFilter(0, 100, deleteBitmap);

  auto* rawBitmap = deleteBitmap->as<uint64_t>();
  for (int i = 0; i < 100; ++i) {
    EXPECT_TRUE(bits::isBitSet(rawBitmap, i)) << "Row " << i << " should be deleted";
  }
  EXPECT_GT(deleteBitmap->size(), 0);
}

TEST_F(DeltaDeletionVectorReaderTest, ApplyDeletionFilterNoRowsDeleted) {
  // The DV has deletions but none overlap with the batch range.
  auto payload = createSerializedPayload({1000, 2000, 3000});

  DeltaDeletionVectorReader reader;
  reader.loadSerializedDeletionVector(payload);

  auto deleteBitmap = AlignedBuffer::allocate<uint64_t>(bits::nwords(100), pool_.get());
  reader.applyDeletionFilter(0, 100, deleteBitmap);

  auto* rawBitmap = deleteBitmap->as<uint64_t>();
  for (int i = 0; i < 100; ++i) {
    EXPECT_FALSE(bits::isBitSet(rawBitmap, i));
  }
  EXPECT_EQ(deleteBitmap->size(), 0);
}

TEST_F(DeltaDeletionVectorReaderTest, ApplyDeletionFilterFirstAndLastRow) {
  // Only the first and last row of the batch are deleted.
  auto payload = createSerializedPayload({100, 199});

  DeltaDeletionVectorReader reader;
  reader.loadSerializedDeletionVector(payload);

  auto deleteBitmap = AlignedBuffer::allocate<uint64_t>(bits::nwords(100), pool_.get());
  reader.applyDeletionFilter(100, 100, deleteBitmap);

  auto* rawBitmap = deleteBitmap->as<uint64_t>();
  EXPECT_TRUE(bits::isBitSet(rawBitmap, 0));
  EXPECT_TRUE(bits::isBitSet(rawBitmap, 99));
  for (int i = 1; i < 99; ++i) {
    EXPECT_FALSE(bits::isBitSet(rawBitmap, i));
  }
}

TEST_F(DeltaDeletionVectorReaderTest, ApplyDeletionFilterLargeOffset) {
  // Test with large row offsets (beyond 32-bit boundary) to exercise
  // multi-bucket Roaring64Map behavior.
  const uint64_t largeBase = static_cast<uint64_t>(3) << 32;
  std::vector<uint64_t> deletedRows = {largeBase + 10, largeBase + 50, largeBase + 99};

  DeltaDeletionVectorReader reader;
  reader.loadSerializedDeletionVector(createSerializedPayload(deletedRows));

  auto deleteBitmap = AlignedBuffer::allocate<uint64_t>(bits::nwords(100), pool_.get());
  reader.applyDeletionFilter(largeBase, 100, deleteBitmap);

  auto* rawBitmap = deleteBitmap->as<uint64_t>();
  EXPECT_TRUE(bits::isBitSet(rawBitmap, 10));
  EXPECT_TRUE(bits::isBitSet(rawBitmap, 50));
  EXPECT_TRUE(bits::isBitSet(rawBitmap, 99));
  EXPECT_FALSE(bits::isBitSet(rawBitmap, 0));
  EXPECT_FALSE(bits::isBitSet(rawBitmap, 11));
  EXPECT_FALSE(bits::isBitSet(rawBitmap, 49));
}

TEST_F(DeltaDeletionVectorReaderTest, ApplyDeletionFilterSingleRowBatch) {
  // Batch of size 1 where the single row is deleted.
  auto payload = createSerializedPayload({42});

  DeltaDeletionVectorReader reader;
  reader.loadSerializedDeletionVector(payload);

  auto deleteBitmap = AlignedBuffer::allocate<uint64_t>(bits::nwords(1), pool_.get());
  reader.applyDeletionFilter(42, 1, deleteBitmap);

  auto* rawBitmap = deleteBitmap->as<uint64_t>();
  EXPECT_TRUE(bits::isBitSet(rawBitmap, 0));
}

TEST_F(DeltaDeletionVectorReaderTest, ApplyDeletionFilterSingleRowBatchNotDeleted) {
  // Batch of size 1 where the single row is NOT deleted.
  auto payload = createSerializedPayload({42});

  DeltaDeletionVectorReader reader;
  reader.loadSerializedDeletionVector(payload);

  auto deleteBitmap = AlignedBuffer::allocate<uint64_t>(bits::nwords(1), pool_.get());
  reader.applyDeletionFilter(43, 1, deleteBitmap);

  EXPECT_EQ(deleteBitmap->size(), 0);
}

TEST_F(DeltaDeletionVectorReaderTest, ApplyDeletionFilterDenseSparseMixed) {
  // Dense cluster at start, sparse in middle, dense at end.
  std::vector<uint64_t> deletedRows;
  // Dense: rows 0-49
  for (uint64_t i = 0; i < 50; ++i) {
    deletedRows.push_back(i);
  }
  // Sparse: every 100th row from 100-900
  for (uint64_t i = 100; i < 1000; i += 100) {
    deletedRows.push_back(i);
  }
  // Dense: rows 950-999
  for (uint64_t i = 950; i < 1000; ++i) {
    deletedRows.push_back(i);
  }

  DeltaDeletionVectorReader reader;
  reader.loadSerializedDeletionVector(createSerializedPayload(deletedRows));

  // Test middle sparse region
  auto deleteBitmap = AlignedBuffer::allocate<uint64_t>(bits::nwords(200), pool_.get());
  reader.applyDeletionFilter(100, 200, deleteBitmap);

  auto* rawBitmap = deleteBitmap->as<uint64_t>();
  // Should have rows 100 and 200 marked (every 100th from our sparse set)
  EXPECT_TRUE(bits::isBitSet(rawBitmap, 0)); // row 100
  EXPECT_TRUE(bits::isBitSet(rawBitmap, 100)); // row 200
  EXPECT_FALSE(bits::isBitSet(rawBitmap, 1)); // row 101
  EXPECT_FALSE(bits::isBitSet(rawBitmap, 99)); // row 199
}

TEST_F(DeltaDeletionVectorReaderTest, ApplyDeletionFilterBatchBeyondAllDeletions) {
  // Batch starts after all deletions.
  auto payload = createSerializedPayload({1, 2, 3, 4, 5});

  DeltaDeletionVectorReader reader;
  reader.loadSerializedDeletionVector(payload);

  auto deleteBitmap = AlignedBuffer::allocate<uint64_t>(bits::nwords(100), pool_.get());
  reader.applyDeletionFilter(1000, 100, deleteBitmap);

  EXPECT_EQ(deleteBitmap->size(), 0);
}

TEST_F(DeltaDeletionVectorReaderTest, ApplyDeletionFilterBatchBeforeAllDeletions) {
  // Batch ends before any deletions start.
  auto payload = createSerializedPayload({500, 600, 700});

  DeltaDeletionVectorReader reader;
  reader.loadSerializedDeletionVector(payload);

  auto deleteBitmap = AlignedBuffer::allocate<uint64_t>(bits::nwords(100), pool_.get());
  reader.applyDeletionFilter(0, 100, deleteBitmap);

  EXPECT_EQ(deleteBitmap->size(), 0);
}

TEST_F(DeltaDeletionVectorReaderTest, ApplyDeletionFilterOverflowProtection) {
  // Verify that baseReadOffset near UINT64_MAX does not overflow rangeEnd.
  // RoaringBitmapArray only supports row indices up to kMaxRepresentableValue,
  // so we test with low-value bitmap entries but call applyDeletionFilter with
  // a baseReadOffset that would cause baseReadOffset + size to overflow.
  auto payload = createSerializedPayload({10, 20, 30});

  DeltaDeletionVectorReader reader;
  reader.loadSerializedDeletionVector(payload);

  // Request a batch of 100 starting near UINT64_MAX — this would overflow
  // without the saturation guard (nearMax + 100 > UINT64_MAX).
  // No deletions exist in this range, so we just verify no crash and empty
  // result.
  const uint64_t nearMax = UINT64_MAX - 50;
  auto deleteBitmap = AlignedBuffer::allocate<uint64_t>(bits::nwords(100), pool_.get());
  reader.applyDeletionFilter(nearMax, 100, deleteBitmap);

  // No rows in bitmap are near UINT64_MAX, so result should be empty.
  EXPECT_EQ(deleteBitmap->size(), 0);
}

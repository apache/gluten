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
#include "compute/delta/DeltaUuidUtils.h"
#include "compute/delta/RoaringBitmapArray.h"
#include "velox/common/base/Crc.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace gluten::delta;

class DeltaDeletionVectorReaderTest : public ::testing::Test {
 protected:
  struct DeltaStoredDVLocation {
    std::string path;
    uint64_t offset;
    uint64_t sizeInBytes;
  };

  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool();
    ioStats_ = std::make_shared<io::IoStatistics>();
    filesystems::registerLocalFileSystem();
    tempDir_ = exec::test::TempDirectoryPath::create();
  }

  void TearDown() override {
    tempDir_.reset();
  }

  // Helper to create a test DV file with specified deleted rows
  std::string createTestDVFile(const std::vector<int64_t>& deletedRows) {
    // Create roaring bitmap with deleted rows
    RoaringBitmapArray bitmap;
    for (auto row : deletedRows) {
      bitmap.addSafe(row);
    }

    // Serialize to buffer
    auto serializedSize = bitmap.serializedSizeInBytes();
    auto buffer = AlignedBuffer::allocate<char>(serializedSize, pool_.get());
    bitmap.serialize(buffer->asMutable<char>());

    // Write to temp file
    auto dvPath = tempDir_->getPath() + "/test_dv.bin";
    auto fs = filesystems::getFileSystem(dvPath, nullptr);
    auto writeFile = fs->openFileForWrite(dvPath);
    writeFile->append(std::string_view(buffer->as<char>(), serializedSize));
    writeFile->close();

    return dvPath;
  }

  // Helper to create inline DV data
  std::string createInlineDVData(const std::vector<int64_t>& deletedRows) {
    RoaringBitmapArray bitmap;
    for (auto row : deletedRows) {
      bitmap.addSafe(row);
    }

    auto serializedSize = bitmap.serializedSizeInBytes();
    auto buffer = AlignedBuffer::allocate<char>(serializedSize, pool_.get());
    bitmap.serialize(buffer->asMutable<char>());

    return DeltaUuidUtils::encodeBytesToBase85(std::string_view(buffer->as<char>(), serializedSize));
  }

  DeltaStoredDVLocation createDeltaStoredDVFile(const std::vector<int64_t>& deletedRows, bool corruptChecksum = false) {
    RoaringBitmapArray bitmap;
    for (auto row : deletedRows) {
      bitmap.addSafe(row);
    }

    const auto serializedSize = bitmap.serializedSizeInBytes();
    auto buffer = AlignedBuffer::allocate<char>(serializedSize, pool_.get());
    bitmap.serialize(buffer->asMutable<char>());

    bits::Crc32 crc;
    crc.process_bytes(buffer->as<char>(), serializedSize);
    auto checksum = crc.checksum();
    if (corruptChecksum) {
      ++checksum;
    }

    std::string payload;
    payload.reserve(1 + 4 + serializedSize + 4);
    payload.push_back('\x01');

    auto appendBigEndianInt = [&payload](uint32_t value) {
      payload.push_back(static_cast<char>((value >> 24) & 0xff));
      payload.push_back(static_cast<char>((value >> 16) & 0xff));
      payload.push_back(static_cast<char>((value >> 8) & 0xff));
      payload.push_back(static_cast<char>(value & 0xff));
    };

    const auto offset = payload.size();
    appendBigEndianInt(serializedSize);
    payload.append(buffer->as<char>(), serializedSize);
    appendBigEndianInt(checksum);

    auto dvPath = tempDir_->getPath() + "/test_delta_stored_dv.bin";
    auto fs = filesystems::getFileSystem(dvPath, nullptr);
    auto writeFile = fs->openFileForWrite(dvPath);
    writeFile->append(payload);
    writeFile->close();

    return DeltaStoredDVLocation{dvPath, static_cast<uint64_t>(offset), static_cast<uint64_t>(serializedSize)};
  }

  std::shared_ptr<memory::MemoryPool> pool_;
  std::shared_ptr<io::IoStatistics> ioStats_;
  std::shared_ptr<exec::test::TempDirectoryPath> tempDir_;
};

TEST_F(DeltaDeletionVectorReaderTest, LoadFromFile) {
  // Create DV file with rows 5, 10, 15 deleted
  auto dvPath = createTestDVFile({5, 10, 15});

  // Load DV
  auto fs = filesystems::getFileSystem(dvPath, nullptr);
  auto reader = std::make_unique<DeltaDeletionVectorReader>(fs, pool_.get(), ioStats_);
  reader->loadDeletionVector(dvPath);

  // Verify deleted rows
  EXPECT_TRUE(reader->isRowDeleted(5));
  EXPECT_TRUE(reader->isRowDeleted(10));
  EXPECT_TRUE(reader->isRowDeleted(15));

  // Verify non-deleted rows
  EXPECT_FALSE(reader->isRowDeleted(0));
  EXPECT_FALSE(reader->isRowDeleted(4));
  EXPECT_FALSE(reader->isRowDeleted(6));
  EXPECT_FALSE(reader->isRowDeleted(9));
  EXPECT_FALSE(reader->isRowDeleted(11));
  EXPECT_FALSE(reader->isRowDeleted(20));
}

TEST_F(DeltaDeletionVectorReaderTest, LoadFromDeltaStoredRange) {
  const auto storedDv = createDeltaStoredDVFile({5, 10, 15});

  auto fs = filesystems::getFileSystem(storedDv.path, nullptr);
  auto reader = std::make_unique<DeltaDeletionVectorReader>(fs, pool_.get(), ioStats_);
  reader->loadDeletionVector(storedDv.path, storedDv.offset, storedDv.sizeInBytes);

  EXPECT_TRUE(reader->isRowDeleted(5));
  EXPECT_TRUE(reader->isRowDeleted(10));
  EXPECT_TRUE(reader->isRowDeleted(15));

  EXPECT_FALSE(reader->isRowDeleted(0));
  EXPECT_FALSE(reader->isRowDeleted(4));
  EXPECT_FALSE(reader->isRowDeleted(6));
  EXPECT_FALSE(reader->isRowDeleted(20));
}

TEST_F(DeltaDeletionVectorReaderTest, LoadFromRealDeltaPortableStoredRange) {
  // Captured from a Delta 3.3.2 table after `DELETE WHERE id < 10`.
  const std::vector<uint8_t> storedDvBytes = {0x01, 0x00, 0x00, 0x00, 0x1f, 0xd1, 0xd3, 0x39, 0x64, 0x01,
                                              0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                              0x00, 0x3b, 0x30, 0x00, 0x00, 0x01, 0x00, 0x00, 0x09, 0x00,
                                              0x01, 0x00, 0x00, 0x00, 0x09, 0x00, 0x56, 0x05, 0xea, 0x77};

  auto dvPath = tempDir_->getPath() + "/real_delta_stored_dv.bin";
  auto fs = filesystems::getFileSystem(dvPath, nullptr);
  auto writeFile = fs->openFileForWrite(dvPath);
  writeFile->append(std::string_view(reinterpret_cast<const char*>(storedDvBytes.data()), storedDvBytes.size()));
  writeFile->close();

  auto reader = std::make_unique<DeltaDeletionVectorReader>(fs, pool_.get(), ioStats_);
  reader->loadDeletionVector(dvPath, 1, 31, 10);

  for (uint64_t deleted = 0; deleted < 10; ++deleted) {
    EXPECT_TRUE(reader->isRowDeleted(deleted));
  }

  EXPECT_FALSE(reader->isRowDeleted(10));
  EXPECT_FALSE(reader->isRowDeleted(11));
  EXPECT_FALSE(reader->isRowDeleted(100));
}

TEST_F(DeltaDeletionVectorReaderTest, LoadInline) {
  // Create inline DV data with rows 2, 7, 12 deleted
  auto inlineData = createInlineDVData({2, 7, 12});

  // Load inline DV
  auto reader = std::make_unique<DeltaDeletionVectorReader>(nullptr, pool_.get(), ioStats_);
  reader->loadInlineDeletionVector(inlineData);

  // Verify deleted rows
  EXPECT_TRUE(reader->isRowDeleted(2));
  EXPECT_TRUE(reader->isRowDeleted(7));
  EXPECT_TRUE(reader->isRowDeleted(12));

  // Verify non-deleted rows
  EXPECT_FALSE(reader->isRowDeleted(0));
  EXPECT_FALSE(reader->isRowDeleted(1));
  EXPECT_FALSE(reader->isRowDeleted(3));
  EXPECT_FALSE(reader->isRowDeleted(8));
  EXPECT_FALSE(reader->isRowDeleted(20));
}

TEST_F(DeltaDeletionVectorReaderTest, ApplyDeletionFilter) {
  // Create DV with rows 2, 5, 8 deleted
  auto inlineData = createInlineDVData({2, 5, 8});

  auto reader = std::make_unique<DeltaDeletionVectorReader>(nullptr, pool_.get(), ioStats_);
  reader->loadInlineDeletionVector(inlineData);

  // Apply filter to batch [0, 10)
  auto deleteBitmap = AlignedBuffer::allocate<uint64_t>(bits::nwords(10), pool_.get());
  reader->applyDeletionFilter(0, 10, deleteBitmap);

  // Check bitmap - deleted rows should be marked
  auto* rawBitmap = deleteBitmap->as<uint64_t>();
  EXPECT_TRUE(bits::isBitSet(rawBitmap, 2));
  EXPECT_TRUE(bits::isBitSet(rawBitmap, 5));
  EXPECT_TRUE(bits::isBitSet(rawBitmap, 8));

  // Non-deleted rows should not be marked
  EXPECT_FALSE(bits::isBitSet(rawBitmap, 0));
  EXPECT_FALSE(bits::isBitSet(rawBitmap, 1));
  EXPECT_FALSE(bits::isBitSet(rawBitmap, 3));
  EXPECT_FALSE(bits::isBitSet(rawBitmap, 4));
  EXPECT_FALSE(bits::isBitSet(rawBitmap, 6));
  EXPECT_FALSE(bits::isBitSet(rawBitmap, 7));
  EXPECT_FALSE(bits::isBitSet(rawBitmap, 9));
}

TEST_F(DeltaDeletionVectorReaderTest, ApplyDeletionFilterWithOffset) {
  // Create DV with rows 10, 15, 20 deleted
  auto inlineData = createInlineDVData({10, 15, 20});

  auto reader = std::make_unique<DeltaDeletionVectorReader>(nullptr, pool_.get(), ioStats_);
  reader->loadInlineDeletionVector(inlineData);

  // Apply filter to batch [10, 25) - should catch rows 10, 15, 20
  auto deleteBitmap = AlignedBuffer::allocate<uint64_t>(bits::nwords(15), pool_.get());
  reader->applyDeletionFilter(10, 15, deleteBitmap);

  // Check bitmap (relative to batch start)
  auto* rawBitmap = deleteBitmap->as<uint64_t>();
  EXPECT_TRUE(bits::isBitSet(rawBitmap, 0)); // Row 10 (absolute)
  EXPECT_TRUE(bits::isBitSet(rawBitmap, 5)); // Row 15 (absolute)
  EXPECT_TRUE(bits::isBitSet(rawBitmap, 10)); // Row 20 (absolute)

  // Non-deleted rows
  EXPECT_FALSE(bits::isBitSet(rawBitmap, 1)); // Row 11
  EXPECT_FALSE(bits::isBitSet(rawBitmap, 4)); // Row 14
  EXPECT_FALSE(bits::isBitSet(rawBitmap, 6)); // Row 16
  EXPECT_FALSE(bits::isBitSet(rawBitmap, 14)); // Row 24
}

TEST_F(DeltaDeletionVectorReaderTest, EmptyDV) {
  // Create reader without loading any DV
  auto reader = std::make_unique<DeltaDeletionVectorReader>(nullptr, pool_.get(), ioStats_);

  // Should report as empty
  EXPECT_TRUE(reader->empty());

  // No rows should be deleted
  EXPECT_FALSE(reader->isRowDeleted(0));
  EXPECT_FALSE(reader->isRowDeleted(100));
  EXPECT_FALSE(reader->isRowDeleted(1000));

  // Apply filter should mark no rows as deleted
  auto deleteBitmap = AlignedBuffer::allocate<uint64_t>(bits::nwords(10), pool_.get());
  reader->applyDeletionFilter(0, 10, deleteBitmap);

  auto* rawBitmap = deleteBitmap->as<uint64_t>();
  for (int i = 0; i < 10; ++i) {
    EXPECT_FALSE(bits::isBitSet(rawBitmap, i));
  }
}

TEST_F(DeltaDeletionVectorReaderTest, LargeDV) {
  // Create DV with many deleted rows (every 100th row from 0 to 100000)
  std::vector<int64_t> deletedRows;
  for (int64_t i = 0; i < 100000; i += 100) {
    deletedRows.push_back(i);
  }

  auto dvPath = createTestDVFile(deletedRows);

  // Load DV
  auto fs = filesystems::getFileSystem(dvPath, nullptr);
  auto reader = std::make_unique<DeltaDeletionVectorReader>(fs, pool_.get(), ioStats_);
  reader->loadDeletionVector(dvPath);

  // Verify some deleted rows
  EXPECT_TRUE(reader->isRowDeleted(0));
  EXPECT_TRUE(reader->isRowDeleted(100));
  EXPECT_TRUE(reader->isRowDeleted(1000));
  EXPECT_TRUE(reader->isRowDeleted(10000));
  EXPECT_TRUE(reader->isRowDeleted(99900));

  // Verify some non-deleted rows
  EXPECT_FALSE(reader->isRowDeleted(1));
  EXPECT_FALSE(reader->isRowDeleted(99));
  EXPECT_FALSE(reader->isRowDeleted(101));
  EXPECT_FALSE(reader->isRowDeleted(999));
  EXPECT_FALSE(reader->isRowDeleted(99999));
}

TEST_F(DeltaDeletionVectorReaderTest, BatchFilteringLargeDV) {
  // Create DV with every 10th row deleted from 0 to 1000
  std::vector<int64_t> deletedRows;
  for (int64_t i = 0; i < 1000; i += 10) {
    deletedRows.push_back(i);
  }

  auto inlineData = createInlineDVData(deletedRows);
  auto reader = std::make_unique<DeltaDeletionVectorReader>(nullptr, pool_.get(), ioStats_);
  reader->loadInlineDeletionVector(inlineData);

  // Apply filter to batch [0, 100)
  auto deleteBitmap = AlignedBuffer::allocate<uint64_t>(bits::nwords(100), pool_.get());
  reader->applyDeletionFilter(0, 100, deleteBitmap);

  // Verify deleted rows (0, 10, 20, ..., 90)
  auto* rawBitmap = deleteBitmap->as<uint64_t>();
  for (int i = 0; i < 100; ++i) {
    if (i % 10 == 0) {
      EXPECT_TRUE(bits::isBitSet(rawBitmap, i)) << "Row " << i;
    } else {
      EXPECT_FALSE(bits::isBitSet(rawBitmap, i)) << "Row " << i;
    }
  }
}

TEST_F(DeltaDeletionVectorReaderTest, InvalidFileThrows) {
  auto fs = filesystems::getFileSystem(tempDir_->getPath(), nullptr);
  auto reader = std::make_unique<DeltaDeletionVectorReader>(fs, pool_.get(), ioStats_);

  // Non-existent file should throw
  VELOX_ASSERT_THROW(
      reader->loadDeletionVector(tempDir_->getPath() + "/nonexistent.bin"), "Failed to load deletion vector");
}

TEST_F(DeltaDeletionVectorReaderTest, DeltaStoredRangeChecksumMismatchThrows) {
  const auto storedDv = createDeltaStoredDVFile({1, 2, 3}, true);
  auto fs = filesystems::getFileSystem(storedDv.path, nullptr);
  auto reader = std::make_unique<DeltaDeletionVectorReader>(fs, pool_.get(), ioStats_);

  VELOX_ASSERT_THROW(
      reader->loadDeletionVector(storedDv.path, storedDv.offset, storedDv.sizeInBytes), "checksum mismatch");
}

TEST_F(DeltaDeletionVectorReaderTest, InvalidInlineDataThrows) {
  auto reader = std::make_unique<DeltaDeletionVectorReader>(nullptr, pool_.get(), ioStats_);

  // Invalid base64 should throw
  VELOX_ASSERT_THROW(reader->loadInlineDeletionVector("not-valid-base64!!!"), "Failed to load inline deletion vector");
}

TEST_F(DeltaDeletionVectorReaderTest, EmptyFileThrows) {
  // Create empty file
  auto dvPath = tempDir_->getPath() + "/empty_dv.bin";
  auto fs = filesystems::getFileSystem(dvPath, nullptr);
  auto writeFile = fs->openFileForWrite(dvPath);
  writeFile->close();

  auto reader = std::make_unique<DeltaDeletionVectorReader>(fs, pool_.get(), ioStats_);

  // Empty file should throw
  VELOX_ASSERT_THROW(reader->loadDeletionVector(dvPath), "Deletion vector file is empty");
}

TEST_F(DeltaDeletionVectorReaderTest, CorruptedMagicNumberThrows) {
  // Create file with wrong magic number
  auto dvPath = tempDir_->getPath() + "/corrupt_dv.bin";
  auto fs = filesystems::getFileSystem(dvPath, nullptr);
  auto writeFile = fs->openFileForWrite(dvPath);

  // Write wrong magic number
  int32_t wrongMagic = 12345678;
  writeFile->append(std::string_view(reinterpret_cast<const char*>(&wrongMagic), sizeof(wrongMagic)));
  writeFile->close();

  auto reader = std::make_unique<DeltaDeletionVectorReader>(fs, pool_.get(), ioStats_);

  // Should throw on deserialization
  VELOX_ASSERT_THROW(reader->loadDeletionVector(dvPath), "Unexpected magic number");
}

TEST_F(DeltaDeletionVectorReaderTest, MultipleLoadsOverwrite) {
  // Create two different DVs
  auto inlineData1 = createInlineDVData({1, 2, 3});
  auto inlineData2 = createInlineDVData({10, 20, 30});

  auto reader = std::make_unique<DeltaDeletionVectorReader>(nullptr, pool_.get(), ioStats_);

  // Load first DV
  reader->loadInlineDeletionVector(inlineData1);
  EXPECT_TRUE(reader->isRowDeleted(1));
  EXPECT_FALSE(reader->isRowDeleted(10));

  // Load second DV (should overwrite)
  reader->loadInlineDeletionVector(inlineData2);
  EXPECT_FALSE(reader->isRowDeleted(1)); // No longer deleted
  EXPECT_TRUE(reader->isRowDeleted(10)); // Now deleted
}

TEST_F(DeltaDeletionVectorReaderTest, IOStatisticsTracking) {
  auto dvPath = createTestDVFile({1, 2, 3});
  auto fs = filesystems::getFileSystem(dvPath, nullptr);

  // Get file size for verification
  auto readFile = fs->openFileForRead(dvPath);
  auto fileSize = readFile->size();

  auto reader = std::make_unique<DeltaDeletionVectorReader>(fs, pool_.get(), ioStats_);

  // Load DV
  reader->loadDeletionVector(dvPath);

  // Verify I/O stats were updated
  EXPECT_EQ(ioStats_->rawBytesRead(), fileSize);
  EXPECT_GT(ioStats_->queryThreadIoLatencyUs().count(), 0);
}

TEST_F(DeltaDeletionVectorReaderTest, EstimatedDeletedRowCount) {
  auto inlineData = createInlineDVData({1, 2, 3, 4, 5});

  auto reader = std::make_unique<DeltaDeletionVectorReader>(nullptr, pool_.get(), ioStats_);

  // Empty reader should return 0
  EXPECT_EQ(reader->estimatedDeletedRowCount(), 0);

  // After loading, should return non-zero estimate
  reader->loadInlineDeletionVector(inlineData);
  EXPECT_GT(reader->estimatedDeletedRowCount(), 0);
}

TEST_F(DeltaDeletionVectorReaderTest, SparseDeletes) {
  // Create DV with sparse deletes (rows 0, 1000, 2000, 3000)
  auto inlineData = createInlineDVData({0, 1000, 2000, 3000});

  auto reader = std::make_unique<DeltaDeletionVectorReader>(nullptr, pool_.get(), ioStats_);
  reader->loadInlineDeletionVector(inlineData);

  // Verify sparse deletes
  EXPECT_TRUE(reader->isRowDeleted(0));
  EXPECT_TRUE(reader->isRowDeleted(1000));
  EXPECT_TRUE(reader->isRowDeleted(2000));
  EXPECT_TRUE(reader->isRowDeleted(3000));

  // Verify non-deleted rows in between
  EXPECT_FALSE(reader->isRowDeleted(500));
  EXPECT_FALSE(reader->isRowDeleted(1500));
  EXPECT_FALSE(reader->isRowDeleted(2500));
  EXPECT_FALSE(reader->isRowDeleted(3500));
}

TEST_F(DeltaDeletionVectorReaderTest, BatchFilteringNoOverlap) {
  // Create DV with rows 100-110 deleted
  std::vector<int64_t> deletedRows;
  for (int64_t i = 100; i <= 110; ++i) {
    deletedRows.push_back(i);
  }
  auto inlineData = createInlineDVData(deletedRows);

  auto reader = std::make_unique<DeltaDeletionVectorReader>(nullptr, pool_.get(), ioStats_);
  reader->loadInlineDeletionVector(inlineData);

  // Apply filter to batch [0, 50) - no overlap with deleted rows
  auto deleteBitmap = AlignedBuffer::allocate<uint64_t>(bits::nwords(50), pool_.get());
  reader->applyDeletionFilter(0, 50, deleteBitmap);

  // No rows should be marked as deleted
  auto* rawBitmap = deleteBitmap->as<uint64_t>();
  for (int i = 0; i < 50; ++i) {
    EXPECT_FALSE(bits::isBitSet(rawBitmap, i)) << "Row " << i;
  }
}

TEST_F(DeltaDeletionVectorReaderTest, BatchFilteringPartialOverlap) {
  // Create DV with rows 45-55 deleted
  std::vector<int64_t> deletedRows;
  for (int64_t i = 45; i <= 55; ++i) {
    deletedRows.push_back(i);
  }
  auto inlineData = createInlineDVData(deletedRows);

  auto reader = std::make_unique<DeltaDeletionVectorReader>(nullptr, pool_.get(), ioStats_);
  reader->loadInlineDeletionVector(inlineData);

  // Apply filter to batch [40, 60) - partial overlap
  auto deleteBitmap = AlignedBuffer::allocate<uint64_t>(bits::nwords(20), pool_.get());
  reader->applyDeletionFilter(40, 20, deleteBitmap);

  // Check bitmap
  auto* rawBitmap = deleteBitmap->as<uint64_t>();

  // Rows 40-44 should not be deleted (relative positions 0-4)
  for (int i = 0; i < 5; ++i) {
    EXPECT_FALSE(bits::isBitSet(rawBitmap, i)) << "Row " << (40 + i);
  }

  // Rows 45-55 should be deleted (relative positions 5-15)
  for (int i = 5; i <= 15; ++i) {
    EXPECT_TRUE(bits::isBitSet(rawBitmap, i)) << "Row " << (40 + i);
  }

  // Rows 56-59 should not be deleted (relative positions 16-19)
  for (int i = 16; i < 20; ++i) {
    EXPECT_FALSE(bits::isBitSet(rawBitmap, i)) << "Row " << (40 + i);
  }
}

TEST_F(DeltaDeletionVectorReaderTest, AllRowsDeleted) {
  // Create DV with all rows in batch deleted
  std::vector<int64_t> deletedRows;
  for (int64_t i = 0; i < 100; ++i) {
    deletedRows.push_back(i);
  }
  auto inlineData = createInlineDVData(deletedRows);

  auto reader = std::make_unique<DeltaDeletionVectorReader>(nullptr, pool_.get(), ioStats_);
  reader->loadInlineDeletionVector(inlineData);

  // Apply filter to batch [0, 100)
  auto deleteBitmap = AlignedBuffer::allocate<uint64_t>(bits::nwords(100), pool_.get());
  reader->applyDeletionFilter(0, 100, deleteBitmap);

  // All rows should be marked as deleted
  auto* rawBitmap = deleteBitmap->as<uint64_t>();
  for (int i = 0; i < 100; ++i) {
    EXPECT_TRUE(bits::isBitSet(rawBitmap, i)) << "Row " << i;
  }
}
TEST_F(DeltaDeletionVectorReaderTest, CardinalityValidationSuccess) {
  // Create DV with known cardinality
  auto inlineData = createInlineDVData({1, 2, 3, 4, 5});

  auto reader = std::make_unique<DeltaDeletionVectorReader>(nullptr, pool_.get(), ioStats_);

  // Should succeed with correct cardinality
  EXPECT_NO_THROW(reader->loadInlineDeletionVector(inlineData, std::nullopt, 5));

  // Verify cardinality
  EXPECT_EQ(reader->estimatedDeletedRowCount(), 5);
}

TEST_F(DeltaDeletionVectorReaderTest, CardinalityValidationMismatchThrows) {
  // Create DV with 5 deleted rows
  auto inlineData = createInlineDVData({1, 2, 3, 4, 5});

  auto reader = std::make_unique<DeltaDeletionVectorReader>(nullptr, pool_.get(), ioStats_);

  // Should throw with incorrect cardinality
  EXPECT_THROW(reader->loadInlineDeletionVector(inlineData, std::nullopt, 3), VeloxUserError);
}

TEST_F(DeltaDeletionVectorReaderTest, CardinalityValidationFileSuccess) {
  // Create DV file with known cardinality
  const auto storedDv = createDeltaStoredDVFile({10, 20, 30});

  auto fs = filesystems::getFileSystem(storedDv.path, nullptr);
  auto reader = std::make_unique<DeltaDeletionVectorReader>(fs, pool_.get(), ioStats_);

  // Should succeed with correct cardinality
  EXPECT_NO_THROW(reader->loadDeletionVector(storedDv.path, storedDv.offset, storedDv.sizeInBytes, 3));

  EXPECT_EQ(reader->estimatedDeletedRowCount(), 3);
}

TEST_F(DeltaDeletionVectorReaderTest, CardinalityValidationFileMismatchThrows) {
  // Create DV file with 3 deleted rows
  const auto storedDv = createDeltaStoredDVFile({10, 20, 30});

  auto fs = filesystems::getFileSystem(storedDv.path, nullptr);
  auto reader = std::make_unique<DeltaDeletionVectorReader>(fs, pool_.get(), ioStats_);

  // Should throw with incorrect cardinality
  EXPECT_THROW(reader->loadDeletionVector(storedDv.path, storedDv.offset, storedDv.sizeInBytes, 5), VeloxUserError);
}

TEST_F(DeltaDeletionVectorReaderTest, EstimatedDeletedRowCountUsesCardinality) {
  // Create DV with sparse deletes
  auto inlineData = createInlineDVData({0, 1000, 2000, 3000, 4000});

  auto reader = std::make_unique<DeltaDeletionVectorReader>(nullptr, pool_.get(), ioStats_);
  reader->loadInlineDeletionVector(inlineData);

  // Should return actual cardinality (5), not estimated from size
  EXPECT_EQ(reader->estimatedDeletedRowCount(), 5);
}

TEST_F(DeltaDeletionVectorReaderTest, LargeCardinalityValidation) {
  // Create DV with many deleted rows
  std::vector<int64_t> deletedRows;
  for (int i = 0; i < 10000; i += 10) {
    deletedRows.push_back(i);
  }

  auto inlineData = createInlineDVData(deletedRows);
  auto reader = std::make_unique<DeltaDeletionVectorReader>(nullptr, pool_.get(), ioStats_);

  // Should succeed with correct cardinality
  EXPECT_NO_THROW(reader->loadInlineDeletionVector(inlineData, std::nullopt, 1000));
  EXPECT_EQ(reader->estimatedDeletedRowCount(), 1000);
}

// Made with Bob

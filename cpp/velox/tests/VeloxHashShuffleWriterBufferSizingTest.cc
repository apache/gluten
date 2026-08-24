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

#include "shuffle/VeloxHashShuffleWriter.h"

#include "VeloxShuffleWriterTestBase.h"
#include "utils/Macros.h"
#include "utils/TestUtils.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace gluten {

namespace {

std::shared_ptr<PartitionWriter> makeLocalPartitionWriter(
    uint32_t numPartitions,
    const std::string& dataFile,
    const std::vector<std::string>& localDirs) {
  GLUTEN_ASSIGN_OR_THROW(auto codec, arrow::util::Codec::Create(arrow::Compression::LZ4_FRAME));
  auto options = std::make_shared<LocalPartitionWriterOptions>();
  return std::make_shared<LocalPartitionWriter>(
      numPartitions, std::move(codec), getDefaultMemoryManager(), options, dataFile, localDirs);
}

} // namespace

// Verifies that calculateSimpleColumnBytes() estimates each fixed-width column with the same
// per-row width the partition buffers are actually allocated with (see
// valueBufferSizeForFixedWidthArray). Short decimal is stored as int64 (8 bytes, not the 16 of
// arrow::bit_width(Decimal128)), and timestamp is stored as int128 (16 bytes, not the 8 of
// arrow::bit_width(Timestamp)).
class HashShuffleWriterBufferSizingTest : public ::testing::Test, public VeloxShuffleWriterTestBase {
 protected:
  static void SetUpTestSuite() {
    setUpVeloxBackend();
  }

  static void TearDownTestSuite() {
    tearDownVeloxBackend();
  }

  void SetUp() override {
    GLUTEN_THROW_NOT_OK(setLocalDirsAndDataFile());
  }

  std::shared_ptr<VeloxHashShuffleWriter> createWriter(uint32_t numPartitions) {
    auto options = std::make_shared<HashShuffleWriterOptions>();
    options->partitioning = Partitioning::kHash;
    options->splitBufferSize = 4096;

    auto partitionWriter = makeLocalPartitionWriter(numPartitions, dataFile_, localDirs_);

    GLUTEN_ASSIGN_OR_THROW(
        auto base,
        VeloxShuffleWriter::create(
            ShuffleWriterType::kHashShuffle, numPartitions, partitionWriter, options, getDefaultMemoryManager()));
    return std::dynamic_pointer_cast<VeloxHashShuffleWriter>(base);
  }

  // Writes a single batch whose first child is the hash partition key (stripped before the
  // writer initializes column types), then returns the writer's per-row fixed-width estimate
  // for the remaining data columns.
  uint32_t bytesPerRowFor(std::vector<facebook::velox::VectorPtr> dataChildren) {
    auto writer = createWriter(2);
    EXPECT_NE(writer, nullptr);

    std::vector<facebook::velox::VectorPtr> children;
    children.push_back(makeFlatVector<int32_t>({0, 1}));
    children.insert(children.end(), dataChildren.begin(), dataChildren.end());
    auto rv = makeRowVector(children);

    std::shared_ptr<ColumnarBatch> cb = std::make_shared<VeloxColumnarBatch>(rv);
    EXPECT_TRUE(writer->write(cb, ShuffleWriter::kMinMemLimit).ok());
    return writer->fixedWidthBufferBytes();
  }
};

// Short decimal is split and allocated as int64: 8 bytes per row, not the 16 bytes implied by
// arrow::bit_width(Decimal128Type).
TEST_F(HashShuffleWriterBufferSizingTest, shortDecimalCountedAsInt64) {
  auto bytesPerRow = bytesPerRowFor({
      makeFlatVector<int64_t>({232, 34567235}, facebook::velox::DECIMAL(12, 4)),
  });
  EXPECT_EQ(bytesPerRow, 8);
}

// Long decimal really is 16 bytes per row.
TEST_F(HashShuffleWriterBufferSizingTest, longDecimalCountedAsInt128) {
  auto bytesPerRow = bytesPerRowFor({
      makeFlatVector<facebook::velox::int128_t>({232, 34567235}, facebook::velox::DECIMAL(20, 4)),
  });
  EXPECT_EQ(bytesPerRow, 16);
}

// Timestamp is split and allocated as int128 (velox Timestamp is 16 bytes), not the 8 bytes
// implied by arrow::bit_width(TimestampType).
TEST_F(HashShuffleWriterBufferSizingTest, timestampCountedAsInt128) {
  auto bytesPerRow = bytesPerRowFor({
      makeFlatVector<facebook::velox::Timestamp>({facebook::velox::Timestamp(1, 0), facebook::velox::Timestamp(2, 0)}),
  });
  EXPECT_EQ(bytesPerRow, static_cast<uint32_t>(sizeof(facebook::velox::Timestamp)));
}

// Mixed schema: the estimate is the sum of the widths the buffers are allocated with, plus the
// length-buffer width for each binary column.
TEST_F(HashShuffleWriterBufferSizingTest, mixedSchema) {
  auto bytesPerRow = bytesPerRowFor({
      makeFlatVector<bool>({true, false}),
      makeFlatVector<int8_t>({1, 2}),
      makeFlatVector<int32_t>({1, 2}),
      makeFlatVector<int64_t>({1, 2}),
      makeFlatVector<double>({1.0, 2.0}),
      makeFlatVector<int64_t>({232, 34567235}, facebook::velox::DECIMAL(12, 4)),
      makeFlatVector<facebook::velox::int128_t>({232, 34567235}, facebook::velox::DECIMAL(20, 4)),
      makeFlatVector<facebook::velox::Timestamp>({facebook::velox::Timestamp(1, 0), facebook::velox::Timestamp(2, 0)}),
      makeFlatVector<facebook::velox::StringView>({"a", "bb"}),
  });
  uint32_t expected = 1 // bool, rounded up to one byte per row
      + 1 // int8
      + 4 // int32
      + 8 // int64
      + 8 // double
      + 8 // short decimal, stored as int64
      + 16 // long decimal
      + 16 // timestamp, stored as int128
      + kSizeOfStringLength; // varchar length buffer
  EXPECT_EQ(bytesPerRow, expected);
}

} // namespace gluten

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

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

class HashShuffleWriterInputEncodingTest : public ::testing::Test, public VeloxShuffleWriterTestBase {
 protected:
  static void SetUpTestSuite() {
    setUpVeloxBackend();
  }

  static void TearDownTestSuite() {
    tearDownVeloxBackend();
  }

  void SetUp() override {
    VeloxShuffleWriterTestBase::setUpTestData();
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

  // Wrap a RowVector into a VeloxColumnarBatch and feed it to the writer
  // (no flatten before the call — the encoding counts are captured on the
  // batch as-is).
  arrow::Status writeBatch(VeloxHashShuffleWriter& writer, facebook::velox::RowVectorPtr rv) {
    std::shared_ptr<ColumnarBatch> cb = std::make_shared<VeloxColumnarBatch>(rv);
    return writer.write(cb, ShuffleWriter::kMinMemLimit);
  }
};

// All-flat input: every child increments the FLAT bucket, other buckets stay 0.
TEST_F(HashShuffleWriterInputEncodingTest, allFlat) {
  auto writer = createWriter(2);
  ASSERT_NE(writer, nullptr);

  // Two batches with 3 flat children each (first is the partition-key column
  // required by hash partitioning).
  for (int i = 0; i < 2; ++i) {
    auto rv = makeRowVector({
        makeFlatVector<int32_t>({0, 1, 0, 1}),
        makeFlatVector<int32_t>({10, 20, 30, 40}),
        makeFlatVector<int64_t>({100, 200, 300, 400}),
    });
    ASSERT_NOT_OK(writeBatch(*writer, rv));
  }

  const auto& counts = writer->inputEncodingCounts();
  EXPECT_EQ(counts[VeloxHashShuffleWriter::kInputEncodingFlat], 6);
  EXPECT_EQ(counts[VeloxHashShuffleWriter::kInputEncodingDictionary], 0);
  EXPECT_EQ(counts[VeloxHashShuffleWriter::kInputEncodingConstant], 0);
  EXPECT_EQ(counts[VeloxHashShuffleWriter::kInputEncodingLazy], 0);
  EXPECT_EQ(counts[VeloxHashShuffleWriter::kInputEncodingComplex], 0);
  EXPECT_EQ(counts[VeloxHashShuffleWriter::kInputEncodingOther], 0);
  EXPECT_EQ(writer->inputEncodingSkippedBatches(), 0);
}

// Mixed flat + dictionary + constant in a single batch: one increment per bucket.
TEST_F(HashShuffleWriterInputEncodingTest, mixedFlatDictConst) {
  auto writer = createWriter(2);
  ASSERT_NE(writer, nullptr);

  // dict-encoded VARCHAR child
  auto dictBase = makeFlatVector<facebook::velox::StringView>({"a", "b", "c", "d"});
  auto indices = makeIndices({0, 1, 2, 3});
  auto dictChild = facebook::velox::BaseVector::wrapInDictionary(nullptr, indices, 4, dictBase);

  // constant int32 child
  auto constChild = facebook::velox::BaseVector::createConstant(
      facebook::velox::INTEGER(), facebook::velox::variant(int32_t{42}), 4, pool());

  auto rv = makeRowVector({
      makeFlatVector<int32_t>({0, 1, 0, 1}), // partition key (FLAT)
      makeFlatVector<int32_t>({10, 20, 30, 40}), // FLAT data
      dictChild, // DICTIONARY
      constChild, // CONSTANT
  });
  ASSERT_NOT_OK(writeBatch(*writer, rv));

  const auto& counts = writer->inputEncodingCounts();
  EXPECT_EQ(counts[VeloxHashShuffleWriter::kInputEncodingFlat], 2);
  EXPECT_EQ(counts[VeloxHashShuffleWriter::kInputEncodingDictionary], 1);
  EXPECT_EQ(counts[VeloxHashShuffleWriter::kInputEncodingConstant], 1);
  EXPECT_EQ(counts[VeloxHashShuffleWriter::kInputEncodingLazy], 0);
  EXPECT_EQ(counts[VeloxHashShuffleWriter::kInputEncodingComplex], 0);
  EXPECT_EQ(counts[VeloxHashShuffleWriter::kInputEncodingOther], 0);
  EXPECT_EQ(writer->inputEncodingSkippedBatches(), 0);
}

// Lazy child should land in the LAZY bucket (encoding is reported before
// any loadedVector() call).
TEST_F(HashShuffleWriterInputEncodingTest, lazy) {
  auto writer = createWriter(2);
  ASSERT_NE(writer, nullptr);

  auto lazyChild = std::make_shared<facebook::velox::LazyVector>(
      pool(),
      facebook::velox::BIGINT(),
      4,
      std::make_unique<facebook::velox::test::SimpleVectorLoader>([&](facebook::velox::RowSet /*rows*/) {
        return makeFlatVector<int64_t>({100, 200, 300, 400});
      }));

  auto rv = makeRowVector({
      makeFlatVector<int32_t>({0, 1, 0, 1}), // partition key
      lazyChild,
  });
  ASSERT_NOT_OK(writeBatch(*writer, rv));

  const auto& counts = writer->inputEncodingCounts();
  EXPECT_EQ(counts[VeloxHashShuffleWriter::kInputEncodingFlat], 1);
  EXPECT_EQ(counts[VeloxHashShuffleWriter::kInputEncodingLazy], 1);
  EXPECT_EQ(counts[VeloxHashShuffleWriter::kInputEncodingDictionary], 0);
  EXPECT_EQ(counts[VeloxHashShuffleWriter::kInputEncodingConstant], 0);
  EXPECT_EQ(counts[VeloxHashShuffleWriter::kInputEncodingComplex], 0);
  EXPECT_EQ(counts[VeloxHashShuffleWriter::kInputEncodingOther], 0);
  EXPECT_EQ(writer->inputEncodingSkippedBatches(), 0);
}

// ARRAY / MAP / ROW children land in the COMPLEX bucket, not OTHER. The sibling
// `VeloxShuffleWriterTest` exercises these via `makeArrayVector` / `makeMapVector`,
// so this is the typical-Spark-workload case, not an edge case.
TEST_F(HashShuffleWriterInputEncodingTest, complex) {
  auto writer = createWriter(2);
  ASSERT_NE(writer, nullptr);

  auto arrayChild = makeArrayVector<int64_t>({{1, 2}, {3, 4}, {5}, {6, 7, 8}});
  auto mapChild = makeMapVector<int32_t, facebook::velox::StringView>(
      {{{1, "a"}, {2, "b"}}, {{3, "c"}}, {{4, "d"}, {5, "e"}}, {{6, "f"}}});

  auto rv = makeRowVector({
      makeFlatVector<int32_t>({0, 1, 0, 1}), // partition key (FLAT)
      arrayChild, // ARRAY
      mapChild, // MAP
  });
  ASSERT_NOT_OK(writeBatch(*writer, rv));

  const auto& counts = writer->inputEncodingCounts();
  EXPECT_EQ(counts[VeloxHashShuffleWriter::kInputEncodingFlat], 1);
  EXPECT_EQ(counts[VeloxHashShuffleWriter::kInputEncodingComplex], 2);
  EXPECT_EQ(counts[VeloxHashShuffleWriter::kInputEncodingDictionary], 0);
  EXPECT_EQ(counts[VeloxHashShuffleWriter::kInputEncodingConstant], 0);
  EXPECT_EQ(counts[VeloxHashShuffleWriter::kInputEncodingLazy], 0);
  EXPECT_EQ(counts[VeloxHashShuffleWriter::kInputEncodingOther], 0);
  EXPECT_EQ(writer->inputEncodingSkippedBatches(), 0);
}

} // namespace gluten

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

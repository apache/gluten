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

#include "compute/iceberg/IcebergWriter.h"

#include <folly/json.h>
#include <gtest/gtest.h>

#include "memory/VeloxColumnarBatch.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
namespace gluten {
namespace {

constexpr int32_t kParquetFormat = 1;

class VeloxIcebergWriteTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    parquet::registerParquetWriterFactory();
    Type::registerSerDe();
    dwio::common::registerFileSinks();
    filesystems::registerLocalFileSystem();
  }

  std::shared_ptr<const connector::hive::iceberg::IcebergPartitionSpec> unpartitionedSpec() const {
    return std::make_shared<const connector::hive::iceberg::IcebergPartitionSpec>(
        0, std::vector<connector::hive::iceberg::IcebergPartitionSpec::Field>{});
  }

  IcebergNestedField fieldIds(size_t numFields) const {
    IcebergNestedField root;
    root.set_id(0);
    for (size_t i = 0; i < numFields; ++i) {
      root.add_children()->set_id(i + 1);
    }
    return root;
  }

  std::unique_ptr<IcebergWriter>
  makeWriter(const RowTypePtr& dataType, const std::string& outputDirectory, IcebergNativeWriteOptions options = {}) {
    return std::make_unique<IcebergWriter>(
        dataType,
        kParquetFormat,
        outputDirectory,
        common::CompressionKind::CompressionKind_ZSTD,
        0,
        0,
        folly::to<std::string>(folly::Random::rand64()),
        unpartitionedSpec(),
        fieldIds(dataType->size()),
        std::unordered_map<std::string, std::string>(),
        pool_,
        connectorPool_,
        std::move(options));
  }

  RowVectorPtr makeMutationInput(
      const std::vector<int32_t>& operations,
      const std::vector<std::optional<int64_t>>& ids,
      const std::vector<std::optional<StringView>>& names,
      const std::vector<std::optional<StringView>>& filePaths,
      const std::vector<std::optional<int64_t>>& positions) {
    return makeRowVector(
        {"operation", "id", "name", "file_path", "pos"},
        {makeFlatVector<int32_t>(operations),
         makeNullableFlatVector<int64_t>(ids),
         makeNullableFlatVector<StringView>(names),
         makeNullableFlatVector<StringView>(filePaths),
         makeNullableFlatVector<int64_t>(positions)});
  }

  static folly::dynamic findCommitMessage(const std::vector<std::string>& messages, const std::string& content) {
    for (const auto& message : messages) {
      auto json = folly::parseJson(message);
      if (json.getDefault("content", "").asString() == content) {
        return json;
      }
    }
    return nullptr;
  }

  std::shared_ptr<exec::test::TempDirectoryPath> tmpDir_{exec::test::TempDirectoryPath::create()};
  std::shared_ptr<memory::MemoryPool> connectorPool_ = rootPool_->addAggregateChild("connector");
};

TEST_F(VeloxIcebergWriteTest, parsesNativeWriteDescriptor) {
  IcebergNativeWriteInfo proto;
  proto.set_write_mode(ICEBERG_NATIVE_WRITE_MODE_MERGE);
  proto.add_data_column_indices(1);
  proto.add_data_column_indices(2);
  proto.set_operation_column_index(0);
  proto.set_file_path_column_index(3);
  proto.set_row_position_column_index(4);
  auto* existing = proto.add_existing_deletion_vectors();
  existing->set_referenced_data_file("data.parquet");
  existing->set_puffin_path("deletes.puffin");
  existing->set_content_offset(16);
  existing->set_content_length(32);
  existing->set_record_count(2);
  existing->set_file_size_in_bytes(128);

  const auto options = parseIcebergNativeWriteInfo(proto);
  EXPECT_EQ(options.writeMode, IcebergWriteMode::kMerge);
  EXPECT_EQ(options.dataColumnIndices, std::vector<column_index_t>({1, 2}));
  EXPECT_EQ(options.operationColumnIndex, 0);
  EXPECT_EQ(options.filePathColumnIndex, 3);
  EXPECT_EQ(options.rowPositionColumnIndex, 4);
  ASSERT_EQ(options.existingDeletionVectors.size(), 1);
  EXPECT_EQ(options.existingDeletionVectors[0].referencedDataFile, "data.parquet");
  EXPECT_EQ(options.existingDeletionVectors[0].puffinPath, "deletes.puffin");
  EXPECT_EQ(options.existingDeletionVectors[0].contentOffset, 16);
  EXPECT_EQ(options.existingDeletionVectors[0].contentLength, 32);
  EXPECT_EQ(options.existingDeletionVectors[0].recordCount, 2);
  EXPECT_EQ(options.existingDeletionVectors[0].fileSizeInBytes, 128);
}

TEST_F(VeloxIcebergWriteTest, writesDataWithDefaultDescriptor) {
  auto vector = makeRowVector({"id", "value"}, {makeFlatVector<int8_t>({1, 2}), makeFlatVector<int16_t>({1, 2})});
  auto writer = makeWriter(asRowType(vector->type()), tmpDir_->getPath());

  writer->write(VeloxColumnarBatch(vector));
  const auto commitMessages = writer->commit();

  ASSERT_EQ(commitMessages.size(), 1);
  EXPECT_EQ(folly::parseJson(commitMessages[0])["content"].asString(), "DATA");
}

TEST_F(VeloxIcebergWriteTest, normalizesSparkUpdateAndWritesMixedDataAndDeletionVector) {
  const auto dataType = ROW({"id", "name"}, {BIGINT(), VARCHAR()});
  IcebergNativeWriteOptions options;
  options.writeMode = IcebergWriteMode::kMerge;
  options.dataColumnIndices = {1, 2};
  options.operationColumnIndex = 0;
  options.filePathColumnIndex = 3;
  options.rowPositionColumnIndex = 4;
  auto writer = makeWriter(dataType, tmpDir_->getPath(), options);
  const auto referencedDataFile = tmpDir_->getPath() + "/existing.parquet";
  auto input = makeMutationInput(
      {2, 3, 4, 1},
      {10, 20, 25, 30},
      {StringView("updated"), StringView("inserted"), StringView("reinserted"), StringView("ignored")},
      {StringView(referencedDataFile), std::nullopt, std::nullopt, StringView(referencedDataFile)},
      {5, std::nullopt, std::nullopt, 7});

  writer->write(VeloxColumnarBatch(input));
  const auto commitMessages = writer->commit();

  ASSERT_EQ(commitMessages.size(), 2);
  const auto dataCommit = findCommitMessage(commitMessages, "DATA");
  ASSERT_FALSE(dataCommit.isNull());
  EXPECT_EQ(dataCommit["metrics"]["recordCount"].asInt(), 3);
  const auto deletionCommit = findCommitMessage(commitMessages, "POSITION_DELETES");
  ASSERT_FALSE(deletionCommit.isNull());
  EXPECT_EQ(deletionCommit["fileFormat"].asString(), "PUFFIN");
  EXPECT_EQ(deletionCommit["referencedDataFile"].asString(), referencedDataFile);
  EXPECT_EQ(deletionCommit["metrics"]["recordCount"].asInt(), 2);
  EXPECT_GT(deletionCommit["contentOffset"].asInt(), 0);
  EXPECT_GT(deletionCommit["contentSizeInBytes"].asInt(), 0);
}

TEST_F(VeloxIcebergWriteTest, rejectsUnsupportedSparkOperation) {
  const auto dataType = ROW({"id", "name"}, {BIGINT(), VARCHAR()});
  IcebergNativeWriteOptions options;
  options.writeMode = IcebergWriteMode::kMerge;
  options.dataColumnIndices = {1, 2};
  options.operationColumnIndex = 0;
  options.filePathColumnIndex = 3;
  options.rowPositionColumnIndex = 4;
  auto writer = makeWriter(dataType, tmpDir_->getPath(), options);
  auto input = makeMutationInput(
      {9}, {10}, {StringView("invalid")}, {StringView(tmpDir_->getPath() + "/existing.parquet")}, {5});

  VELOX_ASSERT_USER_THROW(writer->write(VeloxColumnarBatch(input)), "Unsupported Spark row operation 9");
}

TEST_F(VeloxIcebergWriteTest, rejectsInvalidExistingDeletionVectorMetadata) {
  const auto dataType = ROW({"id"}, {BIGINT()});
  IcebergNativeWriteOptions options;
  options.writeMode = IcebergWriteMode::kDeletionVector;
  options.operationColumnIndex = 0;
  options.filePathColumnIndex = 1;
  options.rowPositionColumnIndex = 2;
  options.existingDeletionVectors.push_back({"data.parquet", "deletes.puffin", 16, 32, 0, 128});

  VELOX_ASSERT_USER_THROW(
      makeWriter(dataType, tmpDir_->getPath(), options), "Existing deletion vector record count must be positive");

  options.existingDeletionVectors[0].recordCount = 2;
  options.existingDeletionVectors[0].contentOffset = 112;
  options.existingDeletionVectors[0].contentLength = 32;
  VELOX_ASSERT_USER_THROW(
      makeWriter(dataType, tmpDir_->getPath(), options), "Existing deletion vector content range exceeds file size");
}

TEST_F(VeloxIcebergWriteTest, seedsAndUnionsExistingDeletionVector) {
  const auto dataType = ROW({"id"}, {BIGINT()});
  const auto referencedDataFile = tmpDir_->getPath() + "/existing.parquet";
  IcebergNativeWriteOptions firstOptions;
  firstOptions.writeMode = IcebergWriteMode::kDeletionVector;
  firstOptions.operationColumnIndex = 0;
  firstOptions.filePathColumnIndex = 1;
  firstOptions.rowPositionColumnIndex = 2;
  auto firstWriter = makeWriter(dataType, tmpDir_->getPath(), firstOptions);
  auto firstInput = makeRowVector(
      {"operation", "file_path", "pos"},
      {makeFlatVector<int32_t>({1, 1}),
       makeFlatVector<StringView>({referencedDataFile, referencedDataFile}),
       makeFlatVector<int64_t>({1, 2})});

  firstWriter->write(VeloxColumnarBatch(firstInput));
  const auto firstMessages = firstWriter->commit();
  ASSERT_EQ(firstMessages.size(), 1);
  const auto firstCommit = folly::parseJson(firstMessages[0]);
  ASSERT_EQ(firstCommit["metrics"]["recordCount"].asInt(), 2);

  IcebergNativeWriteOptions secondOptions = firstOptions;
  secondOptions.existingDeletionVectors.push_back(
      {referencedDataFile,
       firstCommit["path"].asString(),
       firstCommit["contentOffset"].asInt(),
       firstCommit["contentSizeInBytes"].asInt(),
       firstCommit["metrics"]["recordCount"].asInt(),
       firstCommit["fileSizeInBytes"].asInt()});
  auto secondWriter = makeWriter(dataType, tmpDir_->getPath(), secondOptions);
  auto secondInput = makeRowVector(
      {"operation", "file_path", "pos"},
      {makeFlatVector<int32_t>({1, 1}),
       makeFlatVector<StringView>({referencedDataFile, referencedDataFile}),
       makeFlatVector<int64_t>({2, 3})});

  secondWriter->write(VeloxColumnarBatch(secondInput));
  const auto secondMessages = secondWriter->commit();

  ASSERT_EQ(secondMessages.size(), 1);
  const auto secondCommit = folly::parseJson(secondMessages[0]);
  EXPECT_EQ(secondCommit["content"].asString(), "POSITION_DELETES");
  EXPECT_EQ(secondCommit["referencedDataFile"].asString(), referencedDataFile);
  EXPECT_EQ(secondCommit["metrics"]["recordCount"].asInt(), 3);
}

} // namespace
} // namespace gluten

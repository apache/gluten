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
#include "memory/VeloxColumnarBatch.h"
#include "utils/ConfigExtractor.h"
#include "utils/VeloxWriterUtils.h"
#include "velox/common/file/File.h"
#include "velox/connectors/hive/FileConnectorUtil.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"

#include <folly/json.h>
#include <filesystem>
#include <unordered_set>
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

using namespace facebook::velox;
namespace gluten {

class VeloxIcebergWriteTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    dwio::common::registerWriterFactory(std::make_shared<GlutenParquetWriterFactory>());
    Type::registerSerDe();
    dwio::common::registerFileSinks();
    filesystems::registerLocalFileSystem();
  }

  void SetUp() override {
    rootPool_->setReclaimer(memory::MemoryReclaimer::create());
  }
  std::unique_ptr<IcebergWriter> equalityWriter(
      const RowTypePtr& type,
      const IcebergNestedField& field,
      std::vector<int32_t> ids,
      std::vector<connector::hive::iceberg::IcebergPartitionSpec::Field> partitions = {},
      std::unordered_map<std::string, std::string> configs = {}) {
    return std::make_unique<IcebergWriter>(
        type,
        1,
        tmpDir_->getPath(),
        common::CompressionKind::CompressionKind_ZSTD,
        0,
        0,
        folly::to<std::string>(folly::Random::rand64()),
        std::make_shared<const connector::hive::iceberg::IcebergPartitionSpec>(7, std::move(partitions)),
        field,
        configs,
        pool_,
        connectorPool_,
        std::move(ids));
  }

  IcebergNestedField fields(std::initializer_list<int32_t> ids) {
    IcebergNestedField root;
    root.set_id(0);
    for (auto id : ids) {
      root.add_children()->set_id(id);
    }
    return root;
  }

  void
  assertFile(const std::string& path, const RowVectorPtr& expected, std::vector<parquet::ParquetFieldId> fieldIds) {
    dwio::common::ReaderOptions options(pool_.get());
    options.setFileSchema(expected->rowType());
    options.setColumnMappingMode(dwio::common::ColumnMappingMode::kParquetFieldId);
    options.setFieldIds(std::move(fieldIds));
    parquet::ParquetReader reader(
        std::make_unique<dwio::common::BufferedInput>(std::make_shared<LocalReadFile>(path), *pool_), options);
    auto spec = std::make_shared<common::ScanSpec>("root");
    spec->addAllChildFields(*expected->type());
    dwio::common::RowReaderOptions rowOptions;
    rowOptions.setScanSpec(spec);
    rowOptions.setTimestampPrecision(TimestampPrecision::kMicroseconds);
    auto rowReader = reader.createRowReader(rowOptions);
    VectorPtr output = BaseVector::create(expected->type(), 0, pool_.get());
    ASSERT_EQ(rowReader->next(1000, output), expected->size());
    test::assertEqualVectors(expected, output);
    ASSERT_EQ(rowReader->next(1000, output), 0);
  }

  std::shared_ptr<exec::test::TempDirectoryPath> tmpDir_{exec::test::TempDirectoryPath::create()};

  std::shared_ptr<memory::MemoryPool> connectorPool_ = rootPool_->addAggregateChild("connector");
};

TEST_F(VeloxIcebergWriteTest, parquetWriterOptions) {
  GlutenParquetWriterFactory factory;
  const config::ConfigBase empty(std::unordered_map<std::string, std::string>{});
  auto defaults = std::static_pointer_cast<parquet::ParquetWriterOptions>(factory.createFormatOptions(empty, empty));
  EXPECT_EQ(defaults->codecOptions, nullptr);
  EXPECT_FALSE(defaults->useParquetDataPageV2.value_or(false));

  const connector::hive::HiveConfig hiveConfig(
      std::make_shared<config::ConfigBase>(std::unordered_map<std::string, std::string>{}));
  for (auto level : {-5, 1, 9}) {
    for (const auto& version : {"V1", "V2"}) {
      auto sparkConf = std::make_shared<config::ConfigBase>(std::unordered_map<std::string, std::string>{
          {"spark.gluten.sql.columnar.backend.velox.parquet_writer_compression_level", std::to_string(level)},
          {"spark.gluten.sql.columnar.backend.velox.parquet_writer_datapage_version", version}});
      auto session = createHiveConnectorSessionConfig(sparkConf);
      auto scopedConfigs =
          connector::hive::makeFormatScopedConfigs(hiveConfig, *session, dwio::common::FileFormat::PARQUET);
      auto options = std::static_pointer_cast<parquet::ParquetWriterOptions>(
          factory.createFormatOptions(scopedConfigs.connectorConfig, scopedConfigs.sessionProperties));
      ASSERT_NE(options->codecOptions, nullptr);
      EXPECT_EQ(options->codecOptions->compressionLevel, level);
      EXPECT_EQ(options->useParquetDataPageV2.value(), std::string(version) == "V2");
    }
  }
}

TEST_F(VeloxIcebergWriteTest, write) {
  auto vector = makeRowVector({makeFlatVector<int8_t>({1, 2}), makeFlatVector<int16_t>({1, 2})});
  auto tmpPath = tmpDir_->getPath();
  std::vector<connector::hive::iceberg::IcebergPartitionSpec::Field> fields;
  auto partitionSpec = std::make_shared<const connector::hive::iceberg::IcebergPartitionSpec>(0, fields);

  gluten::IcebergNestedField root;
  root.set_id(0);
  gluten::IcebergNestedField* child1 = root.add_children();
  child1->set_id(1);
  gluten::IcebergNestedField* child2 = root.add_children();
  child2->set_id(2);

  auto writer = std::make_unique<IcebergWriter>(
      asRowType(vector->type()),
      1,
      tmpPath + "/iceberg_write_test_table",
      common::CompressionKind::CompressionKind_ZSTD,
      0, // partitionId
      0, // taskId
      folly::to<std::string>(folly::Random::rand64()), // operationId
      partitionSpec,
      root,
      std::unordered_map<std::string, std::string>(),
      rootPool_,
      connectorPool_);
  auto batch = VeloxColumnarBatch(vector);
  writer->write(batch);
  auto commitMessage = writer->commit();
  ASSERT_EQ(commitMessage.size(), 1);
  EXPECT_EQ(folly::parseJson(commitMessage[0])["content"].asString(), "DATA");
}
TEST_F(VeloxIcebergWriteTest, equalityDeletesPreserveFieldIdsAndNulls) {
  auto input = makeRowVector(
      {"id", "value"}, {makeNullableFlatVector<int64_t>({1, std::nullopt, 3}), makeFlatVector<int32_t>({10, 20, 30})});
  auto writer = equalityWriter(input->rowType(), fields({11, 29}), {11});
  writer->write(VeloxColumnarBatch(input));
  auto messages = writer->commit();
  ASSERT_EQ(messages.size(), 1);
  auto metadata = folly::parseJson(messages[0]);
  EXPECT_EQ(metadata["content"].asString(), "EQUALITY_DELETES");
  EXPECT_EQ(metadata["equalityFieldIds"], folly::dynamic::array(11));
  EXPECT_EQ(metadata["metrics"]["recordCount"].asInt(), 3);
  EXPECT_GT(metadata["fileSizeInBytes"].asInt(), 0);
  auto expected = makeRowVector({"renamed_id", "renamed_value"}, input->children());
  assertFile(metadata["path"].asString(), expected, {{11, {}}, {29, {}}});
}

TEST_F(VeloxIcebergWriteTest, mixedTimestampEqualityDeletes) {
  std::vector<std::optional<Timestamp>> values = {Timestamp(-1, 999'999'000), std::nullopt, Timestamp(1, 123'456'000)};
  auto input = makeRowVector(
      {"zoned", "local"}, {makeNullableFlatVector<Timestamp>(values), makeNullableFlatVector<Timestamp>(values)});
  auto writer = equalityWriter(
      input->rowType(),
      fields({11, 29}),
      {11, 29},
      {},
      {{"gluten.iceberg.timestamp-timezone", "UTC"}, {"gluten.iceberg.timestamp-without-timezone-field-ids", "[29]"}});
  writer->write(VeloxColumnarBatch(input));
  auto messages = writer->commit();
  ASSERT_EQ(messages.size(), 1);
  auto expected = makeRowVector(
      {"zoned", "local"},
      {makeNullableFlatVector<Timestamp>(values), makeNullableFlatVector<Timestamp>(values, TIMESTAMP_UTC())});
  assertFile(folly::parseJson(messages[0])["path"].asString(), expected, {{11, {}}, {29, {}}});
  EXPECT_TRUE(input->childAt(1)->type()->equivalent(*TIMESTAMP()));
}

TEST_F(VeloxIcebergWriteTest, compositeAndNestedEqualityDeletes) {
  auto nested = makeRowVector({"key"}, {makeNullableFlatVector<int64_t>({7, std::nullopt, 9})});
  nested->setNull(2, true);
  auto input = makeRowVector({"id", "nested"}, {makeFlatVector<int32_t>({1, 2, 3}), nested});
  auto field = fields({11, 29});
  field.mutable_children(1)->add_children()->set_id(42);
  auto writer = equalityWriter(input->rowType(), field, {42, 11});
  writer->write(VeloxColumnarBatch(input));
  auto messages = writer->commit();
  ASSERT_EQ(messages.size(), 1);
  auto metadata = folly::parseJson(messages[0]);
  EXPECT_EQ(metadata["equalityFieldIds"], folly::dynamic::array(42, 11));
  assertFile(metadata["path"].asString(), input, {{11, {}}, {29, {{42, {}}}}});
}

TEST_F(VeloxIcebergWriteTest, partitionedEqualityDeletes) {
  auto input =
      makeRowVector({"id", "part"}, {makeFlatVector<int64_t>({1, 2, 3}), makeFlatVector<int32_t>({10, 20, 10})});
  auto writer = equalityWriter(
      input->rowType(),
      fields({11, 29}),
      {11},
      {{"part", INTEGER(), connector::hive::iceberg::TransformType::kIdentity, std::nullopt}});
  writer->write(VeloxColumnarBatch(input));
  auto messages = writer->commit();
  ASSERT_EQ(messages.size(), 2);
  int64_t rows = 0;
  std::unordered_set<int64_t> partitions;
  for (const auto& message : messages) {
    auto metadata = folly::parseJson(message);
    EXPECT_EQ(metadata["content"].asString(), "EQUALITY_DELETES");
    EXPECT_EQ(metadata["equalityFieldIds"], folly::dynamic::array(11));
    auto partition = folly::parseJson(metadata["partitionDataJson"].asString());
    partitions.insert(partition["partitionValues"][0].asInt());
    rows += metadata["metrics"]["recordCount"].asInt();
  }
  EXPECT_EQ(rows, 3);
  EXPECT_EQ(partitions, (std::unordered_set<int64_t>{10, 20}));
}

TEST_F(VeloxIcebergWriteTest, rolledEqualityDeletesPreserveMetadataAndAbortAllFiles) {
  auto input = makeRowVector({"id"}, {makeFlatVector<int64_t>(100, [](auto row) { return row; })});
  auto writer = equalityWriter(
      input->rowType(),
      fields({11}),
      {11},
      {},
      {{"spark.gluten.sql.columnar.backend.velox.parquetMaxTargetFileSize", "1B"}});
  for (int i = 0; i < 5; ++i) {
    writer->write(VeloxColumnarBatch(input));
  }
  const auto messages = writer->commit();
  ASSERT_GT(messages.size(), 1);
  int64_t rows = 0;
  std::unordered_set<std::string> paths;
  for (const auto& message : messages) {
    const auto metadata = folly::parseJson(message);
    EXPECT_EQ(metadata["content"].asString(), "EQUALITY_DELETES");
    EXPECT_EQ(metadata["equalityFieldIds"], folly::dynamic::array(11));
    rows += metadata["metrics"]["recordCount"].asInt();
    EXPECT_TRUE(paths.insert(metadata["path"].asString()).second);
  }
  EXPECT_EQ(rows, 500);
  writer->abort();
  for (const auto& path : paths) {
    EXPECT_FALSE(std::filesystem::exists(path));
  }
}

TEST_F(VeloxIcebergWriteTest, invalidEqualityFields) {
  auto type = ROW({{"id", BIGINT()}, {"f", DOUBLE()}, {"items", ARRAY(BIGINT())}});
  auto field = fields({11, 29, 42});
  field.mutable_children(2)->add_children()->set_id(43);
  for (const auto& ids : std::vector<std::vector<int32_t>>{{}, {0}, {-1}, {12}, {11, 11}, {29}, {42}, {43}}) {
    EXPECT_THROW(equalityWriter(type, field, ids), VeloxUserError);
  }
  EXPECT_THROW(equalityWriter(type, fields({11}), {11}), VeloxUserError);
}

TEST_F(VeloxIcebergWriteTest, dataAndDeleteFilesHaveDifferentNames) {
  auto input = makeRowVector({"id"}, {makeFlatVector<int64_t>({1, 2})});
  auto createWriter = [&](std::optional<std::vector<int32_t>> ids) {
    return std::make_unique<IcebergWriter>(
        input->rowType(),
        1,
        tmpDir_->getPath(),
        common::CompressionKind::CompressionKind_ZSTD,
        0,
        0,
        "same-operation",
        std::make_shared<const connector::hive::iceberg::IcebergPartitionSpec>(
            0, std::vector<connector::hive::iceberg::IcebergPartitionSpec::Field>{}),
        fields({11}),
        std::unordered_map<std::string, std::string>{},
        pool_,
        connectorPool_,
        std::move(ids));
  };
  auto dataWriter = createWriter(std::nullopt);
  dataWriter->write(VeloxColumnarBatch(input));
  const auto dataPath = folly::parseJson(dataWriter->commit().at(0))["path"].asString();
  auto deleteWriter = createWriter(std::vector<int32_t>{11});
  deleteWriter->write(VeloxColumnarBatch(input));
  const auto deletePath = folly::parseJson(deleteWriter->commit().at(0))["path"].asString();
  EXPECT_NE(dataPath, deletePath);
  assertFile(dataPath, input, {{11, {}}});
  assertFile(deletePath, input, {{11, {}}});
}

TEST_F(VeloxIcebergWriteTest, emptyEqualityDeleteWriter) {
  auto writer = equalityWriter(ROW({{"id", BIGINT()}}), fields({11}), {11});
  EXPECT_TRUE(writer->commit().empty());
}

TEST_F(VeloxIcebergWriteTest, abortEqualityDeletesRemovesOnlyOwnedFiles) {
  auto input = makeRowVector({"id"}, {makeFlatVector<int64_t>({1, 2})});
  auto other = equalityWriter(input->rowType(), fields({11}), {11});
  other->write(VeloxColumnarBatch(input));
  const auto otherPath = folly::parseJson(other->commit().at(0))["path"].asString();

  for (const auto finish : {false, true}) {
    auto writer = equalityWriter(input->rowType(), fields({11}), {11});
    writer->write(VeloxColumnarBatch(input));
    if (finish) {
      EXPECT_EQ(writer->commit().size(), 1);
    }
    EXPECT_EQ(
        std::distance(std::filesystem::directory_iterator(tmpDir_->getPath()), std::filesystem::directory_iterator{}),
        2);
    writer->abort();
    writer->abort();
    EXPECT_TRUE(std::filesystem::exists(otherPath));
    EXPECT_EQ(
        std::distance(std::filesystem::directory_iterator(tmpDir_->getPath()), std::filesystem::directory_iterator{}),
        1);
  }
}
} // namespace gluten

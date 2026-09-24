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

#include "compute/delta/DeltaConnector.h"
#include "compute/delta/DeltaSplit.h"
#include "compute/delta/RoaringBitmapArray.h"
#include "folly/executors/IOThreadPoolExecutor.h"
#include "folly/init/Init.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

#include <limits>

namespace gluten::delta {

namespace {

class DeltaConnectorTest : public ::testing::Test {
 protected:
  static constexpr const char* kConnectorId = "test-delta";

  void TearDown() override {
    unregisterConnector(kConnectorId);
  }

  void registerDeltaConnector(
      std::shared_ptr<const config::ConfigBase> config =
          std::make_shared<config::ConfigBase>(std::unordered_map<std::string, std::string>{})) {
    unregisterConnector(kConnectorId);

    DeltaConnectorFactory factory;
    registerConnector(factory.newConnector(kConnectorId, std::move(config)));
  }
};

TEST_F(DeltaConnectorTest, connectorConfiguration) {
  auto customConfig = std::make_shared<config::ConfigBase>(std::unordered_map<std::string, std::string>{
      {hive::HiveConfig::kEnableFileHandleCache, "true"}, {hive::HiveConfig::kNumCacheFileHandles, "1000"}});

  registerDeltaConnector(customConfig);

  auto deltaConnector = getConnector(kConnectorId);
  ASSERT_NE(deltaConnector, nullptr);

  hive::HiveConfig hiveConfig(deltaConnector->connectorConfig());
  ASSERT_TRUE(hiveConfig.isFileHandleCacheEnabled());
  ASSERT_EQ(hiveConfig.numCacheFileHandles(), 1000);
}

TEST_F(DeltaConnectorTest, connectorProperties) {
  registerDeltaConnector();

  auto deltaConnector = getConnector(kConnectorId);
  ASSERT_NE(deltaConnector, nullptr);
  ASSERT_TRUE(deltaConnector->canAddDynamicFilter());
  ASSERT_TRUE(deltaConnector->supportsSplitPreload());
}

class DeltaConnectorExecutionTest : public facebook::velox::exec::test::HiveConnectorTestBase {
 protected:
  static constexpr const char* kConnectorId = "test-delta";

  void SetUp() override {
    facebook::velox::exec::test::HiveConnectorTestBase::SetUp();
    registerDeltaConnector();
  }

  void TearDown() override {
    unregisterConnector(kConnectorId);
    deltaIoExecutor_.reset();
    facebook::velox::exec::test::HiveConnectorTestBase::TearDown();
  }

  void registerDeltaConnector(
      std::shared_ptr<const config::ConfigBase> config =
          std::make_shared<config::ConfigBase>(std::unordered_map<std::string, std::string>{})) {
    unregisterConnector(kConnectorId);

    DeltaConnectorFactory factory;
    registerConnector(factory.newConnector(kConnectorId, std::move(config), deltaIoExecutor_.get()));
  }

  std::string createSerializedPayload(const std::vector<int64_t>& deletedRows) {
    RoaringBitmapArray bitmap;
    for (auto row : deletedRows) {
      bitmap.addSafe(row);
    }

    const auto serializedSize = bitmap.serializedSizeInBytes();
    auto buffer = AlignedBuffer::allocate<char>(serializedSize, pool());
    bitmap.serialize(buffer->asMutable<char>());
    return std::string(buffer->as<char>(), serializedSize);
  }

  std::shared_ptr<HiveDeltaSplit> makeDeltaSplit(
      const std::string& filePath,
      const std::string& serializedPayload,
      uint64_t cardinality,
      DeltaRowIndexFilterType filterType = DeltaRowIndexFilterType::kIfContained,
      dwio::common::FileFormat format = dwio::common::FileFormat::DWRF,
      uint64_t start = 0,
      uint64_t length = std::numeric_limits<uint64_t>::max()) {
    SplitPayloadBufferView payloadView{
        reinterpret_cast<const uint8_t*>(serializedPayload.data()), static_cast<int32_t>(serializedPayload.size())};

    return std::make_shared<HiveDeltaSplit>(
        kConnectorId,
        filePath,
        format,
        start,
        length,
        std::unordered_map<std::string, std::optional<std::string>>{},
        std::nullopt,
        std::unordered_map<std::string, std::string>{{"table_format", "delta"}},
        nullptr,
        std::unordered_map<std::string, std::string>{},
        true,
        DeltaDeletionVectorDescriptor::serialized(cardinality, payloadView),
        std::nullopt,
        filterType);
  }

  core::PlanNodePtr
  markingScan(bool includeRowIndex, common::SubfieldFilters filters = {}, bool includeDeletedFlag = true) {
    std::vector<std::string> names{"id"};
    std::vector<TypePtr> types{BIGINT()};
    ColumnHandleMap assignments{{"id", regularColumn("id", BIGINT())}};
    if (includeRowIndex) {
      names.emplace_back("row_index");
      types.emplace_back(BIGINT());
      assignments["row_index"] =
          std::make_shared<HiveColumnHandle>("row_index", HiveColumnHandle::ColumnType::kRowIndex, BIGINT(), BIGINT());
    }
    if (includeDeletedFlag) {
      names.emplace_back(kRowDeletedColumnName);
      types.emplace_back(TINYINT());
      assignments[kRowDeletedColumnName] = std::make_shared<HiveColumnHandle>(
          kRowDeletedColumnName, HiveColumnHandle::ColumnType::kRowIndex, BIGINT(), BIGINT());
    }
    auto handle =
        std::make_shared<HiveTableHandle>(kConnectorId, "delta", std::move(filters), nullptr, ROW({"id"}, {BIGINT()}));
    return std::make_shared<core::TableScanNode>(
        "scan", ROW(std::move(names), std::move(types)), std::move(handle), std::move(assignments));
  }

  std::unique_ptr<folly::IOThreadPoolExecutor> deltaIoExecutor_;
};

TEST_F(DeltaConnectorExecutionTest, filtersRowsUsingMaterializedDeletionVector) {
  const auto rowType = ROW({"id"}, {BIGINT()});
  const auto input = makeRowVector({"id"}, {makeFlatVector<int64_t>({10, 11, 12, 13, 14, 15, 16, 17, 18, 19})});
  const auto file = facebook::velox::exec::test::TempFilePath::create();
  writeToFile(file->getPath(), input);

  const auto plan = facebook::velox::exec::test::PlanBuilder(pool())
                        .startTableScan()
                        .connectorId(kConnectorId)
                        .outputType(rowType)
                        .endTableScan()
                        .planNode();

  const auto payload = createSerializedPayload({2, 5, 8});
  const auto split = makeDeltaSplit(file->getPath(), payload, 3);
  const auto expected = makeRowVector({"id"}, {makeFlatVector<int64_t>({10, 11, 13, 14, 16, 17, 19})});

  facebook::velox::exec::test::AssertQueryBuilder(plan).split(split).assertResults(expected);

  const auto nonMatchingPayload = createSerializedPayload({42});
  const auto nonMatchingSplit = makeDeltaSplit(file->getPath(), nonMatchingPayload, 1);
  facebook::velox::exec::test::AssertQueryBuilder(plan).split(nonMatchingSplit).assertResults(input);
}

TEST_F(DeltaConnectorExecutionTest, materializesFlagsWithoutDroppingRows) {
  const auto input = makeRowVector({"id"}, {makeFlatVector<int64_t>(10, [](auto row) { return 100 + row; })});
  const auto file = exec::test::TempFilePath::create();
  writeToFile(file->getPath(), input);
  const auto payload = createSerializedPayload({0, 2, 7});

  for (bool includeRowIndex : {false, true}) {
    const auto plan = markingScan(includeRowIndex);
    for (auto filterType : {DeltaRowIndexFilterType::kIfContained, DeltaRowIndexFilterType::kIfNotContained}) {
      auto split = makeDeltaSplit(file->getPath(), payload, 3, filterType);
      std::vector<VectorPtr> columns{input->childAt(0)};
      if (includeRowIndex) {
        columns.emplace_back(makeFlatVector<int64_t>(10, [](auto row) { return row; }));
      }
      columns.emplace_back(makeFlatVector<int8_t>(10, [filterType](auto row) {
        const bool contained = row == 0 || row == 2 || row == 7;
        return filterType == DeltaRowIndexFilterType::kIfContained ? contained : !contained;
      }));
      const auto expected = makeRowVector(plan->outputType()->names(), columns);
      exec::test::AssertQueryBuilder(plan)
          .config("preferred_output_batch_rows", "3")
          .split(split)
          .assertResults(expected);
    }
  }
}

TEST_F(DeltaConnectorExecutionTest, distinguishesAbsentAndEmptyDeletionVectorsWhenMarking) {
  const auto input = makeRowVector({"id"}, {makeFlatVector<int64_t>({10, 11, 12})});
  const auto file = exec::test::TempFilePath::create();
  writeToFile(file->getPath(), input);
  const auto payload = createSerializedPayload({});
  const auto plan = markingScan(true);
  const auto allMarked = makeRowVector(
      plan->outputType()->names(),
      {input->childAt(0), makeFlatVector<int64_t>({0, 1, 2}), makeFlatVector<int8_t>({1, 1, 1})});
  const auto allKept = makeRowVector(
      plan->outputType()->names(),
      {input->childAt(0), makeFlatVector<int64_t>({0, 1, 2}), makeFlatVector<int8_t>({0, 0, 0})});

  const auto empty = makeDeltaSplit(file->getPath(), payload, 0, DeltaRowIndexFilterType::kIfNotContained);
  exec::test::AssertQueryBuilder(plan).split(empty).assertResults(allMarked);
  const auto absent = makeDeltaSplit(file->getPath(), payload, 0, DeltaRowIndexFilterType::kIfNotContained);
  absent->deletionVector.reset();
  const std::vector<std::shared_ptr<ConnectorSplit>> mixedSplits{empty, absent};
  exec::test::AssertQueryBuilder(plan).splits(mixedSplits).assertResults({allMarked, allKept});

  const auto nonEmptyPayload = createSerializedPayload({1});
  const auto keepAll = makeDeltaSplit(file->getPath(), nonEmptyPayload, 1, DeltaRowIndexFilterType::kKeepAll);
  exec::test::AssertQueryBuilder(plan).split(keepAll).assertResults(allKept);
}

TEST_F(DeltaConnectorExecutionTest, generatedKeyDynamicFiltersHonorJoinReplacementAcrossSplits) {
  deltaIoExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(2);
  registerDeltaConnector();
  const auto input = makeRowVector({"id"}, {makeFlatVector<int64_t>(10, [](auto row) { return 100 + row; })});
  const auto file = exec::test::TempFilePath::create();
  writeToFile(file->getPath(), input);
  const auto payload = createSerializedPayload({0, 2, 7});

  for (const std::string mode : {"flag", "index-with-flags", "index-only"}) {
    const bool flagKey = mode == "flag";
    const auto key = flagKey ? kRowDeletedColumnName : "row_index";
    for (auto joinType : {core::JoinType::kInner, core::JoinType::kLeftSemiFilter}) {
      for (int preload : {0, 2}) {
        SCOPED_TRACE(
            ::testing::Message() << mode << ", join=" << static_cast<int>(joinType) << ", preload=" << preload);
        const auto ids = std::make_shared<core::PlanNodeIdGenerator>();
        VectorPtr buildKey = flagKey ? VectorPtr(makeFlatVector<int8_t>({1})) : VectorPtr(makeFlatVector<int64_t>({2}));
        const auto build = exec::test::PlanBuilder(ids, pool()).values({makeRowVector({"key"}, {buildKey})}).planNode();
        // One unique build key, no residual predicate and no build/key output allow Velox to
        // replace the hash lookup with the dynamic filter accepted by the scan.
        const auto plan = exec::test::PlanBuilder(markingScan(true, {}, mode != "index-only"), ids, pool())
                              .hashJoin({key}, {"key"}, build, "", {"id"}, joinType)
                              .planNode();
        const auto withDv = makeDeltaSplit(file->getPath(), payload, 3);
        const auto withoutDv = makeDeltaSplit(file->getPath(), payload, 3);
        withoutDv->deletionVector.reset();
        if (mode == "index-only") {
          withDv->deletionVector.reset();
        }
        const auto expected = makeRowVector(
            {"id"}, {flagKey ? makeFlatVector<int64_t>({100, 102, 107}) : makeFlatVector<int64_t>({102, 102})});
        const std::vector<std::shared_ptr<ConnectorSplit>> splits{withDv, withoutDv};
        const auto task = exec::test::AssertQueryBuilder(plan)
                              .maxDrivers(1)
                              .config("preferred_output_batch_rows", "2")
                              .config("max_split_preload_per_driver", std::to_string(preload))
                              .splits("scan", splits)
                              .assertResults(expected);
        if (joinType == core::JoinType::kInner) {
          int64_t replacedRows = 0;
          for (const auto& pipeline : task->taskStats().pipelineStats) {
            for (const auto& op : pipeline.operatorStats) {
              const auto metric = op.runtimeStats.find("replacedWithDynamicFilterRows");
              if (metric != op.runtimeStats.end()) {
                replacedRows += metric->second.sum;
              }
            }
          }
          ASSERT_GT(replacedRows, 0);
        }
      }
    }
  }
}

TEST_F(DeltaConnectorExecutionTest, generatedMetadataUsesAbsoluteParquetPositionsAfterSplitAndPruning) {
  parquet::registerParquetReaderFactory();
  const auto file = exec::test::TempFilePath::create();
  const auto rowType = ROW({"id"}, {BIGINT()});
  dwio::common::WriterOptions options;
  options.memoryPool = rootPool_.get();
  auto sink = dwio::common::FileSink::create("file:" + file->getPath(), {.pool = pool()});
  parquet::Writer writer(std::move(sink), options, rowType);
  for (int group = 0; group < 6; ++group) {
    writer.write(
        makeRowVector({"id"}, {makeFlatVector<int64_t>(10, [group](auto row) { return 1000 + group * 10 + row; })}));
    writer.flush();
  }
  writer.close();

  parquet::ParquetReader reader(
      std::make_unique<dwio::common::BufferedInput>(std::make_shared<LocalReadFile>(file->getPath()), *pool()),
      dwio::common::ReaderOptions(pool()));
  const auto metadata = reader.fileMetaData();
  ASSERT_EQ(metadata.numRowGroups(), 6);
  const auto start = metadata.rowGroup(2).fileOffset();
  const auto end = metadata.rowGroup(5).fileOffset();
  ASSERT_GT(start, 0);
  ASSERT_GT(end, start);
  const auto payload = createSerializedPayload({0, 20, 33, 41, 46, 59});

  common::SubfieldFilters filters;
  filters.emplace(common::Subfield("id"), std::make_unique<common::BigintRange>(1033, 1046, false));
  const auto plan = markingScan(true, std::move(filters));
  const auto expected = makeRowVector(
      plan->outputType()->names(),
      {makeFlatVector<int64_t>(14, [](auto row) { return 1033 + row; }),
       makeFlatVector<int64_t>(14, [](auto row) { return 33 + row; }),
       makeFlatVector<int8_t>(14, [](auto row) { return row == 0 || row == 8 || row == 13; })});
  const auto split = makeDeltaSplit(
      file->getPath(),
      payload,
      6,
      DeltaRowIndexFilterType::kIfContained,
      dwio::common::FileFormat::PARQUET,
      start,
      end - start);
  exec::test::AssertQueryBuilder(plan).config("preferred_output_batch_rows", "3").split(split).assertResults(expected);
}

} // namespace

} // namespace gluten::delta

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}

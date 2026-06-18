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

#include <gtest/gtest.h>

#include "compute/delta/DeltaConnector.h"
#include "compute/delta/DeltaSplit.h"
#include "compute/delta/RoaringBitmapArray.h"
#include "folly/init/Init.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/HiveConfig.h"
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
    facebook::velox::exec::test::HiveConnectorTestBase::TearDown();
  }

  void registerDeltaConnector(
      std::shared_ptr<const config::ConfigBase> config =
          std::make_shared<config::ConfigBase>(std::unordered_map<std::string, std::string>{})) {
    unregisterConnector(kConnectorId);

    DeltaConnectorFactory factory;
    registerConnector(factory.newConnector(kConnectorId, std::move(config)));
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

  std::shared_ptr<HiveDeltaSplit>
  makeDeltaSplit(const std::string& filePath, const std::string& serializedPayload, uint64_t cardinality) {
    SplitPayloadBufferView payloadView{
        reinterpret_cast<const uint8_t*>(serializedPayload.data()), static_cast<int32_t>(serializedPayload.size())};

    return std::make_shared<HiveDeltaSplit>(
        kConnectorId,
        filePath,
        facebook::velox::dwio::common::FileFormat::DWRF,
        0,
        std::numeric_limits<uint64_t>::max(),
        std::unordered_map<std::string, std::optional<std::string>>{},
        std::nullopt,
        std::unordered_map<std::string, std::string>{{"table_format", "delta"}},
        nullptr,
        std::unordered_map<std::string, std::string>{},
        true,
        DeltaDeletionVectorDescriptor::serialized(cardinality, payloadView));
  }
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

} // namespace

} // namespace gluten::delta

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}

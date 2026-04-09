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

#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "compute/delta/DeltaConnector.h"

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
          std::make_shared<config::ConfigBase>(
              std::unordered_map<std::string, std::string>{})) {
    unregisterConnector(kConnectorId);

    DeltaConnectorFactory factory;
    registerConnector(factory.newConnector(kConnectorId, std::move(config)));
  }
};

TEST_F(DeltaConnectorTest, connectorConfiguration) {
  auto customConfig = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{
          {hive::HiveConfig::kEnableFileHandleCache, "true"},
          {hive::HiveConfig::kNumCacheFileHandles, "1000"}});

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

} // namespace

} // namespace gluten::delta

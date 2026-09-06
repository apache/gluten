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

#include "utils/ConfigExtractor.h"

#include <gtest/gtest.h>

#include <string>
#include <unordered_map>

#include "velox/common/config/Config.h"

namespace gluten {

using ConfigBase = facebook::velox::config::ConfigBase;

// Helper: build a ConfigBase from a braced-init map.
static std::shared_ptr<ConfigBase> makeBase(std::unordered_map<std::string, std::string> m) {
  return std::make_shared<ConfigBase>(std::move(m));
}

// ── mergeWithSessionOverrides ─────────────────────────────────────────────────

// Keys that match one of the forwarded prefixes must appear in the result.
TEST(MergeWithSessionOverridesTest, forwardsFsS3aKey) {
  auto base = makeBase({{"other.key", "base-value"}});
  std::unordered_map<std::string, std::string> session = {
      {"fs.s3a.access.key", "SESSION_ACCESS_KEY"},
  };

  auto result = mergeWithSessionOverrides(base, session);
  const auto& raw = result->rawConfigs();

  EXPECT_EQ(raw.at("fs.s3a.access.key"), "SESSION_ACCESS_KEY");
}

TEST(MergeWithSessionOverridesTest, forwardsFsAzureKey) {
  auto base = makeBase({});
  std::unordered_map<std::string, std::string> session = {
      {"fs.azure.account.key.myaccount.dfs.core.windows.net", "TOKEN"},
  };

  auto result = mergeWithSessionOverrides(base, session);
  EXPECT_EQ(result->rawConfigs().at("fs.azure.account.key.myaccount.dfs.core.windows.net"), "TOKEN");
}

TEST(MergeWithSessionOverridesTest, forwardsFsGsKey) {
  auto base = makeBase({});
  std::unordered_map<std::string, std::string> session = {
      {"fs.gs.auth.service.account.email", "sa@project.iam.gserviceaccount.com"},
  };

  auto result = mergeWithSessionOverrides(base, session);
  EXPECT_EQ(result->rawConfigs().at("fs.gs.auth.service.account.email"), "sa@project.iam.gserviceaccount.com");
}

// A session key that does NOT match any forwarded prefix must be ignored.
TEST(MergeWithSessionOverridesTest, ignoresNonPrefixedKey) {
  auto base = makeBase({});
  std::unordered_map<std::string, std::string> session = {
      {"spark.some.other.key", "value"},
  };

  auto result = mergeWithSessionOverrides(base, session);
  EXPECT_EQ(result->rawConfigs().count("spark.some.other.key"), 0u);
}

// Base-config keys that are not overridden must pass through unchanged.
TEST(MergeWithSessionOverridesTest, preservesUnrelatedBaseKeys) {
  auto base = makeBase({{"hive.s3.aws-access-key", "BASE_KEY"}, {"hive.s3.aws-secret-key", "BASE_SECRET"}});
  std::unordered_map<std::string, std::string> session = {};

  auto result = mergeWithSessionOverrides(base, session);
  const auto& raw = result->rawConfigs();

  EXPECT_EQ(raw.at("hive.s3.aws-access-key"), "BASE_KEY");
  EXPECT_EQ(raw.at("hive.s3.aws-secret-key"), "BASE_SECRET");
}

// Session value must win over the same key already present in the base.
TEST(MergeWithSessionOverridesTest, sessionValueOverridesBaseValue) {
  auto base = makeBase({{"fs.s3a.access.key", "BASE_ACCESS_KEY"}});
  std::unordered_map<std::string, std::string> session = {
      {"fs.s3a.access.key", "SESSION_ACCESS_KEY"},
  };

  auto result = mergeWithSessionOverrides(base, session);
  EXPECT_EQ(result->rawConfigs().at("fs.s3a.access.key"), "SESSION_ACCESS_KEY");
}

// The original base config must not be mutated.
TEST(MergeWithSessionOverridesTest, doesNotMutateBase) {
  auto base = makeBase({{"fs.s3a.access.key", "ORIGINAL"}});
  std::unordered_map<std::string, std::string> session = {
      {"fs.s3a.access.key", "SESSION"},
  };

  mergeWithSessionOverrides(base, session);

  // base still holds the original value
  EXPECT_EQ(base->rawConfigs().at("fs.s3a.access.key"), "ORIGINAL");
}

} // namespace gluten

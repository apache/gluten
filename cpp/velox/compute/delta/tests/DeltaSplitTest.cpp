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

#include "compute/delta/DeltaSplit.h"

using namespace gluten::delta;

TEST(DeltaSplitTest, DescriptorCarriesPayloadView) {
  const std::string payload = "payload";
  gluten::SplitPayloadBufferView payloadView{
      reinterpret_cast<const uint8_t*>(payload.data()), static_cast<int32_t>(payload.size())};

  auto descriptor = DeltaDeletionVectorDescriptor::serialized(3, payloadView);

  ASSERT_TRUE(descriptor.serializedPayloadView.has_value());
  EXPECT_EQ(descriptor.serializedPayloadView->size, payload.size());
  EXPECT_EQ(descriptor.cardinality, 3);
  EXPECT_TRUE(descriptor.hasMaterializedPayload());
}

TEST(DeltaSplitTest, SplitCarriesDeletionVectorDescriptor) {
  const std::string payload = "serialized";
  gluten::SplitPayloadBufferView payloadView{
      reinterpret_cast<const uint8_t*>(payload.data()), static_cast<int32_t>(payload.size())};
  auto descriptor = DeltaDeletionVectorDescriptor::serialized(2, payloadView);

  auto split = std::make_shared<HiveDeltaSplit>(
      "test-delta",
      "/tmp/data.parquet",
      facebook::velox::dwio::common::FileFormat::PARQUET,
      0,
      1024,
      std::unordered_map<std::string, std::optional<std::string>>{},
      std::nullopt,
      std::unordered_map<std::string, std::string>{{"table_format", "delta"}},
      nullptr,
      std::unordered_map<std::string, std::string>{},
      true,
      descriptor,
      std::nullopt,
      DeltaRowIndexFilterType::kIfContained,
      std::unordered_map<std::string, std::string>{},
      std::nullopt);

  ASSERT_TRUE(split->deletionVector.has_value());
  EXPECT_EQ(split->deletionVector->cardinality, 2);
  ASSERT_TRUE(split->deletionVector->serializedPayloadView.has_value());
  EXPECT_EQ(split->deletionVector->serializedPayloadView->size, payload.size());
  EXPECT_EQ(split->filterType, DeltaRowIndexFilterType::kIfContained);
}

TEST(DeltaSplitTest, LogicalRowCountSubtractsDeletionVectorCardinality) {
  DeltaFileStatistics stats{.numRecords = 10, .tightBounds = true};
  auto descriptor = DeltaDeletionVectorDescriptor::serialized(3);

  EXPECT_EQ(stats.logicalRowCount(descriptor), 7);
}

TEST(DeltaSplitTest, LogicalRowCountPreservesUnknownCounts) {
  DeltaFileStatistics stats{.numRecords = std::nullopt, .tightBounds = std::nullopt};
  auto descriptor = DeltaDeletionVectorDescriptor::serialized(3);

  EXPECT_EQ(stats.logicalRowCount(descriptor), -1);
}

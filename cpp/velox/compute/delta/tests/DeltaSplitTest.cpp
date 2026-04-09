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

TEST(DeltaSplitTest, fileDeletionVectorDescriptor) {
  auto descriptor =
      DeltaDeletionVectorDescriptor::filePath("/tmp/table/_delta_log/_deletion_vectors/dv.bin", 17, 128, 3);

  EXPECT_FALSE(descriptor.isInline());
  EXPECT_EQ(descriptor.storageType, DeltaDeletionVectorStorageType::kFilePath);
  EXPECT_EQ(descriptor.pathOrInlineData, "/tmp/table/_delta_log/_deletion_vectors/dv.bin");
  ASSERT_TRUE(descriptor.offset.has_value());
  EXPECT_EQ(*descriptor.offset, 17);
  ASSERT_TRUE(descriptor.sizeInBytes.has_value());
  EXPECT_EQ(*descriptor.sizeInBytes, 128);
  ASSERT_TRUE(descriptor.cardinality.has_value());
  EXPECT_EQ(*descriptor.cardinality, 3);
}

TEST(DeltaSplitTest, inlineDeletionVectorDescriptor) {
  auto descriptor = DeltaDeletionVectorDescriptor::inlineData("base64-dv", 5);

  EXPECT_TRUE(descriptor.isInline());
  EXPECT_EQ(descriptor.storageType, DeltaDeletionVectorStorageType::kInlineData);
  EXPECT_EQ(descriptor.pathOrInlineData, "base64-dv");
  EXPECT_FALSE(descriptor.offset.has_value());
  EXPECT_FALSE(descriptor.sizeInBytes.has_value());
  ASSERT_TRUE(descriptor.cardinality.has_value());
  EXPECT_EQ(*descriptor.cardinality, 5);
}

TEST(DeltaSplitTest, splitCarriesDeletionVectorDescriptor) {
  auto descriptor = DeltaDeletionVectorDescriptor::filePath("/tmp/dv.bin", 4, 64, 2);

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
      std::nullopt, // protocolInfo
      std::nullopt, // statistics
      DeltaRowIndexFilterType::kIfContained,
      std::unordered_map<std::string, std::string>{}, // infoColumns
      std::nullopt); // fileProperties

  ASSERT_TRUE(split->deletionVector.has_value());
  EXPECT_EQ(split->deletionVector->storageType, DeltaDeletionVectorStorageType::kFilePath);
  EXPECT_EQ(split->deletionVector->pathOrInlineData, "/tmp/dv.bin");
  ASSERT_TRUE(split->deletionVector->offset.has_value());
  EXPECT_EQ(*split->deletionVector->offset, 4);
  EXPECT_EQ(split->filterType, DeltaRowIndexFilterType::kIfContained);
}

TEST(DeltaSplitTest, uniqueIdWithoutOffset) {
  // Test uniqueId for inline DV (no offset)
  auto descriptor = DeltaDeletionVectorDescriptor::inlineData("base64data", 5);

  // Format: <storageType><pathOrInlineData>
  EXPECT_EQ(descriptor.uniqueId(), "ibase64data");
}

TEST(DeltaSplitTest, uniqueIdWithOffset) {
  // Test uniqueId for file DV with offset
  auto descriptor = DeltaDeletionVectorDescriptor::filePath("/tmp/dv.bin", 128, 64, 3);

  // Format: <storageType><pathOrInlineData>@<offset>
  EXPECT_EQ(descriptor.uniqueId(), "p/tmp/dv.bin@128");
}

TEST(DeltaSplitTest, uniqueIdFileWithoutOffset) {
  // Test uniqueId for file DV without offset
  auto descriptor = DeltaDeletionVectorDescriptor::filePath("/tmp/dv.bin", std::nullopt, 64, 3);

  // Format: <storageType><pathOrInlineData>
  EXPECT_EQ(descriptor.uniqueId(), "p/tmp/dv.bin");
}

TEST(DeltaSplitTest, uniqueIdDifferentiation) {
  // Same file, different offsets should have different uniqueIds
  auto descriptor1 = DeltaDeletionVectorDescriptor::filePath("/tmp/dv.bin", 0, 64, 3);
  auto descriptor2 = DeltaDeletionVectorDescriptor::filePath("/tmp/dv.bin", 128, 64, 3);

  EXPECT_NE(descriptor1.uniqueId(), descriptor2.uniqueId());
  EXPECT_EQ(descriptor1.uniqueId(), "p/tmp/dv.bin@0");
  EXPECT_EQ(descriptor2.uniqueId(), "p/tmp/dv.bin@128");
}

TEST(DeltaSplitTest, uniqueIdForActionReconciliation) {
  // Test that uniqueId can be used for (path, DV) tuple uniqueness
  auto descriptor1 = DeltaDeletionVectorDescriptor::filePath("s3://bucket/table/file.parquet", 0, 100, 5);
  auto descriptor2 = DeltaDeletionVectorDescriptor::filePath("s3://bucket/table/file.parquet", 200, 100, 10);

  // Same file path but different DVs should have different uniqueIds
  EXPECT_NE(descriptor1.uniqueId(), descriptor2.uniqueId());

  // This allows the same file to appear with different DVs in successive versions
  EXPECT_EQ(descriptor1.uniqueId(), "ps3://bucket/table/file.parquet@0");
  EXPECT_EQ(descriptor2.uniqueId(), "ps3://bucket/table/file.parquet@200");
}

TEST(DeltaSplitTest, uuidDeletionVectorDescriptor) {
  // Test UUID-based DV descriptor
  auto descriptor = DeltaDeletionVectorDescriptor::uuidPath("ab^-aqEH.-t@S}K{vb[*k^", 4, 40, 6);

  EXPECT_FALSE(descriptor.isInline());
  EXPECT_TRUE(descriptor.isUuidPath());
  EXPECT_EQ(descriptor.storageType, DeltaDeletionVectorStorageType::kUuidPath);
  EXPECT_EQ(descriptor.pathOrInlineData, "ab^-aqEH.-t@S}K{vb[*k^");
  ASSERT_TRUE(descriptor.offset.has_value());
  EXPECT_EQ(*descriptor.offset, 4);
  ASSERT_TRUE(descriptor.sizeInBytes.has_value());
  EXPECT_EQ(*descriptor.sizeInBytes, 40);
  ASSERT_TRUE(descriptor.cardinality.has_value());
  EXPECT_EQ(*descriptor.cardinality, 6);
}

TEST(DeltaSplitTest, uniqueIdForUuidStorage) {
  // Test uniqueId for UUID storage type
  auto descriptor = DeltaDeletionVectorDescriptor::uuidPath("ab^-aqEH.-t@S}K{vb[*k^", 128, 64, 3);

  // Format: u<pathOrInlineDv>@<offset>
  EXPECT_EQ(descriptor.uniqueId(), "uab^-aqEH.-t@S}K{vb[*k^@128");
}

TEST(DeltaSplitTest, uniqueIdForUuidStorageWithoutOffset) {
  // Test uniqueId for UUID storage without offset
  auto descriptor = DeltaDeletionVectorDescriptor::uuidPath("^-aqEH.-t@S}K{vb[*k^", std::nullopt, 64, 3);

  // Format: u<pathOrInlineDv>
  EXPECT_EQ(descriptor.uniqueId(), "u^-aqEH.-t@S}K{vb[*k^");
}

TEST(DeltaSplitTest, storageTypeDifferentiation) {
  // Same path/data but different storage types should have different uniqueIds
  std::string data = "test_data";

  auto uuidDesc = DeltaDeletionVectorDescriptor::uuidPath(data, std::nullopt, 64, 3);
  auto fileDesc = DeltaDeletionVectorDescriptor::filePath(data, std::nullopt, 64, 3);
  auto inlineDesc = DeltaDeletionVectorDescriptor::inlineData(data, 3);

  EXPECT_NE(uuidDesc.uniqueId(), fileDesc.uniqueId());
  EXPECT_NE(uuidDesc.uniqueId(), inlineDesc.uniqueId());
  EXPECT_NE(fileDesc.uniqueId(), inlineDesc.uniqueId());

  EXPECT_EQ(uuidDesc.uniqueId(), "utest_data");
  EXPECT_EQ(fileDesc.uniqueId(), "ptest_data");
  EXPECT_EQ(inlineDesc.uniqueId(), "itest_data");
}

TEST(DeltaSplitTest, splitCarriesUuidDeletionVector) {
  // Test that HiveDeltaSplit can carry UUID-based DV descriptor
  auto descriptor = DeltaDeletionVectorDescriptor::uuidPath("ab^-aqEH.-t@S}K{vb[*k^", 4, 64, 6);

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
      std::nullopt, // protocolInfo
      std::nullopt, // statistics
      DeltaRowIndexFilterType::kIfContained,
      std::unordered_map<std::string, std::string>{}, // infoColumns
      std::nullopt); // fileProperties

  ASSERT_TRUE(split->deletionVector.has_value());
  EXPECT_EQ(split->deletionVector->storageType, DeltaDeletionVectorStorageType::kUuidPath);
  EXPECT_TRUE(split->deletionVector->isUuidPath());
  EXPECT_FALSE(split->deletionVector->isInline());
  EXPECT_EQ(split->deletionVector->pathOrInlineData, "ab^-aqEH.-t@S}K{vb[*k^");
}

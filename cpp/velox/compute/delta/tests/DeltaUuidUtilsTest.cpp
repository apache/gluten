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

#include "compute/delta/DeltaUuidUtils.h"
#include "velox/common/base/Exceptions.h"

using namespace gluten::delta;

class DeltaUuidUtilsTest : public ::testing::Test {};

TEST_F(DeltaUuidUtilsTest, EncodeDecodeZ85RoundTrip) {
  // Test UUID encoding and decoding round trip
  auto uuid = DeltaUuidUtils::generateUuid();
  std::string encoded = DeltaUuidUtils::encodeUuidToZ85(uuid);

  // Z85-encoded UUID should be exactly 20 characters
  EXPECT_EQ(encoded.size(), 20);

  // Decode and verify we get the same UUID back
  auto decoded = DeltaUuidUtils::decodeZ85ToUuid(encoded);
  EXPECT_EQ(uuid, decoded);
}

TEST_F(DeltaUuidUtilsTest, DecodeZ85InvalidLength) {
  // Z85 requires length to be multiple of 5
  std::string encoded = "abc"; // Length 3, not multiple of 5

  EXPECT_THROW(DeltaUuidUtils::decodeZ85ToUuid(encoded), facebook::velox::VeloxUserError);
}

TEST_F(DeltaUuidUtilsTest, DecodeZ85InvalidCharacter) {
  // Test with invalid character (not in Z85 alphabet)
  std::string encoded = "~~~~~"; // '~' is not in Z85 alphabet

  EXPECT_THROW(DeltaUuidUtils::decodeZ85ToUuid(encoded), facebook::velox::VeloxUserError);
}

TEST_F(DeltaUuidUtilsTest, ExtractUuidFromZ85SpecExample) {
  // From Delta spec: "ab^-aqEH.-t@S}K{vb[*k^"
  // Should decode to UUID: d2c639aa-8816-431a-aaf6-d3fe2512ff61
  // Random prefix: "ab"
  std::string pathOrInlineDv = "ab^-aqEH.-t@S}K{vb[*k^";

  auto [randomPrefix, uuid] = DeltaUuidUtils::extractUuidFromZ85(pathOrInlineDv);

  EXPECT_EQ(randomPrefix, "ab");

  // Convert UUID to string for comparison
  std::string uuidStr = DeltaUuidUtils::uuidToString(uuid);
  EXPECT_EQ(uuidStr, "d2c639aa-8816-431a-aaf6-d3fe2512ff61");
}

TEST_F(DeltaUuidUtilsTest, ExtractUuidFromZ85NoPrefix) {
  // UUID without random prefix (exactly 20 characters)
  // Generate a UUID, encode it, then extract it
  auto originalUuid = DeltaUuidUtils::generateUuid();
  std::string encoded = DeltaUuidUtils::encodeUuidToZ85(originalUuid);

  auto [randomPrefix, extractedUuid] = DeltaUuidUtils::extractUuidFromZ85(encoded);

  EXPECT_EQ(randomPrefix, "");
  EXPECT_EQ(originalUuid, extractedUuid);

  // UUID string should be in canonical format with hyphens
  std::string uuidStr = DeltaUuidUtils::uuidToString(extractedUuid);
  EXPECT_EQ(uuidStr.length(), 36); // xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
  EXPECT_EQ(uuidStr[8], '-');
  EXPECT_EQ(uuidStr[13], '-');
  EXPECT_EQ(uuidStr[18], '-');
  EXPECT_EQ(uuidStr[23], '-');
}

TEST_F(DeltaUuidUtilsTest, ExtractUuidFromZ85TooShort) {
  // Less than 20 characters - invalid
  std::string pathOrInlineDv = "short";

  EXPECT_THROW(DeltaUuidUtils::extractUuidFromZ85(pathOrInlineDv), facebook::velox::VeloxUserError);
}

TEST_F(DeltaUuidUtilsTest, ReconstructUuidPathSpecExample) {
  // From Delta spec example
  std::string tableDir = "s3://mytable";
  std::string randomPrefix = "ab";

  // Create UUID from the spec example
  auto [prefix, uuid] = DeltaUuidUtils::extractUuidFromZ85("ab^-aqEH.-t@S}K{vb[*k^");

  std::string path = DeltaUuidUtils::reconstructUuidPath(tableDir, randomPrefix, uuid);

  EXPECT_EQ(path, "s3://mytable/ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin");
}

TEST_F(DeltaUuidUtilsTest, ReconstructUuidPathNoPrefix) {
  // Without random prefix
  std::string tableDir = "/tmp/table";
  std::string randomPrefix = "";
  auto uuid = DeltaUuidUtils::generateUuid();
  std::string uuidStr = DeltaUuidUtils::uuidToString(uuid);

  std::string path = DeltaUuidUtils::reconstructUuidPath(tableDir, randomPrefix, uuid);

  std::string expected = "/tmp/table/deletion_vector_" + uuidStr + ".bin";
  EXPECT_EQ(path, expected);
}

TEST_F(DeltaUuidUtilsTest, ReconstructUuidPathTableDirWithTrailingSlash) {
  // Table dir already has trailing slash
  std::string tableDir = "/tmp/table/";
  std::string randomPrefix = "prefix";
  auto uuid = DeltaUuidUtils::generateUuid();
  std::string uuidStr = DeltaUuidUtils::uuidToString(uuid);

  std::string path = DeltaUuidUtils::reconstructUuidPath(tableDir, randomPrefix, uuid);

  std::string expected = "/tmp/table/prefix/deletion_vector_" + uuidStr + ".bin";
  EXPECT_EQ(path, expected);
}

TEST_F(DeltaUuidUtilsTest, ReconstructUuidPathEmptyTableDir) {
  // Empty table dir
  std::string tableDir = "";
  std::string randomPrefix = "";
  auto uuid = DeltaUuidUtils::generateUuid();
  std::string uuidStr = DeltaUuidUtils::uuidToString(uuid);

  std::string path = DeltaUuidUtils::reconstructUuidPath(tableDir, randomPrefix, uuid);

  std::string expected = "deletion_vector_" + uuidStr + ".bin";
  EXPECT_EQ(path, expected);
}

TEST_F(DeltaUuidUtilsTest, EndToEndSpecExample) {
  // Complete end-to-end test with Delta spec example
  // Input: "ab^-aqEH.-t@S}K{vb[*k^"
  // Expected output: s3://mytable/ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin

  std::string pathOrInlineDv = "ab^-aqEH.-t@S}K{vb[*k^";
  std::string tableDir = "s3://mytable";

  auto [randomPrefix, uuid] = DeltaUuidUtils::extractUuidFromZ85(pathOrInlineDv);
  std::string reconstructedPath = DeltaUuidUtils::reconstructUuidPath(tableDir, randomPrefix, uuid);

  EXPECT_EQ(reconstructedPath, "s3://mytable/ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin");
}

TEST_F(DeltaUuidUtilsTest, MultipleUuidsHaveDifferentPaths) {
  // Different UUIDs should produce different paths
  std::string tableDir = "/tmp/table";
  std::string prefix = "p";

  auto uuid1 = DeltaUuidUtils::generateUuid();
  auto uuid2 = DeltaUuidUtils::generateUuid();
  std::string uuid1Str = DeltaUuidUtils::uuidToString(uuid1);
  std::string uuid2Str = DeltaUuidUtils::uuidToString(uuid2);

  std::string path1 = DeltaUuidUtils::reconstructUuidPath(tableDir, prefix, uuid1);
  std::string path2 = DeltaUuidUtils::reconstructUuidPath(tableDir, prefix, uuid2);

  EXPECT_NE(path1, path2);
  EXPECT_TRUE(path1.find(uuid1Str) != std::string::npos);
  EXPECT_TRUE(path2.find(uuid2Str) != std::string::npos);
}

// Made with Bob

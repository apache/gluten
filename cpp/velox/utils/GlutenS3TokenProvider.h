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
#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "velox/common/file/TokenProvider.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3AccessToken.h"

namespace gluten {

// Keys of the LocalFiles.read_properties map. Contract with the JVM side
// (GlutenIcebergSourceUtil.vendedReadProperties): the Iceberg FileIO property
// names of the credentials, plus the table location under kReadPropertiesLocation.
constexpr const char* kReadPropertiesLocation = "location";
constexpr const char* kReadPropertiesAccessKeyId = "s3.access-key-id";
constexpr const char* kReadPropertiesSecretAccessKey = "s3.secret-access-key";
constexpr const char* kReadPropertiesSessionToken = "s3.session-token";

struct S3TableCredentials {
  std::string accessKeyId;
  std::string secretAccessKey;
  // Empty when the credentials are not temporary.
  std::string sessionToken;

  bool operator==(const S3TableCredentials& other) const {
    return accessKeyId == other.accessKeyId && secretAccessKey == other.secretAccessKey &&
        sessionToken == other.sessionToken;
  }
};

/// Resolves the per-table S3 credentials for native reads of tables whose
/// credentials an Iceberg REST catalog vends. Holds the credentials of all the
/// task's scans keyed by normalized table-location prefix; getToken()
/// longest-prefix-matches the file path, so a query joining two tables that
/// live in one bucket but carry different credentials cannot mix them up.
///
/// equals()/hash() cover the full contents because they key velox's file handle
/// cache: a rotated credential set can never be served a handle that was opened
/// with the credentials it replaced.
class GlutenS3TokenProvider final : public facebook::velox::filesystems::TokenProvider {
 public:
  explicit GlutenS3TokenProvider(std::map<std::string, S3TableCredentials> credentialsByPrefix);

  /// Builds a provider from the read properties of a task's scans, or nullptr
  /// when none of them carries credentials.
  static std::shared_ptr<GlutenS3TokenProvider> create(
      const std::vector<std::unordered_map<std::string, std::string>>& readProperties);

  bool equals(const facebook::velox::filesystems::TokenProvider& other) const override;

  size_t hash() const override;

  std::shared_ptr<facebook::velox::filesystems::AccessToken> getToken(
      const facebook::velox::filesystems::AccessTokenKey& key) const override;

  /// "s3://bucket/path" (also s3a/s3n) -> "bucket/path", matching the
  /// scheme-stripped paths the S3 file system puts in S3AccessTokenKey.
  static std::string normalizeS3Path(const std::string& path);

 private:
  const std::map<std::string, S3TableCredentials> credentialsByPrefix_;
  size_t hash_;
};

} // namespace gluten

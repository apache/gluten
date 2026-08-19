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
#include "utils/GlutenS3TokenProvider.h"

#include <cstring>
#include <functional>

#include "velox/common/base/BitUtil.h"

namespace gluten {
namespace {

// Segment-boundary-safe prefix match: "bucket/tableA" must not claim
// "bucket/tableAB/part.parquet".
bool prefixMatches(const std::string& path, const std::string& prefix) {
  if (prefix.empty() || path.size() < prefix.size() || path.compare(0, prefix.size(), prefix) != 0) {
    return false;
  }
  return path.size() == prefix.size() || prefix.back() == '/' || path[prefix.size()] == '/';
}

std::string findOrEmpty(const std::unordered_map<std::string, std::string>& properties, const char* key) {
  const auto it = properties.find(key);
  return it == properties.end() ? "" : it->second;
}

} // namespace

GlutenS3TokenProvider::GlutenS3TokenProvider(std::map<std::string, S3TableCredentials> credentialsByPrefix)
    : credentialsByPrefix_(std::move(credentialsByPrefix)) {
  const std::hash<std::string> hasher;
  size_t hash = 0;
  // std::map iteration order is deterministic, so equal contents hash equally.
  for (const auto& [prefix, credentials] : credentialsByPrefix_) {
    hash = facebook::velox::bits::hashMix(hash, hasher(prefix));
    hash = facebook::velox::bits::hashMix(hash, hasher(credentials.accessKeyId));
    hash = facebook::velox::bits::hashMix(hash, hasher(credentials.secretAccessKey));
    hash = facebook::velox::bits::hashMix(hash, hasher(credentials.sessionToken));
  }
  hash_ = hash;
}

std::shared_ptr<GlutenS3TokenProvider> GlutenS3TokenProvider::create(
    const std::vector<std::unordered_map<std::string, std::string>>& readProperties) {
  std::map<std::string, S3TableCredentials> credentialsByPrefix;
  for (const auto& properties : readProperties) {
    const auto location = findOrEmpty(properties, kReadPropertiesLocation);
    const auto accessKeyId = findOrEmpty(properties, kReadPropertiesAccessKeyId);
    const auto secretAccessKey = findOrEmpty(properties, kReadPropertiesSecretAccessKey);
    if (location.empty() || accessKeyId.empty() || secretAccessKey.empty()) {
      continue;
    }
    credentialsByPrefix[normalizeS3Path(location)] =
        S3TableCredentials{accessKeyId, secretAccessKey, findOrEmpty(properties, kReadPropertiesSessionToken)};
  }
  if (credentialsByPrefix.empty()) {
    return nullptr;
  }
  return std::make_shared<GlutenS3TokenProvider>(std::move(credentialsByPrefix));
}

bool GlutenS3TokenProvider::equals(const facebook::velox::filesystems::TokenProvider& other) const {
  const auto* typedOther = dynamic_cast<const GlutenS3TokenProvider*>(&other);
  return typedOther != nullptr && credentialsByPrefix_ == typedOther->credentialsByPrefix_;
}

size_t GlutenS3TokenProvider::hash() const {
  return hash_;
}

std::shared_ptr<facebook::velox::filesystems::AccessToken> GlutenS3TokenProvider::getToken(
    const facebook::velox::filesystems::AccessTokenKey& key) const {
  const auto* s3Key = dynamic_cast<const facebook::velox::filesystems::S3AccessTokenKey*>(&key);
  if (s3Key == nullptr) {
    return nullptr;
  }
  const auto& path = s3Key->path();
  const S3TableCredentials* longestMatch = nullptr;
  size_t longestMatchSize = 0;
  for (const auto& [prefix, credentials] : credentialsByPrefix_) {
    if (prefix.size() >= longestMatchSize && prefixMatches(path, prefix)) {
      longestMatch = &credentials;
      longestMatchSize = prefix.size();
    }
  }
  if (longestMatch == nullptr) {
    return nullptr;
  }
  return std::make_shared<facebook::velox::filesystems::S3AccessToken>(
      longestMatch->accessKeyId, longestMatch->secretAccessKey, longestMatch->sessionToken);
}

std::string GlutenS3TokenProvider::normalizeS3Path(const std::string& path) {
  for (const char* scheme : {"s3://", "s3a://", "s3n://"}) {
    if (path.rfind(scheme, 0) == 0) {
      return path.substr(std::strlen(scheme));
    }
  }
  return path;
}

} // namespace gluten

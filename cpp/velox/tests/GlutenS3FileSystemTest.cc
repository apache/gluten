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

#include "filesystem/GlutenS3FileSystem.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <unordered_map>

#include "utils/ConfigExtractor.h"
#include "velox/common/config/Config.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Config.h"

namespace gluten {
namespace {

namespace velox = facebook::velox;
namespace filesystems = facebook::velox::filesystems;

TEST(GlutenS3FileSystemTest, registeredFileSystemUsesGlutenSubclass) {
  registerGlutenS3FileSystem();

  auto config = std::make_shared<velox::config::ConfigBase>(std::unordered_map<std::string, std::string>{
      {filesystems::S3Config::baseConfigKey(filesystems::S3Config::Keys::kEndpoint), "http://127.0.0.1:9000"},
      {filesystems::S3Config::baseConfigKey(filesystems::S3Config::Keys::kAccessKey), "access"},
      {filesystems::S3Config::baseConfigKey(filesystems::S3Config::Keys::kSecretKey), "secret"},
      {filesystems::S3Config::baseConfigKey(filesystems::S3Config::Keys::kSSLEnabled), "false"},
      {filesystems::S3Config::baseConfigKey(filesystems::S3Config::Keys::kPathStyleAccess), "true"},
      {filesystems::S3Config::baseConfigKey(filesystems::S3Config::Keys::kIMDSEnabled), "false"}});

  auto fileSystem = filesystems::getFileSystem("s3://gluten-test-bucket/test", config);

  EXPECT_NE(dynamic_cast<GlutenS3FileSystem*>(fileSystem.get()), nullptr);
  EXPECT_EQ(fileSystem->name(), "S3");

  fileSystem.reset();
  finalizeGlutenS3FileSystem();
}

TEST(GlutenS3FileSystemTest, extractsAsyncUploadConfigs) {
  auto config = std::make_shared<velox::config::ConfigBase>(std::unordered_map<std::string, std::string>{
      {"spark.gluten.velox.s3UploadPartAsync", "true"},
      {"spark.gluten.velox.s3MaxConcurrentUploadNum", "8"},
      {"spark.gluten.velox.s3UploadThreads", "32"}});

  const auto hiveConfig = createHiveConnectorConfig(config, FileSystemType::kS3);

  const std::string prefix = filesystems::S3Config::kS3Prefix;
  EXPECT_EQ(hiveConfig->get<std::string>(prefix + "part-upload-async"), "true");
  EXPECT_EQ(hiveConfig->get<std::string>(prefix + "max-concurrent-upload-num"), "8");
  EXPECT_EQ(hiveConfig->get<std::string>(prefix + "upload-threads"), "32");
}

TEST(GlutenS3FileSystemTest, extractsPerBucketConfigs) {
  using Keys = filesystems::S3Config::Keys;
  auto config = std::make_shared<velox::config::ConfigBase>(std::unordered_map<std::string, std::string>{
      {"spark.hadoop.fs.s3a.bucket.plain-bucket.endpoint", "http://plain:9000"},
      {"spark.hadoop.fs.s3a.bucket.my.data.bucket.endpoint", "http://dotted:9000"},
      {"spark.hadoop.fs.s3a.bucket.my.data.bucket.access.key", "dotted-access"},
      {"spark.hadoop.fs.s3a.bucket.my.data.bucket.endpoint.region", "us-west-2"},
      {"spark.hadoop.fs.s3a.bucket.my.data.bucket.iam.role.session.name", "dotted-session"},
      {"spark.hadoop.fs.s3a.bucket.my.data.bucket.unknown.option", "ignored"}});

  const auto hiveConfig = createHiveConnectorConfig(config, FileSystemType::kS3);

  auto bucketValue = [&](Keys key, std::string_view bucket) {
    return hiveConfig->get<std::string>(filesystems::S3Config::bucketConfigKey(key, bucket));
  };
  EXPECT_EQ(bucketValue(Keys::kEndpoint, "plain-bucket"), "http://plain:9000");
  EXPECT_EQ(bucketValue(Keys::kEndpoint, "my.data.bucket"), "http://dotted:9000");
  EXPECT_EQ(bucketValue(Keys::kAccessKey, "my.data.bucket"), "dotted-access");
  EXPECT_EQ(bucketValue(Keys::kEndpointRegion, "my.data.bucket"), "us-west-2");
  EXPECT_EQ(bucketValue(Keys::kIamRoleSessionName, "my.data.bucket"), "dotted-session");

  // Nothing is registered under a bucket name truncated at the first dot.
  EXPECT_FALSE(bucketValue(Keys::kEndpoint, "my").has_value());

  // Only the five recognized options above produce per-bucket Velox configs.
  size_t numBucketConfigs = 0;
  for (const auto& [key, value] : hiveConfig->rawConfigs()) {
    if (key.rfind(filesystems::S3Config::kS3BucketPrefix, 0) == 0) {
      ++numBucketConfigs;
    }
  }
  EXPECT_EQ(numBucketConfigs, 5);
}

} // namespace
} // namespace gluten

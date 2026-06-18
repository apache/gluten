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

#include <memory>
#include <string_view>

#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3FileSystem.h"

namespace gluten {

namespace velox = facebook::velox;

class GlutenS3FileSystem : public velox::filesystems::S3FileSystem {
 public:
  GlutenS3FileSystem(std::string_view bucketName, const std::shared_ptr<const velox::config::ConfigBase>& config)
      : S3FileSystem(bucketName, config) {}

  std::unique_ptr<velox::WriteFile> openFileForWrite(
      std::string_view s3Path,
      const velox::filesystems::FileOptions& options) override;
};

void registerGlutenS3FileSystem(velox::filesystems::CacheKeyFn cacheKeyFunc = nullptr);

void finalizeGlutenS3FileSystem();

} // namespace gluten

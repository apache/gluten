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

#include <cstdint>
#include <memory>
#include <mutex>
#include <string_view>

#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3FileSystem.h"

namespace Aws::S3 {
class S3Client;
} // namespace Aws::S3

namespace folly {
class CPUThreadPoolExecutor;
} // namespace folly

namespace gluten {

namespace velox = facebook::velox;

class GlutenS3FileSystem : public velox::filesystems::S3FileSystem {
 public:
  GlutenS3FileSystem(std::string_view bucketName, const std::shared_ptr<const velox::config::ConfigBase>& config);

  std::unique_ptr<velox::WriteFile> openFileForWrite(
      std::string_view s3Path,
      const velox::filesystems::FileOptions& options) override;

 private:
  bool uploadPartAsync() const;
  std::shared_ptr<folly::CPUThreadPoolExecutor> uploadThreadPool(uint32_t uploadThreads);
  Aws::S3::S3Client* writeClient();

  std::string bucketName_;
  std::shared_ptr<velox::filesystems::S3Config> s3Config_;
  std::shared_ptr<Aws::S3::S3Client> writeClient_;
  std::shared_ptr<folly::CPUThreadPoolExecutor> uploadThreadPool_;
  std::mutex writeClientMutex_;
  std::mutex uploadThreadPoolMutex_;
};

void registerGlutenS3FileSystem(velox::filesystems::CacheKeyFn cacheKeyFunc = nullptr);

void finalizeGlutenS3FileSystem();

} // namespace gluten

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

#include <memory>
#include <utility>

#include "velox/common/file/File.h"

namespace gluten {

namespace velox = facebook::velox;
namespace filesystems = facebook::velox::filesystems;

namespace {

std::shared_ptr<filesystems::FileSystem> glutenS3FileSystemFactory(
    std::string_view bucketName,
    std::shared_ptr<const velox::config::ConfigBase> config) {
  return std::make_shared<GlutenS3FileSystem>(bucketName, config);
}

} // namespace

std::unique_ptr<velox::WriteFile> GlutenS3FileSystem::openFileForWrite(
    std::string_view s3Path,
    const filesystems::FileOptions& options) {
  return filesystems::S3FileSystem::openFileForWrite(s3Path, options);
}

void registerGlutenS3FileSystem(filesystems::CacheKeyFn cacheKeyFunc) {
  filesystems::registerS3FileSystem(std::move(cacheKeyFunc), glutenS3FileSystemFactory);
}

void finalizeGlutenS3FileSystem() {
  filesystems::finalizeS3FileSystem();
}

} // namespace gluten

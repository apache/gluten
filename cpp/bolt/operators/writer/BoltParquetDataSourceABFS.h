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

#include "operators/writer/BoltParquetDataSource.h"
#include "utils/ConfigExtractor.h"
#include "utils/BoltArrowUtils.h"

#include <string>

#include "arrow/c/bridge.h"
#include "compute/BoltRuntime.h"

#include "bolt/common/compression/Compression.h"
#include "bolt/core/QueryConfig.h"
#include "bolt/core/QueryCtx.h"
#include "bolt/dwio/common/Options.h"

namespace gluten {

class BoltParquetDataSourceABFS final : public BoltParquetDataSource {
 public:
  BoltParquetDataSourceABFS(
      const std::string& filePath,
      std::shared_ptr<bytedance::bolt::memory::MemoryPool> boltPool,
      std::shared_ptr<bytedance::bolt::memory::MemoryPool> sinkPool,
      std::shared_ptr<arrow::Schema> schema)
      : BoltParquetDataSource(filePath, boltPool, sinkPool, schema) {}

  void initSink(const std::unordered_map<std::string, std::string>& sparkConfs) override {
    auto hiveConf = getHiveConfig(
        std::make_shared<bytedance::bolt::config::ConfigBase>(std::unordered_map<std::string, std::string>(sparkConfs)),
        FileSystemType::kAbfs);
    auto fileSystem = filesystems::getFileSystem(filePath_, hiveConf);
    auto* abfsFileSystem = dynamic_cast<filesystems::AbfsFileSystem*>(fileSystem.get());
    sink_ = std::make_unique<dwio::common::WriteFileSink>(
        abfsFileSystem->openFileForWrite(filePath_, {{}, sinkPool_.get()}), filePath_);
  }
};

} // namespace gluten

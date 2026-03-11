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

#include "GlutenDirectBufferedInput.h"
#include "velox/connectors/hive/BufferedInputBuilder.h"
#include "velox/connectors/hive/FileHandle.h"
#include "velox/dwio/common/CachedBufferedInput.h"

namespace gluten {

class GlutenBufferedInputBuilder : public facebook::velox::connector::hive::BufferedInputBuilder {
 public:
  std::unique_ptr<facebook::velox::dwio::common::BufferedInput> create(
      const facebook::velox::FileHandle& fileHandle,
      const facebook::velox::dwio::common::ReaderOptions& readerOpts,
      const facebook::velox::connector::ConnectorQueryCtx* connectorQueryCtx,
      std::shared_ptr<facebook::velox::io::IoStatistics> ioStatistics,
      std::shared_ptr<facebook::velox::IoStats> ioStats,
      folly::Executor* executor,
      const folly::F14FastMap<std::string, std::string>& fileReadOps = {}) override {
    if (connectorQueryCtx->cache()) {
      return std::make_unique<facebook::velox::dwio::common::CachedBufferedInput>(
          fileHandle.file,
          dwio::common::MetricsLog::voidLog(),
          fileHandle.uuid,
          connectorQueryCtx->cache(),
          facebook::velox::connector::Connector::getTracker(connectorQueryCtx->scanId(), readerOpts.loadQuantum()),
          fileHandle.groupId,
          std::move(ioStatistics),
          std::move(ioStats),
          executor,
          readerOpts,
          fileReadOps);
    }
    return std::make_unique<GlutenDirectBufferedInput>(
        fileHandle.file,
        dwio::common::MetricsLog::voidLog(),
        fileHandle.uuid,
        facebook::velox::connector::Connector::getTracker(connectorQueryCtx->scanId(), readerOpts.loadQuantum()),
        fileHandle.groupId,
        std::move(ioStatistics),
        std::move(ioStats),
        executor,
        readerOpts,
        fileReadOps);
  }
};

} // namespace gluten

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

#pragma once

#include "compute/delta/DeltaDeletionVectorReader.h"
#include "compute/delta/DeltaSplit.h"

#ifndef GLUTEN_VELOX_DELTA_USE_FILE_SPLIT_READER
#if __has_include("velox/connectors/hive/FileSplitReader.h")
#define GLUTEN_VELOX_DELTA_USE_FILE_SPLIT_READER 1
#else
#define GLUTEN_VELOX_DELTA_USE_FILE_SPLIT_READER 0
#endif
#endif

#if GLUTEN_VELOX_DELTA_USE_FILE_SPLIT_READER
#include "velox/connectors/hive/HiveSplitReader.h"
#elif __has_include("velox/connectors/hive/SplitReader.h")
#include "velox/connectors/hive/SplitReader.h"
#else
#include "velox/connectors/hive/HiveDataSource.h"
#endif
#include "velox/connectors/hive/TableHandle.h"

namespace gluten::delta {

using namespace facebook::velox;
using namespace facebook::velox::connector;
using namespace facebook::velox::connector::hive;

#if GLUTEN_VELOX_DELTA_USE_FILE_SPLIT_READER
using DeltaSplitReaderBase = HiveSplitReader;
using DeltaConfig = FileConfig;
using DeltaTableHandlePtr = FileTableHandlePtr;
using DeltaColumnHandleMap = std::unordered_map<std::string, FileColumnHandlePtr>;
#else
using DeltaSplitReaderBase = SplitReader;
using DeltaConfig = HiveConfig;
using DeltaTableHandlePtr = HiveTableHandlePtr;
using DeltaColumnHandleMap = HiveColumnHandleMap;
#endif

class DeltaSplitReader : public DeltaSplitReaderBase {
 public:
  DeltaSplitReader(
      const std::shared_ptr<const HiveDeltaSplit>& hiveSplit,
      const DeltaTableHandlePtr& tableHandle,
      const DeltaColumnHandleMap* partitionKeys,
      const ConnectorQueryCtx* connectorQueryCtx,
      const std::shared_ptr<const DeltaConfig>& fileConfig,
      const RowTypePtr& readerOutputType,
#if GLUTEN_VELOX_DELTA_USE_FILE_SPLIT_READER
      const std::shared_ptr<io::IoStatistics>& dataIoStats,
      const std::shared_ptr<io::IoStatistics>& metadataIoStats,
#else
      const std::shared_ptr<io::IoStatistics>& ioStatistics,
#endif
      const std::shared_ptr<IoStats>& ioStats,
      FileHandleFactory* fileHandleFactory,
      folly::Executor* executor,
      const std::shared_ptr<common::ScanSpec>& scanSpec,
#if GLUTEN_VELOX_DELTA_USE_FILE_SPLIT_READER
      const std::unordered_map<std::string, FileColumnHandlePtr>* infoColumns,
      std::vector<column_index_t> bucketChannels = {},
#endif
      const common::SubfieldFilters* subfieldFiltersForValidation = nullptr);

  void prepareSplit(
      std::shared_ptr<common::MetadataFilter> metadataFilter,
      dwio::common::RuntimeStatistics& runtimeStats,
      const folly::F14FastMap<std::string, std::string>& fileReadOps = {}) override;

  uint64_t next(uint64_t size, VectorPtr& output) override;

 private:
  /// Validate that file statistics are consistent with deletion vector.
  /// Per Delta spec: numRecords is required when DV is present.
  /// Also validates that cardinality doesn't exceed numRecords.
  void validateStatisticsForDeletionVectors(const DeltaFileStatistics& stats, const DeltaDeletionVectorDescriptor& dv);

  // Delta deletion vectors use file-global row positions, not split-relative
  // row numbers.
  uint64_t baseReadRowNumber_;
  std::unique_ptr<DeltaDeletionVectorReader> deletionVectorReader_;
  BufferPtr deleteBitmap_;
};

} // namespace gluten::delta

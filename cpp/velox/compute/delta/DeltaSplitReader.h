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

#include "velox/connectors/hive/SplitReader.h"
#include "velox/connectors/hive/TableHandle.h"
#include "compute/delta/DeltaDeletionVectorReader.h"
#include "compute/delta/DeltaSplit.h"

namespace gluten::delta {

using namespace facebook::velox;
using namespace facebook::velox::connector;
using namespace facebook::velox::connector::hive;

class DeltaSplitReader : public SplitReader {
 public:
  DeltaSplitReader(
      const std::shared_ptr<const hive::HiveConnectorSplit>& hiveSplit,
      const HiveTableHandlePtr& hiveTableHandle,
      const HiveColumnHandleMap* partitionKeys,
      const ConnectorQueryCtx* connectorQueryCtx,
      const std::shared_ptr<const HiveConfig>& hiveConfig,
      const RowTypePtr& readerOutputType,
      const std::shared_ptr<io::IoStatistics>& ioStatistics,
      const std::shared_ptr<IoStats>& ioStats,
      FileHandleFactory* fileHandleFactory,
      folly::Executor* executor,
      const std::shared_ptr<common::ScanSpec>& scanSpec,
      const common::SubfieldFilters* subfieldFiltersForValidation = nullptr);

  void prepareSplit(
      std::shared_ptr<common::MetadataFilter> metadataFilter,
      dwio::common::RuntimeStatistics& runtimeStats,
      const folly::F14FastMap<std::string, std::string>& fileReadOps = {})
      override;

 uint64_t next(uint64_t size, VectorPtr& output) override;

 private:
  /// Validate that the protocol supports deletion vectors.
  /// Throws if protocol version is too low or feature flag is missing.
  /// This is defense-in-depth validation - Gluten should already validate
  /// at table level, but we check again at split level for safety.
  void validateProtocolForDeletionVectors(const DeltaProtocolInfo& protocol);

  /// Validate that file statistics are consistent with deletion vector.
  /// Per Delta spec: numRecords is required when DV is present.
  /// Also validates that cardinality doesn't exceed numRecords.
  void validateStatisticsForDeletionVectors(
      const DeltaFileStatistics& stats,
      const DeltaDeletionVectorDescriptor& dv);

  // Delta deletion vectors use file-global row positions, not split-relative
  // row numbers.
  uint64_t baseReadRowNumber_;
  std::unique_ptr<DeltaDeletionVectorReader> deletionVectorReader_;
  BufferPtr deleteBitmap_;
};

} // namespace gluten::delta

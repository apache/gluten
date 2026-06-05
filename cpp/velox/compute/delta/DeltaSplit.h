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

#include <limits>
#include <optional>
#include <vector>

#include "compute/Runtime.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"

namespace gluten::delta {

using namespace facebook::velox;
using namespace facebook::velox::connector;
using namespace facebook::velox::connector::hive;

enum class DeltaRowIndexFilterType {
  kKeepAll,
  kIfContained,
  kIfNotContained,
};

struct DeltaDeletionVectorDescriptor {
  std::optional<uint64_t> cardinality;
  std::optional<SplitPayloadBufferView> serializedPayloadView;

  static DeltaDeletionVectorDescriptor serialized(
      std::optional<uint64_t> cardinality = std::nullopt,
      std::optional<SplitPayloadBufferView> serializedPayloadView = std::nullopt) {
    return {cardinality, serializedPayloadView};
  }

  bool hasMaterializedPayload() const {
    return serializedPayloadView.has_value();
  }
};

/// File-level statistics for a Delta data file.
/// Used to validate consistency with deletion vectors and
/// calculate logical row counts.
struct DeltaFileStatistics {
  /// Physical number of rows in the Parquet file.
  /// Required when deletion vector is present (per Delta spec).
  std::optional<int64_t> numRecords;

  /// Whether column statistics (min/max) are tight bounds.
  /// - true: min/max values exist in the valid (non-deleted) rows
  /// - false: min/max are bounds only, may not exist in valid rows
  /// When false with a DV, statistics may be stale and unsuitable
  /// for aggregations like max(column).
  std::optional<bool> tightBounds;

  /// Calculate the logical row count accounting for deletion vectors.
  /// Returns the number of valid (non-deleted) rows.
  /// Returns -1 if numRecords is not available.
  int64_t logicalRowCount(const std::optional<DeltaDeletionVectorDescriptor>& dv) const {
    if (!numRecords.has_value()) {
      return -1; // Unknown
    }
    if (!dv.has_value() || !dv->cardinality.has_value()) {
      return *numRecords; // No deletions
    }
    return *numRecords - static_cast<int64_t>(*dv->cardinality);
  }
};

struct HiveDeltaSplit : public connector::hive::HiveConnectorSplit {
  std::optional<DeltaDeletionVectorDescriptor> deletionVector;
  std::optional<DeltaFileStatistics> statistics;
  DeltaRowIndexFilterType filterType;

  HiveDeltaSplit(
      const std::string& connectorId,
      const std::string& filePath,
      dwio::common::FileFormat fileFormat,
      uint64_t start = 0,
      uint64_t length = std::numeric_limits<uint64_t>::max(),
      const std::unordered_map<std::string, std::optional<std::string>>& partitionKeys = {},
      std::optional<int32_t> tableBucketNumber = std::nullopt,
      const std::unordered_map<std::string, std::string>& customSplitInfo = {},
      const std::shared_ptr<std::string>& extraFileInfo = {},
      const std::unordered_map<std::string, std::string>& serdeParameters = {},
      bool cacheable = true,
      std::optional<DeltaDeletionVectorDescriptor> deletionVector = std::nullopt,
      std::optional<DeltaFileStatistics> statistics = std::nullopt,
      DeltaRowIndexFilterType filterType = DeltaRowIndexFilterType::kKeepAll,
      const std::unordered_map<std::string, std::string>& infoColumns = {},
      std::optional<FileProperties> fileProperties = std::nullopt);
};

} // namespace gluten::delta

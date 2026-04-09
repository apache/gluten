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

#include <algorithm>
#include <limits>
#include <optional>
#include <string>
#include <vector>

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

enum class DeltaDeletionVectorStorageType {
  kUuidPath,    // 'u' - UUID-based relative path
  kFilePath,    // 'p' - Absolute path
  kInlineData,  // 'i' - Inline Base85 data
};

/// Protocol version information for a Delta table.
/// Used to validate that the table supports deletion vectors.
/// Per Delta spec: DVs require Reader v3+ and Writer v7+ with
/// 'deletionVectors' feature flag.
struct DeltaProtocolInfo {
  int32_t minReaderVersion;
  int32_t minWriterVersion;
  std::optional<std::vector<std::string>> readerFeatures;
  std::optional<std::vector<std::string>> writerFeatures;

  /// Check if this protocol supports deletion vectors.
  /// Returns true if:
  /// - minReaderVersion >= 3
  /// - minWriterVersion >= 7
  /// - 'deletionVectors' is in readerFeatures
  bool supportsDeletionVectors() const {
    if (minReaderVersion < 3 || minWriterVersion < 7) {
      return false;
    }
    if (!readerFeatures.has_value()) {
      return false;
    }
    return std::find(
               readerFeatures->begin(),
               readerFeatures->end(),
               "deletionVectors") != readerFeatures->end();
  }
};

struct DeltaDeletionVectorDescriptor {
  DeltaDeletionVectorStorageType storageType;
  const std::string pathOrInlineData;
  std::optional<uint64_t> offset;
  std::optional<uint64_t> sizeInBytes;
  std::optional<uint64_t> cardinality;

  /// Computes the uniqueId for this deletion vector descriptor.
  /// Used for snapshot reconstruction to differentiate the same file
  /// with different DVs in successive versions.
  /// Format: If offset is None then <storageType><pathOrInlineDv>
  ///         Otherwise <storageType><pathOrInlineDv>@<offset>
  std::string uniqueId() const {
    char storageTypeChar;
    switch (storageType) {
      case DeltaDeletionVectorStorageType::kUuidPath:
        storageTypeChar = 'u';
        break;
      case DeltaDeletionVectorStorageType::kFilePath:
        storageTypeChar = 'p';
        break;
      case DeltaDeletionVectorStorageType::kInlineData:
        storageTypeChar = 'i';
        break;
    }
    
    if (offset.has_value()) {
      return std::string(1, storageTypeChar) + pathOrInlineData + "@" +
             std::to_string(offset.value());
    }
    return std::string(1, storageTypeChar) + pathOrInlineData;
  }

  static DeltaDeletionVectorDescriptor inlineData(
      std::string data,
      std::optional<uint64_t> cardinality = std::nullopt) {
    return {
        DeltaDeletionVectorStorageType::kInlineData,
        std::move(data),
        std::nullopt,
        std::nullopt,
        cardinality};
  }

  static DeltaDeletionVectorDescriptor uuidPath(
      std::string z85EncodedUuid,
      std::optional<uint64_t> offset = std::nullopt,
      std::optional<uint64_t> sizeInBytes = std::nullopt,
      std::optional<uint64_t> cardinality = std::nullopt) {
    return {
        DeltaDeletionVectorStorageType::kUuidPath,
        std::move(z85EncodedUuid),
        offset,
        sizeInBytes,
        cardinality};
  }

  static DeltaDeletionVectorDescriptor filePath(
      std::string path,
      std::optional<uint64_t> offset = std::nullopt,
      std::optional<uint64_t> sizeInBytes = std::nullopt,
      std::optional<uint64_t> cardinality = std::nullopt) {
    return {
        DeltaDeletionVectorStorageType::kFilePath,
        std::move(path),
        offset,
        sizeInBytes,
        cardinality};
  }

  bool isInline() const {
    return storageType == DeltaDeletionVectorStorageType::kInlineData;
  }

  bool isUuidPath() const {
    return storageType == DeltaDeletionVectorStorageType::kUuidPath;
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
  int64_t logicalRowCount(
      const std::optional<DeltaDeletionVectorDescriptor>& dv) const {
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
  std::optional<DeltaProtocolInfo> protocolInfo;
  std::optional<DeltaFileStatistics> statistics;
  DeltaRowIndexFilterType filterType;

  HiveDeltaSplit(
      const std::string& connectorId,
      const std::string& filePath,
      dwio::common::FileFormat fileFormat,
      uint64_t start = 0,
      uint64_t length = std::numeric_limits<uint64_t>::max(),
      const std::unordered_map<std::string, std::optional<std::string>>&
          partitionKeys = {},
      std::optional<int32_t> tableBucketNumber = std::nullopt,
      const std::unordered_map<std::string, std::string>& customSplitInfo = {},
      const std::shared_ptr<std::string>& extraFileInfo = {},
      const std::unordered_map<std::string, std::string>& serdeParameters = {},
      bool cacheable = true,
      std::optional<DeltaDeletionVectorDescriptor> deletionVector =
          std::nullopt,
      std::optional<DeltaProtocolInfo> protocolInfo = std::nullopt,
      std::optional<DeltaFileStatistics> statistics = std::nullopt,
      DeltaRowIndexFilterType filterType = DeltaRowIndexFilterType::kKeepAll,
      const std::unordered_map<std::string, std::string>& infoColumns = {},
      std::optional<FileProperties> fileProperties = std::nullopt);
};

} // namespace gluten::delta

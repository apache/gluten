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

#include "compute/delta/DeltaSplitReader.h"

#include <sstream>

#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "compute/delta/DeltaSplit.h"
#include "compute/delta/DeltaUuidUtils.h"
#include "velox/dwio/common/BufferUtil.h"

using namespace facebook::velox::dwio::common;

namespace gluten::delta {

DeltaSplitReader::DeltaSplitReader(
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
    const common::SubfieldFilters* subfieldFiltersForValidation)
    : SplitReader(
          hiveSplit,
          hiveTableHandle,
          partitionKeys,
          connectorQueryCtx,
          hiveConfig,
          readerOutputType,
          ioStatistics,
          ioStats,
          fileHandleFactory,
          executor,
          scanSpec,
          subfieldFiltersForValidation),
      baseReadRowNumber_(0),
      deleteBitmap_(nullptr) {}

void DeltaSplitReader::prepareSplit(
    std::shared_ptr<common::MetadataFilter> metadataFilter,
    dwio::common::RuntimeStatistics& runtimeStats,
    const folly::F14FastMap<std::string, std::string>& fileReadOps) {
  SplitReader::prepareSplit(
      std::move(metadataFilter), runtimeStats, fileReadOps);
  if (emptySplit_ || !baseRowReader_) {
    return;
  }

  baseReadRowNumber_ = 0;
  deleteBitmap_.reset();
  deletionVectorReader_.reset();

  auto deltaSplit = checkedPointerCast<const HiveDeltaSplit>(hiveSplit_);
  if (!deltaSplit->deletionVector.has_value()) {
    return;
  }

  const auto& descriptor = *deltaSplit->deletionVector;
  
  // Extract and validate protocol from Hadoop configuration (passed via fileReadOps)
  auto readerVersionIt = fileReadOps.find("spark.gluten.delta.protocol.reader.version");
  auto writerVersionIt = fileReadOps.find("spark.gluten.delta.protocol.writer.version");
  
  if (readerVersionIt != fileReadOps.end() && writerVersionIt != fileReadOps.end()) {
    DeltaProtocolInfo protocolInfo;
    protocolInfo.minReaderVersion = std::stoi(readerVersionIt->second);
    protocolInfo.minWriterVersion = std::stoi(writerVersionIt->second);
    
    // Extract reader features if present
    auto readerFeaturesIt = fileReadOps.find("spark.gluten.delta.protocol.reader.features");
    if (readerFeaturesIt != fileReadOps.end() && !readerFeaturesIt->second.empty()) {
      std::vector<std::string> features;
      std::istringstream iss(readerFeaturesIt->second);
      std::string feature;
      while (std::getline(iss, feature, ',')) {
        features.push_back(feature);
      }
      protocolInfo.readerFeatures = std::move(features);
    }
    
    // Extract writer features if present
    auto writerFeaturesIt = fileReadOps.find("spark.gluten.delta.protocol.writer.features");
    if (writerFeaturesIt != fileReadOps.end() && !writerFeaturesIt->second.empty()) {
      std::vector<std::string> features;
      std::istringstream iss(writerFeaturesIt->second);
      std::string feature;
      while (std::getline(iss, feature, ',')) {
        features.push_back(feature);
      }
      protocolInfo.writerFeatures = std::move(features);
    }
    
    // Validate protocol for deletion vectors
    validateProtocolForDeletionVectors(protocolInfo);
  }
  
  // Validate protocol if also provided in split (backward compatibility)
  if (deltaSplit->protocolInfo.has_value()) {
    validateProtocolForDeletionVectors(*deltaSplit->protocolInfo);
  }
  
  // Validate statistics if provided
  if (deltaSplit->statistics.has_value()) {
    validateStatisticsForDeletionVectors(*deltaSplit->statistics, descriptor);
  }
  
  // Determine the actual file path and file system
  std::string dvPath;
  std::shared_ptr<filesystems::FileSystem> fileSystem = nullptr;
  
  if (descriptor.isInline()) {
    // Inline DV - no file system needed
    dvPath = descriptor.pathOrInlineData;
  } else if (descriptor.isUuidPath()) {
    // UUID-based path - need to reconstruct from Z85-encoded UUID
    auto [randomPrefix, uuid] =
        DeltaUuidUtils::extractUuidFromZ85(descriptor.pathOrInlineData);
    
    // Get table directory from split file path
    const std::string& filePath = hiveSplit_->filePath;
    size_t lastSlash = filePath.find_last_of('/');
    std::string tableDir = (lastSlash != std::string::npos)
        ? filePath.substr(0, lastSlash)
        : ".";
    
    dvPath = DeltaUuidUtils::reconstructUuidPath(tableDir, randomPrefix, uuid);
    fileSystem = filesystems::getFileSystem(dvPath, hiveConfig_->config());
  } else {
    // Absolute path
    dvPath = descriptor.pathOrInlineData;
    fileSystem = filesystems::getFileSystem(dvPath, hiveConfig_->config());
  }

  deletionVectorReader_ = std::make_unique<DeltaDeletionVectorReader>(
      std::move(fileSystem), connectorQueryCtx_->memoryPool(), ioStatistics_);

  if (descriptor.isInline()) {
    deletionVectorReader_->loadInlineDeletionVector(
        dvPath, descriptor.cardinality);
  } else {
    deletionVectorReader_->loadDeletionVector(
        dvPath,
        descriptor.offset,
        descriptor.sizeInBytes,
        descriptor.cardinality);
  }
}

uint64_t DeltaSplitReader::next(uint64_t size, VectorPtr& output) {
  Mutation mutation;
  mutation.randomSkip = baseReaderOpts_.randomSkip().get();
  mutation.deletedRows = nullptr;

  const auto actualSize = baseRowReader_->nextReadSize(size);
  baseReadRowNumber_ = baseRowReader_->nextRowNumber();
  if (actualSize == RowReader::kAtEnd) {
    return 0;
  }

  const auto deltaSplit = checkedPointerCast<const HiveDeltaSplit>(hiveSplit_);
  if (deletionVectorReader_ && !deletionVectorReader_->empty()) {
    const auto numBytes = bits::nbytes(actualSize);
    ensureCapacity<int8_t>(
        deleteBitmap_, numBytes, connectorQueryCtx_->memoryPool(), false, true);
    deleteBitmap_->setSize(numBytes);
    deletionVectorReader_->applyDeletionFilter(
        baseReadRowNumber_, actualSize, deleteBitmap_);
    if (deltaSplit->filterType == DeltaRowIndexFilterType::kIfNotContained) {
      bits::negate(deleteBitmap_->asMutable<uint64_t>(), actualSize);
      deleteBitmap_->setSize(numBytes);
    }
  } else if (deleteBitmap_) {
    deleteBitmap_->setSize(0);
  }

  mutation.deletedRows = deleteBitmap_ && deleteBitmap_->size() > 0
      ? deleteBitmap_->as<uint64_t>()
      : nullptr;

  auto rowsScanned = baseRowReader_->next(actualSize, output, &mutation);
  if (rowsScanned > 0 && output->size() > 0 && !bucketChannels().empty()) {
    applyBucketConversion(
        output, bucketConversionRows(*output->asChecked<RowVector>()));
  }
  return rowsScanned;
}

void DeltaSplitReader::validateProtocolForDeletionVectors(
    const DeltaProtocolInfo& protocol) {
  if (!protocol.supportsDeletionVectors()) {
    std::string readerFeatures = "none";
    if (protocol.readerFeatures.has_value()) {
      readerFeatures = folly::join(", ", *protocol.readerFeatures);
    }
    
    VELOX_USER_FAIL(
        "Deletion vectors require reader protocol version 3+ and writer "
        "protocol version 7+ with 'deletionVectors' feature enabled. "
        "Found: minReaderVersion={}, minWriterVersion={}, readerFeatures=[{}]",
        protocol.minReaderVersion,
        protocol.minWriterVersion,
        readerFeatures);
  }
}

void DeltaSplitReader::validateStatisticsForDeletionVectors(
    const DeltaFileStatistics& stats,
    const DeltaDeletionVectorDescriptor& dv) {
  // Per Delta spec: numRecords is required when DV is present
  if (!stats.numRecords.has_value()) {
    VELOX_USER_FAIL(
        "File statistics must include numRecords when deletion vector "
        "is present. This is required by the Delta Lake protocol.");
  }
  
  // Validate cardinality doesn't exceed numRecords
  if (dv.cardinality.has_value() &&
      static_cast<int64_t>(*dv.cardinality) > *stats.numRecords) {
    VELOX_USER_FAIL(
        "Deletion vector cardinality ({}) exceeds file numRecords ({}). "
        "This indicates data corruption or an invalid deletion vector.",
        *dv.cardinality,
        *stats.numRecords);
  }
  
  // Log warning if tightBounds is false (statistics may be stale)
  if (stats.tightBounds.has_value() && !*stats.tightBounds) {
    LOG(WARNING) << "File has deletion vector with loose bounds (tightBounds=false). "
                 << "Column statistics (min/max) may not be accurate for aggregations. "
                 << "Consider running OPTIMIZE to compact the deletion vector.";
  }
}

} // namespace gluten::delta

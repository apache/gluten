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

#include "compute/delta/DeltaSplitReader.h"

#include <string_view>

#include "compute/delta/DeltaSplit.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/dwio/common/BufferUtil.h"

using namespace facebook::velox::dwio::common;

namespace gluten::delta {

DeltaSplitReader::DeltaSplitReader(
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
    std::vector<column_index_t> bucketChannels,
#endif
    const common::SubfieldFilters* subfieldFiltersForValidation)
#if GLUTEN_VELOX_DELTA_USE_FILE_SPLIT_READER
    : HiveSplitReader(
          hiveSplit,
          tableHandle,
          partitionKeys,
          connectorQueryCtx,
          fileConfig,
          readerOutputType,
          dataIoStats,
          metadataIoStats,
          ioStats,
          fileHandleFactory,
          executor,
          scanSpec,
          infoColumns,
          std::move(bucketChannels),
          subfieldFiltersForValidation),
#else
    : SplitReader(
          hiveSplit,
          tableHandle,
          partitionKeys,
          connectorQueryCtx,
          fileConfig,
          readerOutputType,
          ioStatistics,
          ioStats,
          fileHandleFactory,
          executor,
          scanSpec,
          subfieldFiltersForValidation),
#endif
      baseReadRowNumber_(0),
      deleteBitmap_(nullptr) {
}

void DeltaSplitReader::prepareSplit(
    std::shared_ptr<common::MetadataFilter> metadataFilter,
    dwio::common::RuntimeStatistics& runtimeStats,
    const folly::F14FastMap<std::string, std::string>& fileReadOps) {
  (void)fileReadOps;
#if GLUTEN_VELOX_DELTA_USE_FILE_SPLIT_READER
  HiveSplitReader::prepareSplit(std::move(metadataFilter), runtimeStats, fileReadOps);
  if (emptySplit() || !baseRowReader_) {
    return;
  }
#else
  SplitReader::prepareSplit(std::move(metadataFilter), runtimeStats, fileReadOps);
  if (emptySplit_ || !baseRowReader_) {
    return;
  }
#endif

  baseReadRowNumber_ = 0;
  deleteBitmap_.reset();
  deletionVectorReader_.reset();

  auto deltaSplit = checkedPointerCast<const HiveDeltaSplit>(hiveSplit_);
  if (!deltaSplit->deletionVector.has_value()) {
    return;
  }

  const auto& descriptor = *deltaSplit->deletionVector;

  // Validate statistics if provided
  if (deltaSplit->statistics.has_value()) {
    validateStatisticsForDeletionVectors(*deltaSplit->statistics, descriptor);
  }

  VELOX_USER_CHECK(
      descriptor.hasMaterializedPayload(),
      "Delta deletion vector payload was not materialized on the JVM side for split {}",
      hiveSplit_->filePath);

  deletionVectorReader_ = std::make_unique<DeltaDeletionVectorReader>();
  const auto& payloadView = descriptor.serializedPayloadView.value();
  deletionVectorReader_->loadSerializedDeletionVector(
      std::string_view(reinterpret_cast<const char*>(payloadView.data), payloadView.size), descriptor.cardinality);
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
    ensureCapacity<int8_t>(deleteBitmap_, numBytes, connectorQueryCtx_->memoryPool(), false, true);
    deleteBitmap_->setSize(numBytes);
    deletionVectorReader_->applyDeletionFilter(baseReadRowNumber_, actualSize, deleteBitmap_);
    if (deltaSplit->filterType == DeltaRowIndexFilterType::kIfNotContained) {
      bits::negate(deleteBitmap_->asMutable<uint64_t>(), actualSize);
      deleteBitmap_->setSize(numBytes);
    }
  } else if (deleteBitmap_) {
    deleteBitmap_->setSize(0);
  }

  mutation.deletedRows = deleteBitmap_ && deleteBitmap_->size() > 0 ? deleteBitmap_->as<uint64_t>() : nullptr;

  auto rowsScanned = baseRowReader_->next(actualSize, output, &mutation);
  if (rowsScanned > 0 && output->size() > 0 && !bucketChannels().empty()) {
    applyBucketConversion(output, bucketConversionRows(*output->asChecked<RowVector>()));
  }
  return rowsScanned;
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
  if (dv.cardinality.has_value() && static_cast<int64_t>(*dv.cardinality) > *stats.numRecords) {
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

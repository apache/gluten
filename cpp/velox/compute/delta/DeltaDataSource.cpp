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

#include "compute/delta/DeltaDataSource.h"

#include "compute/delta/DeltaSplitReader.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/SelectivityVector.h"

namespace gluten::delta {

namespace {
RowTypePtr readerOutputType(const RowTypePtr& outputType, const ColumnHandleMap& assignments) {
  auto types = outputType->children();
  bool hasDeletedColumn = false;
  for (column_index_t i = 0; i < outputType->size(); ++i) {
    const auto handle = checkedPointerCast<const HiveColumnHandle>(assignments.at(outputType->nameOf(i)));
    if (handle->name() == kRowDeletedColumnName && handle->columnType() == HiveColumnHandle::ColumnType::kRowIndex) {
      VELOX_USER_CHECK(types[i]->isTinyint(), "Delta deleted-row output must be TINYINT");
      types[i] = BIGINT();
      hasDeletedColumn = true;
    }
  }
  if (!hasDeletedColumn) {
    return outputType;
  }
  auto names = outputType->names();
  return ROW(std::move(names), std::move(types));
}
} // namespace

DeltaDataSource::DeltaDataSource(
    const RowTypePtr& outputType,
    const ConnectorTableHandlePtr& tableHandle,
    const ColumnHandleMap& assignments,
    FileHandleFactory* fileHandleFactory,
    folly::Executor* ioExecutor,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<HiveConfig>& hiveConfig)
    : HiveDataSource(
          readerOutputType(outputType, assignments),
          tableHandle,
          assignments,
          fileHandleFactory,
          ioExecutor,
          connectorQueryCtx,
          hiveConfig),
      deltaOutputType_(outputType) {
  // Hive tracks one row-index name. A Delta marking scan can also project a separate row index;
  // both channels must be generated, rather than treating one as a missing physical column.
  for (const auto& [_, assignment] : assignments) {
    const auto handle = checkedPointerCast<const HiveColumnHandle>(assignment);
    if (handle->columnType() == HiveColumnHandle::ColumnType::kRowIndex) {
      auto* spec = scanSpec_->childByName(handle->name());
      VELOX_CHECK_NOT_NULL(spec);
      spec->setConstantValue(nullptr);
      spec->setColumnType(common::ScanSpec::ColumnType::kRowIndex);
      if (handle->name() == kRowDeletedColumnName) {
        rowDeletedChannel_ = spec->channel();
      }
    }
  }
}

std::optional<RowVectorPtr> DeltaDataSource::next(uint64_t size, ContinueFuture& future) {
  auto result = HiveDataSource::next(size, future);
  if (!result.has_value() || !*result) {
    return result;
  }
  auto rows = *result;
  if (rows->size() == 0) {
    return RowVector::createEmpty(deltaOutputType_, connectorQueryCtx_->memoryPool());
  }
  if (!generatedFilters_.empty()) {
    SelectivityVector selected(rows->size());
    for (const auto& [channel, filter] : generatedFilters_) {
      DecodedVector values(*rows->childAt(channel));
      selected.applyToSelected([&](auto i) {
        if (values.isNullAt(i) ? !filter->testNull() : !filter->testInt64(values.valueAt<int64_t>(i))) {
          selected.setValid(i, false);
        }
      });
      selected.updateBounds();
    }
    const auto count = selected.countSelected();
    if (count == 0) {
      // No matching rows in this batch is not end-of-split.
      return RowVector::createEmpty(deltaOutputType_, connectorQueryCtx_->memoryPool());
    }
    if (count != rows->size()) {
      auto indices = AlignedBuffer::allocate<vector_size_t>(count, connectorQueryCtx_->memoryPool());
      auto* rawIndices = indices->asMutable<vector_size_t>();
      vector_size_t index = 0;
      selected.applyToSelected([&](auto i) { rawIndices[index++] = i; });
      auto columns = rows->children();
      for (auto& column : columns) {
        column = BaseVector::wrapInDictionary(nullptr, indices, count, column);
      }
      rows = std::make_shared<RowVector>(
          connectorQueryCtx_->memoryPool(), rows->rowType(), nullptr, count, std::move(columns));
    }
  }
  if (!rowDeletedChannel_.has_value()) {
    return rows;
  }
  auto columns = rows->children();
  DecodedVector flags(*columns[*rowDeletedChannel_]);
  auto bytes = BaseVector::create(TINYINT(), rows->size(), connectorQueryCtx_->memoryPool());
  auto* values = bytes->asFlatVector<int8_t>();
  for (vector_size_t i = 0; i < rows->size(); ++i) {
    VELOX_CHECK(!flags.isNullAt(i), "Missing generated Delta deleted-row flag");
    const auto flag = flags.valueAt<int64_t>(i);
    VELOX_CHECK(flag == 0 || flag == 1, "Invalid generated Delta deleted-row flag: {}", flag);
    values->set(i, static_cast<int8_t>(flag));
  }
  columns[*rowDeletedChannel_] = std::move(bytes);
  return std::make_shared<RowVector>(
      connectorQueryCtx_->memoryPool(), deltaOutputType_, rows->nulls(), rows->size(), std::move(columns));
}

void DeltaDataSource::addDynamicFilter(column_index_t outputChannel, const std::shared_ptr<common::Filter>& filter) {
  // Velox may replace a join with its dynamic filter. Honor the filter after materialization,
  // rather than attaching it to a row-index ScanSpec (which cannot evaluate filters).
  if (scanSpec_->getChildByChannel(outputChannel).columnType() == common::ScanSpec::ColumnType::kRowIndex) {
    common::Filter::merge(filter, generatedFilters_[outputChannel]);
    return;
  }
  HiveDataSource::addDynamicFilter(outputChannel, filter);
}

void DeltaDataSource::setFromDataSource(std::unique_ptr<DataSource> source) {
  auto* deltaSource = dynamic_cast<DeltaDataSource*>(source.get());
  VELOX_CHECK_NOT_NULL(deltaSource, "Expected a preloaded Delta data source");
  for (const auto& [channel, filter] : deltaSource->generatedFilters_) {
    common::Filter::merge(filter, generatedFilters_[channel]);
  }
  HiveDataSource::setFromDataSource(std::move(source));
}

#if GLUTEN_VELOX_DELTA_USE_FILE_SPLIT_READER
std::unique_ptr<FileSplitReader> DeltaDataSource::createSplitReader() {
  auto bucketChannels = prepareSplit();
  auto deltaSplit = checkedPointerCast<const HiveDeltaSplit>(split_);

  return std::make_unique<DeltaSplitReader>(
      deltaSplit,
      tableHandle_,
      &partitionKeys_,
      connectorQueryCtx_,
      fileConfig_,
      readerOutputType_,
      dataIoStats_,
      metadataIoStats_,
      ioStats_,
      fileHandleFactory_,
      ioExecutor_,
      scanSpec_,
      &infoColumns_,
      std::move(bucketChannels),
      /*subfieldFiltersForValidation=*/getFilters());
}
#else
std::unique_ptr<SplitReader> DeltaDataSource::createSplitReader() {
  auto deltaSplit = checkedPointerCast<const HiveDeltaSplit>(split_);

  return std::make_unique<DeltaSplitReader>(
      deltaSplit,
      hiveTableHandle_,
      &partitionKeys_,
      connectorQueryCtx_,
      hiveConfig_,
      readerOutputType_,
      ioStatistics_,
      ioStats_,
      fileHandleFactory_,
      ioExecutor_,
      scanSpec_,
      /*subfieldFiltersForValidation=*/getFilters());
}
#endif

} // namespace gluten::delta

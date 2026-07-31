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

#include "IcebergWriter.h"

#include <functional>
#include <limits>
#include <numeric>
#include <unordered_set>

#include "IcebergNativeWriteInfo.pb.h"
#include "IcebergNestedField.pb.h"
#include "IcebergPartitionSpec.pb.h"
#include "compute/ProtobufUtils.h"
#include "compute/iceberg/IcebergFormat.h"
#include "config/VeloxConfig.h"
#include "utils/ConfigExtractor.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/Nulls.h"
#include "velox/connectors/hive/iceberg/IcebergDataSink.h"
#include "velox/connectors/hive/iceberg/IcebergDeletionVectorSink.h"
#include "velox/connectors/hive/iceberg/IcebergMergeSink.h"
#include "velox/vector/DecodedVector.h"

using namespace facebook::velox;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::connector::hive::iceberg;
namespace {

// Custom Iceberg file name generator for Gluten
class GlutenIcebergFileNameGenerator : public connector::hive::FileNameGenerator {
 public:
  GlutenIcebergFileNameGenerator(
      int32_t partitionId,
      int64_t taskId,
      const std::string& operationId,
      dwio::common::FileFormat fileFormat)
      : partitionId_(partitionId), taskId_(taskId), operationId_(operationId), fileFormat_(fileFormat), fileCount_(0) {}

  std::pair<std::string, std::string> gen(
      std::optional<uint32_t> bucketId,
      const std::shared_ptr<const connector::hive::HiveInsertTableHandle> insertTableHandle,
      const connector::ConnectorQueryCtx& connectorQueryCtx,
      uint32_t maxNumBuckets,
      bool commitRequired) const override {
    auto targetFileName = insertTableHandle->locationHandle()->targetFileName();
    if (targetFileName.empty()) {
      // Generate file name following Iceberg format:
      // {partitionId:05d}-{taskId}-{operationId}-{fileCount:05d}{suffix}
      fileCount_++;

      std::string fileExtension;
      switch (fileFormat_) {
        case dwio::common::FileFormat::PARQUET:
          fileExtension = ".parquet";
          break;
        case dwio::common::FileFormat::ORC:
          fileExtension = ".orc";
          break;
        default:
          fileExtension = ".parquet";
      }

      char buffer[256];
      snprintf(
          buffer,
          sizeof(buffer),
          "%05d-%" PRId64 "-%s-%05d%s",
          partitionId_,
          taskId_,
          operationId_.c_str(),
          fileCount_,
          fileExtension.c_str());
      targetFileName = std::string(buffer);
    }

    return {targetFileName, targetFileName};
  }

  folly::dynamic serialize() const override {
    VELOX_UNREACHABLE("Unexpected code path, implement serialize() first.");
  }

  std::string toString() const override {
    return fmt::format(
        "GlutenIcebergFileNameGenerator(partitionId={}, taskId={}, operationId={})",
        partitionId_,
        taskId_,
        operationId_);
  }

 private:
  int32_t partitionId_;
  int64_t taskId_;
  std::string operationId_;
  dwio::common::FileFormat fileFormat_;
  mutable int32_t fileCount_;
};

parquet::ParquetFieldId convertToIcebergNestedField(const gluten::IcebergNestedField& protoField) {
  parquet::ParquetFieldId result;
  result.fieldId = protoField.id();

  // Recursively convert children
  result.children.reserve(protoField.children_size());
  for (const auto& protoChild : protoField.children()) {
    result.children.push_back(convertToIcebergNestedField(protoChild));
  }

  return result;
}

std::shared_ptr<IcebergInsertTableHandle> createIcebergInsertTableHandle(
    const RowTypePtr& outputRowType,
    const std::string& outputDirectoryPath,
    dwio::common::FileFormat fileFormat,
    facebook::velox::common::CompressionKind compressionKind,
    int32_t partitionId,
    int64_t taskId,
    const std::string& operationId,
    std::shared_ptr<const IcebergPartitionSpec> spec,
    const parquet::ParquetFieldId& nestedField,
    IcebergInsertTableHandle::WriteKind writeKind,
    std::unordered_map<std::string, IcebergInsertTableHandle::ExistingDeletionVector> existingDeletionVectors,
    facebook::velox::memory::MemoryPool* pool) {
  std::vector<std::shared_ptr<const iceberg::IcebergColumnHandle>> columnHandles;

  std::vector<std::string> columnNames = outputRowType->names();
  columnHandles.reserve(columnNames.size());
  std::vector<TypePtr> columnTypes = outputRowType->children();
  std::vector<std::string> partitionColumns;
  partitionColumns.reserve(spec->fields.size());
  for (const auto& field : spec->fields) {
    partitionColumns.push_back(field.name);
  }
  for (auto i = 0; i < columnNames.size(); ++i) {
    if (std::find(partitionColumns.begin(), partitionColumns.end(), columnNames[i]) != partitionColumns.end()) {
      columnHandles.push_back(std::make_shared<iceberg::IcebergColumnHandle>(
          columnNames.at(i),
          connector::hive::HiveColumnHandle::ColumnType::kPartitionKey,
          columnTypes.at(i),
          nestedField.children[i]));
    } else {
      columnHandles.push_back(std::make_shared<iceberg::IcebergColumnHandle>(
          columnNames.at(i),
          connector::hive::HiveColumnHandle::ColumnType::kRegular,
          columnTypes.at(i),
          nestedField.children[i]));
    }
  }

  auto fileNameGenerator =
      std::make_shared<const GlutenIcebergFileNameGenerator>(partitionId, taskId, operationId, fileFormat);

  std::shared_ptr<const connector::hive::LocationHandle> locationHandle =
      std::make_shared<connector::hive::LocationHandle>(
          outputDirectoryPath, outputDirectoryPath, connector::hive::LocationHandle::TableType::kExisting);
  const std::unordered_map<std::string, std::string> serdeParameters;
  return std::make_shared<connector::hive::iceberg::IcebergInsertTableHandle>(
      columnHandles,
      locationHandle,
      fileFormat,
      spec,
      compressionKind,
      serdeParameters,
      writeKind,
      std::move(existingDeletionVectors),
      fileNameGenerator);
}

IcebergInsertTableHandle::WriteKind toVeloxWriteKind(gluten::IcebergWriteMode writeMode) {
  switch (writeMode) {
    case gluten::IcebergWriteMode::kData:
      return IcebergInsertTableHandle::WriteKind::kData;
    case gluten::IcebergWriteMode::kDeletionVector:
      return IcebergInsertTableHandle::WriteKind::kDeletionVector;
    case gluten::IcebergWriteMode::kMerge:
      return IcebergInsertTableHandle::WriteKind::kMerge;
  }
  VELOX_UNREACHABLE("Unknown Iceberg write mode");
}

std::unordered_map<std::string, IcebergInsertTableHandle::ExistingDeletionVector> toVeloxExistingDeletionVectors(
    const std::vector<gluten::IcebergExistingDeletionVectorInfo>& descriptors) {
  std::unordered_map<std::string, IcebergInsertTableHandle::ExistingDeletionVector> result;
  result.reserve(descriptors.size());
  for (const auto& descriptor : descriptors) {
    VELOX_USER_CHECK(!descriptor.referencedDataFile.empty(), "Existing deletion vector data-file path is empty");
    VELOX_USER_CHECK(!descriptor.puffinPath.empty(), "Existing deletion vector Puffin path is empty");
    VELOX_USER_CHECK_GE(descriptor.contentOffset, 0, "Existing deletion vector content offset is negative");
    VELOX_USER_CHECK_GT(descriptor.contentLength, 0, "Existing deletion vector content length must be positive");
    VELOX_USER_CHECK_GT(descriptor.recordCount, 0, "Existing deletion vector record count must be positive");
    VELOX_USER_CHECK_GT(descriptor.fileSizeInBytes, 0, "Existing deletion vector file size must be positive");
    VELOX_USER_CHECK_LE(
        descriptor.contentOffset,
        descriptor.fileSizeInBytes,
        "Existing deletion vector content offset exceeds file size");
    VELOX_USER_CHECK_LE(
        descriptor.contentLength,
        descriptor.fileSizeInBytes - descriptor.contentOffset,
        "Existing deletion vector content range exceeds file size");
    auto [unused, inserted] = result.emplace(
        descriptor.referencedDataFile,
        IcebergInsertTableHandle::ExistingDeletionVector{
            descriptor.puffinPath,
            descriptor.contentOffset,
            descriptor.contentLength,
            descriptor.recordCount,
            descriptor.fileSizeInBytes});
    VELOX_USER_CHECK(inserted, "Duplicate existing deletion vector for data file {}", descriptor.referencedDataFile);
  }
  return result;
}

void validateChannel(
    const RowTypePtr& inputType,
    column_index_t channel,
    const char* name,
    const std::function<bool(const TypePtr&)>& typeCheck,
    const char* expectedType) {
  VELOX_USER_CHECK_LT(channel, inputType->size(), "{} channel {} is out of range", name, channel);
  VELOX_USER_CHECK(
      typeCheck(inputType->childAt(channel)),
      "{} channel {} must be {}, got {}",
      name,
      channel,
      expectedType,
      inputType->childAt(channel)->toString());
}

int32_t sparkOperationAt(const DecodedVector& operations, const TypePtr& type, vector_size_t row) {
  VELOX_USER_CHECK(!operations.isNullAt(row), "Spark row operation is null at row {}", row);
  switch (type->kind()) {
    case TypeKind::TINYINT:
      return operations.valueAt<int8_t>(row);
    case TypeKind::SMALLINT:
      return operations.valueAt<int16_t>(row);
    case TypeKind::INTEGER:
      return operations.valueAt<int32_t>(row);
    case TypeKind::BIGINT: {
      const auto value = operations.valueAt<int64_t>(row);
      VELOX_USER_CHECK_GE(value, std::numeric_limits<int32_t>::min(), "Spark row operation is out of range");
      VELOX_USER_CHECK_LE(value, std::numeric_limits<int32_t>::max(), "Spark row operation is out of range");
      return static_cast<int32_t>(value);
    }
    default:
      VELOX_USER_FAIL("Spark row operation must be an integer type, got {}", type->toString());
  }
}

constexpr int32_t kSparkDeleteOperation = 1;
constexpr int32_t kSparkUpdateOperation = 2;
constexpr int32_t kSparkInsertOperation = 3;
constexpr int32_t kSparkReinsertOperation = 4;

struct NormalizedOperation {
  vector_size_t sourceRow;
  int8_t operation;
  bool insertFromUpdate;
};

std::vector<NormalizedOperation>
normalizeSparkOperations(const RowVectorPtr& input, column_index_t operationChannel, bool deletionVectorOnly) {
  const auto& operationVector = input->childAt(operationChannel);
  SelectivityVector rows(input->size());
  DecodedVector operations(*operationVector, rows);
  std::vector<NormalizedOperation> normalized;
  normalized.reserve(deletionVectorOnly ? input->size() : input->size() * 2);
  for (vector_size_t row = 0; row < input->size(); ++row) {
    const auto operation = sparkOperationAt(operations, operationVector->type(), row);
    switch (operation) {
      case kSparkDeleteOperation:
        normalized.push_back({row, IcebergMergeSink::kDeleteOperationNumber, false});
        break;
      case kSparkUpdateOperation:
        VELOX_USER_CHECK(
            !deletionVectorOnly,
            "Deletion-vector-only writes accept only Spark DELETE operations; got UPDATE at row {}",
            row);
        normalized.push_back({row, IcebergMergeSink::kDeleteOperationNumber, false});
        normalized.push_back({row, IcebergMergeSink::kInsertOperationNumber, true});
        break;
      case kSparkInsertOperation:
      case kSparkReinsertOperation:
        VELOX_USER_CHECK(
            !deletionVectorOnly,
            "Deletion-vector-only writes accept only Spark DELETE operations; got operation {} at row {}",
            operation,
            row);
        normalized.push_back({row, IcebergMergeSink::kInsertOperationNumber, false});
        break;
      default:
        VELOX_USER_FAIL("Unsupported Spark row operation {} at row {}", operation, row);
    }
  }
  return normalized;
}

std::vector<column_index_t> resolveDataChannels(
    const RowTypePtr& inputType,
    const RowTypePtr& dataType,
    const gluten::IcebergNativeWriteOptions& options) {
  std::vector<column_index_t> channels = options.dataColumnIndices;
  if (channels.empty()) {
    VELOX_USER_CHECK_EQ(
        inputType->size(),
        dataType->size(),
        "Data write input has {} columns but the Iceberg data schema has {}; provide explicit data column indices",
        inputType->size(),
        dataType->size());
    channels.resize(dataType->size());
    std::iota(channels.begin(), channels.end(), 0);
  }
  VELOX_USER_CHECK_EQ(
      channels.size(), dataType->size(), "Expected {} data column indices, got {}", dataType->size(), channels.size());
  std::unordered_set<column_index_t> seen;
  for (size_t i = 0; i < channels.size(); ++i) {
    const auto channel = channels[i];
    VELOX_USER_CHECK_LT(channel, inputType->size(), "Data column index {} is out of range", channel);
    VELOX_USER_CHECK(seen.emplace(channel).second, "Duplicate data column index {}", channel);
    VELOX_USER_CHECK(
        inputType->childAt(channel)->equivalent(*dataType->childAt(i)),
        "Data column {} type {} does not match Iceberg column {} type {}",
        channel,
        inputType->childAt(channel)->toString(),
        i,
        dataType->childAt(i)->toString());
  }
  return channels;
}

RowTypePtr normalizedMergeType(const RowTypePtr& dataType) {
  auto outputNames = dataType->names();
  outputNames.emplace_back("operation");
  outputNames.emplace_back("row_id");
  outputNames.emplace_back("insert_from_update");
  auto outputTypes = dataType->children();
  outputTypes.emplace_back(TINYINT());
  outputTypes.emplace_back(ROW({"file_path", "pos"}, {VARCHAR(), BIGINT()}));
  outputTypes.emplace_back(TINYINT());
  return ROW(std::move(outputNames), std::move(outputTypes));
}

RowVectorPtr selectDataColumns(
    const RowVectorPtr& input,
    const RowTypePtr& dataType,
    const gluten::IcebergNativeWriteOptions& options,
    memory::MemoryPool* pool) {
  const auto inputType = asRowType(input->type());
  auto channels = resolveDataChannels(inputType, dataType, options);
  std::vector<VectorPtr> children;
  children.reserve(channels.size());
  for (auto channel : channels) {
    children.push_back(input->childAt(channel));
  }
  return std::make_shared<RowVector>(pool, dataType, input->nulls(), input->size(), std::move(children));
}

RowVectorPtr normalizeDeletionVectorInput(
    const RowVectorPtr& input,
    const gluten::IcebergNativeWriteOptions& options,
    memory::MemoryPool* pool) {
  VELOX_USER_CHECK(options.operationColumnIndex.has_value(), "Deletion-vector write requires an operation channel");
  VELOX_USER_CHECK(options.filePathColumnIndex.has_value(), "Deletion-vector write requires a file-path channel");
  VELOX_USER_CHECK(options.rowPositionColumnIndex.has_value(), "Deletion-vector write requires a row-position channel");
  const auto inputType = asRowType(input->type());
  validateChannel(
      inputType,
      *options.operationColumnIndex,
      "Operation",
      [](const TypePtr& type) {
        return type->isTinyint() || type->isSmallint() || type->isInteger() || type->isBigint();
      },
      "an integer");
  validateChannel(
      inputType,
      *options.filePathColumnIndex,
      "File path",
      [](const TypePtr& type) { return type->isVarchar(); },
      "VARCHAR");
  validateChannel(
      inputType,
      *options.rowPositionColumnIndex,
      "Row position",
      [](const TypePtr& type) { return type->isBigint(); },
      "BIGINT");
  const auto operations = normalizeSparkOperations(input, *options.operationColumnIndex, true);
  VELOX_CHECK_EQ(operations.size(), input->size());
  return std::make_shared<RowVector>(
      pool,
      ROW({"file_path", "pos"}, {VARCHAR(), BIGINT()}),
      input->nulls(),
      input->size(),
      std::vector<VectorPtr>{
          input->childAt(*options.filePathColumnIndex), input->childAt(*options.rowPositionColumnIndex)});
}

RowVectorPtr normalizeMergeInput(
    const RowVectorPtr& input,
    const RowTypePtr& dataType,
    const gluten::IcebergNativeWriteOptions& options,
    memory::MemoryPool* pool) {
  VELOX_USER_CHECK(options.operationColumnIndex.has_value(), "Merge write requires an operation channel");
  VELOX_USER_CHECK(options.filePathColumnIndex.has_value(), "Merge write requires a file-path channel");
  VELOX_USER_CHECK(options.rowPositionColumnIndex.has_value(), "Merge write requires a row-position channel");
  const auto inputType = asRowType(input->type());
  validateChannel(
      inputType,
      *options.operationColumnIndex,
      "Operation",
      [](const TypePtr& type) {
        return type->isTinyint() || type->isSmallint() || type->isInteger() || type->isBigint();
      },
      "an integer");
  validateChannel(
      inputType,
      *options.filePathColumnIndex,
      "File path",
      [](const TypePtr& type) { return type->isVarchar(); },
      "VARCHAR");
  validateChannel(
      inputType,
      *options.rowPositionColumnIndex,
      "Row position",
      [](const TypePtr& type) { return type->isBigint(); },
      "BIGINT");
  const auto dataChannels = resolveDataChannels(inputType, dataType, options);
  const auto operations = normalizeSparkOperations(input, *options.operationColumnIndex, false);
  const auto outputSize = static_cast<vector_size_t>(operations.size());
  if (outputSize == 0) {
    return BaseVector::create<RowVector>(normalizedMergeType(dataType), 0, pool);
  }
  auto indices = AlignedBuffer::allocate<vector_size_t>(outputSize, pool);
  auto* rawIndices = indices->asMutable<vector_size_t>();
  auto dataNulls = allocateNulls(outputSize, pool);
  auto rowIdNulls = allocateNulls(outputSize, pool);
  auto* rawDataNulls = dataNulls->asMutable<uint64_t>();
  auto* rawRowIdNulls = rowIdNulls->asMutable<uint64_t>();
  auto operationVector = BaseVector::create(TINYINT(), outputSize, pool);
  auto insertFromUpdateVector = BaseVector::create(TINYINT(), outputSize, pool);
  auto* rawOperation = operationVector->asFlatVector<int8_t>();
  auto* rawInsertFromUpdate = insertFromUpdateVector->asFlatVector<int8_t>();
  for (vector_size_t outputRow = 0; outputRow < outputSize; ++outputRow) {
    const auto& operation = operations[outputRow];
    rawIndices[outputRow] = operation.sourceRow;
    const bool isDelete = operation.operation == IcebergMergeSink::kDeleteOperationNumber;
    bits::setNull(rawDataNulls, outputRow, isDelete);
    bits::setNull(rawRowIdNulls, outputRow, !isDelete);
    rawOperation->set(outputRow, operation.operation);
    rawInsertFromUpdate->set(outputRow, operation.insertFromUpdate ? 1 : 0);
    if (isDelete) {
      VELOX_USER_CHECK(
          !input->childAt(*options.filePathColumnIndex)->isNullAt(operation.sourceRow),
          "File path is null for Spark DELETE/UPDATE row {}",
          operation.sourceRow);
      VELOX_USER_CHECK(
          !input->childAt(*options.rowPositionColumnIndex)->isNullAt(operation.sourceRow),
          "Row position is null for Spark DELETE/UPDATE row {}",
          operation.sourceRow);
    }
  }

  std::vector<VectorPtr> outputChildren;
  outputChildren.reserve(dataChannels.size() + 3);
  for (auto channel : dataChannels) {
    outputChildren.push_back(BaseVector::wrapInDictionary(dataNulls, indices, outputSize, input->childAt(channel)));
  }
  auto filePath =
      BaseVector::wrapInDictionary(nullptr, indices, outputSize, input->childAt(*options.filePathColumnIndex));
  auto rowPosition =
      BaseVector::wrapInDictionary(nullptr, indices, outputSize, input->childAt(*options.rowPositionColumnIndex));
  auto rowIdType = ROW({"file_path", "pos"}, {VARCHAR(), BIGINT()});
  auto rowId = std::make_shared<RowVector>(
      pool, rowIdType, rowIdNulls, outputSize, std::vector<VectorPtr>{std::move(filePath), std::move(rowPosition)});
  outputChildren.push_back(std::move(operationVector));
  outputChildren.push_back(std::move(rowId));
  outputChildren.push_back(std::move(insertFromUpdateVector));

  return std::make_shared<RowVector>(
      pool, normalizedMergeType(dataType), nullptr, outputSize, std::move(outputChildren));
}

} // namespace

namespace gluten {
IcebergWriter::IcebergWriter(
    const RowTypePtr& rowType,
    int32_t format,
    const std::string& outputDirectory,
    facebook::velox::common::CompressionKind compressionKind,
    int32_t partitionId,
    int64_t taskId,
    const std::string& operationId,
    std::shared_ptr<const iceberg::IcebergPartitionSpec> spec,
    const gluten::IcebergNestedField& field,
    const std::unordered_map<std::string, std::string>& sparkConfs,
    std::shared_ptr<facebook::velox::memory::MemoryPool> memoryPool,
    std::shared_ptr<facebook::velox::memory::MemoryPool> connectorPool,
    IcebergNativeWriteOptions writeOptions)
    : rowType_(rowType),
      field_(convertToIcebergNestedField(field)),
      partitionId_(partitionId),
      taskId_(taskId),
      operationId_(operationId),
      pool_(memoryPool),
      connectorPool_(connectorPool),
      writeOptions_(std::move(writeOptions)),
      createTimeNs_(getCurrentTimeNano()) {
  if (writeOptions_.writeMode != IcebergWriteMode::kData) {
    VELOX_USER_CHECK_EQ(format, 1, "Iceberg deletion-vector writes require Parquet data files");
  }
  auto veloxCfg =
      std::make_shared<facebook::velox::config::ConfigBase>(std::unordered_map<std::string, std::string>(sparkConfs));
  connectorSessionProperties_ = createHiveConnectorSessionConfig(veloxCfg);
  connectorConfig_ =
      std::make_shared<facebook::velox::connector::hive::HiveConfig>(createHiveConnectorConfig(veloxCfg));
  std::unordered_map<std::string, std::shared_ptr<facebook::velox::config::ConfigBase>> connectorConfigs;
  connectorConfigs[kHiveConnectorId] = connectorSessionProperties_;
  auto queryConfigBase =
      std::make_shared<facebook::velox::config::ConfigBase>(std::unordered_map<std::string, std::string>(sparkConfs));
  queryCtx_ = facebook::velox::core::QueryCtx::create(
      nullptr,
      facebook::velox::core::QueryConfig{facebook::velox::core::QueryConfig::ConfigTag{}, queryConfigBase},
      connectorConfigs,
      nullptr, // cache
      pool_,
      nullptr, // spillExecutor
      "IcebergWriter");

  auto expressionEvaluator =
      std::make_unique<facebook::velox::exec::SimpleExpressionEvaluator>(queryCtx_.get(), pool_.get());

  connectorQueryCtx_ = std::make_unique<connector::ConnectorQueryCtx>(
      pool_.get(),
      connectorPool_.get(),
      connectorSessionProperties_.get(),
      nullptr,
      common::PrefixSortConfig(),
      std::move(expressionEvaluator),
      nullptr,
      "query.IcebergDataSink",
      "task.IcebergDataSink",
      "planNodeId.IcebergDataSink",
      0,
      "");
  auto icebergConfig = std::make_shared<facebook::velox::connector::hive::iceberg::IcebergConfig>(veloxCfg);
  auto insertTableHandle = createIcebergInsertTableHandle(
      rowType_,
      outputDirectory,
      icebergFormatToVelox(format),
      compressionKind,
      partitionId_,
      taskId_,
      operationId_,
      spec,
      field_,
      toVeloxWriteKind(writeOptions_.writeMode),
      toVeloxExistingDeletionVectors(writeOptions_.existingDeletionVectors),
      pool_.get());
  switch (writeOptions_.writeMode) {
    case IcebergWriteMode::kData:
      dataSink_ = std::make_unique<IcebergDataSink>(
          rowType_,
          std::move(insertTableHandle),
          connectorQueryCtx_.get(),
          facebook::velox::connector::CommitStrategy::kNoCommit,
          connectorConfig_,
          icebergConfig);
      break;
    case IcebergWriteMode::kDeletionVector:
      dataSink_ = std::make_unique<IcebergDeletionVectorSink>(
          ROW({"file_path", "pos"}, {VARCHAR(), BIGINT()}),
          std::move(insertTableHandle),
          connectorQueryCtx_.get(),
          facebook::velox::connector::CommitStrategy::kNoCommit,
          connectorConfig_);
      break;
    case IcebergWriteMode::kMerge: {
      std::vector<column_index_t> targetColumnChannels(rowType_->size());
      std::iota(targetColumnChannels.begin(), targetColumnChannels.end(), 0);
      dataSink_ = std::make_unique<IcebergMergeSink>(
          normalizedMergeType(rowType_),
          std::move(insertTableHandle),
          connectorQueryCtx_.get(),
          facebook::velox::connector::CommitStrategy::kNoCommit,
          connectorConfig_,
          icebergConfig,
          std::move(targetColumnChannels),
          static_cast<column_index_t>(rowType_->size()),
          static_cast<column_index_t>(rowType_->size() + 1));
      break;
    }
  }
}

void IcebergWriter::write(const VeloxColumnarBatch& batch) {
  auto inputRowVector = batch.getRowVector();
  RowVectorPtr normalizedInput;
  switch (writeOptions_.writeMode) {
    case IcebergWriteMode::kData: {
      auto inputRowType = asRowType(inputRowVector->type());
      if (writeOptions_.dataColumnIndices.empty() && inputRowType->size() != rowType_->size()) {
        VELOX_USER_CHECK_GE(
            inputRowType->size(),
            rowType_->size() + 1,
            "Legacy Iceberg write input does not contain enough data columns");
        auto legacyOptions = writeOptions_;
        legacyOptions.dataColumnIndices.resize(rowType_->size());
        std::iota(legacyOptions.dataColumnIndices.begin(), legacyOptions.dataColumnIndices.end(), 1);
        normalizedInput = selectDataColumns(inputRowVector, rowType_, legacyOptions, pool_.get());
      } else {
        normalizedInput = selectDataColumns(inputRowVector, rowType_, writeOptions_, pool_.get());
      }
      break;
    }
    case IcebergWriteMode::kDeletionVector:
      normalizedInput = normalizeDeletionVectorInput(inputRowVector, writeOptions_, pool_.get());
      break;
    case IcebergWriteMode::kMerge:
      normalizedInput = normalizeMergeInput(inputRowVector, rowType_, writeOptions_, pool_.get());
      break;
  }
  dataSink_->appendData(std::move(normalizedInput));
}

std::vector<std::string> IcebergWriter::commit() {
  try {
    constexpr int32_t kMaxFinishIterations = 1'000'000;
    int32_t finishIterations = 0;
    while (!dataSink_->finish()) {
      VELOX_CHECK_LT(
          ++finishIterations,
          kMaxFinishIterations,
          "Iceberg writer did not finish after {} iterations",
          kMaxFinishIterations);
    }
    return dataSink_->close();
  } catch (...) {
    try {
      dataSink_->abort();
    } catch (...) {
      // Preserve the original write failure.
    }
    throw;
  }
}

void IcebergWriter::abort() {
  dataSink_->abort();
}

WriteStats IcebergWriter::writeStats() const {
  const auto currentTimeNs = getCurrentTimeNano();
  VELOX_CHECK_GE(currentTimeNs, createTimeNs_);
  const auto sinkStats = dataSink_->stats();
  return WriteStats{
      sinkStats.numWrittenBytes,
      sinkStats.numWrittenFiles,
      sinkStats.writeIOTimeUs * 1000,
      currentTimeNs - createTimeNs_};
}

IcebergNativeWriteOptions parseIcebergNativeWriteInfo(const gluten::IcebergNativeWriteInfo& writeInfo) {
  IcebergNativeWriteOptions options;
  switch (writeInfo.write_mode()) {
    case gluten::ICEBERG_NATIVE_WRITE_MODE_DATA:
      options.writeMode = IcebergWriteMode::kData;
      break;
    case gluten::ICEBERG_NATIVE_WRITE_MODE_DELETION_VECTOR:
      options.writeMode = IcebergWriteMode::kDeletionVector;
      break;
    case gluten::ICEBERG_NATIVE_WRITE_MODE_MERGE:
      options.writeMode = IcebergWriteMode::kMerge;
      break;
    default:
      VELOX_USER_FAIL("Unknown Iceberg native write mode {}", static_cast<int32_t>(writeInfo.write_mode()));
  }

  options.dataColumnIndices.reserve(writeInfo.data_column_indices_size());
  for (const auto index : writeInfo.data_column_indices()) {
    VELOX_USER_CHECK_GE(index, 0, "Data column index must be non-negative");
    options.dataColumnIndices.push_back(index);
  }
  if (writeInfo.has_operation_column_index()) {
    VELOX_USER_CHECK_GE(writeInfo.operation_column_index(), 0, "Operation column index must be non-negative");
    options.operationColumnIndex = writeInfo.operation_column_index();
  }
  if (writeInfo.has_file_path_column_index()) {
    VELOX_USER_CHECK_GE(writeInfo.file_path_column_index(), 0, "File-path column index must be non-negative");
    options.filePathColumnIndex = writeInfo.file_path_column_index();
  }
  if (writeInfo.has_row_position_column_index()) {
    VELOX_USER_CHECK_GE(writeInfo.row_position_column_index(), 0, "Row-position column index must be non-negative");
    options.rowPositionColumnIndex = writeInfo.row_position_column_index();
  }
  options.existingDeletionVectors.reserve(writeInfo.existing_deletion_vectors_size());
  for (const auto& descriptor : writeInfo.existing_deletion_vectors()) {
    options.existingDeletionVectors.push_back(
        {descriptor.referenced_data_file(),
         descriptor.puffin_path(),
         descriptor.content_offset(),
         descriptor.content_length(),
         descriptor.record_count(),
         descriptor.file_size_in_bytes()});
  }
  return options;
}

std::shared_ptr<const iceberg::IcebergPartitionSpec>
parseIcebergPartitionSpec(const uint8_t* data, const int32_t length, RowTypePtr rowType) {
  gluten::IcebergPartitionSpec protoSpec;
  gluten::parseProtobuf(data, length, &protoSpec);
  std::vector<iceberg::IcebergPartitionSpec::Field> fields;
  fields.reserve(protoSpec.fields_size());

  for (const auto& protoField : protoSpec.fields()) {
    // Convert protobuf enum to C++ enum
    iceberg::TransformType transform;
    switch (protoField.transform()) {
      case gluten::IDENTITY:
        transform = iceberg::TransformType::kIdentity;
        break;
      case gluten::YEAR:
        transform = iceberg::TransformType::kYear;
        break;
      case gluten::MONTH:
        transform = iceberg::TransformType::kMonth;
        break;
      case gluten::DAY:
        transform = iceberg::TransformType::kDay;
        break;
      case gluten::HOUR:
        transform = iceberg::TransformType::kHour;
        break;
      case gluten::BUCKET:
        transform = iceberg::TransformType::kBucket;
        break;
      case gluten::TRUNCATE:
        transform = iceberg::TransformType::kTruncate;
        break;
      default:
        throw std::runtime_error("Unknown transform type");
    }

    // Handle optional parameter
    std::optional<int32_t> parameter;
    if (protoField.has_parameter()) {
      parameter = protoField.parameter();
    }

    fields.push_back({protoField.name(), rowType->findChild(protoField.name()), transform, parameter});
  }

  return std::make_shared<iceberg::IcebergPartitionSpec>(protoSpec.spec_id(), fields);
}

} // namespace gluten

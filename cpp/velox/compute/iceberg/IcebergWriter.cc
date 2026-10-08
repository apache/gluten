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

#include <folly/json.h>
#include <atomic>
#include <unordered_set>

#include "IcebergNestedField.pb.h"
#include "IcebergPartitionSpec.pb.h"
#include "compute/ProtobufUtils.h"
#include "compute/VeloxBackend.h"
#include "compute/iceberg/IcebergFormat.h"
#include "config/VeloxConfig.h"
#include "utils/ConfigExtractor.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/iceberg/IcebergDataSink.h"
#include "velox/connectors/hive/iceberg/IcebergDeleteFile.h"

using namespace facebook::velox;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::connector::hive::iceberg;
namespace {

class GlutenIcebergDataSink : public IcebergDataSink {
 public:
  using IcebergDataSink::IcebergDataSink;

  void abortAndDeleteFiles(const std::shared_ptr<const config::ConfigBase>& config) {
    std::unordered_set<std::string> paths;
    for (const auto& info : writerInfo_) {
      const auto& directory = info->writerParameters.writeDirectory();
      for (const auto& file : info->writtenFiles) {
        paths.insert(directory + "/" + file.writeFileName);
      }
      if (!info->currentWriteFileName.empty()) {
        paths.insert(directory + "/" + info->currentWriteFileName);
      }
    }
    if (state_ != State::kClosed && state_ != State::kAborted) {
      IcebergDataSink::abort();
    }
    for (const auto& path : paths) {
      auto fs = filesystems::getFileSystem(path, config);
      if (fs->exists(path)) {
        fs->remove(path);
      }
    }
  }
};

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

TypePtr withTimestampSemantics(
    const TypePtr& type,
    const parquet::ParquetFieldId& field,
    const std::unordered_set<int32_t>& timestampsWithoutZone) {
  if (type->isRow()) {
    std::vector<TypePtr> children;
    children.reserve(type->size());
    for (size_t i = 0; i < type->size(); ++i) {
      children.push_back(withTimestampSemantics(type->childAt(i), field.children[i], timestampsWithoutZone));
    }
    return ROW(type->asRow().names(), std::move(children));
  }
  if (type->kind() == TypeKind::TIMESTAMP) {
    // Arrow exports TIMESTAMP_UTC without a timezone, even when the writer's
    // timezone is UTC. This preserves Iceberg's per-field Parquet annotations.
    return timestampsWithoutZone.count(field.fieldId) ? TIMESTAMP_UTC() : TIMESTAMP();
  }
  return type;
}

void collectEqualityFields(
    const TypePtr& type,
    const parquet::ParquetFieldId& field,
    std::unordered_set<int32_t>& eligibleIds) {
  VELOX_USER_CHECK_EQ(type->size(), field.children.size(), "Iceberg field IDs do not match the writer schema");
  if (type->isRow()) {
    for (size_t i = 0; i < type->size(); ++i) {
      collectEqualityFields(type->childAt(i), field.children[i], eligibleIds);
    }
  } else if (type->isPrimitiveType() && type->kind() != TypeKind::REAL && type->kind() != TypeKind::DOUBLE) {
    VELOX_USER_CHECK_GT(field.fieldId, 0, "Iceberg equality field IDs must be positive");
    VELOX_USER_CHECK(eligibleIds.insert(field.fieldId).second, "Duplicate Iceberg field ID: {}", field.fieldId);
  }
}

void validateEqualityFields(
    const RowTypePtr& rowType,
    const parquet::ParquetFieldId& field,
    const std::vector<int32_t>& equalityFieldIds) {
  VELOX_USER_CHECK(!equalityFieldIds.empty(), "Equality field IDs cannot be empty");
  std::unordered_set<int32_t> eligibleIds;
  collectEqualityFields(rowType, field, eligibleIds);
  std::unordered_set<int32_t> seen;
  for (const auto id : equalityFieldIds) {
    VELOX_USER_CHECK(
        eligibleIds.count(id),
        "Equality field ID {} must identify a non-floating-point primitive field outside lists and maps in the writer schema",
        id);
    VELOX_USER_CHECK(seen.insert(id).second, "Duplicate equality field ID: {}", id);
  }
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
    const std::unordered_map<std::string, std::string>& serdeParameters,
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
  auto writeKind = connector::hive::iceberg::IcebergInsertTableHandle::WriteKind::kData;
  return std::make_shared<connector::hive::iceberg::IcebergInsertTableHandle>(
      columnHandles,
      locationHandle,
      fileFormat,
      spec,
      compressionKind,
      serdeParameters,
      writeKind,
      std::unordered_map<std::string, connector::hive::iceberg::IcebergInsertTableHandle::ExistingDeletionVector>{},
      fileNameGenerator);
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
    std::optional<std::vector<int32_t>> equalityFieldIds)
    : rowType_(rowType),
      field_(convertToIcebergNestedField(field)),
      equalityFieldIds_(std::move(equalityFieldIds)),
      partitionId_(partitionId),
      taskId_(taskId),
      operationId_(equalityFieldIds_ ? operationId + "-equality-delete" : operationId),
      pool_(memoryPool),
      connectorPool_(connectorPool),
      createTimeNs_(getCurrentTimeNano()) {
  if (equalityFieldIds_) {
    validateEqualityFields(rowType_, field_, *equalityFieldIds_);
    if (const auto it = sparkConfs.find("gluten.iceberg.timestamp-without-timezone-field-ids");
        it != sparkConfs.end()) {
      std::unordered_set<int32_t> timestampsWithoutZone;
      for (const auto& id : folly::parseJson(it->second)) {
        timestampsWithoutZone.insert(id.asInt());
      }
      rowType_ = asRowType(withTimestampSemantics(rowType_, field_, timestampsWithoutZone));
    }
  }
  static std::atomic_uint64_t writerId{0};
  connectorPool_ = connectorPool_->addAggregateChild("iceberg.writer." + std::to_string(writerId++));
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
  std::unordered_map<std::string, std::string> serdeParameters;
  if (equalityFieldIds_) {
    if (const auto it = sparkConfs.find("gluten.iceberg.timestamp-timezone"); it != sparkConfs.end()) {
      serdeParameters["gluten.iceberg.timestamp-timezone"] = it->second;
    }
  }
  dataSink_ = std::make_unique<GlutenIcebergDataSink>(
      rowType_,
      createIcebergInsertTableHandle(
          rowType_,
          outputDirectory,
          icebergFormatToVelox(format),
          compressionKind,
          partitionId_,
          taskId_,
          operationId_,
          spec,
          field_,
          serdeParameters,
          pool_.get()),
      connectorQueryCtx_.get(),
      facebook::velox::connector::CommitStrategy::kNoCommit,
      connectorConfig_,
      icebergConfig);
}

void IcebergWriter::write(const VeloxColumnarBatch& batch) {
  auto inputRowVector = batch.getRowVector();
  auto inputRowType = asRowType(inputRowVector->type());

  const auto& children = inputRowVector->children();

  std::vector<VectorPtr> dataColumns;
  dataColumns.reserve(rowType_->size());

  if (inputRowType->size() != rowType_->size()) {
    dataColumns.insert(dataColumns.end(), children.begin() + 1, children.begin() + 1 + rowType_->size());
  } else {
    dataColumns.insert(dataColumns.end(), children.begin(), children.end());
  }

  if (equalityFieldIds_) {
    for (size_t i = 0; i < dataColumns.size(); ++i) {
      const auto& targetType = rowType_->childAt(i);
      if (!dataColumns[i]->type()->equivalent(*targetType)) {
        // Row-to-columnar conversion may have erased timestamp annotations.
        // Copy into the declared type without changing or mutating input values.
        VELOX_CHECK_EQ(dataColumns[i]->typeKind(), targetType->kind());
        auto column = BaseVector::create(targetType, inputRowVector->size(), pool_.get());
        column->copy(dataColumns[i].get(), 0, 0, inputRowVector->size());
        dataColumns[i] = std::move(column);
      }
    }
  }
  auto rowVector = std::make_shared<RowVector>(
      pool_.get(), rowType_, inputRowVector->nulls(), inputRowVector->size(), std::move(dataColumns));

  dataSink_->appendData(rowVector);
}

std::vector<std::string> IcebergWriter::commit() {
  auto finished = dataSink_->finish();
  VELOX_CHECK(finished);
  auto messages = dataSink_->close();
  if (equalityFieldIds_) {
    for (auto& message : messages) {
      auto metadata = folly::parseJson(message);
      metadata["content"] = "EQUALITY_DELETES";
      auto ids = folly::dynamic::array();
      for (const auto id : *equalityFieldIds_) {
        ids.push_back(id);
      }
      metadata["equalityFieldIds"] = std::move(ids);
      message = folly::toJson(metadata);
    }
  }
  return messages;
}

void IcebergWriter::abort() {
  static_cast<GlutenIcebergDataSink*>(dataSink_.get())->abortAndDeleteFiles(connectorConfig_->config());
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

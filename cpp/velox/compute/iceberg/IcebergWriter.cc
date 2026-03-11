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

#include "IcebergPartitionSpec.pb.h"
#include "compute/ProtobufUtils.h"
#include "compute/iceberg/IcebergFormat.h"
#include "config/VeloxConfig.h"
#include "utils/ConfigExtractor.h"
#include "velox/connectors/hive/iceberg/IcebergDataSink.h"
#include "velox/connectors/hive/iceberg/IcebergDeleteFile.h"

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
      : partitionId_(partitionId),
        taskId_(taskId),
        operationId_(operationId),
        fileFormat_(fileFormat),
        fileCount_(0) {}

  std::pair<std::string, std::string> gen(
      std::optional<uint32_t> bucketId,
      const std::shared_ptr<const connector::hive::HiveInsertTableHandle> insertTableHandle,
      const connector::ConnectorQueryCtx& connectorQueryCtx,
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
        partitionId_, taskId_, operationId_);
  }

 private:
  int32_t partitionId_;
  int64_t taskId_;
  std::string operationId_;
  dwio::common::FileFormat fileFormat_;
  mutable int32_t fileCount_;
};

iceberg::IcebergNestedField convertToIcebergNestedField(const gluten::IcebergNestedField& protoField) {
  IcebergNestedField result;
  result.id = protoField.id();

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
    const iceberg::IcebergNestedField& nestedField,
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
      columnHandles.push_back(
          std::make_shared<iceberg::IcebergColumnHandle>(
              columnNames.at(i),
              connector::hive::HiveColumnHandle::ColumnType::kPartitionKey,
              columnTypes.at(i),
              columnTypes.at(i),
              nestedField.children[i]));
    } else {
      columnHandles.push_back(
          std::make_shared<iceberg::IcebergColumnHandle>(
              columnNames.at(i),
              connector::hive::HiveColumnHandle::ColumnType::kRegular,
              columnTypes.at(i),
              columnTypes.at(i),
              nestedField.children[i]));
    }
  }
  
  auto fileNameGenerator = std::make_shared<const GlutenIcebergFileNameGenerator>(
      partitionId, taskId, operationId, fileFormat);
  
  std::shared_ptr<const connector::hive::LocationHandle> locationHandle =
      std::make_shared<connector::hive::LocationHandle>(
          outputDirectoryPath, outputDirectoryPath, connector::hive::LocationHandle::TableType::kExisting);
  const std::vector<IcebergSortingColumn> sortedBy;
  const std::unordered_map<std::string, std::string> serdeParameters;
  return std::make_shared<connector::hive::iceberg::IcebergInsertTableHandle>(
      columnHandles, locationHandle, spec, pool, fileFormat, sortedBy, compressionKind, serdeParameters, fileNameGenerator);
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
    std::shared_ptr<facebook::velox::memory::MemoryPool> connectorPool)
    : rowType_(rowType), field_(convertToIcebergNestedField(field)), partitionId_(partitionId), taskId_(taskId), operationId_(operationId), pool_(memoryPool), connectorPool_(connectorPool), createTimeNs_(getCurrentTimeNano()) {
  auto veloxCfg =
      std::make_shared<facebook::velox::config::ConfigBase>(std::unordered_map<std::string, std::string>(sparkConfs));
  connectorSessionProperties_ = createHiveConnectorSessionConfig(veloxCfg);
  connectorConfig_ = std::make_shared<facebook::velox::connector::hive::HiveConfig>(createHiveConnectorConfig(veloxCfg));
  connectorQueryCtx_ = std::make_unique<connector::ConnectorQueryCtx>(
      pool_.get(),
      connectorPool_.get(),
      connectorSessionProperties_.get(),
      nullptr,
      common::PrefixSortConfig(),
      nullptr,
      nullptr,
      "query.IcebergDataSink",
      "task.IcebergDataSink",
      "planNodeId.IcebergDataSink",
      0,
      "");

  dataSink_ = std::make_unique<IcebergDataSink>(
      rowType_,
      createIcebergInsertTableHandle(
          rowType_, outputDirectory, icebergFormatToVelox(format), compressionKind, partitionId_, taskId_, operationId_, spec, field_, pool_.get()),
      connectorQueryCtx_.get(),
      facebook::velox::connector::CommitStrategy::kNoCommit,
      connectorConfig_);
}

void IcebergWriter::write(const VeloxColumnarBatch& batch) {
  dataSink_->appendData(batch.getRowVector());
}

std::vector<std::string> IcebergWriter::commit() {
  auto finished = dataSink_->finish();
  VELOX_CHECK(finished);
  return dataSink_->close();
}

WriteStats IcebergWriter::writeStats() const {
  const auto currentTimeNs = getCurrentTimeNano();
  VELOX_CHECK_GE(currentTimeNs, createTimeNs_);
  const auto sinkStats = dataSink_->stats();
  return WriteStats(
    sinkStats.numWrittenBytes,
    sinkStats.numWrittenFiles,
    sinkStats.writeIOTimeUs * 1000,
    currentTimeNs - createTimeNs_);
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

    fields.emplace_back(protoField.name(), rowType->findChild(protoField.name()), transform, parameter);
  }

  return std::make_shared<iceberg::IcebergPartitionSpec>(protoSpec.spec_id(), fields);
}

} // namespace gluten

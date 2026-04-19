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

#include "VeloxPlanConverter.h"
#include <filesystem>
#include <optional>

#include <google/protobuf/wrappers.pb.h>
#include "config/GlutenConfig.h"
#include "iceberg/IcebergPlanConverter.h"
#include "operators/plannodes/IteratorSplit.h"

namespace gluten {

using namespace facebook;

VeloxPlanConverter::VeloxPlanConverter(
    velox::memory::MemoryPool* veloxPool,
    const facebook::velox::config::ConfigBase* veloxCfg,
    const std::vector<std::shared_ptr<ResultIterator>>& rowVectors,
    const std::optional<std::string> writeFilesTempPath,
    const std::optional<std::string> writeFileName,
    bool validationMode)
    : validationMode_(validationMode),
      veloxCfg_(veloxCfg),
      substraitVeloxPlanConverter_(veloxPool, veloxCfg, rowVectors, writeFilesTempPath, writeFileName, validationMode) {
  VELOX_USER_CHECK_NOT_NULL(veloxCfg_);
}

namespace {
const std::string kDeltaDvPayloadIndex = "delta_dv_payload_index";

std::optional<std::string> unpackMetadataValue(const google::protobuf::Any& value) {
  google::protobuf::BytesValue bytesValue;
  if (value.UnpackTo(&bytesValue)) {
    return bytesValue.value();
  }

  google::protobuf::StringValue stringValue;
  if (value.UnpackTo(&stringValue)) {
    return stringValue.value();
  }

  google::protobuf::Int32Value int32Value;
  if (value.UnpackTo(&int32Value)) {
    return std::to_string(int32Value.value());
  }

  google::protobuf::Int64Value int64Value;
  if (value.UnpackTo(&int64Value)) {
    return std::to_string(int64Value.value());
  }

  google::protobuf::DoubleValue doubleValue;
  if (value.UnpackTo(&doubleValue)) {
    return std::to_string(doubleValue.value());
  }

  return std::nullopt;
}

std::shared_ptr<SplitInfo> parseScanSplitInfo(
    const facebook::velox::config::ConfigBase* veloxCfg,
    const google::protobuf::RepeatedPtrField<substrait::ReadRel_LocalFiles_FileOrFiles>& fileList,
    const std::vector<SplitPayloadBufferView>* splitPayloads) {
  using SubstraitFileFormatCase = ::substrait::ReadRel_LocalFiles_FileOrFiles::FileFormatCase;

  auto splitInfo = std::make_shared<SplitInfo>();
  splitInfo->leafType = SplitInfo::LeafType::TABLE_SCAN;
  splitInfo->paths.reserve(fileList.size());
  splitInfo->starts.reserve(fileList.size());
  splitInfo->lengths.reserve(fileList.size());
  splitInfo->partitionColumns.reserve(fileList.size());
  splitInfo->properties.reserve(fileList.size());
  splitInfo->metadataColumns.reserve(fileList.size());
  splitInfo->deletionVectorPayloads.reserve(fileList.size());
  for (const auto& file : fileList) {
    // Expect all Partitions share the same index.
    splitInfo->partitionIndex = file.partition_index();

    std::unordered_map<std::string, std::string> partitionColumnMap;
    for (const auto& partitionColumn : file.partition_columns()) {
      partitionColumnMap[partitionColumn.key()] = partitionColumn.value();
    }
    splitInfo->partitionColumns.emplace_back(partitionColumnMap);

    std::unordered_map<std::string, std::string> metadataColumnMap;
    for (const auto& metadataColumn : file.metadata_columns()) {
      metadataColumnMap[metadataColumn.key()] = metadataColumn.value();
    }
    for (const auto& otherMetadataColumn : file.other_const_metadata_columns()) {
      if (auto unpackedValue = unpackMetadataValue(otherMetadataColumn.value())) {
        metadataColumnMap[otherMetadataColumn.key()] = std::move(*unpackedValue);
      }
    }
    if (auto payloadIndexIt = metadataColumnMap.find(kDeltaDvPayloadIndex);
        payloadIndexIt != metadataColumnMap.end()) {
      VELOX_USER_CHECK_NOT_NULL(splitPayloads, "Split payload index found without an external payload buffer");
      const auto payloadIndex = static_cast<size_t>(std::stoul(payloadIndexIt->second));
      VELOX_USER_CHECK_LT(
          payloadIndex,
          splitPayloads->size(),
          "Split payload index {} is out of range for {} payload buffers",
          payloadIndex,
          splitPayloads->size());
      splitInfo->deletionVectorPayloads.emplace_back(splitPayloads->at(payloadIndex));
      metadataColumnMap.erase(payloadIndexIt);
    } else {
      splitInfo->deletionVectorPayloads.emplace_back(std::nullopt);
    }
    splitInfo->metadataColumns.emplace_back(metadataColumnMap);

    splitInfo->paths.emplace_back(file.uri_file());
    splitInfo->starts.emplace_back(file.start());
    splitInfo->lengths.emplace_back(file.length());

    facebook::velox::FileProperties fileProps;
    if (file.has_properties()) {
      fileProps.fileSize = file.properties().filesize();
      fileProps.modificationTime = file.properties().modificationtime();
    }
    splitInfo->properties.emplace_back(fileProps);
    switch (file.file_format_case()) {
      case SubstraitFileFormatCase::kOrc:
        splitInfo->format = dwio::common::FileFormat::ORC;
        break;
      case SubstraitFileFormatCase::kDwrf:
        splitInfo->format = dwio::common::FileFormat::DWRF;
        break;
      case SubstraitFileFormatCase::kParquet:
        splitInfo->format = dwio::common::FileFormat::PARQUET;
        break;
      case SubstraitFileFormatCase::kText:
        splitInfo->format = dwio::common::FileFormat::TEXT;
        break;
      case SubstraitFileFormatCase::kIceberg:
        splitInfo = IcebergPlanConverter::parseIcebergSplitInfo(file, std::move(splitInfo));
        break;
      default:
        splitInfo->format = dwio::common::FileFormat::UNKNOWN;
        break;
    }

    // The schema in file represents the table schema, it is set when the TableScan requires the
    // table schema to be present, currently when the option is set to map columns by index rather
    // than by name in Parquet or ORC files. Since the table schema should be the same for all
    // files, we set it in the SplitInfo based on the first file we encounter with the schema set.
    if (!splitInfo->tableSchema && file.has_schema()) {
      const auto& schema = file.schema();

      std::vector<std::string> names;
      std::vector<TypePtr> types;
      names.reserve(schema.names().size());

      const bool asLowerCase = !veloxCfg->get<bool>(kCaseSensitive, false);
      for (const auto& name : schema.names()) {
        std::string fieldName = name;
        if (asLowerCase) {
          folly::toLowerAscii(fieldName);
        }
        names.emplace_back(std::move(fieldName));
      }
      types = SubstraitParser::parseNamedStruct(schema, asLowerCase);

      splitInfo->tableSchema = ROW(std::move(names), std::move(types));
    }
  }
  return splitInfo;
}

void parseLocalFileNodes(
    SubstraitToVeloxPlanConverter* planConverter,
    const facebook::velox::config::ConfigBase* veloxCfg,
    std::vector<::substrait::ReadRel_LocalFiles>& localFiles,
    const std::unordered_map<int32_t, std::vector<SplitPayloadBufferView>>& splitPayloads) {
  std::vector<std::shared_ptr<SplitInfo>> splitInfos;
  splitInfos.reserve(localFiles.size());
  for (size_t splitIndex = 0; splitIndex < localFiles.size(); ++splitIndex) {
    const auto& localFile = localFiles[splitIndex];
    const auto& fileList = localFile.items();
    auto payloadIt = splitPayloads.find(splitIndex);
    splitInfos.push_back(
        parseScanSplitInfo(veloxCfg, fileList, payloadIt == splitPayloads.end() ? nullptr : &payloadIt->second));
  }

  planConverter->setSplitInfos(std::move(splitInfos));
}
} // namespace

std::shared_ptr<const facebook::velox::core::PlanNode> VeloxPlanConverter::toVeloxPlan(
    const ::substrait::Plan& substraitPlan,
    std::vector<::substrait::ReadRel_LocalFiles> localFiles,
    const std::unordered_map<int32_t, std::vector<SplitPayloadBufferView>>& splitPayloads) {
  if (!validationMode_) {
    parseLocalFileNodes(&substraitVeloxPlanConverter_, veloxCfg_, localFiles, splitPayloads);
  }

  return substraitVeloxPlanConverter_.toVeloxPlan(substraitPlan);
}

} // namespace gluten

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
std::shared_ptr<SplitInfo> parseScanSplitInfo(
    const facebook::velox::config::ConfigBase* veloxCfg,
    const google::protobuf::RepeatedPtrField<substrait::ReadRel_LocalFiles_FileOrFiles>& fileList) {
  using SubstraitFileFormatCase = ::substrait::ReadRel_LocalFiles_FileOrFiles::FileFormatCase;

  auto splitInfo = std::make_shared<SplitInfo>();
  splitInfo->leafType = SplitInfo::LeafType::TABLE_SCAN;
  splitInfo->paths.reserve(fileList.size());
  splitInfo->starts.reserve(fileList.size());
  splitInfo->lengths.reserve(fileList.size());
  splitInfo->partitionColumns.reserve(fileList.size());
  splitInfo->properties.reserve(fileList.size());
  splitInfo->metadataColumns.reserve(fileList.size());
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
    std::vector<::substrait::ReadRel_LocalFiles>& localFiles) {
  std::vector<std::shared_ptr<SplitInfo>> splitInfos;
  splitInfos.reserve(localFiles.size());
  for (const auto& localFile : localFiles) {
    const auto& fileList = localFile.items();
    splitInfos.push_back(parseScanSplitInfo(veloxCfg, fileList));
  }

  planConverter->setSplitInfos(std::move(splitInfos));
}
} // namespace

std::shared_ptr<const facebook::velox::core::PlanNode> VeloxPlanConverter::toVeloxPlan(
    const ::substrait::Plan& substraitPlan,
    std::vector<::substrait::ReadRel_LocalFiles> localFiles) {
  if (!validationMode_) {
    parseLocalFileNodes(&substraitVeloxPlanConverter_, veloxCfg_, localFiles);
  }

  return substraitVeloxPlanConverter_.toVeloxPlan(substraitPlan);
}

} // namespace gluten

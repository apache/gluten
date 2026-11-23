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

#include "IcebergPlanConverter.h"

#include "IcebergReadExtension.pb.h"

namespace gluten {

namespace {

using SubstraitDeleteBoundsMap = ::substrait::ReadRel_LocalFiles_FileOrFiles::IcebergReadOptions::DeleteFile::Map;

dwio::common::ParquetFieldId parseFieldId(
    const ::substrait::ReadRel_LocalFiles_FileOrFiles::IcebergReadOptions::FieldId& field) {
  dwio::common::ParquetFieldId result{field.id(), {}};
  for (const auto& child : field.children()) {
    result.children.push_back(parseFieldId(child));
  }
  return result;
}

std::unordered_map<int32_t, std::string> parseBounds(const SubstraitDeleteBoundsMap& bounds) {
  std::unordered_map<int32_t, std::string> parsed;
  parsed.reserve(bounds.key_values_size());

  for (int i = 0; i < bounds.key_values_size(); ++i) {
    const auto& kv = bounds.key_values(i);
    parsed.emplace(kv.key(), kv.value());
  }

  return parsed;
}

} // namespace

std::shared_ptr<IcebergSplitInfo> IcebergPlanConverter::parseIcebergSplitInfo(
    substrait::ReadRel_LocalFiles_FileOrFiles file,
    const substrait::extensions::AdvancedExtension& extension,
    std::shared_ptr<SplitInfo> splitInfo) {
  using SubstraitFileFormatCase = ::substrait::ReadRel_LocalFiles_FileOrFiles::IcebergReadOptions::FileFormatCase;
  using SubstraitDeleteFileFormatCase =
      ::substrait::ReadRel_LocalFiles_FileOrFiles::IcebergReadOptions::DeleteFile::FileFormatCase;
  auto icebergSplitInfo = std::dynamic_pointer_cast<IcebergSplitInfo>(splitInfo)
      ? std::dynamic_pointer_cast<IcebergSplitInfo>(splitInfo)
      : std::make_shared<IcebergSplitInfo>(*splitInfo);
  auto icebergReadOption = file.iceberg();
  if (icebergSplitInfo->fieldIds.empty()) {
    for (const auto& field : icebergReadOption.schema_field_ids()) {
      icebergSplitInfo->fieldIds.push_back(parseFieldId(field));
    }
  }
  icebergSplitInfo->dataSequenceNumbers.push_back(icebergReadOption.data_sequence_number());
  auto& identityKeys = icebergSplitInfo->identityPartitionKeys.emplace_back();
  for (const auto& key : icebergReadOption.identity_partition_keys()) {
    identityKeys.emplace(key.source_id(), key.is_null() ? std::nullopt : std::make_optional(key.value()));
  }
  switch (icebergReadOption.file_format_case()) {
    case SubstraitFileFormatCase::kParquet:
      icebergSplitInfo->format = dwio::common::FileFormat::PARQUET;
      break;
    case SubstraitFileFormatCase::kOrc:
      icebergSplitInfo->format = dwio::common::FileFormat::ORC;
      break;
    default:
      icebergSplitInfo->format = dwio::common::FileFormat::UNKNOWN;
      break;
  }

  if (icebergSplitInfo->columns.empty() && extension.has_enhancement() &&
      extension.enhancement().Is<::gluten::IcebergReadExtension>()) {
    ::gluten::IcebergReadExtension icebergExtension;
    VELOX_USER_CHECK(extension.enhancement().UnpackTo(&icebergExtension), "Failed to unpack Iceberg read extension");
    for (const auto& column : icebergExtension.column_field_ids()) {
      auto [it, inserted] =
          icebergSplitInfo->columns.try_emplace(column.name(), IcebergColumnInfo{column.field_id(), std::nullopt});
      if (!inserted) {
        VELOX_USER_CHECK_EQ(
            it->second.fieldId, column.field_id(), "Conflicting Iceberg field IDs for column '{}'", column.name());
      }
    }
    for (const auto& column : icebergExtension.column_defaults()) {
      auto [it, inserted] = icebergSplitInfo->columns.try_emplace(
          column.name(), IcebergColumnInfo{column.field_id(), column.initial_default()});
      if (!inserted) {
        VELOX_USER_CHECK_EQ(
            it->second.fieldId, column.field_id(), "Conflicting Iceberg field IDs for column '{}'", column.name());
        it->second.initialDefault = column.initial_default();
      }
    }
  }
  if (icebergReadOption.delete_files_size() > 0) {
    auto deleteFiles = icebergReadOption.delete_files();
    std::vector<IcebergDeleteFile> deletes;
    deletes.reserve(icebergReadOption.delete_files_size());
    for (auto i = 0; i < icebergReadOption.delete_files_size(); i++) {
      auto deleteFile = icebergReadOption.delete_files().Get(i);
      dwio::common::FileFormat format;
      FileContent fileContent;
      switch (deleteFile.file_format_case()) {
        case SubstraitDeleteFileFormatCase::kParquet:
          format = dwio::common::FileFormat::PARQUET;
          break;
        case SubstraitDeleteFileFormatCase::kOrc:
          format = dwio::common::FileFormat::ORC;
          break;
        default:
          format = dwio::common::FileFormat::UNKNOWN;
      }
      switch (deleteFile.filecontent()) {
        case ::substrait::ReadRel_LocalFiles_FileOrFiles_IcebergReadOptions_FileContent_POSITION_DELETES:
          fileContent = FileContent::kPositionalDeletes;
          break;
        case ::substrait::ReadRel_LocalFiles_FileOrFiles_IcebergReadOptions_FileContent_EQUALITY_DELETES:
          fileContent = FileContent::kEqualityDeletes;
          break;
        default:
          fileContent = FileContent::kData;
          break;
      }

      std::vector<int32_t> equalityFieldIds;
      equalityFieldIds.reserve(deleteFile.equalityfieldids_size());
      for (int fieldIdx = 0; fieldIdx < deleteFile.equalityfieldids_size(); ++fieldIdx) {
        equalityFieldIds.emplace_back(deleteFile.equalityfieldids(fieldIdx));
      }

      deletes.emplace_back(IcebergDeleteFile(
          fileContent,
          deleteFile.filepath(),
          format,
          deleteFile.recordcount(),
          deleteFile.filesize(),
          std::move(equalityFieldIds),
          parseBounds(deleteFile.lowerbounds()),
          parseBounds(deleteFile.upperbounds()),
          deleteFile.data_sequence_number()));
    }
    icebergSplitInfo->deleteFilesVec.emplace_back(deletes);
  } else {
    // Add an empty delete files vector to indicate that this data file has no delete file.
    icebergSplitInfo->deleteFilesVec.emplace_back(std::vector<IcebergDeleteFile>{});
  }

  return icebergSplitInfo;
}

} // namespace gluten

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

#include <folly/String.h>

#include "IcebergReadExtension.pb.h"

namespace gluten {

namespace {

using SubstraitDeleteBoundsMap = ::substrait::ReadRel_LocalFiles_FileOrFiles::IcebergReadOptions::DeleteFile::Map;

std::unordered_map<int32_t, std::string> parseBounds(const SubstraitDeleteBoundsMap& bounds) {
  std::unordered_map<int32_t, std::string> parsed;
  parsed.reserve(bounds.key_values_size());

  for (int i = 0; i < bounds.key_values_size(); ++i) {
    const auto& kv = bounds.key_values(i);
    parsed.emplace(kv.key(), kv.value());
  }

  return parsed;
}

IcebergFieldIdInfo parseFieldId(const ::gluten::IcebergReadExtension::ColumnFieldId& field) {
  IcebergFieldIdInfo parsed{field.name(), field.field_id(), {}};
  parsed.children.reserve(field.children_size());
  for (const auto& child : field.children()) {
    parsed.children.emplace_back(parseFieldId(child));
  }
  return parsed;
}

const IcebergFieldIdInfo* findChild(const IcebergFieldIdInfo& field, const std::string& name, bool asLowerCase) {
  for (const auto& child : field.children) {
    if (child.name == name) {
      return &child;
    }
    if (asLowerCase) {
      auto normalizedName = child.name;
      folly::toLowerAscii(normalizedName);
      if (normalizedName == name) {
        return &child;
      }
    }
  }
  return nullptr;
}

} // namespace

facebook::velox::parquet::ParquetFieldId IcebergPlanConverter::toParquetFieldId(
    const IcebergFieldIdInfo& field,
    const facebook::velox::TypePtr& type,
    bool asLowerCase) {
  facebook::velox::parquet::ParquetFieldId result{field.fieldId, {}};
  // Plans produced before nested field IDs were added only contain the root ID.
  if (field.children.empty()) {
    return result;
  }
  auto appendChild = [&](const std::string& childName, const facebook::velox::TypePtr& childType) {
    const auto* child = findChild(field, childName, asLowerCase);
    VELOX_USER_CHECK(child != nullptr, "Missing Iceberg field ID for nested field '{}.{}'", field.name, childName);
    result.children.emplace_back(toParquetFieldId(*child, childType, asLowerCase));
  };

  switch (type->kind()) {
    case facebook::velox::TypeKind::ROW: {
      const auto& rowType = type->asRow();
      result.children.reserve(rowType.size());
      for (facebook::velox::column_index_t i = 0; i < rowType.size(); ++i) {
        appendChild(rowType.nameOf(i), rowType.childAt(i));
      }
      break;
    }
    case facebook::velox::TypeKind::ARRAY:
      result.children.reserve(1);
      appendChild("element", type->childAt(0));
      break;
    case facebook::velox::TypeKind::MAP:
      result.children.reserve(2);
      appendChild("key", type->childAt(0));
      appendChild("value", type->childAt(1));
      break;
    default:
      break;
  }
  return result;
}

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
      auto parsedField = parseFieldId(column);
      auto [it, inserted] =
          icebergSplitInfo->columns.try_emplace(column.name(), IcebergColumnInfo{std::move(parsedField), std::nullopt});
      if (!inserted) {
        VELOX_USER_CHECK_EQ(
            it->second.field.fieldId,
            column.field_id(),
            "Conflicting Iceberg field IDs for column '{}'",
            column.name());
      }
    }
    for (const auto& column : icebergExtension.column_defaults()) {
      auto [it, inserted] = icebergSplitInfo->columns.try_emplace(
          column.name(),
          IcebergColumnInfo{IcebergFieldIdInfo{column.name(), column.field_id(), {}}, column.initial_default()});
      if (!inserted) {
        VELOX_USER_CHECK_EQ(
            it->second.field.fieldId,
            column.field_id(),
            "Conflicting Iceberg field IDs for column '{}'",
            column.name());
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
      deletes.emplace_back(IcebergDeleteFile(
          fileContent,
          deleteFile.filepath(),
          format,
          deleteFile.recordcount(),
          deleteFile.filesize(),
          {},
          parseBounds(deleteFile.lowerbounds()),
          parseBounds(deleteFile.upperbounds())));
    }
    icebergSplitInfo->deleteFilesVec.emplace_back(deletes);
  } else {
    // Add an empty delete files vector to indicate that this data file has no delete file.
    icebergSplitInfo->deleteFilesVec.emplace_back(std::vector<IcebergDeleteFile>{});
  }

  return icebergSplitInfo;
}

} // namespace gluten

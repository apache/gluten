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

} // namespace

std::shared_ptr<IcebergSplitInfo> IcebergPlanConverter::parseIcebergSplitInfo(
    substrait::ReadRel_LocalFiles_FileOrFiles file,
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

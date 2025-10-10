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

#include "compute/paimon/PaimonPlanUtils.h"

namespace gluten::paimon {

std::shared_ptr<PaimonSplitInfo> PaimonPlanUtils::parsePaimonSplitInfo(
    substrait::ReadRel_LocalFiles_FileOrFiles file,
    std::shared_ptr<SplitInfo> splitInfo) {
  BOLT_CHECK(file.has_paimon(), "LocalFiles_FileOrFiles must have paimon read options if parsePaimonSplit is called");
  const auto paimonReadOption = file.paimon();

  auto bucket = paimonReadOption.bucket();
  auto firstRowId = paimonReadOption.first_row_id();
  auto maxSequenceNumber = paimonReadOption.max_sequence_number();
  auto splitGroup = paimonReadOption.split_group();
  auto useHiveSplit = paimonReadOption.use_hive_split();
  auto format = dwio::common::FileFormat::UNKNOWN;

  // Enforce parquet-only for Paimon and set SplitInfo's format.
  switch (paimonReadOption.file_format_case()) {
    case substrait::ReadRel_LocalFiles_FileOrFiles_PaimonReadOptions::FileFormatCase::kParquet:
      format = dwio::common::FileFormat::PARQUET;
      break;
    case substrait::ReadRel_LocalFiles_FileOrFiles_PaimonReadOptions::FileFormatCase::kOrc:
      format = dwio::common::FileFormat::ORC;
      break;
    default:
      BOLT_FAIL("Paimon splits only support parquet format");
  }

  std::vector<std::string> primaryKeys;
  primaryKeys.reserve(paimonReadOption.primary_keys_size());
  for (int32_t i = 0; i < paimonReadOption.primary_keys_size(); ++i) {
    primaryKeys.push_back(paimonReadOption.primary_keys(i));
  }
  auto rawConvertible = paimonReadOption.raw_convertible();
  const auto& path = file.uri_file();
  auto fileMeta = PaimonSplitInfo::FileMeta{
        format, bucket, firstRowId, maxSequenceNumber, splitGroup, useHiveSplit, std::move(primaryKeys), rawConvertible};
  auto existing = std::dynamic_pointer_cast<PaimonSplitInfo>(splitInfo);

  if (!existing) {
    existing = std::make_shared<PaimonSplitInfo>(*splitInfo);
  }

  existing->metaByPath_[path] = fileMeta;
  return existing;
}

} // namespace gluten::paimon

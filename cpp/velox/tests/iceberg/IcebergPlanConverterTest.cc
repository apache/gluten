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

#include "compute/iceberg/IcebergPlanConverter.h"

#include <gtest/gtest.h>

namespace gluten {
namespace {

using facebook::velox::connector::hive::iceberg::FileContent;
using facebook::velox::dwio::common::FileFormat;

TEST(IcebergPlanConverterTest, convertsDeletionVectorMetadata) {
  substrait::ReadRel_LocalFiles_FileOrFiles file;
  file.mutable_iceberg()->mutable_parquet();
  auto* deleteFile = file.mutable_iceberg()->add_delete_files();
  deleteFile->set_filecontent(
      substrait::ReadRel_LocalFiles_FileOrFiles_IcebergReadOptions_FileContent_POSITION_DELETES);
  deleteFile->set_filepath("file:/table/metadata/delete.puffin");
  deleteFile->set_filesize(256);
  deleteFile->set_recordcount(3);
  deleteFile->mutable_puffin();
  deleteFile->set_contentoffset(64);
  deleteFile->set_contentsizeinbytes(32);
  deleteFile->set_referenceddatafile("file:/table/data/data.parquet");
  deleteFile->set_datasequencenumber(7);

  auto splitInfo = std::make_shared<SplitInfo>();
  auto result = IcebergPlanConverter::parseIcebergSplitInfo(file, splitInfo);

  ASSERT_EQ(result->deleteFilesVec.size(), 1);
  ASSERT_EQ(result->deleteFilesVec[0].size(), 1);
  const auto& actual = result->deleteFilesVec[0][0];
  EXPECT_EQ(actual.content, FileContent::kDeletionVector);
  EXPECT_EQ(actual.fileFormat, FileFormat::PUFFIN);
  EXPECT_EQ(actual.filePath, "file:/table/metadata/delete.puffin");
  EXPECT_EQ(actual.fileSizeInBytes, 256);
  EXPECT_EQ(actual.recordCount, 3);
  EXPECT_EQ(actual.contentOffset, 64);
  EXPECT_EQ(actual.contentLength, 32);
  EXPECT_EQ(actual.referencedDataFile, "file:/table/data/data.parquet");
  EXPECT_EQ(actual.dataSequenceNumber, 7);
}

TEST(IcebergPlanConverterTest, preservesLegacyDeleteMetadataAndDefaults) {
  substrait::ReadRel_LocalFiles_FileOrFiles file;
  file.mutable_iceberg()->mutable_parquet();
  auto* deleteFile = file.mutable_iceberg()->add_delete_files();
  deleteFile->set_filecontent(
      substrait::ReadRel_LocalFiles_FileOrFiles_IcebergReadOptions_FileContent_EQUALITY_DELETES);
  deleteFile->set_filepath("file:/table/delete.parquet");
  deleteFile->set_filesize(128);
  deleteFile->set_recordcount(2);
  deleteFile->mutable_parquet();
  deleteFile->add_equalityfieldids(1);
  deleteFile->add_equalityfieldids(3);
  auto* lowerBound = deleteFile->mutable_lowerbounds()->add_key_values();
  lowerBound->set_key(1);
  lowerBound->set_value("AQ==");
  auto* upperBound = deleteFile->mutable_upperbounds()->add_key_values();
  upperBound->set_key(1);
  upperBound->set_value("CQ==");

  auto result = IcebergPlanConverter::parseIcebergSplitInfo(file, std::make_shared<SplitInfo>());

  const auto& actual = result->deleteFilesVec[0][0];
  EXPECT_EQ(actual.content, FileContent::kEqualityDeletes);
  EXPECT_EQ(actual.fileFormat, FileFormat::PARQUET);
  EXPECT_EQ(actual.equalityFieldIds, (std::vector<int32_t>{1, 3}));
  EXPECT_EQ(actual.lowerBounds.at(1), "AQ==");
  EXPECT_EQ(actual.upperBounds.at(1), "CQ==");
  EXPECT_EQ(actual.dataSequenceNumber, 0);
  EXPECT_EQ(actual.contentOffset, 0);
  EXPECT_EQ(actual.contentLength, 0);
  EXPECT_TRUE(actual.referencedDataFile.empty());
}

} // namespace
} // namespace gluten

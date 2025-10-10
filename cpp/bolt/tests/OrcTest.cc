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

#include "arrow/c/abi.h"
#include "benchmarks/BatchStreamIterator.h"
#include "benchmarks/common/BenchmarkUtils.h"

#include "utils/TestUtils.h"

#include <arrow/adapters/orc/adapter.h>
#include <arrow/io/file.h>

#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/type_fwd.h>
#include <gtest/gtest.h>

#include <iostream>
#include <string>
#include <vector>

namespace gluten {

static const unsigned kFileNum = 2;

struct OrcTestEntry {
  std::string orcFilename;

  std::shared_ptr<arrow::Schema> writeSchema;
  std::shared_ptr<arrow::Schema> readSchema;
  size_t writeRowNums = 0;
  size_t readRowNums = 0;

  void Assert() {
    ASSERT_TRUE(writeSchema->Equals(*readSchema));
    ASSERT_EQ(writeRowNums, readRowNums);
  }
};

struct OrcTestData {
  std::vector<OrcTestEntry> entries;

  OrcTestData() {
    entries.resize(kFileNum);
    entries[0].orcFilename = "example_orders.orc";
    entries[1].orcFilename = "example_lineitem.orc";
  }

  ~OrcTestData() {
    for (auto& x : entries) {
      std::filesystem::remove(x.orcFilename);
      // std::cout << "remove file " << x.orcFilename << std::endl;
    }
  }

  void check() {
    for (auto& x : entries) {
      x.Assert();
    }
  }
} orcTestData;

arrow::Status parquet2Orc(unsigned index, const std::string& parquetFile, const std::string& orcFile) {
  ParquetBatchStreamIterator parquetIterator(parquetFile);

  orcTestData.entries[index].writeSchema = parquetIterator.getSchema();

  std::shared_ptr<arrow::io::FileOutputStream> outputStream;

  ARROW_ASSIGN_OR_RAISE(outputStream, arrow::io::FileOutputStream::Open(orcFile));

  auto writerOptions = arrow::adapters::orc::WriteOptions();
  auto maybeWriter = arrow::adapters::orc::ORCFileWriter::Open(outputStream.get(), writerOptions);
  EXPECT_TRUE(maybeWriter.ok());
  auto& writer = *maybeWriter;

  while (true) {
    // 1. read from Parquet
    auto cb = parquetIterator.next();
    if (cb == nullptr) {
      break;
    }

    auto arrowColumnarBatch = std::dynamic_pointer_cast<gluten::ArrowColumnarBatch>(cb);
    auto recordBatch = arrowColumnarBatch->getRecordBatch();

    // std::cout << "==========\n" << recordBatch->ToString() << std::endl;

    // 2. write to Orc
    if (!(writer->Write(*recordBatch)).ok()) {
      return arrow::Status::IOError("Write failed");
    }

    orcTestData.entries[index].writeRowNums += recordBatch->num_rows();
  }

  if (!(writer->Close()).ok()) {
    return arrow::Status::IOError("Close failed");
  }

  return arrow::Status::OK();
}

void testWriteOrc() {
  std::vector<std::string> inputFiles(kFileNum);
  inputFiles[0] = getGeneratedFilePath("example_orders");
  inputFiles[1] = getGeneratedFilePath("example_lineitem");

  ASSERT_EQ(inputFiles.size(), orcTestData.entries.size());

  for (auto i = 0; i != inputFiles.size(); ++i) {
    ASSERT_NOT_OK(parquet2Orc(i, inputFiles[i], orcTestData.entries[i].orcFilename));
  }
}

void testReadOrc() {
  for (auto i = 0; i != orcTestData.entries.size(); ++i) {
    // Open File
    auto input = arrow::io::ReadableFile::Open(orcTestData.entries[i].orcFilename);
    EXPECT_TRUE(input.ok());

    // Open ORC File Reader
    auto maybeReader = arrow::adapters::orc::ORCFileReader::Open(*input, arrow::default_memory_pool());
    EXPECT_TRUE(maybeReader.ok());
    auto& reader = *maybeReader;

    // read schema
    auto schema = reader->ReadSchema();
    EXPECT_TRUE(schema.ok());
    orcTestData.entries[i].readSchema = *schema;
    // std::cout << "schema:\n" << (*schema)->ToString() << std::endl;

    // read record batch
    auto recordBatchReader = reader->GetRecordBatchReader(4096, std::vector<std::string>());
    EXPECT_TRUE(recordBatchReader.ok());

    while (true) {
      auto batch = (*recordBatchReader)->Next();
      EXPECT_TRUE(batch.ok());
      if (!(*batch)) {
        break;
      }
      orcTestData.entries[i].readRowNums += (*batch)->num_rows();
      // std::cout << (*batch)->ToString() << std::endl;
    }
  }
}

class OrcTest : public ::testing::Test {};

TEST_F(OrcTest, testOrc) {
  GTEST_SKIP() << "Issue 2862";
  testWriteOrc();
  testReadOrc();
  orcTestData.check();
}

} // namespace gluten

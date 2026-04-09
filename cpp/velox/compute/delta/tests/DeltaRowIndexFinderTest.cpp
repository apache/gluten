/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "compute/delta/DeltaRowIndexFinder.h"

#include <gtest/gtest.h>

#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/core/Expressions.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/type/Type.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace gluten::delta;
using namespace facebook::velox::test;

class DeltaRowIndexFinderTest : public testing::Test,
                                  public VectorTestBase {
 protected:
  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool();
    fileSystem_ = filesystems::getFileSystem("", nullptr);
    tempDir_ = exec::test::TempDirectoryPath::create();
  }

  /// Create a test Parquet file with sample data
  std::string createTestParquetFile(
      const std::string& name,
      const RowVectorPtr& data) {
    auto filePath = tempDir_->getPath() + "/" + name + ".parquet";
    
    // Write Parquet file
    auto writeFile = fileSystem_->openFileForWrite(filePath);
    auto writer = parquet::Writer::create(
        std::move(writeFile),
        data->type(),
        pool_.get());
    
    writer->write(data);
    writer->close();
    
    return filePath;
  }

  /// Create sample data for testing
  RowVectorPtr createSampleData(vector_size_t size) {
    auto rowType = ROW({"id", "value", "name"}, {BIGINT(), DOUBLE(), VARCHAR()});
    
    auto idVector = makeFlatVector<int64_t>(
        size,
        [](vector_size_t i) { return i; });
    
    auto valueVector = makeFlatVector<double>(
        size,
        [](vector_size_t i) { return i * 1.5; });
    
    auto nameVector = makeFlatVector<StringView>(
        size,
        [](vector_size_t i) {
          return StringView(fmt::format("name_{}", i));
        });
    
    return makeRowVector({idVector, valueVector, nameVector});
  }

  /// Create a simple filter expression: id < threshold
  std::shared_ptr<const core::ITypedExpr> createLessThanFilter(
      int64_t threshold) {
    // Create: id < threshold
    auto idField = std::make_shared<core::FieldAccessTypedExpr>(
        BIGINT(),
        "id");
    
    auto thresholdConst = std::make_shared<core::ConstantTypedExpr>(
        BIGINT(),
        variant(threshold));
    
    return std::make_shared<core::CallTypedExpr>(
        BOOLEAN(),
        std::vector<std::shared_ptr<const core::ITypedExpr>>{
            idField, thresholdConst},
        "lt");
  }

  /// Create a range filter: id >= start AND id < end
  std::shared_ptr<const core::ITypedExpr> createRangeFilter(
      int64_t start,
      int64_t end) {
    auto idField = std::make_shared<core::FieldAccessTypedExpr>(
        BIGINT(),
        "id");
    
    auto startConst = std::make_shared<core::ConstantTypedExpr>(
        BIGINT(),
        variant(start));
    
    auto endConst = std::make_shared<core::ConstantTypedExpr>(
        BIGINT(),
        variant(end));
    
    // id >= start
    auto geExpr = std::make_shared<core::CallTypedExpr>(
        BOOLEAN(),
        std::vector<std::shared_ptr<const core::ITypedExpr>>{
            idField, startConst},
        "gte");
    
    // id < end
    auto ltExpr = std::make_shared<core::CallTypedExpr>(
        BOOLEAN(),
        std::vector<std::shared_ptr<const core::ITypedExpr>>{
            idField, endConst},
        "lt");
    
    // AND
    return std::make_shared<core::CallTypedExpr>(
        BOOLEAN(),
        std::vector<std::shared_ptr<const core::ITypedExpr>>{
            geExpr, ltExpr},
        "and");
  }

  std::shared_ptr<memory::MemoryPool> pool_;
  std::shared_ptr<filesystems::FileSystem> fileSystem_;
  std::shared_ptr<exec::test::TempDirectoryPath> tempDir_;
};

TEST_F(DeltaRowIndexFinderTest, findMatchingRowsSimpleFilter) {
  // Create test data: 1000 rows
  auto data = createSampleData(1000);
  auto filePath = createTestParquetFile("test_simple", data);
  
  // Create finder
  DeltaRowIndexFinder finder(pool_.get(), fileSystem_);
  
  // Find rows where id < 100
  auto filter = createLessThanFilter(100);
  auto indices = finder.findMatchingRows(filePath, filter, data->type());
  
  // Verify results
  EXPECT_EQ(indices.size(), 100);
  for (size_t i = 0; i < indices.size(); ++i) {
    EXPECT_EQ(indices[i], i);
  }
  
  // Check stats
  auto stats = finder.getStats();
  EXPECT_EQ(stats.rowsMatched, 100);
  EXPECT_GT(stats.executionTimeMs, 0);
}

TEST_F(DeltaRowIndexFinderTest, findMatchingRowsRangeFilter) {
  // Create test data: 1000 rows
  auto data = createSampleData(1000);
  auto filePath = createTestParquetFile("test_range", data);
  
  // Create finder
  DeltaRowIndexFinder finder(pool_.get(), fileSystem_);
  
  // Find rows where 100 <= id < 200
  auto filter = createRangeFilter(100, 200);
  auto indices = finder.findMatchingRows(filePath, filter, data->type());
  
  // Verify results
  EXPECT_EQ(indices.size(), 100);
  for (size_t i = 0; i < indices.size(); ++i) {
    EXPECT_EQ(indices[i], 100 + i);
  }
}

TEST_F(DeltaRowIndexFinderTest, findMatchingRowsNoMatches) {
  // Create test data: 1000 rows (id: 0-999)
  auto data = createSampleData(1000);
  auto filePath = createTestParquetFile("test_no_matches", data);
  
  // Create finder
  DeltaRowIndexFinder finder(pool_.get(), fileSystem_);
  
  // Find rows where id < 0 (no matches)
  auto filter = createLessThanFilter(0);
  auto indices = finder.findMatchingRows(filePath, filter, data->type());
  
  // Verify no results
  EXPECT_EQ(indices.size(), 0);
  
  // Check stats
  auto stats = finder.getStats();
  EXPECT_EQ(stats.rowsMatched, 0);
}

TEST_F(DeltaRowIndexFinderTest, findMatchingRowsAllMatches) {
  // Create test data: 1000 rows
  auto data = createSampleData(1000);
  auto filePath = createTestParquetFile("test_all_matches", data);
  
  // Create finder
  DeltaRowIndexFinder finder(pool_.get(), fileSystem_);
  
  // Find rows where id < 10000 (all match)
  auto filter = createLessThanFilter(10000);
  auto indices = finder.findMatchingRows(filePath, filter, data->type());
  
  // Verify all rows returned
  EXPECT_EQ(indices.size(), 1000);
  for (size_t i = 0; i < indices.size(); ++i) {
    EXPECT_EQ(indices[i], i);
  }
}

TEST_F(DeltaRowIndexFinderTest, findMatchingRowsNoFilter) {
  // Create test data: 1000 rows
  auto data = createSampleData(1000);
  auto filePath = createTestParquetFile("test_no_filter", data);
  
  // Create finder
  DeltaRowIndexFinder finder(pool_.get(), fileSystem_);
  
  // Find all rows (no filter)
  auto indices = finder.findMatchingRows(filePath, nullptr, data->type());
  
  // Verify all rows returned
  EXPECT_EQ(indices.size(), 1000);
  for (size_t i = 0; i < indices.size(); ++i) {
    EXPECT_EQ(indices[i], i);
  }
}

TEST_F(DeltaRowIndexFinderTest, findMatchingRowsBatch) {
  // Create multiple test files
  auto data1 = createSampleData(1000);
  auto file1 = createTestParquetFile("batch_1", data1);
  
  auto data2 = createSampleData(2000);
  auto file2 = createTestParquetFile("batch_2", data2);
  
  auto data3 = createSampleData(500);
  auto file3 = createTestParquetFile("batch_3", data3);
  
  // Create finder
  DeltaRowIndexFinder finder(pool_.get(), fileSystem_);
  
  // Create filters
  auto filter1 = createLessThanFilter(100);  // 100 matches
  auto filter2 = createLessThanFilter(500);  // 500 matches
  auto filter3 = createLessThanFilter(250);  // 250 matches
  
  // Batch find
  std::vector<std::tuple<
      std::string,
      std::shared_ptr<const core::ITypedExpr>,
      std::shared_ptr<const RowType>>> files = {
      {file1, filter1, data1->type()},
      {file2, filter2, data2->type()},
      {file3, filter3, data3->type()}
  };
  
  auto results = finder.findMatchingRowsBatch(files);
  
  // Verify results
  EXPECT_EQ(results.size(), 3);
  EXPECT_EQ(results[file1].size(), 100);
  EXPECT_EQ(results[file2].size(), 500);
  EXPECT_EQ(results[file3].size(), 250);
}

TEST_F(DeltaRowIndexFinderTest, findMatchingRowsWithConfig) {
  // Create test data
  auto data = createSampleData(10000);
  auto filePath = createTestParquetFile("test_config", data);
  
  // Create finder with custom config
  DeltaRowIndexFinder::Config config;
  config.batchSize = 1000;
  config.numThreads = 2;
  config.memoryLimit = 100 * 1024 * 1024; // 100MB
  
  DeltaRowIndexFinder finder(pool_.get(), fileSystem_, config);
  
  // Find rows
  auto filter = createLessThanFilter(5000);
  auto indices = finder.findMatchingRows(filePath, filter, data->type());
  
  // Verify results
  EXPECT_EQ(indices.size(), 5000);
}

TEST_F(DeltaRowIndexFinderTest, findMatchingRowsLargeFile) {
  // Create large test data: 100K rows
  auto data = createSampleData(100000);
  auto filePath = createTestParquetFile("test_large", data);
  
  // Create finder
  DeltaRowIndexFinder finder(pool_.get(), fileSystem_);
  
  // Find 5% of rows
  auto filter = createLessThanFilter(5000);
  auto indices = finder.findMatchingRows(filePath, filter, data->type());
  
  // Verify results
  EXPECT_EQ(indices.size(), 5000);
  
  // Check performance
  auto stats = finder.getStats();
  EXPECT_GT(stats.rowsScanned, 0);
  EXPECT_EQ(stats.rowsMatched, 5000);
  EXPECT_GT(stats.executionTimeMs, 0);
  
  // Should be fast (< 1 second for 100K rows)
  EXPECT_LT(stats.executionTimeMs, 1000);
}

TEST_F(DeltaRowIndexFinderTest, findMatchingRowsInvalidFile) {
  // Create finder
  DeltaRowIndexFinder finder(pool_.get(), fileSystem_);
  
  // Try to find rows in non-existent file
  auto filter = createLessThanFilter(100);
  auto schema = ROW({"id"}, {BIGINT()});
  
  EXPECT_THROW(
      finder.findMatchingRows("/invalid/path.parquet", filter, schema),
      VeloxException);
}

// Made with Bob
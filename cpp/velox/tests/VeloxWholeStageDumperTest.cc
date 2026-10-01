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

#include "utils/VeloxWholeStageDumper.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <vector>

#include "memory/VeloxColumnarBatch.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

namespace gluten {
namespace {
class ColumnarBatchArray final : public ColumnarBatchIterator {
 public:
  explicit ColumnarBatchArray(std::vector<std::shared_ptr<ColumnarBatch>> batches) : batches_(std::move(batches)) {}

  std::shared_ptr<ColumnarBatch> next() override {
    if (cursor_ >= batches_.size()) {
      return nullptr;
    }
    return batches_[cursor_++];
  }

 private:
  const std::vector<std::shared_ptr<ColumnarBatch>> batches_;
  size_t cursor_{0};
};
} // namespace

class VeloxWholeStageDumperTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    filesystems::registerLocalFileSystem();
  }

  std::unique_ptr<VeloxWholeStageDumper> makeDumper() {
    return std::make_unique<VeloxWholeStageDumper>(taskInfo_, tmpDir_->getPath(), 4096, rootPool_.get());
  }

  std::filesystem::path dataFile(int32_t iteratorIndex) const {
    return std::filesystem::path{tmpDir_->getPath()} /
        fmt::format("data_{}_{}_{}_{}.parquet", taskInfo_.stageId, taskInfo_.partitionId, taskInfo_.vId, iteratorIndex);
  }

  std::shared_ptr<ColumnarBatch> newBatch(int32_t numRows) {
    auto rowVector = makeRowVector({makeFlatVector<int32_t>(numRows, [](auto row) { return row; })});
    return std::make_shared<VeloxColumnarBatch>(std::move(rowVector));
  }

  SparkTaskInfo taskInfo_{.stageId = 1, .partitionId = 2, .taskId = 3, .vId = 4};
  std::shared_ptr<exec::test::TempDirectoryPath> tmpDir_{exec::test::TempDirectoryPath::create()};
};

// An input iterator that yields nothing used to leave the parquet writer uninitialized, since the
// writer takes its schema from the first batch, and closing it dereferenced a null writer. The
// build side of a broadcast hash join is exactly such an iterator: its hash table is handed to the
// native operator through a process local cache rather than streamed.
// See https://github.com/apache/gluten/issues/12504.
TEST_F(VeloxWholeStageDumperTest, dumpEmptyInputIterator) {
  auto dumper = makeDumper();
  auto input = std::make_shared<ColumnarBatchArray>(std::vector<std::shared_ptr<ColumnarBatch>>{});

  std::shared_ptr<ColumnarBatchIterator> dumped;
  ASSERT_NO_THROW(dumped = dumper->dumpInputIterator(0, input));

  // The task keeps running against what the original iterator produced: nothing.
  ASSERT_NE(dumped, nullptr);
  ASSERT_EQ(dumped->next(), nullptr);

  // No parquet file can be written without a schema, so the gap is recorded explicitly. --data
  // binds files to iterator indexes by position, so an unrecorded gap would silently shift every
  // later iterator onto the wrong input.
  ASSERT_FALSE(std::filesystem::exists(dataFile(0)));
  ASSERT_TRUE(std::filesystem::exists(dataFile(0).string() + ".empty"));
}

TEST_F(VeloxWholeStageDumperTest, dumpInputIterator) {
  auto dumper = makeDumper();
  auto input =
      std::make_shared<ColumnarBatchArray>(std::vector<std::shared_ptr<ColumnarBatch>>{newBatch(10), newBatch(20)});

  auto dumped = dumper->dumpInputIterator(0, input);
  ASSERT_NE(dumped, nullptr);
  ASSERT_TRUE(std::filesystem::exists(dataFile(0)));

  // The returned iterator replays the dumped file, so the task sees the same rows.
  int32_t numRows = 0;
  while (auto batch = dumped->next()) {
    numRows += batch->numRows();
  }
  ASSERT_EQ(numRows, 30);
}

} // namespace gluten

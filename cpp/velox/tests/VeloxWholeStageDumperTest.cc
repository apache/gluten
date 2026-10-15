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
#include "compute/VeloxBackend.h"
#include "memory/VeloxColumnarBatch.h"
#include "memory/VeloxMemoryManager.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

#include <filesystem>

using namespace facebook::velox;

namespace gluten {
namespace {

// A ColumnarBatchIterator that serves a fixed list of row vectors.
class RowVectorBatchIterator final : public ColumnarBatchIterator {
 public:
  explicit RowVectorBatchIterator(std::vector<RowVectorPtr> vectors) : vectors_(std::move(vectors)) {}

  std::shared_ptr<ColumnarBatch> next() override {
    if (idx_ >= vectors_.size()) {
      return nullptr;
    }
    return std::make_shared<VeloxColumnarBatch>(vectors_[idx_++]);
  }

 private:
  std::vector<RowVectorPtr> vectors_;
  size_t idx_{0};
};

} // namespace

class VeloxWholeStageDumperTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    VeloxBackend::create(AllocationListener::noop(), {});
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  static void TearDownTestSuite() {
    VeloxBackend::get()->tearDown();
  }

  void SetUp() override {
    vmm_ = std::make_unique<VeloxMemoryManager>(
        kVeloxBackendKind, AllocationListener::noop(), *VeloxBackend::get()->getBackendConf());
    saveDir_ = std::filesystem::path(::testing::TempDir()) /
        fmt::format("gluten-whole-stage-dumper-test-{}", ::testing::UnitTest::GetInstance()->random_seed());
    std::filesystem::remove_all(saveDir_);
  }

  void TearDown() override {
    vmm_.reset();
    std::filesystem::remove_all(saveDir_);
  }

  std::unique_ptr<VeloxMemoryManager> vmm_;
  std::filesystem::path saveDir_;
};

// Regression test: batches read back from the dumped file must stay valid after the reader iterator is gone.
// The reader iterator is dropped by the value stream as soon as the input is exhausted, while vectors it produced
// may still be referenced by downstream operators or by batches handed to Java. If they were allocated from a pool
// owned by the reader, freeing them later would hit a destroyed pool and crash in AlignedBuffer::freeToPool.
TEST_F(VeloxWholeStageDumperTest, dumpedBatchesOutliveReaderIterator) {
  constexpr int32_t kNumBatches = 3;
  constexpr vector_size_t kRowsPerBatch = 1000;

  std::vector<RowVectorPtr> inputs;
  for (int32_t i = 0; i < kNumBatches; ++i) {
    inputs.push_back(makeRowVector(
        {"c0", "c1", "c2"},
        {makeFlatVector<int64_t>(kRowsPerBatch, [i](auto row) { return i * kRowsPerBatch + row; }),
         makeFlatVector<double>(kRowsPerBatch, [](auto row) { return row * 0.5; }),
         makeFlatVector<StringView>(kRowsPerBatch, [](auto row) {
           return row % 2 == 0 ? StringView("short") : StringView("a string that is not inlined");
         })}));
  }
  auto input = std::make_shared<RowVectorBatchIterator>(inputs);

  const SparkTaskInfo taskInfo{/*stageId=*/1, /*partitionId=*/2, /*taskId=*/3, /*vId=*/4};
  VeloxWholeStageDumper dumper(taskInfo, saveDir_.string(), kRowsPerBatch, vmm_.get());

  // Capture the baseline before the reader exists: the reader allocates its own state from the leaf pool as soon
  // as it is created, and all of it must be gone by the end of the test.
  const auto leafPool = vmm_->getLeafMemoryPool();
  const auto baselineBytes = leafPool->usedBytes();

  auto reader = dumper.dumpInputIterator(0, input);
  ASSERT_NE(reader, nullptr);
  ASSERT_TRUE(std::filesystem::exists(saveDir_ / "data_1_2_4_0.parquet"));

  std::vector<std::shared_ptr<ColumnarBatch>> batches;
  int64_t numRows = 0;
  while (auto cb = reader->next()) {
    numRows += cb->numRows();
    // Vectors must be backed by the Runtime-lifetime leaf pool rather than a pool owned by the reader.
    ASSERT_EQ(VeloxColumnarBatch::from(leafPool.get(), cb)->getRowVector()->pool(), leafPool.get());
    batches.push_back(std::move(cb));
  }
  ASSERT_EQ(numRows, kNumBatches * kRowsPerBatch);
  ASSERT_GT(leafPool->usedBytes(), baselineBytes);

  // Destroying the reader frees the reader's own buffers, but must not destroy the pool backing the batches,
  // so the memory held by the batches is still accounted for.
  reader.reset();
  const auto bytesHeldByBatches = leafPool->usedBytes();
  ASSERT_GT(bytesHeldByBatches, baselineBytes);

  // Releasing the batches after the reader is gone must be safe,
  // and must give the memory back to the pool, which is still alive.
  batches.clear();
  ASSERT_LT(leafPool->usedBytes(), bytesHeldByBatches);
  ASSERT_EQ(leafPool->usedBytes(), baselineBytes);
}

} // namespace gluten

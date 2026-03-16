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

#include <gtest/gtest.h>

#include "memory/VeloxColumnarBatch.h"
#include "operators/plannodes/RowVectorStream.h"
#include "velox/type/Filter.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::common;

namespace facebook::velox::test {

/// A ColumnarBatchIterator that yields pre-built RowVectors as VeloxColumnarBatches.
class TestBatchIterator final : public gluten::ColumnarBatchIterator {
 public:
  explicit TestBatchIterator(std::vector<RowVectorPtr> batches) : batches_(std::move(batches)) {}

  std::shared_ptr<gluten::ColumnarBatch> next() override {
    if (idx_ >= batches_.size()) {
      return nullptr;
    }
    return std::make_shared<gluten::VeloxColumnarBatch>(batches_[idx_++]);
  }

 private:
  std::vector<RowVectorPtr> batches_;
  size_t idx_ = 0;
};

class ValueStreamDynamicFilterTest : public ::testing::Test, public VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    // Register the connector if not already registered.
    if (!connector::hasConnector(gluten::kIteratorConnectorId)) {
      auto config = std::make_shared<config::ConfigBase>(
          std::unordered_map<std::string, std::string>());
      connector::registerConnector(std::make_shared<gluten::ValueStreamConnector>(
          gluten::kIteratorConnectorId, config, /*dynamicFilterEnabled=*/true));
    }
  }

  /// Build a TableScanNode that reads from the value-stream connector.
  std::shared_ptr<core::TableScanNode> makeTableScanNode(
      const std::string& nodeId,
      const RowTypePtr& outputType) {
    auto tableHandle =
        std::make_shared<gluten::ValueStreamTableHandle>(gluten::kIteratorConnectorId);

    connector::ColumnHandleMap assignments;
    for (int idx = 0; idx < outputType->size(); idx++) {
      auto name = outputType->nameOf(idx);
      auto type = outputType->childAt(idx);
      assignments[name] =
          std::make_shared<gluten::ValueStreamColumnHandle>(name, type);
    }

    return std::make_shared<core::TableScanNode>(
        nodeId, outputType, tableHandle, assignments);
  }

  /// Create a split wrapping the given batches.
  std::shared_ptr<connector::ConnectorSplit> makeSplit(
      std::vector<RowVectorPtr> batches) {
    auto iter = std::make_shared<gluten::ResultIterator>(
        std::make_unique<TestBatchIterator>(std::move(batches)));
    return std::make_shared<gluten::IteratorConnectorSplit>(
        gluten::kIteratorConnectorId, std::move(iter));
  }

  /// Read all int64 values from column 0 of a serial-mode task.
  std::vector<int64_t> readAllInt64(Task* task) {
    std::vector<int64_t> result;
    ContinueFuture future = ContinueFuture::makeEmpty();
    while (true) {
      auto batch = task->next(&future);
      if (!batch) {
        break;
      }
      DecodedVector decoded(*batch->childAt(0));
      for (vector_size_t i = 0; i < batch->size(); i++) {
        result.push_back(decoded.valueAt<int64_t>(i));
      }
    }
    return result;
  }
};

// Test that without any filter, all rows pass through.
TEST_F(ValueStreamDynamicFilterTest, noFilterPassesAllRows) {
  auto batch = makeRowVector({"id"}, {makeFlatVector<int64_t>({10, 20, 30})});
  auto outputType = asRowType(batch->type());
  auto scanNode = makeTableScanNode("vs0", outputType);

  auto queryCtx = core::QueryCtx::create();
  auto task = Task::create(
      "test-nofilter",
      core::PlanFragment{scanNode},
      0,
      queryCtx,
      Task::ExecutionMode::kSerial);

  task->addSplit(scanNode->id(), Split{makeSplit({batch})});
  task->noMoreSplits(scanNode->id());

  auto ids = readAllInt64(task.get());
  ASSERT_EQ(ids, (std::vector<int64_t>{10, 20, 30}));
}

// Test that filtering works when filter is injected after first batch.
TEST_F(ValueStreamDynamicFilterTest, filterBigintRange) {
  auto batch1 = makeRowVector({"id"}, {makeFlatVector<int64_t>({1, 2, 3, 4, 5})});
  auto batch2 = makeRowVector({"id"}, {makeFlatVector<int64_t>({6, 7, 8, 9, 10})});
  auto outputType = asRowType(batch1->type());
  auto scanNode = makeTableScanNode("vs1", outputType);

  auto queryCtx = core::QueryCtx::create();
  auto task = Task::create(
      "test-bigint",
      core::PlanFragment{scanNode},
      0,
      queryCtx,
      Task::ExecutionMode::kSerial);

  // Add both batches as a single split.
  task->addSplit(scanNode->id(), Split{makeSplit({batch1, batch2})});
  task->noMoreSplits(scanNode->id());

  // First next() creates drivers and returns first batch (unfiltered).
  ContinueFuture future = ContinueFuture::makeEmpty();
  auto firstBatch = task->next(&future);
  ASSERT_NE(firstBatch, nullptr);
  ASSERT_EQ(firstBatch->size(), 5);

  // Inject a BigintRange filter: keep only id >= 8.
  task->testingVisitDrivers([&](Driver* driver) {
    auto* op = driver->findOperator(scanNode->id());
    if (!op) {
      return;
    }
    ASSERT_TRUE(op->canAddDynamicFilter());
    PushdownFilters pf;
    pf.filters[0] = std::make_shared<BigintRange>(8, 10, false);
    pf.dynamicFilteredColumns.insert(0);
    op->addDynamicFilterLocked("producer", pf);
  });

  // Second next() should return filtered batch.
  auto secondBatch = task->next(&future);
  ASSERT_NE(secondBatch, nullptr);

  DecodedVector decoded(*secondBatch->childAt(0));
  std::vector<int64_t> outputIds;
  for (vector_size_t i = 0; i < secondBatch->size(); i++) {
    outputIds.push_back(decoded.valueAt<int64_t>(i));
  }
  ASSERT_EQ(outputIds, (std::vector<int64_t>{8, 9, 10}));

  auto end = task->next(&future);
  ASSERT_EQ(end, nullptr);
}

// Test that a filter eliminates all rows from a batch.
TEST_F(ValueStreamDynamicFilterTest, filterEliminatesEntireBatch) {
  auto batch1 = makeRowVector({"id"}, {makeFlatVector<int64_t>({1, 2, 3})});
  auto batch2 = makeRowVector({"id"}, {makeFlatVector<int64_t>({100, 200, 300})});
  auto outputType = asRowType(batch1->type());
  auto scanNode = makeTableScanNode("vs2", outputType);

  auto queryCtx = core::QueryCtx::create();
  auto task = Task::create(
      "test-eliminate",
      core::PlanFragment{scanNode},
      0,
      queryCtx,
      Task::ExecutionMode::kSerial);

  task->addSplit(scanNode->id(), Split{makeSplit({batch1, batch2})});
  task->noMoreSplits(scanNode->id());

  ContinueFuture future = ContinueFuture::makeEmpty();
  auto firstBatch = task->next(&future);
  ASSERT_NE(firstBatch, nullptr);
  ASSERT_EQ(firstBatch->size(), 3);

  task->testingVisitDrivers([&](Driver* driver) {
    auto* op = driver->findOperator(scanNode->id());
    if (!op) {
      return;
    }
    PushdownFilters pf;
    pf.filters[0] = std::make_shared<BigintRange>(100, 300, false);
    pf.dynamicFilteredColumns.insert(0);
    op->addDynamicFilterLocked("producer", pf);
  });

  auto secondBatch = task->next(&future);
  ASSERT_NE(secondBatch, nullptr);

  DecodedVector decoded(*secondBatch->childAt(0));
  std::vector<int64_t> outputIds;
  for (vector_size_t i = 0; i < secondBatch->size(); i++) {
    outputIds.push_back(decoded.valueAt<int64_t>(i));
  }
  ASSERT_EQ(outputIds, (std::vector<int64_t>{100, 200, 300}));

  auto end = task->next(&future);
  ASSERT_EQ(end, nullptr);
}

// Test that nulls are filtered out when nullAllowed is false.
TEST_F(ValueStreamDynamicFilterTest, filterWithNulls) {
  auto batch1 = makeRowVector({"id"}, {makeFlatVector<int64_t>({10, 20})});
  auto batch2 = makeRowVector(
      {"id"},
      {makeNullableFlatVector<int64_t>({1, std::nullopt, 3, std::nullopt, 5})});
  auto outputType = asRowType(batch1->type());
  auto scanNode = makeTableScanNode("vs3", outputType);

  auto queryCtx = core::QueryCtx::create();
  auto task = Task::create(
      "test-nulls",
      core::PlanFragment{scanNode},
      0,
      queryCtx,
      Task::ExecutionMode::kSerial);

  task->addSplit(scanNode->id(), Split{makeSplit({batch1, batch2})});
  task->noMoreSplits(scanNode->id());

  ContinueFuture future = ContinueFuture::makeEmpty();
  auto firstBatch = task->next(&future);
  ASSERT_NE(firstBatch, nullptr);

  task->testingVisitDrivers([&](Driver* driver) {
    auto* op = driver->findOperator(scanNode->id());
    if (!op) {
      return;
    }
    PushdownFilters pf;
    pf.filters[0] = std::make_shared<BigintRange>(3, 100, false);
    pf.dynamicFilteredColumns.insert(0);
    op->addDynamicFilterLocked("producer", pf);
  });

  auto secondBatch = task->next(&future);
  ASSERT_NE(secondBatch, nullptr);

  DecodedVector decoded(*secondBatch->childAt(0));
  std::vector<int64_t> outputIds;
  for (vector_size_t i = 0; i < secondBatch->size(); i++) {
    ASSERT_FALSE(decoded.isNullAt(i));
    outputIds.push_back(decoded.valueAt<int64_t>(i));
  }
  ASSERT_EQ(outputIds, (std::vector<int64_t>{3, 5}));

  auto end = task->next(&future);
  ASSERT_EQ(end, nullptr);
}

// Test canAddDynamicFilter returns true through the connector.
TEST_F(ValueStreamDynamicFilterTest, canAddDynamicFilter) {
  auto batch = makeRowVector({"id"}, {makeFlatVector<int64_t>({1})});
  auto outputType = asRowType(batch->type());
  auto scanNode = makeTableScanNode("vs4", outputType);

  auto queryCtx = core::QueryCtx::create();
  auto task = Task::create(
      "test-can-add",
      core::PlanFragment{scanNode},
      0,
      queryCtx,
      Task::ExecutionMode::kSerial);

  task->addSplit(scanNode->id(), Split{makeSplit({batch})});
  task->noMoreSplits(scanNode->id());

  ContinueFuture future = ContinueFuture::makeEmpty();
  task->next(&future);

  bool found = false;
  task->testingVisitDrivers([&](Driver* driver) {
    auto* op = driver->findOperator(scanNode->id());
    if (op) {
      ASSERT_TRUE(op->canAddDynamicFilter());
      found = true;
    }
  });
  ASSERT_TRUE(found);

  auto end = task->next(&future);
  ASSERT_EQ(end, nullptr);
}

} // namespace facebook::velox::test

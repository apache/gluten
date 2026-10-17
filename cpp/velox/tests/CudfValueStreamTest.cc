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

#include "operators/plannodes/CudfVectorStream.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/experimental/cudf/CudfConfig.h"
#include "velox/experimental/cudf/exec/ToCudf.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace facebook::velox::test {
namespace {

class TestBatchIterator final : public gluten::ColumnarBatchIterator {
 public:
  explicit TestBatchIterator(std::vector<RowVectorPtr> batches) : batches_(std::move(batches)) {}

  std::shared_ptr<gluten::ColumnarBatch> next() override {
    if (index_ == batches_.size()) {
      return nullptr;
    }
    return std::make_shared<gluten::VeloxColumnarBatch>(batches_[index_++]);
  }

 private:
  std::vector<RowVectorPtr> batches_;
  size_t index_ = 0;
};

class CudfValueStreamTest : public ::testing::Test, public VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    cudf_velox::registerCudf();
    Operator::registerOperator(std::make_unique<gluten::CudfVectorStreamOperatorTranslator>());
  }

  static void TearDownTestSuite() {
    cudf_velox::unregisterCudf();
    Operator::unregisterAllOperators();
  }

  std::shared_ptr<Task> makeTask(std::vector<RowVectorPtr> batches, const RowTypePtr& outputType) {
    auto iterator = std::make_shared<gluten::ResultIterator>(std::make_unique<TestBatchIterator>(std::move(batches)));
    auto node = std::make_shared<gluten::CudfValueStreamNode>("cudf-vs", outputType, std::move(iterator));
    // Inspect the source's GPU output before the driver adapter adds CudfToVelox.
    auto queryCtx =
        core::QueryCtx::create(nullptr, core::QueryConfig{{{cudf_velox::CudfConfig::kCudfEnabled, "false"}}});
    return Task::create("test-cudf-value-stream", core::PlanFragment{node}, 0, queryCtx, Task::ExecutionMode::kSerial);
  }

  void assertGpuValues(const RowVectorPtr& expected, const cudf_velox::CudfVectorPtr& actual) {
    auto host = cudf_velox::with_arrow::toVeloxColumn(
        actual->getTableView(), pool(), asRowType(expected->type()), "", actual->stream(), cudf_velox::get_output_mr());
    assertEqualVectors(expected, host);
  }
};

TEST_F(CudfValueStreamTest, convertsHostBatchToCudfVector) {
  auto batch = makeRowVector({"id"}, {makeNullableFlatVector<int64_t>({10, std::nullopt, 30})});
  auto task = makeTask({batch}, asRowType(batch->type()));

  ContinueFuture future = ContinueFuture::makeEmpty();
  auto output = task->next(&future);
  ASSERT_NE(output, nullptr);
  auto cudfVector = std::dynamic_pointer_cast<cudf_velox::CudfVector>(output);
  ASSERT_NE(cudfVector, nullptr);
  EXPECT_EQ(cudfVector->type(), batch->type());
  EXPECT_EQ(cudfVector->size(), 3);
  EXPECT_EQ(cudfVector->getTableView().num_rows(), 3);
  EXPECT_EQ(cudfVector->getTableView().num_columns(), 1);
  assertGpuValues(batch, cudfVector);
  EXPECT_EQ(task->next(&future), nullptr);
}

TEST_F(CudfValueStreamTest, trimsExtraHostColumns) {
  auto batch =
      makeRowVector({"id", "col_0"}, {makeFlatVector<int64_t>({10, 20, 30}), makeFlatVector<int64_t>({101, 202, 303})});
  auto outputType = ROW({"id"}, {BIGINT()});
  auto expected = makeRowVector({"id"}, {batch->childAt(0)});
  auto task = makeTask({batch}, outputType);

  ContinueFuture future = ContinueFuture::makeEmpty();
  auto output = task->next(&future);
  ASSERT_NE(output, nullptr);
  auto cudfVector = std::dynamic_pointer_cast<cudf_velox::CudfVector>(output);
  ASSERT_NE(cudfVector, nullptr);
  EXPECT_EQ(cudfVector->type(), outputType);
  EXPECT_EQ(cudfVector->size(), 3);
  EXPECT_EQ(cudfVector->getTableView().num_columns(), 1);
  assertGpuValues(expected, cudfVector);
  EXPECT_EQ(task->next(&future), nullptr);
}

TEST_F(CudfValueStreamTest, preservesZeroColumnRowCount) {
  auto outputType = ROW({}, {});
  auto batch = makeRowVector(outputType, 3);
  auto task = makeTask({batch}, outputType);

  ContinueFuture future = ContinueFuture::makeEmpty();
  auto output = task->next(&future);
  ASSERT_NE(output, nullptr);
  auto cudfVector = std::dynamic_pointer_cast<cudf_velox::CudfVector>(output);
  ASSERT_NE(cudfVector, nullptr);
  EXPECT_EQ(cudfVector->type(), outputType);
  EXPECT_EQ(cudfVector->size(), 3);
  EXPECT_EQ(cudfVector->getTableView().num_columns(), 0);
  EXPECT_EQ(task->next(&future), nullptr);
}

TEST_F(CudfValueStreamTest, rejectsHostBatchWithMissingColumns) {
  for (int32_t numColumns : {0, 1}) {
    SCOPED_TRACE(numColumns);
    auto batch = numColumns == 0 ? makeRowVector(ROW({}, {}), 3)
                                 : makeRowVector({"id"}, {makeFlatVector<int64_t>({10, 20, 30})});
    auto task = makeTask({batch}, ROW({"id", "value"}, {BIGINT(), BIGINT()}));
    ContinueFuture future = ContinueFuture::makeEmpty();
    VELOX_ASSERT_THROW(task->next(&future), "Value stream batch has fewer columns than the declared output type");
  }
}

TEST_F(CudfValueStreamTest, loadsLazyHostColumns) {
  auto lazy = makeLazyFlatVector<int64_t>(3, [](vector_size_t row) { return (row + 1) * 10; });
  auto batch = makeRowVector({"id"}, {lazy});
  auto expected = makeRowVector({"id"}, {makeFlatVector<int64_t>({10, 20, 30})});
  auto task = makeTask({batch}, asRowType(batch->type()));
  EXPECT_FALSE(lazy->isLoaded());

  ContinueFuture future = ContinueFuture::makeEmpty();
  auto output = task->next(&future);
  ASSERT_NE(output, nullptr);
  auto cudfVector = std::dynamic_pointer_cast<cudf_velox::CudfVector>(output);
  ASSERT_NE(cudfVector, nullptr);
  EXPECT_TRUE(lazy->isLoaded());
  assertGpuValues(expected, cudfVector);
  EXPECT_EQ(task->next(&future), nullptr);
}

TEST_F(CudfValueStreamTest, preservesGpuBatchAndStream) {
  auto host = makeRowVector({"id"}, {makeNullableFlatVector<int64_t>({10, std::nullopt, 30})});
  auto outputType = asRowType(host->type());
  auto stream = cudf_velox::cudfGlobalStreamPool().get_stream();
  auto table = cudf_velox::with_arrow::toCudfTable(host, pool(), stream, cudf_velox::get_output_mr());
  const auto* data = table->view().column(0).head<int64_t>();
  auto batch = std::make_shared<cudf_velox::CudfVector>(pool(), outputType, host->size(), std::move(table), stream);
  auto task = makeTask({batch}, outputType);

  ContinueFuture future = ContinueFuture::makeEmpty();
  auto output = task->next(&future);
  ASSERT_NE(output, nullptr);
  auto cudfVector = std::dynamic_pointer_cast<cudf_velox::CudfVector>(output);
  ASSERT_NE(cudfVector, nullptr);
  EXPECT_EQ(cudfVector->type(), outputType);
  EXPECT_EQ(cudfVector->size(), host->size());
  EXPECT_EQ(cudfVector->stream(), stream);
  EXPECT_EQ(cudfVector->getTableView().column(0).head<int64_t>(), data);
  assertGpuValues(host, cudfVector);
  EXPECT_EQ(task->next(&future), nullptr);
}

} // namespace
} // namespace facebook::velox::test

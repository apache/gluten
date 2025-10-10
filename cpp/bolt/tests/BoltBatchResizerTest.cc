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

#include <limits>

#include "utils/BoltBatchResizer.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

using namespace bytedance::bolt;

namespace gluten {
class ColumnarBatchArray : public ColumnarBatchIterator {
 public:
  explicit ColumnarBatchArray(const std::vector<std::shared_ptr<ColumnarBatch>> batches)
      : batches_(std::move(batches)) {}

  std::shared_ptr<ColumnarBatch> next() override {
    if (cursor_ >= batches_.size()) {
      return nullptr;
    }
    return batches_[cursor_++];
  }

 private:
  const std::vector<std::shared_ptr<ColumnarBatch>> batches_;
  int32_t cursor_ = 0;
};

class BoltBatchResizerTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  RowVectorPtr newVector(size_t numRows) {
    auto constant = makeConstant(1, numRows);
    auto out =
        std::make_shared<RowVector>(pool(), ROW({INTEGER()}), nullptr, numRows, std::vector<VectorPtr>{constant});
    return out;
  }

  void checkResize(int32_t min, int32_t max, std::vector<int32_t> inSizes, std::vector<int32_t> outSizes) {
    auto inBatches = std::vector<std::shared_ptr<ColumnarBatch>>();
    for (const auto& size : inSizes) {
      inBatches.push_back(std::make_shared<BoltColumnarBatch>(newVector(size)));
    }
    BoltBatchResizer resizer(pool(), min, max, std::make_unique<ColumnarBatchArray>(std::move(inBatches)));
    auto actualOutSizes = std::vector<int32_t>();
    while (true) {
      auto next = resizer.next();
      if (next == nullptr) {
        break;
      }
      actualOutSizes.push_back(next->numRows());
    }
    ASSERT_EQ(actualOutSizes, outSizes);
  }
};

TEST_F(BoltBatchResizerTest, sanity) {
  checkResize(100, std::numeric_limits<int32_t>::max(), {30, 50, 30, 40, 30}, {110, 70});
  checkResize(1, 40, {10, 20, 50, 30, 40, 30}, {10, 20, 40, 10, 30, 40, 30});
  checkResize(1, 39, {10, 20, 50, 30, 40, 30}, {10, 20, 39, 11, 30, 39, 1, 30});
  checkResize(40, 40, {10, 20, 50, 30, 40, 30}, {30, 40, 10, 30, 40, 30});
  checkResize(39, 39, {10, 20, 50, 30, 40, 30}, {30, 39, 11, 30, 39, 1, 30});
  checkResize(100, 200, {5, 900, 50}, {5, 200, 200, 200, 200, 100, 50});
  checkResize(100, 200, {5, 900, 30, 80}, {5, 200, 200, 200, 200, 100, 110});
  checkResize(100, 200, {5, 900, 700}, {5, 200, 200, 200, 200, 100, 200, 200, 200, 100});
  ASSERT_ANY_THROW(checkResize(0, 0, {}, {}));
}

} // namespace gluten

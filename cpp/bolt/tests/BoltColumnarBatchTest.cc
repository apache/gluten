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

#include "memory/BoltColumnarBatch.h"
#include "bolt/vector/arrow/Bridge.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

using namespace bytedance::bolt;

namespace gluten {
class BoltColumnarBatchTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }
};

TEST_F(BoltColumnarBatchTest, flattenTruncatedVector) {
  vector_size_t inputSize = 1'00;
  vector_size_t childSize = 1'000;
  auto mapVector = makeMapVector<int32_t, int64_t>(
      childSize, [](auto row) { return 1; }, [](auto row) { return row; }, [](auto row) { return row; });
  auto mapKeys = mapVector->mapKeys();
  auto mapValues = mapVector->mapValues();

  // First, make a row vector with the mapKeys and mapValues as children.
  // Make the row vector size less than the children size.
  auto input = std::make_shared<RowVector>(
      pool(),
      ROW({INTEGER(), BIGINT(), MAP(INTEGER(), BIGINT())}),
      nullptr,
      inputSize,
      std::vector<VectorPtr>{mapKeys, mapValues});

  auto batch = std::make_shared<BoltColumnarBatch>(input);
  ASSERT_NO_THROW(batch->getFlattenedRowVector());

  // Allocate a dummy indices and wrap the original mapVector with it as a dictionary, to force it get decoded in
  // flattenVector.
  auto indices = allocateIndices(childSize, pool());
  auto* rawIndices = indices->asMutable<vector_size_t>();
  for (vector_size_t i = 0; i < childSize; i++) {
    rawIndices[i] = i;
  }
  auto encodedMapVector = BaseVector::wrapInDictionary(nullptr, indices, inputSize, mapVector);
  auto inputOfMap = makeRowVector({encodedMapVector});
  auto batchOfMap = std::make_shared<BoltColumnarBatch>(inputOfMap);
  ASSERT_NO_THROW(batchOfMap->getFlattenedRowVector());
}

} // namespace gluten

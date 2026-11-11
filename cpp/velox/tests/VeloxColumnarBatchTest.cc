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

#include "memory/VeloxColumnarBatch.h"
#include "velox/vector/arrow/Bridge.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

namespace gluten {
class VeloxColumnarBatchTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }
};

TEST_F(VeloxColumnarBatchTest, flattenTruncatedVector) {
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

  auto batch = std::make_shared<VeloxColumnarBatch>(input);
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
  auto batchOfMap = std::make_shared<VeloxColumnarBatch>(inputOfMap);
  ASSERT_NO_THROW(batchOfMap->getFlattenedRowVector());
}

TEST_F(VeloxColumnarBatchTest, firstNullColumnIndex) {
  auto noNulls = makeFlatVector<int64_t>({1, 2, 3});
  auto withNull = makeNullableFlatVector<int64_t>({1, std::nullopt, 3});
  auto batch = std::make_shared<VeloxColumnarBatch>(makeRowVector({noNulls, withNull, noNulls}));

  ASSERT_EQ(batch->firstNullColumnIndex({}), -1);
  ASSERT_EQ(batch->firstNullColumnIndex({0, 2}), -1);
  ASSERT_EQ(batch->firstNullColumnIndex({1}), 0);
  ASSERT_EQ(batch->firstNullColumnIndex({0, 2, 1}), 2);
  ASSERT_ANY_THROW(batch->firstNullColumnIndex({3}));
  ASSERT_ANY_THROW(batch->firstNullColumnIndex({-1}));
}

TEST_F(VeloxColumnarBatchTest, firstNullColumnIndexIgnoresCachedNullCount) {
  // A stale cached null count of 0 must not hide a null in the nulls buffer.
  auto staleCount = makeNullableFlatVector<int64_t>({1, std::nullopt, 3});
  staleCount->setNullCount(0);
  ASSERT_EQ(std::make_shared<VeloxColumnarBatch>(makeRowVector({staleCount}))->firstNullColumnIndex({0}), 0);

  // A dictionary without nulls of its own, and with a null count of 0, over base values with a null.
  auto base = makeNullableFlatVector<int64_t>({std::nullopt, 1, 2});
  auto dictionary = wrapInDictionary(makeIndices({1, 0, 2}), 3, base);
  dictionary->setNullCount(0);
  ASSERT_EQ(std::make_shared<VeloxColumnarBatch>(makeRowVector({dictionary}))->firstNullColumnIndex({0}), 0);

  // The same base values, with indices that skip the null.
  auto dictionaryWithoutNull = wrapInDictionary(makeIndices({1, 2, 1}), 3, base);
  ASSERT_EQ(
      std::make_shared<VeloxColumnarBatch>(makeRowVector({dictionaryWithoutNull}))->firstNullColumnIndex({0}), -1);

  auto nullConstant = makeNullConstant(TypeKind::BIGINT, 3);
  ASSERT_EQ(std::make_shared<VeloxColumnarBatch>(makeRowVector({nullConstant}))->firstNullColumnIndex({0}), 0);
}

TEST_F(VeloxColumnarBatchTest, firstNullColumnIndexRowNullsAndTruncatedChildren) {
  // A null row is a null in every column.
  auto rowVector = makeRowVector({makeFlatVector<int64_t>({1, 2, 3})});
  rowVector->setNull(1, true);
  ASSERT_EQ(std::make_shared<VeloxColumnarBatch>(rowVector)->firstNullColumnIndex({0}), 0);

  // A null past the end of the batch, in a child longer than the batch, is not in the batch.
  auto child = makeNullableFlatVector<int64_t>({1, 2, std::nullopt});
  auto truncated = std::make_shared<RowVector>(pool(), ROW({BIGINT()}), nullptr, 2, std::vector<VectorPtr>{child});
  ASSERT_EQ(std::make_shared<VeloxColumnarBatch>(truncated)->firstNullColumnIndex({0}), -1);
}

} // namespace gluten

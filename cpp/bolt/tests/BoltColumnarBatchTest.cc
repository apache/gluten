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
#include "utils/BoltArrowUtils.h"

#include <arrow/api.h>

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

TEST_F(BoltColumnarBatchTest, importArrowNullType) {
  arrow::StringBuilder strings;
  ASSERT_TRUE(strings.Append("one").ok());
  ASSERT_TRUE(strings.Append("two").ok());
  std::shared_ptr<arrow::Array> values;
  ASSERT_TRUE(strings.Finish(&values).ok());
  auto nulls = arrow::MakeArrayOfNull(arrow::null(), 2).ValueOrDie();
  auto input = std::make_shared<ArrowColumnarBatch>(arrow::RecordBatch::Make(
      arrow::schema({arrow::field("a", arrow::utf8()), arrow::field("b", arrow::null())}), 2, {values, nulls}));
  auto result = BoltColumnarBatch::from(pool(), input)->getRowVector();
  ASSERT_EQ(result->size(), 2);
  ASSERT_EQ(result->childAt(1)->typeKind(), TypeKind::UNKNOWN);
  EXPECT_TRUE(result->childAt(1)->isNullAt(0));
  EXPECT_TRUE(result->childAt(1)->isNullAt(1));
  EXPECT_EQ(result->childAt(0)->as<SimpleVector<StringView>>()->valueAt(0), "one");
  EXPECT_EQ(result->childAt(0)->as<SimpleVector<StringView>>()->valueAt(1), "two");
  EXPECT_FALSE(ArrowUtils::getBridgeOptions().exportToArrowIPC);
}

TEST_F(BoltColumnarBatchTest, importArrowRegularAndDictionaryTypes) {
  arrow::Int32Builder integers;
  ASSERT_TRUE(integers.Append(42).ok());
  ASSERT_TRUE(integers.AppendNull().ok());
  std::shared_ptr<arrow::Array> values;
  ASSERT_TRUE(integers.Finish(&values).ok());
  arrow::StringBuilder strings;
  ASSERT_TRUE(strings.Append("one").ok());
  ASSERT_TRUE(strings.Append("two").ok());
  std::shared_ptr<arrow::Array> stringValues;
  ASSERT_TRUE(strings.Finish(&stringValues).ok());
  arrow::Int32Builder dictionaryIndices;
  ASSERT_TRUE(dictionaryIndices.Append(0).ok());
  ASSERT_TRUE(dictionaryIndices.Append(1).ok());
  std::shared_ptr<arrow::Array> indices;
  ASSERT_TRUE(dictionaryIndices.Finish(&indices).ok());
  auto dictionary =
      arrow::DictionaryArray::FromArrays(arrow::dictionary(arrow::int32(), arrow::utf8()), indices, stringValues)
          .ValueOrDie();
  auto timestampType = arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
  arrow::TimestampBuilder timestamps(timestampType, arrow::default_memory_pool());
  ASSERT_TRUE(timestamps.Append(1'234'567).ok());
  ASSERT_TRUE(timestamps.AppendNull().ok());
  std::shared_ptr<arrow::Array> times;
  ASSERT_TRUE(timestamps.Finish(&times).ok());
  auto input = std::make_shared<ArrowColumnarBatch>(arrow::RecordBatch::Make(
      arrow::schema(
          {arrow::field("a", arrow::int32()), arrow::field("b", dictionary->type()), arrow::field("t", timestampType)}),
      2,
      {values, dictionary, times}));
  auto result = BoltColumnarBatch::from(pool(), input)->getRowVector();
  EXPECT_EQ(result->childAt(0)->as<SimpleVector<int32_t>>()->valueAt(0), 42);
  EXPECT_TRUE(result->childAt(0)->isNullAt(1));
  EXPECT_EQ(result->childAt(1)->as<SimpleVector<StringView>>()->valueAt(0), "one");
  EXPECT_EQ(result->childAt(1)->as<SimpleVector<StringView>>()->valueAt(1), "two");
  EXPECT_EQ(result->childAt(2)->as<SimpleVector<Timestamp>>()->valueAt(0), Timestamp(1, 234'567'000));
  EXPECT_TRUE(result->childAt(2)->isNullAt(1));
}

TEST_F(BoltColumnarBatchTest, importExistingBoltBatchKeepsIdentity) {
  auto original = std::make_shared<BoltColumnarBatch>(makeRowVector({makeFlatVector<int32_t>({42, 99})}));
  EXPECT_EQ(BoltColumnarBatch::from(pool(), original), original);
}

} // namespace gluten

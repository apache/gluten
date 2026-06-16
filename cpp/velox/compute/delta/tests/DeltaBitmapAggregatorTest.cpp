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

#include <optional>

#include <gtest/gtest.h>

#include "compute/delta/RoaringBitmapArray.h"
#include "operators/functions/delta/DeltaBitmapAggregator.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace gluten::delta {
namespace {

using facebook::velox::StringView;
using facebook::velox::exec::test::AssertQueryBuilder;
using facebook::velox::exec::test::HiveConnectorTestBase;
using facebook::velox::exec::test::PlanBuilder;

class DeltaBitmapAggregatorTest : public HiveConnectorTestBase {
 protected:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
    gluten::registerDeltaBitmapAggregator();
  }

  RoaringBitmapArray extractBitmap(
      const facebook::velox::RowVectorPtr& results,
      facebook::velox::vector_size_t row,
      facebook::velox::column_index_t column,
      int64_t expectedCardinality,
      std::optional<int64_t> expectedLast) {
    const auto bitmapResult = results->childAt(column)->as<facebook::velox::RowVector>();
    EXPECT_EQ(bitmapResult->childAt(0)->asFlatVector<int64_t>()->valueAt(row), expectedCardinality);
    if (expectedLast.has_value()) {
      EXPECT_FALSE(bitmapResult->childAt(1)->isNullAt(row));
      EXPECT_EQ(bitmapResult->childAt(1)->asFlatVector<int64_t>()->valueAt(row), expectedLast.value());
    } else {
      EXPECT_TRUE(bitmapResult->childAt(1)->isNullAt(row));
    }

    const auto payload = bitmapResult->childAt(2)->asFlatVector<StringView>()->valueAt(row);
    RoaringBitmapArray bitmap;
    bitmap.deserialize(payload.data(), payload.size());
    return bitmap;
  }
};

TEST_F(DeltaBitmapAggregatorTest, SingleAggregationIgnoresNullsAndDuplicates) {
  const auto input = makeRowVector(
      {"row_index"}, {makeNullableFlatVector<int64_t>({1, 7, 7, std::nullopt, static_cast<int64_t>(1ULL << 33)})});

  const auto plan =
      PlanBuilder(pool()).values({input}).singleAggregation({}, {"bitmapaggregator(row_index) AS dv"}).planNode();
  const auto results = AssertQueryBuilder(plan).copyResults(pool());
  ASSERT_EQ(results->size(), 1);

  const auto bitmap = extractBitmap(results, 0, 0, 3, static_cast<int64_t>(1ULL << 33));
  EXPECT_TRUE(bitmap.containsSafe(1));
  EXPECT_TRUE(bitmap.containsSafe(7));
  EXPECT_TRUE(bitmap.containsSafe(1ULL << 33));
  EXPECT_FALSE(bitmap.containsSafe(2));
}

TEST_F(DeltaBitmapAggregatorTest, PartialFinalAggregationMergesPayloadsByGroup) {
  const auto input = makeRowVector(
      {"file_id", "row_index"},
      {makeFlatVector<int64_t>({10, 10, 20, 10, 20, 20}),
       makeFlatVector<int64_t>({1, 1, 3, static_cast<int64_t>(1ULL << 32), 5, 5})});

  const auto plan = PlanBuilder(pool())
                        .values({input})
                        .partialAggregation({"file_id"}, {"bitmapaggregator(row_index) AS dv"})
                        .finalAggregation()
                        .planNode();
  const auto results = AssertQueryBuilder(plan).copyResults(pool());
  ASSERT_EQ(results->size(), 2);

  for (facebook::velox::vector_size_t row = 0; row < results->size(); ++row) {
    const auto fileId = results->childAt(0)->asFlatVector<int64_t>()->valueAt(row);
    if (fileId == 10) {
      const auto bitmap = extractBitmap(results, row, 1, 2, static_cast<int64_t>(1ULL << 32));
      EXPECT_TRUE(bitmap.containsSafe(1));
      EXPECT_TRUE(bitmap.containsSafe(1ULL << 32));
    } else {
      ASSERT_EQ(fileId, 20);
      const auto bitmap = extractBitmap(results, row, 1, 2, 5);
      EXPECT_TRUE(bitmap.containsSafe(3));
      EXPECT_TRUE(bitmap.containsSafe(5));
    }
  }
}

TEST_F(DeltaBitmapAggregatorTest, AllNullInputProducesEmptyBitmap) {
  const auto input = makeRowVector({"row_index"}, {makeNullableFlatVector<int64_t>({std::nullopt})});
  const auto plan =
      PlanBuilder(pool()).values({input}).singleAggregation({}, {"bitmapaggregator(row_index) AS dv"}).planNode();
  const auto results = AssertQueryBuilder(plan).copyResults(pool());
  ASSERT_EQ(results->size(), 1);

  const auto bitmap = extractBitmap(results, 0, 0, 0, std::nullopt);
  EXPECT_EQ(bitmap.cardinality(), 0);
}

TEST_F(DeltaBitmapAggregatorTest, RejectsNegativeRowIndexes) {
  const auto input = makeRowVector({"row_index"}, {makeFlatVector<int64_t>({-1})});
  const auto plan =
      PlanBuilder(pool()).values({input}).singleAggregation({}, {"bitmapaggregator(row_index) AS dv"}).planNode();

  VELOX_ASSERT_THROW(AssertQueryBuilder(plan).copyResults(pool()), "Delta bitmap row index cannot be negative");
}

TEST_F(DeltaBitmapAggregatorTest, RejectsRowIndexesAboveDeltaMax) {
  const auto tooLarge = static_cast<int64_t>(RoaringBitmapArray::kMaxRepresentableValue + 1);
  const auto input = makeRowVector({"row_index"}, {makeFlatVector<int64_t>({tooLarge})});
  const auto plan =
      PlanBuilder(pool()).values({input}).singleAggregation({}, {"bitmapaggregator(row_index) AS dv"}).planNode();

  VELOX_ASSERT_THROW(AssertQueryBuilder(plan).copyResults(pool()), "exceeds max representable value");
}

} // namespace
} // namespace gluten::delta

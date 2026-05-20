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
#include <optional>
#include <string>
#include <vector>

#include "utils/VeloxBatchResizer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

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

class VeloxBatchResizerTest : public ::testing::Test, public test::VectorTestBase {
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

  RowVectorPtr newDenseFlatVector(size_t numRows, int32_t start = 0) {
    std::vector<std::optional<int64_t>> nullableValues;
    nullableValues.reserve(numRows);
    std::vector<std::string> strings;
    strings.reserve(numRows);
    for (auto i = 0; i < numRows; ++i) {
      nullableValues.emplace_back(i % 3 == 0 ? std::nullopt : std::optional<int64_t>(start + i));
      strings.emplace_back("long-string-value-" + std::to_string(start + i));
    }
    return makeRowVector(
        {"i32", "i64", "flag", "str"},
        {makeFlatVector<int32_t>(numRows, [start](auto row) { return start + row; }),
         makeNullableFlatVector<int64_t>(nullableValues),
         makeFlatVector<bool>(numRows, [](auto row) { return row % 2 == 0; }),
         makeFlatVector<StringView>(numRows, [&strings](auto row) { return StringView(strings[row]); })});
  }

  RowVectorPtr newNullableBoolVector(size_t numRows, int32_t start = 0) {
    std::vector<std::optional<bool>> flags;
    flags.reserve(numRows);
    for (auto i = 0; i < numRows; ++i) {
      if ((start + i) % 5 == 0) {
        flags.emplace_back(std::nullopt);
      } else {
        flags.emplace_back((start + i) % 3 == 0);
      }
    }
    return makeRowVector({"flag"}, {makeNullableFlatVector<bool>(flags)});
  }

  RowVectorPtr newDenseFlatVectorWithNullableBool(size_t numRows, int32_t start = 0) {
    std::vector<std::optional<bool>> nullableFlags;
    nullableFlags.reserve(numRows);
    std::vector<std::optional<int64_t>> nullableValues;
    nullableValues.reserve(numRows);
    for (auto i = 0; i < numRows; ++i) {
      if ((start + i) % 7 == 0) {
        nullableFlags.emplace_back(std::nullopt);
      } else {
        nullableFlags.emplace_back((start + i) % 2 == 0);
      }
      nullableValues.emplace_back((start + i) % 4 == 0 ? std::nullopt : std::optional<int64_t>(start + i));
    }
    return makeRowVector(
        {"i32", "flag", "i64"},
        {makeFlatVector<int32_t>(numRows, [start](auto row) { return start + row; }),
         makeNullableFlatVector<bool>(nullableFlags),
         makeNullableFlatVector<int64_t>(nullableValues)});
  }

  RowVectorPtr newFlatIntVector(size_t numRows, int32_t start = 0) {
    return makeRowVector({"i32"}, {makeFlatVector<int32_t>(numRows, [start](auto row) { return start + row; })});
  }

  RowVectorPtr newComplexVector(size_t numRows, int32_t start = 0) {
    std::vector<std::vector<int32_t>> arrays;
    arrays.reserve(numRows);
    for (auto i = 0; i < numRows; ++i) {
      arrays.push_back({start + static_cast<int32_t>(i), start + static_cast<int32_t>(i) + 1});
    }
    return makeRowVector(
        {"i32", "arr"},
        {makeFlatVector<int32_t>(numRows, [start](auto row) { return start + row; }),
         makeArrayVector<int32_t>(arrays)});
  }

  RowVectorPtr newNullableComplexVector(size_t numRows, int32_t start = 0) {
    std::vector<std::optional<std::vector<std::optional<int32_t>>>> arrays;
    arrays.reserve(numRows);
    for (auto i = 0; i < numRows; ++i) {
      if (i % 5 == 0) {
        arrays.emplace_back(std::nullopt);
      } else {
        arrays.emplace_back(std::vector<std::optional<int32_t>>{
            start + static_cast<int32_t>(i), std::nullopt, start + static_cast<int32_t>(i) + 1});
      }
    }
    return makeRowVector(
        {"i32", "arr"},
        {makeFlatVector<int32_t>(numRows, [start](auto row) { return start + row; }),
         makeNullableArrayVector<int32_t>(arrays)});
  }

  RowVectorPtr newMapVector(size_t numRows, int32_t start = 0) {
    std::vector<std::vector<std::pair<int32_t, std::optional<int32_t>>>> maps;
    maps.reserve(numRows);
    for (auto i = 0; i < numRows; ++i) {
      const auto value = start + static_cast<int32_t>(i);
      maps.push_back({{value, value + 1}, {value + 2, value + 3}});
    }
    return makeRowVector(
        {"i32", "map"},
        {makeFlatVector<int32_t>(numRows, [start](auto row) { return start + row; }),
         makeMapVector<int32_t, int32_t>(maps)});
  }

  RowVectorPtr newNullableMapVector(size_t numRows, int32_t start = 0) {
    std::vector<std::optional<std::vector<std::pair<int32_t, std::optional<int32_t>>>>> maps;
    maps.reserve(numRows);
    for (auto i = 0; i < numRows; ++i) {
      if (i % 5 == 0) {
        maps.emplace_back(std::nullopt);
      } else {
        const auto value = start + static_cast<int32_t>(i);
        maps.emplace_back(
            std::vector<std::pair<int32_t, std::optional<int32_t>>>{{value, value + 1}, {value + 2, std::nullopt}});
      }
    }
    return makeRowVector(
        {"i32", "map"},
        {makeFlatVector<int32_t>(numRows, [start](auto row) { return start + row; }),
         makeNullableMapVector<int32_t, int32_t>(maps)});
  }

  RowVectorPtr newDictionaryVector(size_t numRows, int32_t start = 0) {
    auto base = makeFlatVector<int32_t>(numRows, [start](auto row) { return start + row; });
    auto indices = makeIndices(numRows, [](auto row) { return row; });
    return makeRowVector({"dict"}, {wrapInDictionary(indices, numRows, base)});
  }

  RowVectorPtr newTopLevelNullVector(size_t numRows, int32_t start = 0) {
    auto nulls = allocateNulls(numRows, pool());
    bits::setNull(nulls->asMutable<uint64_t>(), 0, true);
    return std::make_shared<RowVector>(
        pool(),
        ROW({"i32"}, {INTEGER()}),
        nulls,
        numRows,
        std::vector<VectorPtr>{makeFlatVector<int32_t>(numRows, [start](auto row) { return start + row; })},
        1);
  }

  void checkResize(
      int32_t min,
      int32_t max,
      int64_t preferredBatchBytes,
      std::vector<int32_t> inSizes,
      std::vector<int32_t> outSizes) {
    auto inBatches = std::vector<std::shared_ptr<ColumnarBatch>>();
    inBatches.reserve(inSizes.size());
    for (const auto& size : inSizes) {
      inBatches.push_back(std::make_shared<VeloxColumnarBatch>(newVector(size)));
    }
    VeloxBatchResizer resizer(
        pool(), min, max, preferredBatchBytes, std::make_unique<ColumnarBatchArray>(std::move(inBatches)));
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

  RowVectorPtr
  resizeOnce(const std::vector<RowVectorPtr>& vectors, bool enableDenseFlatCopy, VeloxBatchResizeStats* stats) {
    auto out = resizeAll(vectors, 100, std::numeric_limits<int32_t>::max(), (10L << 20), enableDenseFlatCopy, stats);
    EXPECT_EQ(out.size(), 1);
    return out[0];
  }

  std::vector<RowVectorPtr> resizeAll(
      const std::vector<RowVectorPtr>& vectors,
      int32_t minOutputBatchSize,
      int32_t maxOutputBatchSize,
      int64_t preferredBatchBytes,
      bool enableDenseFlatCopy,
      VeloxBatchResizeStats* stats) {
    std::vector<std::shared_ptr<ColumnarBatch>> inBatches;
    inBatches.reserve(vectors.size());
    for (const auto& vector : vectors) {
      inBatches.push_back(std::make_shared<VeloxColumnarBatch>(vector));
    }
    VeloxBatchResizer resizer(
        pool(),
        minOutputBatchSize,
        maxOutputBatchSize,
        preferredBatchBytes,
        std::make_unique<ColumnarBatchArray>(std::move(inBatches)),
        enableDenseFlatCopy,
        stats);
    std::vector<RowVectorPtr> out;
    while (auto next = resizer.next()) {
      auto veloxBatch = std::dynamic_pointer_cast<VeloxColumnarBatch>(next);
      EXPECT_NE(veloxBatch, nullptr);
      out.push_back(veloxBatch->getRowVector());
    }
    return out;
  }
};

TEST_F(VeloxBatchResizerTest, sanity) {
  checkResize(100, std::numeric_limits<int32_t>::max(), (10L << 20), {30, 50, 30, 40, 30}, {110, 70});
  checkResize(1, 40, (10L << 20), {10, 20, 50, 30, 40, 30}, {10, 20, 40, 10, 30, 40, 30});
  checkResize(1, 39, (10L << 20), {10, 20, 50, 30, 40, 30}, {10, 20, 39, 11, 30, 39, 1, 30});
  checkResize(40, 40, (10L << 20), {10, 20, 50, 30, 40, 30}, {30, 40, 10, 30, 40, 30});
  checkResize(39, 39, (10L << 20), {10, 20, 50, 30, 40, 30}, {30, 39, 11, 30, 39, 1, 30});
  checkResize(100, 200, (10L << 20), {5, 900, 50}, {5, 200, 200, 200, 200, 100, 50});
  checkResize(100, 200, (10L << 20), {5, 900, 30, 80}, {5, 200, 200, 200, 200, 100, 110});
  checkResize(100, 200, (10L << 20), {5, 900, 700}, {5, 200, 200, 200, 200, 100, 200, 200, 200, 100});
  ASSERT_ANY_THROW(checkResize(0, 0, (10L << 20), {}, {}));
}

TEST_F(VeloxBatchResizerTest, preferredBatchBytesTest) {
  checkResize(100, std::numeric_limits<int32_t>::max(), 0, {30, 50, 30, 40, 30}, {30, 50, 30, 40, 30});
  checkResize(40, 40, 0, {10, 20, 50, 30, 40, 30}, {10, 20, 40, 10, 30, 40, 30});
  checkResize(39, 39, 0, {10, 20, 50, 30, 40, 30}, {10, 20, 39, 11, 30, 39, 1, 30});
  checkResize(100, 200, 0, {5, 900, 50}, {5, 200, 200, 200, 200, 100, 50});
  checkResize(100, 200, 0, {5, 900, 30, 80}, {5, 200, 200, 200, 200, 100, 30, 80});
  checkResize(100, 200, 0, {5, 900, 700}, {5, 200, 200, 200, 200, 100, 200, 200, 200, 100});
  ASSERT_ANY_THROW(checkResize(0, 0, 0, {}, {}));
}

TEST_F(VeloxBatchResizerTest, denseFlatCopyDisabledUsesAppendPath) {
  VeloxBatchResizeStats stats;
  auto vectors = std::vector<RowVectorPtr>{newDenseFlatVector(30, 0), newDenseFlatVector(40, 100)};
  auto actual = resizeOnce(vectors, false, &stats);
  auto appendStats = VeloxBatchResizeStats{};
  auto expected = resizeOnce(vectors, false, &appendStats);
  test::assertEqualVectors(expected, actual);
  EXPECT_EQ(stats.copyRangesBatches, 0);
  EXPECT_EQ(stats.appendCopyBatches, 2);
}

TEST_F(VeloxBatchResizerTest, denseFlatCopyEnabledUsesCopyRangesForFixedWidthAndString) {
  VeloxBatchResizeStats stats;
  auto vectors = std::vector<RowVectorPtr>{newDenseFlatVector(30, 0), newDenseFlatVector(40, 100)};
  auto appendStats = VeloxBatchResizeStats{};
  auto expected = resizeOnce(vectors, false, &appendStats);

  auto actual = resizeOnce(vectors, true, &stats);

  test::assertEqualVectors(expected, actual);
  EXPECT_EQ(stats.copyRangesBatches, 2);
  EXPECT_EQ(stats.copyRangesOutputBatches, 1);
  EXPECT_EQ(stats.appendCopyBatches, 0);
}

TEST_F(VeloxBatchResizerTest, copyRangesEnabledHandlesNullableBoolBitmaps) {
  VeloxBatchResizeStats stats;
  auto vectors = std::vector<RowVectorPtr>{
      newNullableBoolVector(3, 0),
      newNullableBoolVector(5, 100),
      newNullableBoolVector(9, 200),
      newNullableBoolVector(17, 300),
  };
  auto appendStats = VeloxBatchResizeStats{};
  auto expected = resizeOnce(vectors, false, &appendStats);

  auto actual = resizeOnce(vectors, true, &stats);

  test::assertEqualVectors(expected, actual);
  EXPECT_EQ(stats.copyRangesBatches, 4);
  EXPECT_EQ(stats.copyRangesOutputBatches, 1);
  EXPECT_EQ(stats.appendCopyBatches, 0);
  EXPECT_EQ(stats.copyRangesFallbackBatches, 0);
}

TEST_F(VeloxBatchResizerTest, copyRangesEnabledHandlesMixedNullableBitmapsAtUnalignedOffsets) {
  VeloxBatchResizeStats stats;
  auto vectors = std::vector<RowVectorPtr>{
      newDenseFlatVectorWithNullableBool(1, 0),
      newDenseFlatVectorWithNullableBool(6, 100),
      newDenseFlatVectorWithNullableBool(10, 200),
      newDenseFlatVectorWithNullableBool(15, 300),
  };
  auto appendStats = VeloxBatchResizeStats{};
  auto expected = resizeOnce(vectors, false, &appendStats);

  auto actual = resizeOnce(vectors, true, &stats);

  test::assertEqualVectors(expected, actual);
  EXPECT_EQ(stats.copyRangesBatches, 4);
  EXPECT_EQ(stats.copyRangesOutputBatches, 1);
  EXPECT_EQ(stats.appendCopyBatches, 0);
  EXPECT_EQ(stats.copyRangesFallbackBatches, 0);
}

TEST_F(VeloxBatchResizerTest, veloxCopyRangesHandlesNullableBoolBitmapsAtUnalignedOffsets) {
  auto sourceRow = newNullableBoolVector(48, 100);
  auto source = sourceRow->childAt(0)->loadedVector();
  auto actual = newNullableBoolVector(64, 500)->childAt(0);
  auto expected = newNullableBoolVector(64, 500)->childAt(0);
  auto expectedFlat = expected->asFlatVector<bool>();
  auto sourceFlat = source->asFlatVector<bool>();
  const std::vector<BaseVector::CopyRange> ranges{
      {.sourceIndex = 1, .targetIndex = 2, .count = 5},
      {.sourceIndex = 7, .targetIndex = 11, .count = 9},
      {.sourceIndex = 18, .targetIndex = 24, .count = 13},
      {.sourceIndex = 33, .targetIndex = 43, .count = 4},
  };
  for (const auto& range : ranges) {
    for (auto i = 0; i < range.count; ++i) {
      const auto sourceIndex = range.sourceIndex + i;
      const auto targetIndex = range.targetIndex + i;
      if (source->isNullAt(sourceIndex)) {
        expectedFlat->setNull(targetIndex, true);
      } else {
        expectedFlat->set(targetIndex, sourceFlat->valueAt(sourceIndex));
      }
    }
  }

  actual->copyRanges(source, ranges);

  test::assertEqualVectors(expected, actual);
}

TEST_F(VeloxBatchResizerTest, copyRangesEnabledSupportsComplexType) {
  VeloxBatchResizeStats stats;
  auto vectors = std::vector<RowVectorPtr>{newComplexVector(30, 0), newComplexVector(40, 100)};
  auto appendStats = VeloxBatchResizeStats{};
  auto expected = resizeOnce(vectors, false, &appendStats);

  auto actual = resizeOnce(vectors, true, &stats);

  test::assertEqualVectors(expected, actual);
  EXPECT_EQ(stats.copyRangesBatches, 2);
  EXPECT_EQ(stats.copyRangesOutputBatches, 1);
  EXPECT_EQ(stats.appendCopyBatches, 0);
  EXPECT_EQ(stats.copyRangesFallbackBatches, 0);
}

TEST_F(VeloxBatchResizerTest, copyRangesEnabledSupportsNullableComplexType) {
  VeloxBatchResizeStats stats;
  auto vectors = std::vector<RowVectorPtr>{newNullableComplexVector(30, 0), newNullableComplexVector(40, 100)};
  auto appendStats = VeloxBatchResizeStats{};
  auto expected = resizeOnce(vectors, false, &appendStats);

  auto actual = resizeOnce(vectors, true, &stats);

  test::assertEqualVectors(expected, actual);
  EXPECT_EQ(stats.copyRangesBatches, 2);
  EXPECT_EQ(stats.copyRangesOutputBatches, 1);
  EXPECT_EQ(stats.appendCopyBatches, 0);
  EXPECT_EQ(stats.copyRangesFallbackBatches, 0);
}

TEST_F(VeloxBatchResizerTest, copyRangesEnabledSupportsMapType) {
  VeloxBatchResizeStats stats;
  auto vectors = std::vector<RowVectorPtr>{newMapVector(30, 0), newMapVector(40, 100)};
  auto appendStats = VeloxBatchResizeStats{};
  auto expected = resizeOnce(vectors, false, &appendStats);

  auto actual = resizeOnce(vectors, true, &stats);

  test::assertEqualVectors(expected, actual);
  EXPECT_EQ(stats.copyRangesBatches, 2);
  EXPECT_EQ(stats.copyRangesOutputBatches, 1);
  EXPECT_EQ(stats.appendCopyBatches, 0);
  EXPECT_EQ(stats.copyRangesFallbackBatches, 0);
}

TEST_F(VeloxBatchResizerTest, copyRangesEnabledSupportsNullableMapType) {
  VeloxBatchResizeStats stats;
  auto vectors = std::vector<RowVectorPtr>{newNullableMapVector(30, 0), newNullableMapVector(40, 100)};
  auto appendStats = VeloxBatchResizeStats{};
  auto expected = resizeOnce(vectors, false, &appendStats);

  auto actual = resizeOnce(vectors, true, &stats);

  test::assertEqualVectors(expected, actual);
  EXPECT_EQ(stats.copyRangesBatches, 2);
  EXPECT_EQ(stats.copyRangesOutputBatches, 1);
  EXPECT_EQ(stats.appendCopyBatches, 0);
  EXPECT_EQ(stats.copyRangesFallbackBatches, 0);
}

TEST_F(VeloxBatchResizerTest, copyRangesEnabledFallsBackForConstantEncoding) {
  VeloxBatchResizeStats stats;
  auto vectors = std::vector<RowVectorPtr>{newVector(30), newVector(40)};
  auto appendStats = VeloxBatchResizeStats{};
  auto expected = resizeOnce(vectors, false, &appendStats);

  auto actual = resizeOnce(vectors, true, &stats);

  test::assertEqualVectors(expected, actual);
  EXPECT_EQ(stats.copyRangesBatches, 0);
  EXPECT_EQ(stats.appendCopyBatches, 2);
  EXPECT_EQ(stats.copyRangesFallbackBatches, 2);
}

TEST_F(VeloxBatchResizerTest, copyRangesEnabledFallsBackForTopLevelNulls) {
  VeloxBatchResizeStats stats;
  auto vectors = std::vector<RowVectorPtr>{newTopLevelNullVector(30, 0), newTopLevelNullVector(40, 100)};
  auto appendStats = VeloxBatchResizeStats{};
  auto expected = resizeOnce(vectors, false, &appendStats);

  auto actual = resizeOnce(vectors, true, &stats);

  test::assertEqualVectors(expected, actual);
  EXPECT_EQ(stats.copyRangesBatches, 0);
  EXPECT_EQ(stats.copyRangesOutputBatches, 0);
  EXPECT_EQ(stats.appendCopyBatches, 2);
  EXPECT_EQ(stats.copyRangesFallbackBatches, 2);
}

TEST_F(VeloxBatchResizerTest, copyRangesEnabledCanMixSmallDenseSparseAndDenseBatches) {
  VeloxBatchResizeStats stats;
  auto vectors = std::vector<RowVectorPtr>{newFlatIntVector(30, 0), newVector(40), newFlatIntVector(20, 100)};
  auto appendStats = VeloxBatchResizeStats{};
  auto expected = resizeOnce(vectors, false, &appendStats);

  auto actual = resizeOnce(vectors, true, &stats);

  ASSERT_EQ(actual->size(), expected->size());
  EXPECT_EQ(actual->size(), 90);
  test::assertEqualVectors(expected, actual);
  EXPECT_EQ(stats.copyRangesBatches, 2);
  EXPECT_EQ(stats.copyRangesOutputBatches, 1);
  EXPECT_EQ(stats.appendCopyBatches, 1);
  EXPECT_EQ(stats.copyRangesFallbackBatches, 1);
}

TEST_F(VeloxBatchResizerTest, copyRangesEnabledFlushesCollectedInputsBeforeSplit) {
  VeloxBatchResizeStats stats;
  auto vectors =
      std::vector<RowVectorPtr>{newFlatIntVector(40, 0), newFlatIntVector(40, 100), newFlatIntVector(40, 200)};

  auto actual = resizeAll(vectors, 100, 50, (10L << 20), true, &stats);

  ASSERT_EQ(actual.size(), 3);
  test::assertEqualVectors(vectors[0], actual[0]);
  test::assertEqualVectors(vectors[1], actual[1]);
  test::assertEqualVectors(vectors[2], actual[2]);
  EXPECT_EQ(stats.copyRangesBatches, 2);
  EXPECT_EQ(stats.copyRangesOutputBatches, 2);
  EXPECT_EQ(stats.appendCopyBatches, 0);
  EXPECT_EQ(stats.copyRangesFallbackBatches, 0);
}

TEST_F(VeloxBatchResizerTest, copyRangesEnabledFlushesCollectedInputsAtEndOfInput) {
  VeloxBatchResizeStats stats;
  auto vectors =
      std::vector<RowVectorPtr>{newFlatIntVector(30, 0), newFlatIntVector(40, 100), newFlatIntVector(20, 200)};
  auto appendStats = VeloxBatchResizeStats{};
  auto expected = resizeAll(vectors, 100, std::numeric_limits<int32_t>::max(), (10L << 20), false, &appendStats);

  auto actual = resizeAll(vectors, 100, std::numeric_limits<int32_t>::max(), (10L << 20), true, &stats);

  ASSERT_EQ(actual.size(), 1);
  ASSERT_EQ(expected.size(), 1);
  test::assertEqualVectors(expected[0], actual[0]);
  EXPECT_EQ(actual[0]->size(), 90);
  EXPECT_EQ(stats.copyRangesBatches, 3);
  EXPECT_EQ(stats.copyRangesOutputBatches, 1);
  EXPECT_EQ(stats.appendCopyBatches, 0);
  EXPECT_EQ(stats.copyRangesFallbackBatches, 0);
}

TEST_F(VeloxBatchResizerTest, copyRangesEnabledFallsBackForDictionaryEncoding) {
  VeloxBatchResizeStats stats;
  auto vectors = std::vector<RowVectorPtr>{newDictionaryVector(30, 0), newDictionaryVector(40, 100)};
  auto appendStats = VeloxBatchResizeStats{};
  auto expected = resizeOnce(vectors, false, &appendStats);

  auto actual = resizeOnce(vectors, true, &stats);

  test::assertEqualVectors(expected, actual);
  EXPECT_EQ(stats.copyRangesBatches, 0);
  EXPECT_EQ(stats.appendCopyBatches, 2);
  EXPECT_EQ(stats.copyRangesFallbackBatches, 2);
}

} // namespace gluten

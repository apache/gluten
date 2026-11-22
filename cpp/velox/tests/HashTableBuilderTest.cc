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

#include <arrow/memory_pool.h>
#include <fmt/ranges.h>
#include <gtest/gtest.h>
#include <array>
#include <limits>
#include <stdexcept>

#include "jni/JniHashTable.h"
#include "memory/VeloxColumnarBatch.h"
#include "operators/serializer/VeloxColumnarBatchSerializer.h"
#include "substrait/algebra.pb.h"
#include "utils/ObjectStore.h"
#include "utils/VeloxArrowUtils.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/vector/arrow/Bridge.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

namespace gluten {

class HashTableBuilderTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  struct BuildOptions {
    std::vector<std::string> joinKeys{"key"};
    int joinType{substrait::JoinRel_JoinType_JOIN_TYPE_INNER};
    bool nullAware{false};
    bool withFilter{false};
    bool existenceJoin{false};
    std::vector<std::string> filterColumns;
    bool filterPropagatesNulls{false};
    uint32_t abandonDedupMinRows{100000};
    uint32_t abandonDedupMinPct{0};
  };

  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  std::shared_ptr<HashTableBuilder> build(
      const std::function<std::shared_ptr<ColumnarBatch>()>& nextBatch,
      int joinType = substrait::JoinRel_JoinType_JOIN_TYPE_INNER,
      bool nullAware = false) {
    BuildOptions options;
    options.joinType = joinType;
    options.nullAware = nullAware;
    return build(ROW({"key", "payload"}, {BIGINT(), VARCHAR()}), options, nextBatch);
  }

  std::shared_ptr<HashTableBuilder> build(
      const RowTypePtr& rowType,
      const BuildOptions& options,
      const std::function<std::shared_ptr<ColumnarBatch>()>& nextBatch) {
    return nativeHashTableBuild(
        options.joinKeys,
        options.filterColumns,
        options.filterPropagatesNulls,
        rowType->names(),
        rowType->children(),
        options.joinType,
        options.withFilter,
        options.existenceJoin,
        options.nullAware,
        0,
        1000,
        1000000,
        options.abandonDedupMinRows,
        options.abandonDedupMinPct,
        nextBatch,
        pool_);
  }

  std::shared_ptr<HashTableBuilder> build(const std::vector<RowVectorPtr>& inputs, const BuildOptions& options) {
    VELOX_CHECK(!inputs.empty());
    size_t batchIndex = 0;
    return build(asRowType(inputs.front()->type()), options, [&]() -> std::shared_ptr<ColumnarBatch> {
      return batchIndex < inputs.size() ? std::make_shared<VeloxColumnarBatch>(inputs[batchIndex++]) : nullptr;
    });
  }

  std::unique_ptr<exec::BaseHashTable> finishBuild(const std::shared_ptr<HashTableBuilder>& builder) {
    auto table = builder->uniqueTable();
    table->prepareJoinTable(
        {},
        exec::BaseHashTable::kNoSpillInputStartPartitionBit,
        builder->joinBuildVectorHasherMaxNumDistinct(),
        builder->dropDuplicates(),
        nullptr);
    return table;
  }

  RowVectorPtr extractRows(exec::BaseHashTable* table, const std::vector<char*>& rows) {
    std::vector<VectorPtr> columns;
    const auto& types = table->rows()->columnTypes();
    for (size_t column = 0; column < types.size(); ++column) {
      auto values = BaseVector::create(types[column], rows.size(), pool_.get());
      if (!rows.empty()) {
        table->rows()->extractColumn(rows.data(), rows.size(), column, values);
      }
      columns.push_back(std::move(values));
    }
    return makeRowVector(columns);
  }

  void assertTableRows(exec::BaseHashTable* table, const std::vector<RowVectorPtr>& expected) {
    std::vector<char*> rows(table->rows()->numRows());
    exec::RowContainerIterator iterator;
    if (!rows.empty()) {
      ASSERT_EQ(table->rows()->listRows(&iterator, rows.size(), rows.data()), rows.size());
    }
    ASSERT_TRUE(exec::test::assertEqualResults(expected, {extractRows(table, rows)}));
  }

  void assertProbe(exec::BaseHashTable* table, const RowVectorPtr& probe, const std::vector<RowVectorPtr>& expected) {
    exec::HashLookup lookup(table->hashers(), pool_.get());
    SelectivityVector activeRows(probe->size());
    table->prepareForJoinProbe(lookup, probe, activeRows, true);
    if (!lookup.rows.empty()) {
      table->joinProbe(lookup);
    }
    exec::BaseHashTable::JoinResultIterator iterator({}, 0, std::nullopt);
    iterator.reset(lookup);
    std::array<vector_size_t, 2> inputRows;
    std::array<char*, 2> outputRows;
    std::vector<char*> matches;
    while (!iterator.atEnd()) {
      auto count = table->listJoinResults(
          iterator,
          false,
          folly::Range(inputRows.data(), inputRows.size()),
          folly::Range(outputRows.data(), outputRows.size()),
          std::numeric_limits<uint64_t>::max());
      matches.insert(matches.end(), outputRows.begin(), outputRows.begin() + count);
    }
    ASSERT_TRUE(exec::test::assertEqualResults(expected, {extractRows(table, matches)}));
  }
};

TEST_F(HashTableBuilderTest, joinTypesAndNullKeys) {
  struct JoinCase {
    int joinType;
    bool existenceJoin;
    bool keepNullKeys;
    bool dropDuplicates;
  };
  const std::vector<JoinCase> cases = {
      {substrait::JoinRel_JoinType_JOIN_TYPE_INNER, false, false, false},
      {substrait::JoinRel_JoinType_JOIN_TYPE_LEFT, false, false, false},
      {substrait::JoinRel_JoinType_JOIN_TYPE_RIGHT, false, true, false},
      {substrait::JoinRel_JoinType_JOIN_TYPE_OUTER, false, true, false},
      {substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_SEMI, false, false, true},
      {substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_SEMI, true, false, true},
      {substrait::JoinRel_JoinType_JOIN_TYPE_RIGHT_SEMI, false, false, false},
      {substrait::JoinRel_JoinType_JOIN_TYPE_RIGHT_SEMI, true, true, false},
      {substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_ANTI, false, false, true}};
  auto first = makeRowVector(
      {"key", "payload"},
      {makeNullableFlatVector<int64_t>({1, 2, std::nullopt}),
       makeFlatVector<StringView>({"first", "second", "null_key"})});
  auto second = makeRowVector(
      {"key", "payload"}, {makeFlatVector<int64_t>({1, 3}), makeFlatVector<StringView>({"duplicate", "third"})});
  auto nonNullRows = makeRowVector(
      {makeFlatVector<int64_t>({1, 2, 1, 3}), makeFlatVector<StringView>({"first", "second", "duplicate", "third"})});
  auto distinctKeys = makeRowVector({makeFlatVector<int64_t>({1, 2, 3})});
  auto probe = makeRowVector({makeNullableFlatVector<int64_t>({3, 1, 99, std::nullopt, 2})});

  for (const auto& testCase : cases) {
    for (bool withFilter : {false, true}) {
      SCOPED_TRACE(fmt::format(
          "joinType={}, existenceJoin={}, withFilter={}", testCase.joinType, testCase.existenceJoin, withFilter));
      BuildOptions options;
      options.joinType = testCase.joinType;
      options.existenceJoin = testCase.existenceJoin;
      options.withFilter = withFilter;
      options.abandonDedupMinPct = 100;
      auto builder = build({first, second}, options);
      const bool dropDuplicates = testCase.dropDuplicates && !withFilter;
      EXPECT_EQ(builder->dropDuplicates(), dropDuplicates);
      EXPECT_FALSE(builder->noMoreInput());
      auto table = finishBuild(builder);
      if (dropDuplicates) {
        assertTableRows(table.get(), {distinctKeys});
        assertProbe(table.get(), probe, {distinctKeys});
      } else {
        assertTableRows(
            table.get(),
            testCase.keepNullKeys ? std::vector<RowVectorPtr>{first, second} : std::vector<RowVectorPtr>{nonNullRows});
        assertProbe(table.get(), probe, {nonNullRows});
      }
    }
  }
}

TEST_F(HashTableBuilderTest, deduplicationAndAdaptiveAbandonment) {
  struct DedupCase {
    uint32_t minRows;
    uint32_t minPct;
    size_t storedRows;
  };
  const std::vector<DedupCase> cases = {{100000, 0, 8}, {100000, 100, 4}, {1, 50, 8}};
  auto input = makeRowVector(
      {"key", "payload"},
      {makeFlatVector<int64_t>({1, 2, 3, 4}), makeFlatVector<StringView>({"one", "two", "three", "four"})});
  auto expected = makeRowVector({makeFlatVector<int64_t>({1, 2, 3, 4})});
  for (auto joinType :
       {substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_SEMI, substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_ANTI}) {
    for (const auto& testCase : cases) {
      SCOPED_TRACE(fmt::format(
          "joinType={}, minRows={}, minPct={}", static_cast<int>(joinType), testCase.minRows, testCase.minPct));
      BuildOptions options;
      options.joinType = joinType;
      options.abandonDedupMinRows = testCase.minRows;
      options.abandonDedupMinPct = testCase.minPct;
      auto builder = build({input, input}, options);
      EXPECT_TRUE(builder->dropDuplicates());
      auto table = builder->uniqueTable();
      EXPECT_EQ(table->rows()->numRows(), testCase.storedRows);
      table->prepareJoinTable(
          {},
          exec::BaseHashTable::kNoSpillInputStartPartitionBit,
          builder->joinBuildVectorHasherMaxNumDistinct(),
          builder->dropDuplicates(),
          nullptr);
      assertProbe(table.get(), makeRowVector({makeFlatVector<int64_t>({4, 3, 2, 1, 99})}), {expected});
    }
  }
}

TEST_F(HashTableBuilderTest, antiJoinFilterNullPropagation) {
  auto input = makeRowVector(
      {"key", "payload"},
      {makeNullableFlatVector<int64_t>({1, std::nullopt, 2, 3}),
       makeNullableFlatVector<StringView>({"one", "null_key", std::nullopt, "three"})});
  auto keyFiltered = makeRowVector(
      {makeFlatVector<int64_t>({1, 2, 3}), makeNullableFlatVector<StringView>({"one", std::nullopt, "three"})});
  auto payloadFiltered = makeRowVector(
      {makeNullableFlatVector<int64_t>({1, std::nullopt, 3}),
       makeFlatVector<StringView>({"one", "null_key", "three"})});
  auto bothFiltered = makeRowVector({makeFlatVector<int64_t>({1, 3}), makeFlatVector<StringView>({"one", "three"})});
  const std::vector<std::pair<std::vector<std::string>, RowVectorPtr>> cases = {
      {{"key"}, keyFiltered}, {{"payload"}, payloadFiltered}, {{"payload", "key", "payload"}, bothFiltered}};
  for (const auto& [filterColumns, expected] : cases) {
    for (bool propagatesNulls : {false, true}) {
      SCOPED_TRACE(fmt::format("filterColumns={}, propagatesNulls={}", fmt::join(filterColumns, ","), propagatesNulls));
      BuildOptions options;
      options.joinType = substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_ANTI;
      options.nullAware = true;
      options.withFilter = true;
      options.filterColumns = filterColumns;
      options.filterPropagatesNulls = propagatesNulls;
      auto builder = build({input}, options);
      EXPECT_FALSE(builder->dropDuplicates());
      EXPECT_FALSE(builder->noMoreInput());
      EXPECT_TRUE(builder->joinHasNullKeys());
      auto table = finishBuild(builder);
      assertTableRows(table.get(), {propagatesNulls ? expected : input});
    }
  }
}

TEST_F(HashTableBuilderTest, compositeAndRepeatedKeyChannels) {
  auto input = makeRowVector(
      {"payload", "key", "region"},
      {makeFlatVector<StringView>({"first", "second", "third"}),
       makeFlatVector<int64_t>({1, 2, 1}),
       makeFlatVector<StringView>({"west_region_long_name", "east_region_long_name", "east_region_long_name"})});
  BuildOptions options;
  options.joinKeys = {"region", "key"};
  auto builder = build({input}, options);
  auto table = finishBuild(builder);
  auto compositeExpected = makeRowVector({input->childAt(2), input->childAt(1), input->childAt(0)});
  assertTableRows(table.get(), {compositeExpected});
  assertProbe(table.get(), input, {compositeExpected});

  options.joinKeys = {"key", "key", "key", "key"};
  builder = build({input}, options);
  table = finishBuild(builder);
  auto repeatedExpected = makeRowVector(
      {input->childAt(1),
       input->childAt(1),
       input->childAt(1),
       input->childAt(1),
       input->childAt(0),
       input->childAt(2)});
  assertTableRows(table.get(), {repeatedExpected});
  auto probe = makeRowVector(
      {makeFlatVector<StringView>({"unused", "unused", "unused"}),
       makeFlatVector<int64_t>({2, 1, 99}),
       makeFlatVector<StringView>({"unused", "unused", "unused"})});
  assertProbe(table.get(), probe, {repeatedExpected});
}

TEST_F(HashTableBuilderTest, antiJoinFilterCanRemoveAllRows) {
  BuildOptions options;
  options.joinType = substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_ANTI;
  options.nullAware = true;
  options.withFilter = true;
  options.filterColumns = {"payload"};
  options.filterPropagatesNulls = true;
  auto input = makeRowVector(
      {"key", "payload"},
      {makeFlatVector<int64_t>({1, 2}), makeNullableFlatVector<StringView>({std::nullopt, std::nullopt})});
  auto builder = build({input}, options);
  EXPECT_FALSE(builder->noMoreInput());
  EXPECT_FALSE(builder->joinHasNullKeys());
  auto table = finishBuild(builder);
  EXPECT_EQ(table->rows()->numRows(), 0);
  EXPECT_EQ(table->numDistinct(), 0);
}

TEST_F(HashTableBuilderTest, encodedStringKeysAndComplexPayloads) {
  std::shared_ptr<HashTableBuilder> builder;
  {
    auto input = makeRowVector(
        {"key", "payload", "items", "details"},
        {makeFlatVector<StringView>({"long_string_join_key_one", "long_string_join_key_two"}),
         makeNullableFlatVector<StringView>({"long_string_payload_one", std::nullopt}),
         makeArrayVector<int64_t>({{10, 11}, {20}}),
         makeRowVector({makeFlatVector<int64_t>({1, 2}), makeFlatVector<StringView>({"detail_one", "detail_two"})})});
    std::vector<VectorPtr> dictionaryColumns;
    std::vector<VectorPtr> constantColumns;
    for (const auto& child : input->children()) {
      dictionaryColumns.push_back(wrapInDictionary(makeIndices({1, 0}), 2, child));
      constantColumns.push_back(BaseVector::wrapInConstant(2, 1, child));
    }
    auto dictionaryInput = makeRowVector({"key", "payload", "items", "details"}, dictionaryColumns);
    auto constantInput = makeRowVector({"key", "payload", "items", "details"}, constantColumns);
    builder = build({dictionaryInput, constantInput}, BuildOptions{});
  }
  auto expected = makeRowVector(
      {makeFlatVector<StringView>(
           {"long_string_join_key_two",
            "long_string_join_key_one",
            "long_string_join_key_two",
            "long_string_join_key_two"}),
       makeNullableFlatVector<StringView>({std::nullopt, "long_string_payload_one", std::nullopt, std::nullopt}),
       makeArrayVector<int64_t>({{20}, {10, 11}, {20}, {20}}),
       makeRowVector(
           {makeFlatVector<int64_t>({2, 1, 2, 2}),
            makeFlatVector<StringView>({"detail_two", "detail_one", "detail_two", "detail_two"})})});
  auto table = finishBuild(builder);
  assertTableRows(table.get(), {expected});
  assertProbe(
      table.get(),
      makeRowVector(
          {makeFlatVector<StringView>({"long_string_join_key_one", "missing_key", "long_string_join_key_two"})}),
      {expected});
}

TEST_F(HashTableBuilderTest, emptyAndAllNullBatchesDoNotEndInput) {
  auto empty = makeRowVector(
      {"key", "payload"},
      {makeFlatVector<int64_t>(std::vector<int64_t>{}), makeFlatVector<StringView>(std::vector<StringView>{})});
  auto first = makeRowVector({"key", "payload"}, {makeFlatVector<int64_t>({1}), makeFlatVector<StringView>({"first"})});
  auto nullKeys = makeRowVector(
      {"key", "payload"}, {makeNullableFlatVector<int64_t>({std::nullopt}), makeFlatVector<StringView>({"null_key"})});
  auto last = makeRowVector({"key", "payload"}, {makeFlatVector<int64_t>({2}), makeFlatVector<StringView>({"last"})});
  auto builder = build({empty, first, empty, nullKeys, last, empty}, BuildOptions{});
  EXPECT_FALSE(builder->noMoreInput());
  auto table = finishBuild(builder);
  assertTableRows(table.get(), {first, last});
  assertProbe(table.get(), makeRowVector({makeFlatVector<int64_t>({2, 1, 99})}), {first, last});

  builder = build({empty, empty}, BuildOptions{});
  table = finishBuild(builder);
  assertTableRows(table.get(), {empty});
}

TEST_F(HashTableBuilderTest, nullAwareAntiJoinWithoutNullKeys) {
  BuildOptions options;
  options.joinType = substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_ANTI;
  options.nullAware = true;
  auto input = makeRowVector(
      {"key", "payload"},
      {makeFlatVector<int64_t>({1, 2, 1}), makeFlatVector<StringView>({"first", "second", "duplicate"})});
  auto builder = build({input, input}, options);
  EXPECT_FALSE(builder->noMoreInput());
  EXPECT_FALSE(builder->joinHasNullKeys());
  auto table = finishBuild(builder);
  assertProbe(
      table.get(),
      makeRowVector({makeNullableFlatVector<int64_t>({2, std::nullopt, 99, 1})}),
      {makeRowVector({makeFlatVector<int64_t>({1, 2})})});
}

TEST_F(HashTableBuilderTest, rejectsUnsupportedJoinTypeAndMissingKey) {
  auto input =
      makeRowVector({"key", "payload"}, {makeFlatVector<int64_t>({1}), makeFlatVector<StringView>({"payload"})});
  BuildOptions options;
  options.joinType = substrait::JoinRel_JoinType_JOIN_TYPE_UNSPECIFIED;
  VELOX_ASSERT_THROW(build({input}, options), "Unsupported Join type");
  options.joinType = substrait::JoinRel_JoinType_JOIN_TYPE_INNER;
  options.joinKeys = {"missing_key"};
  EXPECT_THROW(build({input}, options), VeloxException);
}

TEST_F(HashTableBuilderTest, releasesEachBatchBeforeReadingTheNext) {
  auto* arrowPool = arrow::default_memory_pool();
  std::vector<std::vector<uint8_t>> serializedBatches;
  std::unique_ptr<VeloxColumnarBatchSerializer> deserializer;
  for (int batchId = 1; batchId <= 3; ++batchId) {
    auto vector = makeRowVector(
        {makeFlatVector<int64_t>({batchId}),
         makeFlatVector<std::string>({"long_string_payload_" + std::to_string(batchId)})});
    VeloxColumnarBatchSerializer serializer(arrowPool, pool_, nullptr);
    serializer.append(std::make_shared<VeloxColumnarBatch>(vector));
    auto& bytes = serializedBatches.emplace_back(serializer.maxSerializedSize());
    serializer.serializeTo(bytes.data(), bytes.size());
    if (deserializer == nullptr) {
      ArrowSchema schema;
      exportToArrow(vector, schema, ArrowUtils::getBridgeOptions());
      deserializer = std::make_unique<VeloxColumnarBatchSerializer>(arrowPool, pool_, &schema);
    }
  }

  std::weak_ptr<ColumnarBatch> previousBatch;
  std::weak_ptr<RowVector> previousVector;
  int batchCount = 0;
  auto builder = build([&]() -> std::shared_ptr<ColumnarBatch> {
    EXPECT_TRUE(previousBatch.expired());
    EXPECT_TRUE(previousVector.expired());
    if (batchCount == 3) {
      return nullptr;
    }
    auto& bytes = serializedBatches[batchCount++];
    auto batch = deserializer->deserialize(bytes.data(), bytes.size());
    previousBatch = batch;
    previousVector = std::dynamic_pointer_cast<VeloxColumnarBatch>(batch)->getRowVector();
    return batch;
  });

  EXPECT_EQ(batchCount, 3);
  EXPECT_TRUE(previousBatch.expired());
  EXPECT_TRUE(previousVector.expired());
  auto table = finishBuild(builder);
  auto expected = makeRowVector(
      {makeFlatVector<int64_t>({1, 2, 3}),
       makeFlatVector<StringView>({"long_string_payload_1", "long_string_payload_2", "long_string_payload_3"})});
  assertTableRows(table.get(), {expected});
  assertProbe(table.get(), makeRowVector({makeFlatVector<int64_t>({3, 1, 99, 2})}), {expected});
}

TEST_F(HashTableBuilderTest, materializedBatchesSurviveHandleRelease) {
  auto store = ObjectStore::create();
  std::vector<std::shared_ptr<ColumnarBatch>> batches;
  std::vector<std::weak_ptr<ColumnarBatch>> weakBatches;
  for (int batchId = 1; batchId <= 3; ++batchId) {
    auto vector = makeRowVector(
        {makeFlatVector<int64_t>({batchId}),
         makeFlatVector<std::string>({"long_string_payload_" + std::to_string(batchId)})});
    std::shared_ptr<ColumnarBatch> batch = std::make_shared<VeloxColumnarBatch>(vector);
    auto handle = store->save(batch);
    weakBatches.push_back(batch);
    batches.push_back(ObjectStore::retrieve<ColumnarBatch>(handle));
    batch.reset();
    vector.reset();
    ObjectStore::release(handle);
    EXPECT_FALSE(weakBatches.back().expired());
  }

  size_t batchIndex = 0;
  auto builder = build([&]() -> std::shared_ptr<ColumnarBatch> {
    return batchIndex < batches.size() ? batches[batchIndex++] : nullptr;
  });
  batches.clear();
  for (const auto& batch : weakBatches) {
    EXPECT_TRUE(batch.expired());
  }
  auto table = finishBuild(builder);
  auto expected = makeRowVector(
      {makeFlatVector<int64_t>({1, 2, 3}),
       makeFlatVector<StringView>({"long_string_payload_1", "long_string_payload_2", "long_string_payload_3"})});
  assertTableRows(table.get(), {expected});
  assertProbe(table.get(), makeRowVector({makeFlatVector<int64_t>({2, 99, 3, 1})}), {expected});
}

TEST_F(HashTableBuilderTest, emptyInput) {
  for (bool nullAware : {false, true}) {
    SCOPED_TRACE(nullAware);
    int requests = 0;
    auto builder = build(
        [&]() -> std::shared_ptr<ColumnarBatch> {
          ++requests;
          return nullptr;
        },
        nullAware ? substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_ANTI : substrait::JoinRel_JoinType_JOIN_TYPE_INNER,
        nullAware);
    EXPECT_EQ(requests, 1);
    EXPECT_FALSE(builder->noMoreInput());
    EXPECT_FALSE(builder->joinHasNullKeys());
    auto table = finishBuild(builder);
    EXPECT_EQ(table->rows()->numRows(), 0);
    EXPECT_EQ(table->numDistinct(), 0);
  }
}

TEST_F(HashTableBuilderTest, stopsReadingAfterNullAwareAntiJoinNullKey) {
  int batchCount = 0;
  std::weak_ptr<ColumnarBatch> previousBatch;
  auto builder = build(
      [&]() -> std::shared_ptr<ColumnarBatch> {
        if (++batchCount > 1) {
          ADD_FAILURE() << "Null-aware anti join must not request another batch";
          return nullptr;
        }
        auto vector =
            makeRowVector({makeNullableFlatVector<int64_t>({std::nullopt}), makeFlatVector<StringView>({"payload"})});
        auto batch = std::make_shared<VeloxColumnarBatch>(vector);
        previousBatch = batch;
        return batch;
      },
      substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_ANTI,
      true);
  EXPECT_EQ(batchCount, 1);
  EXPECT_TRUE(builder->noMoreInput());
  EXPECT_TRUE(builder->joinHasNullKeys());
  EXPECT_TRUE(previousBatch.expired());
}

TEST_F(HashTableBuilderTest, releasesInputWhenReadingFails) {
  std::weak_ptr<ColumnarBatch> previousBatch;
  std::weak_ptr<RowVector> previousVector;
  int batchCount = 0;
  auto initialBytes = pool_->usedBytes();
  EXPECT_THROW(
      build([&]() -> std::shared_ptr<ColumnarBatch> {
        if (++batchCount > 1) {
          throw std::runtime_error("Input deserialization failed");
        }
        auto vector = makeRowVector(
            {makeFlatVector<int64_t>({1}), makeFlatVector<StringView>({"long_string_payload_for_failed_build"})});
        auto batch = std::make_shared<VeloxColumnarBatch>(vector);
        previousBatch = batch;
        previousVector = vector;
        return batch;
      }),
      std::runtime_error);
  EXPECT_EQ(batchCount, 2);
  EXPECT_TRUE(previousBatch.expired());
  EXPECT_TRUE(previousVector.expired());
  EXPECT_EQ(pool_->usedBytes(), initialBytes);
}

TEST_F(HashTableBuilderTest, stopsReadingAfterNullKeyInLaterBatch) {
  int requests = 0;
  std::weak_ptr<ColumnarBatch> previousBatch;
  std::weak_ptr<RowVector> previousVector;
  auto builder = build(
      [&]() -> std::shared_ptr<ColumnarBatch> {
        EXPECT_TRUE(previousBatch.expired());
        EXPECT_TRUE(previousVector.expired());
        if (++requests > 2) {
          ADD_FAILURE() << "Null-aware anti join must stop at the batch containing a null key";
          return nullptr;
        }
        auto keys = requests == 1 ? makeNullableFlatVector<int64_t>({1, 2})
                                  : makeNullableFlatVector<int64_t>({3, std::nullopt});
        auto vector = makeRowVector({keys, makeFlatVector<StringView>({"first", "second"})});
        auto batch = std::make_shared<VeloxColumnarBatch>(vector);
        previousBatch = batch;
        previousVector = vector;
        return batch;
      },
      substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_ANTI,
      true);
  EXPECT_EQ(requests, 2);
  EXPECT_TRUE(builder->noMoreInput());
  EXPECT_TRUE(builder->joinHasNullKeys());
  EXPECT_TRUE(previousBatch.expired());
  EXPECT_TRUE(previousVector.expired());
  auto table = finishBuild(builder);
  assertTableRows(table.get(), {makeRowVector({makeFlatVector<int64_t>({1, 2})})});
}

TEST_F(HashTableBuilderTest, nullAwareExistenceJoinDoesNotStopAtNullKey) {
  BuildOptions options;
  options.joinType = substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_SEMI;
  options.existenceJoin = true;
  options.nullAware = true;
  auto first = makeRowVector(
      {"key", "payload"},
      {makeNullableFlatVector<int64_t>({1, std::nullopt}), makeFlatVector<StringView>({"first", "null_key"})});
  auto second =
      makeRowVector({"key", "payload"}, {makeFlatVector<int64_t>({2}), makeFlatVector<StringView>({"second"})});
  auto builder = build({first, second}, options);
  EXPECT_FALSE(builder->noMoreInput());
  EXPECT_TRUE(builder->joinHasNullKeys());
  auto table = finishBuild(builder);
  assertProbe(
      table.get(),
      makeRowVector({makeFlatVector<int64_t>({1, 2, 99})}),
      {makeRowVector({makeFlatVector<int64_t>({1, 2})})});
}

TEST_F(HashTableBuilderTest, releasesInputWhenDeserializationFails) {
  auto* arrowPool = arrow::default_memory_pool();
  auto input = makeRowVector(
      {makeFlatVector<int64_t>({1}),
       makeFlatVector<StringView>({"long_string_payload_before_deserialization_failure"})});
  VeloxColumnarBatchSerializer serializer(arrowPool, pool_, nullptr);
  serializer.append(std::make_shared<VeloxColumnarBatch>(input));
  std::vector<uint8_t> bytes(serializer.maxSerializedSize());
  serializer.serializeTo(bytes.data(), bytes.size());
  ArrowSchema schema;
  exportToArrow(input, schema, ArrowUtils::getBridgeOptions());
  VeloxColumnarBatchSerializer deserializer(arrowPool, pool_, &schema);
  std::weak_ptr<ColumnarBatch> previousBatch;
  std::weak_ptr<RowVector> previousVector;
  int requests = 0;
  const auto initialBytes = pool_->usedBytes();
  EXPECT_THROW(
      build([&]() -> std::shared_ptr<ColumnarBatch> {
        auto batch = deserializer.deserialize(bytes.data(), ++requests == 1 ? bytes.size() : 1);
        previousBatch = batch;
        previousVector = std::dynamic_pointer_cast<VeloxColumnarBatch>(batch)->getRowVector();
        return batch;
      }),
      VeloxException);
  EXPECT_EQ(requests, 2);
  EXPECT_TRUE(previousBatch.expired());
  EXPECT_TRUE(previousVector.expired());
  EXPECT_EQ(pool_->usedBytes(), initialBytes);
}

} // namespace gluten

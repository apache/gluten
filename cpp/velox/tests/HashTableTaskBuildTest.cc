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

#include <folly/executors/CPUThreadPoolExecutor.h>

#include "jni/JniHashTable.h"
#include "memory/VeloxColumnarBatch.h"
#include "substrait/algebra.pb.h"
#include "velox/exec/HashTableCache.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/functions/sparksql/registration/Register.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace gluten {
namespace {

struct JoinCase {
  std::string name;
  int substraitJoinType;
  bool isExistenceJoin{false};
  bool nullAware{false};
  bool withFilter{false};
  bool nullBuildKeys{false};
  uint32_t numThreads{1};
};

std::string caseName(const testing::TestParamInfo<JoinCase>& info) {
  return info.param.name;
}

constexpr auto kInner = ::substrait::JoinRel_JoinType_JOIN_TYPE_INNER;
constexpr auto kLeft = ::substrait::JoinRel_JoinType_JOIN_TYPE_LEFT;
constexpr auto kLeftSemi = ::substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_SEMI;
constexpr auto kLeftAnti = ::substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_ANTI;

class HashTableTaskBuildTest : public testing::TestWithParam<JoinCase>, public facebook::velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    functions::sparksql::registerFunctions("");
  }

  void SetUp() override {
    queryPool_ = memory::memoryManager()->addRootPool("HashTableTaskBuildTest");
    executor_ = std::make_unique<folly::CPUThreadPoolExecutor>(4);
  }

  void TearDown() override {
    executor_.reset();
    queryPool_.reset();
  }

  // Build side batches: 'numBatches' x 'batchSize' rows with duplicate keys.
  std::vector<RowVectorPtr> makeBuildVectors(bool nullKeys, int numBatches = 8, int batchSize = 500) {
    std::vector<RowVectorPtr> vectors;
    for (int batch = 0; batch < numBatches; ++batch) {
      vectors.push_back(makeRowVector(
          {"u_k", "u_v"},
          {makeFlatVector<int32_t>(
               batchSize,
               [batch](auto row) { return (row * 7 + batch) % 997; },
               [nullKeys, batch](auto row) { return nullKeys && batch == 3 && row == 17; }),
           makeFlatVector<int64_t>(
               batchSize, [](auto row) { return row % 101; }, [](auto row) { return row % 37 == 0; })}));
    }
    return vectors;
  }

  std::vector<RowVectorPtr> makeProbeVectors() {
    std::vector<RowVectorPtr> vectors;
    for (int batch = 0; batch < 4; ++batch) {
      vectors.push_back(makeRowVector(
          {"t_k", "t_v"},
          {makeFlatVector<int32_t>(
               1'000, [batch](auto row) { return (row + batch * 1'000) % 1'500; }, [](auto row) { return row == 5; }),
           makeFlatVector<int64_t>(1'000, [](auto row) { return row % 103; })}));
    }
    return vectors;
  }

  static std::vector<std::shared_ptr<ColumnarBatch>> toBatches(const std::vector<RowVectorPtr>& vectors) {
    std::vector<std::shared_ptr<ColumnarBatch>> batches;
    for (const auto& vector : vectors) {
      batches.push_back(std::make_shared<VeloxColumnarBatch>(vector));
    }
    return batches;
  }

  // t_v > u_v. For an anti join it drops build rows with a null u_v.
  static core::TypedExprPtr makeFilter() {
    return std::make_shared<core::CallTypedExpr>(
        BOOLEAN(),
        std::vector<core::TypedExprPtr>{
            std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "t_v"),
            std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "u_v")},
        "greaterthan");
  }

  static core::JoinType veloxJoinType(const JoinCase& c) {
    switch (c.substraitJoinType) {
      case kInner:
        return core::JoinType::kInner;
      case kLeft:
        return core::JoinType::kLeft;
      case kLeftAnti:
        return core::JoinType::kAnti;
      default:
        return c.isExistenceJoin ? core::JoinType::kLeftSemiProject : core::JoinType::kLeftSemiFilter;
    }
  }

  std::shared_ptr<BroadcastHashTable>
  build(const JoinCase& c, const std::string& key, const std::vector<RowVectorPtr>& vectors) {
    auto batches = toBatches(vectors);
    return nativeHashTableBuildWithTask(
        key,
        {"u_k"},
        c.withFilter ? makeFilter() : nullptr,
        {"u_k", "u_v"},
        {INTEGER(), BIGINT()},
        c.substraitJoinType,
        c.isExistenceJoin,
        c.nullAware,
        -1,
        1,
        1'000'000,
        100'000,
        0,
        batches,
        c.numThreads,
        executor_.get(),
        queryPool_,
        pool_);
  }

  // Joins the probe vectors with 'buildVectors'. With 'cacheKey', the probe
  // uses the table cached under that key and 'buildVectors' must be empty.
  RowVectorPtr join(
      const JoinCase& c,
      const std::vector<RowVectorPtr>& buildVectors,
      const std::optional<std::string>& cacheKey = std::nullopt) {
    const auto joinType = veloxJoinType(c);
    auto probeNode = std::make_shared<core::ValuesNode>("0", makeProbeVectors());
    auto buildNode = std::make_shared<core::ValuesNode>(
        "1",
        buildVectors.empty()
            ? std::vector<RowVectorPtr>{makeRowVector(
                  {"u_k", "u_v"},
                  {makeFlatVector<int32_t>(std::vector<int32_t>{}), makeFlatVector<int64_t>(std::vector<int64_t>{})})}
            : buildVectors);
    RowTypePtr outputType;
    if (joinType == core::JoinType::kInner || joinType == core::JoinType::kLeft) {
      outputType = ROW({"t_k", "t_v", "u_k", "u_v"}, {INTEGER(), BIGINT(), INTEGER(), BIGINT()});
    } else if (joinType == core::JoinType::kLeftSemiProject) {
      outputType = ROW({"t_k", "t_v", "match"}, {INTEGER(), BIGINT(), BOOLEAN()});
    } else {
      outputType = ROW({"t_k", "t_v"}, {INTEGER(), BIGINT()});
    }
    auto joinNode = core::HashJoinNode::Builder()
                        .id("2")
                        .joinType(joinType)
                        .leftKeys({std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "t_k")})
                        .rightKeys({std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "u_k")})
                        .filter(c.withFilter ? makeFilter() : nullptr)
                        .left(probeNode)
                        .right(buildNode)
                        .outputType(outputType)
                        .nullAware(c.nullAware)
                        .useHashTableCache(cacheKey.has_value())
                        .cacheKey(cacheKey)
                        .build();
    return exec::test::AssertQueryBuilder(joinNode).copyResults(pool());
  }

  std::shared_ptr<memory::MemoryPool> queryPool_;
  std::unique_ptr<folly::CPUThreadPoolExecutor> executor_;
};

// Probing the task-built table gives the same result as a Velox hash join.
TEST_P(HashTableTaskBuildTest, sameResultAsHashJoin) {
  const auto& c = GetParam();
  auto* cache = HashTableCache::instance();
  const auto key = "task_" + c.name;
  auto vectors = makeBuildVectors(c.nullBuildKeys);

  auto hashTable = build(c, key, vectors);
  ASSERT_NE(hashTable, nullptr);
  ASSERT_TRUE(cache->exist(key));
  EXPECT_GT(hashTable->memoryUsage, 0);
  if (c.numThreads > 1 && !hashTable->joinHasNullKeys) {
    // HashBuild built one table per driver and merged them.
    EXPECT_EQ(hashTable->table->allRows().size(), c.numThreads);
  }

  exec::test::assertEqualResults({join(c, vectors)}, {join(c, {}, key)});

  // clearHashTable() drops the cache entry first and then releases the table.
  // The table must keep its memory pool alive in between.
  cache->drop(key);
  auto table = hashTable->table;
  hashTable.reset();
  EXPECT_NE(table->rows(), nullptr);
  table.reset();
}

// Releasing the last reference to the table drops the cache entry.
TEST_P(HashTableTaskBuildTest, releasingTableDropsCacheEntry) {
  const auto& c = GetParam();
  auto* cache = HashTableCache::instance();
  const auto key = "release_" + c.name;
  auto hashTable = build(c, key, makeBuildVectors(c.nullBuildKeys));
  ASSERT_TRUE(cache->exist(key));
  hashTable.reset();
  EXPECT_FALSE(cache->exist(key));
}

INSTANTIATE_TEST_SUITE_P(
    HashTableTaskBuildTest,
    HashTableTaskBuildTest,
    testing::Values(
        JoinCase{"inner", kInner},
        JoinCase{"innerParallel", kInner, false, false, false, false, 4},
        JoinCase{"innerFilter", kInner, false, false, true},
        JoinCase{"leftParallel", kLeft, false, false, false, true, 4},
        JoinCase{"leftFilter", kLeft, false, false, true, true},
        JoinCase{"leftSemi", kLeftSemi},
        JoinCase{"leftSemiParallel", kLeftSemi, false, false, false, true, 4},
        JoinCase{"leftSemiFilterParallel", kLeftSemi, false, false, true, false, 4},
        JoinCase{"existence", kLeftSemi, true},
        JoinCase{"existenceFilter", kLeftSemi, true, false, true, true},
        JoinCase{"anti", kLeftAnti, false, false, false, true},
        JoinCase{"antiParallel", kLeftAnti, false, false, false, true, 4},
        JoinCase{"antiFilter", kLeftAnti, false, false, true, true},
        JoinCase{"antiFilterParallel", kLeftAnti, false, false, true, true, 4},
        JoinCase{"nullAwareAnti", kLeftAnti, false, true},
        JoinCase{"nullAwareAntiParallel", kLeftAnti, false, true, false, false, 4},
        JoinCase{"nullAwareAntiNullKey", kLeftAnti, false, true, false, true},
        JoinCase{"nullAwareAntiNullKeyParallel", kLeftAnti, false, true, false, true, 4},
        JoinCase{"nullAwareAntiFilterNullKey", kLeftAnti, false, true, true, true}),
    caseName);

} // namespace
} // namespace gluten

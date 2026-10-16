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

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "jni/JniHashTable.h"
#include "operators/hashjoin/HashTableSerializer.h"
#include "operators/hashjoin/HashTableTaskBuilder.h"
#include "velox/exec/HashTableCache.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/functions/sparksql/registration/Register.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace gluten {
namespace {

enum class Filter {
  kNone,
  // t_v > u_v: null-intolerant, so an anti join drops build rows with a null u_v.
  kNullIntolerant,
  // u_v IS NULL OR t_v > u_v: an anti join keeps build rows with a null u_v.
  kNullTolerant,
};

struct JoinCase {
  std::string name;
  core::JoinType joinType;
  bool nullAware{false};
  Filter filter{Filter::kNone};
  bool nullBuildKeys{false};
  uint32_t numDrivers{1};
};

std::string caseName(const testing::TestParamInfo<JoinCase>& info) {
  return info.param.name;
}

class HashTableTaskBuilderTest : public exec::test::OperatorTestBase, public testing::WithParamInterface<JoinCase> {
 protected:
  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();
    functions::sparksql::registerFunctions("");
  }

  // Build side: 'numBatches' x 'batchSize' rows with duplicate keys.
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

  // The real join filter, see Filter.
  static core::TypedExprPtr makeFilter(Filter filter) {
    if (filter == Filter::kNone) {
      return nullptr;
    }
    auto uv = std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "u_v");
    core::TypedExprPtr greaterThan = std::make_shared<core::CallTypedExpr>(
        BOOLEAN(),
        std::vector<core::TypedExprPtr>{std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "t_v"), uv},
        "greaterthan");
    if (filter == Filter::kNullIntolerant) {
      return greaterThan;
    }
    return std::make_shared<core::CallTypedExpr>(
        BOOLEAN(),
        std::vector<core::TypedExprPtr>{
            std::make_shared<core::CallTypedExpr>(BOOLEAN(), std::vector<core::TypedExprPtr>{uv}, "isnull"),
            greaterThan},
        "or");
  }

  // Builds with the filter properties the Scala side derives from makeFilter().
  std::shared_ptr<BroadcastHashTable>
  build(const JoinCase& c, const std::string& key, const std::vector<RowVectorPtr>& vectors) {
    HashTableTaskBuildSpec spec;
    spec.joinType = c.joinType;
    spec.nullAware = c.nullAware;
    spec.withFilter = c.filter != Filter::kNone;
    if (spec.withFilter) {
      spec.filterBuildColumns = {"u_v"};
    }
    spec.filterPropagatesNulls = c.filter == Filter::kNullIntolerant;
    spec.buildType = ROW({"u_k", "u_v"}, {INTEGER(), BIGINT()});
    spec.joinKeys = {"u_k"};
    // Build in parallel whatever the table size, so that parallel cases merge per-driver tables.
    spec.queryConfigs = {{core::QueryConfig::kMinTableRowsForParallelJoinBuild, "1"}};
    return buildHashTableWithTask(key, spec, vectors, c.numDrivers, driverExecutor_.get(), rootPool_, pool());
  }

  // Joins the probe vectors with 'buildVectors'. With 'cacheKey', the probe
  // uses the table cached under that key and 'buildVectors' must be empty.
  RowVectorPtr join(
      const JoinCase& c,
      const std::vector<RowVectorPtr>& buildVectors,
      const std::optional<std::string>& cacheKey = std::nullopt) {
    auto probeNode = std::make_shared<core::ValuesNode>("0", makeProbeVectors());
    auto buildNode = std::make_shared<core::ValuesNode>(
        "1",
        buildVectors.empty()
            ? std::vector<RowVectorPtr>{makeRowVector(
                  {"u_k", "u_v"},
                  {makeFlatVector<int32_t>(std::vector<int32_t>{}), makeFlatVector<int64_t>(std::vector<int64_t>{})})}
            : buildVectors);
    RowTypePtr outputType;
    switch (c.joinType) {
      case core::JoinType::kInner:
      case core::JoinType::kLeft:
      case core::JoinType::kRight:
      case core::JoinType::kFull:
        outputType = ROW({"t_k", "t_v", "u_k", "u_v"}, {INTEGER(), BIGINT(), INTEGER(), BIGINT()});
        break;
      case core::JoinType::kLeftSemiProject:
        outputType = ROW({"t_k", "t_v", "match"}, {INTEGER(), BIGINT(), BOOLEAN()});
        break;
      case core::JoinType::kRightSemiFilter:
        outputType = ROW({"u_k", "u_v"}, {INTEGER(), BIGINT()});
        break;
      case core::JoinType::kRightSemiProject:
        outputType = ROW({"u_k", "u_v", "match"}, {INTEGER(), BIGINT(), BOOLEAN()});
        break;
      default:
        outputType = ROW({"t_k", "t_v"}, {INTEGER(), BIGINT()});
    }
    auto joinNode = core::HashJoinNode::Builder()
                        .id("2")
                        .joinType(c.joinType)
                        .leftKeys({std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "t_k")})
                        .rightKeys({std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "u_k")})
                        .filter(makeFilter(c.filter))
                        .left(probeNode)
                        .right(buildNode)
                        .outputType(outputType)
                        .nullAware(c.nullAware)
                        .useHashTableCache(cacheKey.has_value())
                        .cacheKey(cacheKey)
                        .build();
    return exec::test::AssertQueryBuilder(joinNode).copyResults(pool());
  }
};

// Probing the table built by the fake join gives the same result as a regular
// Velox hash join over the same build rows.
TEST_P(HashTableTaskBuilderTest, sameResultAsHashJoin) {
  const auto& c = GetParam();
  auto* cache = HashTableCache::instance();
  const auto key = "task_" + c.name;
  auto vectors = makeBuildVectors(c.nullBuildKeys);

  auto hashTable = build(c, key, vectors);
  ASSERT_NE(hashTable, nullptr);
  ASSERT_TRUE(cache->exist(key));
  EXPECT_GT(hashTable->memoryUsage(), 0);
  if (c.numDrivers > 1 && !hashTable->joinHasNullKeys()) {
    // HashBuild built one table per driver and merged them.
    EXPECT_EQ(hashTable->hashTable()->allRows().size(), c.numDrivers);
  }

  exec::test::assertEqualResults({join(c, vectors)}, {join(c, {}, key)});

  // The cache entry, which owns the table's pool, can go first. The table
  // must keep its memory pool alive in between.
  cache->drop(key);
  auto table = hashTable->hashTable();
  hashTable.reset();
  EXPECT_NE(table->rows(), nullptr);
  table.reset();
}

// Releasing the last reference to the table drops the cache entry.
TEST_P(HashTableTaskBuilderTest, releasingTableDropsCacheEntry) {
  const auto& c = GetParam();
  auto* cache = HashTableCache::instance();
  const auto key = "release_" + c.name;
  auto hashTable = build(c, key, makeBuildVectors(c.nullBuildKeys));
  ASSERT_TRUE(cache->exist(key));
  hashTable.reset();
  EXPECT_FALSE(cache->exist(key));
}

// A probe task takes the table from the cache and can outlive the cache entry
// and the returned builder, e.g. when the task fails while the broadcast table
// is released. The table it holds must keep its memory pool alive.
TEST_P(HashTableTaskBuilderTest, cachedTableOutlivesBuilder) {
  const auto& c = GetParam();
  auto* cache = HashTableCache::instance();
  const auto key = "outlive_" + c.name;
  auto hashTable = build(c, key, makeBuildVectors(c.nullBuildKeys));

  ContinueFuture future = ContinueFuture::makeEmpty();
  auto probeQueryCtx = core::QueryCtx::create();
  auto entry = cache->get(key, "probe_task", probeQueryCtx.get(), &future);
  ASSERT_FALSE(future.valid());
  auto probeTable = entry->table;
  std::weak_ptr<memory::MemoryPool> tablePool = entry->tablePool;
  entry.reset();

  hashTable.reset();
  EXPECT_FALSE(cache->exist(key));
  EXPECT_FALSE(tablePool.expired());
  EXPECT_NE(probeTable->rows(), nullptr);
  probeTable.reset();
  EXPECT_TRUE(tablePool.expired());
}

// The driver-side build serializes the table and executors deserialize it.
TEST_P(HashTableTaskBuilderTest, serializeRoundTrip) {
  const auto& c = GetParam();
  auto* cache = HashTableCache::instance();
  const auto key = "serialize_" + c.name;
  auto vectors = makeBuildVectors(c.nullBuildKeys);
  auto hashTable = build(c, key, vectors);

  std::vector<uint8_t> data(serializedHashTableSize(*hashTable));
  serializeHashTableTo(*hashTable, data.data(), data.size());
  // As deserializeHashTable(), with the test's pool.
  std::shared_ptr<BaseHashTable> deserialized;
  if (dynamic_cast<HashTable<true>*>(hashTable->hashTable().get()) != nullptr) {
    deserialized = HashTableSerializer::deserialize<true>(data.data(), data.size(), pool());
  } else {
    deserialized = HashTableSerializer::deserialize<false>(data.data(), data.size(), pool());
  }
  const auto deserializedKey = "deserialized_" + c.name;
  cache->add(deserializedKey, deserialized, hashTable->joinHasNullKeys(), pool_);

  exec::test::assertEqualResults({join(c, vectors)}, {join(c, {}, deserializedKey)});
  cache->drop(deserializedKey);
}

// For tests that do not take a JoinCase parameter.
class HashTableTaskBuilderSingleTest : public HashTableTaskBuilderTest {};

// A build without input rows still caches an empty table.
TEST_F(HashTableTaskBuilderSingleTest, emptyBuildSide) {
  const JoinCase c{"empty", core::JoinType::kInner};
  auto* cache = HashTableCache::instance();
  auto hashTable = build(c, "empty", {});
  ASSERT_TRUE(cache->exist("empty"));
  EXPECT_EQ(hashTable->hashTable()->numDistinct(), 0);
  hashTable.reset();
  EXPECT_FALSE(cache->exist("empty"));
}

INSTANTIATE_TEST_SUITE_P(
    HashTableTaskBuilderTest,
    HashTableTaskBuilderTest,
    testing::Values(
        JoinCase{"inner", core::JoinType::kInner},
        JoinCase{"innerParallel", core::JoinType::kInner, false, Filter::kNone, false, 4},
        JoinCase{"innerFilter", core::JoinType::kInner, false, Filter::kNullIntolerant},
        JoinCase{"leftParallel", core::JoinType::kLeft, false, Filter::kNone, true, 4},
        JoinCase{"leftFilter", core::JoinType::kLeft, false, Filter::kNullIntolerant, true},
        JoinCase{"right", core::JoinType::kRight, false, Filter::kNone, true},
        JoinCase{"rightFilterParallel", core::JoinType::kRight, false, Filter::kNullIntolerant, true, 4},
        JoinCase{"full", core::JoinType::kFull, false, Filter::kNone, true},
        JoinCase{"fullParallel", core::JoinType::kFull, false, Filter::kNone, true, 4},
        JoinCase{"rightSemi", core::JoinType::kRightSemiFilter, false, Filter::kNone, true},
        JoinCase{"rightSemiFilterParallel", core::JoinType::kRightSemiFilter, false, Filter::kNullIntolerant, true, 4},
        JoinCase{"rightSemiProject", core::JoinType::kRightSemiProject, false, Filter::kNone, true},
        JoinCase{"rightSemiProjectParallel", core::JoinType::kRightSemiProject, false, Filter::kNone, true, 4},
        JoinCase{"leftSemi", core::JoinType::kLeftSemiFilter},
        JoinCase{"leftSemiParallel", core::JoinType::kLeftSemiFilter, false, Filter::kNone, true, 4},
        JoinCase{"leftSemiFilterParallel", core::JoinType::kLeftSemiFilter, false, Filter::kNullIntolerant, false, 4},
        JoinCase{"existence", core::JoinType::kLeftSemiProject},
        JoinCase{"existenceFilter", core::JoinType::kLeftSemiProject, false, Filter::kNullIntolerant, true},
        JoinCase{"anti", core::JoinType::kAnti, false, Filter::kNone, true},
        JoinCase{"antiParallel", core::JoinType::kAnti, false, Filter::kNone, true, 4},
        JoinCase{"antiFilter", core::JoinType::kAnti, false, Filter::kNullIntolerant, true},
        JoinCase{"antiFilterParallel", core::JoinType::kAnti, false, Filter::kNullIntolerant, true, 4},
        JoinCase{"antiNullTolerantFilter", core::JoinType::kAnti, false, Filter::kNullTolerant, true},
        JoinCase{"antiNullTolerantFilterParallel", core::JoinType::kAnti, false, Filter::kNullTolerant, true, 4},
        JoinCase{"nullAwareAnti", core::JoinType::kAnti, true},
        JoinCase{"nullAwareAntiParallel", core::JoinType::kAnti, true, Filter::kNone, false, 4},
        JoinCase{"nullAwareAntiNullKey", core::JoinType::kAnti, true, Filter::kNone, true},
        JoinCase{"nullAwareAntiNullKeyParallel", core::JoinType::kAnti, true, Filter::kNone, true, 4},
        JoinCase{"nullAwareAntiFilterNullKey", core::JoinType::kAnti, true, Filter::kNullIntolerant, true}),
    caseName);

} // namespace
} // namespace gluten

// folly::init() registers folly singletons, e.g. the Timekeeper that
// AssertQueryBuilder waits with. Without it Linux builds abort.
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}

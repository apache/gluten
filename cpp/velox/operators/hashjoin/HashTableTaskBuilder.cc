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

#include "operators/hashjoin/HashTableTaskBuilder.h"

#include <atomic>

#include "velox/exec/HashTableCache.h"
#include "velox/exec/RoundRobinPartitionFunction.h"
#include "velox/exec/Task.h"

using namespace facebook::velox;

namespace gluten {
namespace {

// Output type of the fake join. The probe side is empty, so the join outputs
// nothing and needs no columns, except the match column of a semi project join.
RowTypePtr fakeJoinOutputType(core::JoinType joinType) {
  if (core::isLeftSemiProjectJoin(joinType) || core::isRightSemiProjectJoin(joinType)) {
    return ROW({"__fake_match"}, {BOOLEAN()});
  }
  return ROW({}, {});
}

// A stand-in for the join filter with the properties HashBuild looks at, see
// HashTableTaskBuildSpec. It is compiled by HashProbe but never evaluated.
core::TypedExprPtr makePlaceholderFilter(const HashTableTaskBuildSpec& spec) {
  if (!spec.withFilter) {
    return nullptr;
  }
  if (!spec.filterPropagatesNulls || spec.filterBuildColumns.empty()) {
    // References no build column, so HashBuild drops no build rows for it.
    return std::make_shared<core::ConstantTypedExpr>(BOOLEAN(), Variant(true));
  }
  // Chains the build columns, cast to VARCHAR to fit any type, with equalto.
  // Both have default null behavior, so the result is null-intolerant in each
  // of them.
  auto toVarchar = [&](const core::TypedExprPtr& expr) -> core::TypedExprPtr {
    return std::make_shared<core::CastTypedExpr>(VARCHAR(), expr, false);
  };
  auto column = [&](const std::string& name) -> core::TypedExprPtr {
    return toVarchar(std::make_shared<core::FieldAccessTypedExpr>(spec.buildType->findChild(name), name));
  };
  auto equalTo = [](const core::TypedExprPtr& left, const core::TypedExprPtr& right) -> core::TypedExprPtr {
    return std::make_shared<core::CallTypedExpr>(BOOLEAN(), std::vector<core::TypedExprPtr>{left, right}, "equalto");
  };
  auto filter = equalTo(column(spec.filterBuildColumns[0]), column(spec.filterBuildColumns[0]));
  for (auto i = 1; i < spec.filterBuildColumns.size(); ++i) {
    filter = equalTo(toVarchar(filter), column(spec.filterBuildColumns[i]));
  }
  return filter;
}

// An empty join table with the join keys of 'spec'.
std::shared_ptr<exec::BaseHashTable> makeEmptyJoinTable(const HashTableTaskBuildSpec& spec, memory::MemoryPool* pool) {
  std::vector<std::unique_ptr<exec::VectorHasher>> hashers;
  for (auto i = 0; i < spec.joinKeys.size(); ++i) {
    hashers.emplace_back(std::make_unique<exec::VectorHasher>(spec.buildType->findChild(spec.joinKeys[i]), i));
  }
  auto table = exec::HashTable<false>::createForJoin(std::move(hashers), {}, true, false, false, 1'000, pool);
  table->prepareJoinTable({}, exec::BaseHashTable::kNoSpillInputStartPartitionBit, 1'000'000, false, nullptr);
  return table;
}

std::string nextBuildId() {
  static std::atomic<uint64_t> id{0};
  return std::to_string(id++);
}

// A table and the pool it is allocated from. Members are destroyed in reverse
// order, so 'table' returns its memory before 'pool' goes.
struct TableWithPool {
  std::shared_ptr<memory::MemoryPool> pool;
  std::shared_ptr<exec::BaseHashTable> table;
};

} // namespace

std::shared_ptr<BroadcastHashTable> buildHashTableWithTask(
    const std::string& cacheKey,
    const HashTableTaskBuildSpec& spec,
    const std::vector<RowVectorPtr>& buildVectors,
    uint32_t numDrivers,
    folly::Executor* executor,
    std::shared_ptr<memory::MemoryPool> queryPool,
    memory::MemoryPool* vectorPool) {
  VELOX_CHECK_GT(numDrivers, 0);
  VELOX_CHECK(numDrivers == 1 || executor != nullptr, "Parallel hash table build requires an executor");
  const auto buildId = nextBuildId();
  auto queryCtx = core::QueryCtx::create(
      numDrivers > 1 ? executor : nullptr,
      core::QueryConfig(spec.queryConfigs),
      {},
      nullptr,
      std::move(queryPool),
      nullptr,
      fmt::format("Gluten_HashTableBuild_{}_{}", cacheKey, buildId));

  // Probe side: an empty input with one column per join key.
  std::vector<std::string> probeNames;
  std::vector<TypePtr> probeTypes;
  std::vector<core::FieldAccessTypedExprPtr> leftKeys;
  std::vector<core::FieldAccessTypedExprPtr> rightKeys;
  for (auto i = 0; i < spec.joinKeys.size(); ++i) {
    const auto& type = spec.buildType->findChild(spec.joinKeys[i]);
    probeNames.emplace_back(fmt::format("__fake_probe_k{}", i));
    probeTypes.emplace_back(type);
    leftKeys.emplace_back(std::make_shared<core::FieldAccessTypedExpr>(type, probeNames.back()));
    rightKeys.emplace_back(std::make_shared<core::FieldAccessTypedExpr>(type, spec.joinKeys[i]));
  }
  auto probeType = ROW(std::move(probeNames), std::move(probeTypes));

  int planNodeId = 0;
  auto nextPlanNodeId = [&]() { return std::to_string(planNodeId++); };
  core::PlanNodePtr probe = std::make_shared<core::ValuesNode>(
      nextPlanNodeId(), std::vector<RowVectorPtr>{BaseVector::create<RowVector>(probeType, 0, vectorPool)});

  // Build side: the broadcast vectors, renamed to 'buildType'.
  std::vector<RowVectorPtr> values;
  values.reserve(buildVectors.size());
  for (const auto& vector : buildVectors) {
    values.emplace_back(std::make_shared<RowVector>(
        vector->pool(), spec.buildType, vector->nulls(), vector->size(), vector->children()));
  }
  if (values.empty()) {
    values.emplace_back(BaseVector::create<RowVector>(spec.buildType, 0, vectorPool));
  }
  core::PlanNodePtr build = std::make_shared<core::ValuesNode>(nextPlanNodeId(), std::move(values));
  if (numDrivers > 1) {
    // Fan the broadcast vectors out to all HashBuild drivers.
    build = std::make_shared<core::LocalPartitionNode>(
        nextPlanNodeId(),
        core::LocalPartitionNode::Type::kRepartition,
        false,
        std::make_shared<exec::RoundRobinPartitionFunctionSpec>(),
        std::vector<core::PlanNodePtr>{build});
  }

  auto joinNode = core::HashJoinNode::Builder()
                      .id(nextPlanNodeId())
                      .joinType(spec.joinType)
                      .leftKeys(std::move(leftKeys))
                      .rightKeys(std::move(rightKeys))
                      .filter(makePlaceholderFilter(spec))
                      .left(probe)
                      .right(build)
                      .outputType(fakeJoinOutputType(spec.joinType))
                      .nullAware(spec.nullAware)
                      .useHashTableCache(true)
                      .cacheKey(cacheKey)
                      .build();

  std::shared_ptr<exec::Task> task;
  if (numDrivers == 1) {
    task = exec::Task::create(
        queryCtx->queryId(), core::PlanFragment{joinNode}, 0, queryCtx, exec::Task::ExecutionMode::kSerial);
    while (true) {
      ContinueFuture future = ContinueFuture::makeEmpty();
      if (task->next(&future) == nullptr) {
        if (!future.valid()) {
          break;
        }
        future.wait();
      }
    }
  } else {
    task = exec::Task::create(
        queryCtx->queryId(),
        core::PlanFragment{joinNode},
        0,
        queryCtx,
        exec::Task::ExecutionMode::kParallel,
        [](RowVectorPtr, bool, ContinueFuture*) { return exec::BlockingReason::kNotBlocked; });
    task->start(numDrivers);
    task->taskCompletionFuture().wait();
  }
  if (task->state() != exec::TaskState::kFinished) {
    if (auto error = task->error()) {
      std::rethrow_exception(error);
    }
    VELOX_FAIL("Hash table build task {} ended in state {}", task->taskId(), exec::taskStateString(task->state()));
  }
  // Drivers can hold the task, and so the QueryCtx, briefly after it finishes.
  // Wait for the task to go, so that releasing the returned table releases the
  // QueryCtx and with it the cache entry.
  const auto taskId = task->taskId();
  auto taskDeleted = task->taskDeletionFuture();
  task.reset();
  taskDeleted.wait();

  auto* cache = exec::HashTableCache::instance();
  if (!cache->exist(cacheKey)) {
    // HashBuild does not cache the table of a null-aware anti join without
    // filter whose build side has a null key. Such a join returns nothing
    // whatever the table holds, so cache an empty table flagged with null keys.
    VELOX_CHECK(
        core::isAntiJoin(spec.joinType) && spec.nullAware && !spec.withFilter,
        "Hash table build task did not cache table {}",
        cacheKey);
    auto tablePool = queryCtx->pool()->addLeafChild(fmt::format("hash_table_build_empty_{}", buildId));
    cache->add(cacheKey, makeEmptyJoinTable(spec, tablePool.get()), true, tablePool);
    queryCtx->addReleaseCallback([cacheKey]() { exec::HashTableCache::instance()->drop(cacheKey); });
  }
  ContinueFuture future = ContinueFuture::makeEmpty();
  auto entry = cache->get(cacheKey, taskId, queryCtx.get(), &future);
  VELOX_CHECK(!future.valid());
  VELOX_CHECK(entry->buildComplete);
  // When HashBuild abandons dedup, it allows duplicates in a table whose rows
  // have no next-row pointer. The flag no longer matters once the table is
  // built, but HashTable::serializeTo() records it as the row layout, so make
  // it match for the driver-side build. Nothing probes the table yet.
  if (entry->table->rows()->nextOffset() == 0) {
    entry->table->setAllowDuplicates(false);
  }

  // HashBuild allocated the table from 'entry->tablePool', but cached it as a
  // plain pointer that does not keep that pool alive. Probe tasks copy that
  // pointer out of the entry and can outlive both the entry and the returned
  // table, e.g. when a task fails while Java releases the broadcast table.
  // The pool would then go first and the table would free into a dead pool.
  //
  // So replace the cached pointer with one built by the shared_ptr aliasing
  // constructor: it points to the table but shares ownership of a
  // TableWithPool, so every copy keeps both the table and its pool alive. Velox
  // does the same for cached Nimble tablets, see ReaderBase.cpp.
  auto tableWithPool = std::make_shared<TableWithPool>(TableWithPool{entry->tablePool, entry->table});
  entry->table = std::shared_ptr<exec::BaseHashTable>(tableWithPool, tableWithPool->table.get());

  return std::make_shared<BroadcastHashTable>(
      entry->table, entry->hasNullKeys, entry->tablePool->usedBytes(), std::move(queryCtx));
}

} // namespace gluten

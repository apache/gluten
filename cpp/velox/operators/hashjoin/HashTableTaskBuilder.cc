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

#include <algorithm>
#include <atomic>
#include <thread>

#include "velox/exec/HashTableCache.h"
#include "velox/exec/RoundRobinPartitionFunction.h"
#include "velox/exec/Task.h"

using namespace facebook::velox;

namespace gluten {
namespace {

// Output type of the fake join. The probe side is empty, so the output is never
// produced; it only has to be valid for the join type.
RowTypePtr fakeJoinOutputType(core::JoinType joinType, const RowTypePtr& probeType, const RowTypePtr& buildType) {
  const auto& firstBuildName = buildType->nameOf(0);
  const auto& firstBuildType = buildType->childAt(0);
  switch (joinType) {
    case core::JoinType::kLeftSemiProject: {
      auto names = probeType->names();
      auto types = probeType->children();
      names.emplace_back("__fake_match");
      types.emplace_back(BOOLEAN());
      return ROW(std::move(names), std::move(types));
    }
    case core::JoinType::kRightSemiFilter:
      return ROW({firstBuildName}, {firstBuildType});
    case core::JoinType::kRightSemiProject:
      return ROW({firstBuildName, "__fake_match"}, {firstBuildType, BOOLEAN()});
    default:
      return probeType;
  }
}

// A table and the pool it is allocated from. 'table' is released first.
struct TableWithPool {
  std::shared_ptr<memory::MemoryPool> pool;
  std::shared_ptr<exec::BaseHashTable> table;
};

// What a task-built table keeps alive: the task's QueryCtx, which owns the
// HashTableCache entry, and the pool of the fake probe input.
struct BuildTaskResources {
  std::shared_ptr<core::QueryCtx> queryCtx;
  std::shared_ptr<memory::MemoryPool> inputPool;
};

// Appends the input columns 'expr' references to 'fields'.
void collectInputFields(const core::TypedExprPtr& expr, std::vector<core::FieldAccessTypedExprPtr>& fields) {
  if (auto field = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(expr);
      field != nullptr && field->isInputColumn()) {
    fields.push_back(field);
    return;
  }
  for (const auto& input : expr->inputs()) {
    collectInputFields(input, fields);
  }
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

void runSerial(const std::shared_ptr<exec::Task>& task) {
  while (true) {
    ContinueFuture future = ContinueFuture::makeEmpty();
    auto output = task->next(&future);
    if (output != nullptr) {
      continue;
    }
    if (!future.valid()) {
      break;
    }
    future.wait();
  }
}

void runParallel(const std::shared_ptr<exec::Task>& task, uint32_t numDrivers) {
  task->start(numDrivers);
  task->taskCompletionFuture().wait();
  // Drivers may still hold the task, and so the plan and its input vectors,
  // briefly after completion. Wait for them so that the input pool can go.
  for (int i = 0; i < 10'000 && task.use_count() > 1; ++i) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
}

} // namespace

std::shared_ptr<BroadcastHashTable> buildHashTableWithTask(
    const std::string& cacheKey,
    const HashTableTaskBuildSpec& spec,
    const std::vector<RowVectorPtr>& buildVectors,
    uint32_t numDrivers,
    folly::Executor* executor,
    std::shared_ptr<memory::MemoryPool> queryPool) {
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

  // Probe side: an empty input with one column per join key, plus the probe
  // columns the filter references.
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
  if (spec.filter != nullptr) {
    std::vector<core::FieldAccessTypedExprPtr> fields;
    collectInputFields(spec.filter, fields);
    for (const auto& field : fields) {
      if (!spec.buildType->containsChild(field->name()) &&
          std::find(probeNames.begin(), probeNames.end(), field->name()) == probeNames.end()) {
        probeNames.emplace_back(field->name());
        probeTypes.emplace_back(field->type());
      }
    }
  }
  auto probeType = ROW(std::move(probeNames), std::move(probeTypes));
  auto* pool = queryCtx->pool();
  auto vectorPool = pool->addLeafChild(fmt::format("hash_table_build_input_{}", buildId));

  int planNodeId = 0;
  auto nextPlanNodeId = [&]() { return std::to_string(planNodeId++); };
  core::PlanNodePtr probe = std::make_shared<core::ValuesNode>(
      nextPlanNodeId(), std::vector<RowVectorPtr>{BaseVector::create<RowVector>(probeType, 0, vectorPool.get())});

  // Build side: the broadcast vectors, renamed to 'buildType'.
  std::vector<RowVectorPtr> values;
  values.reserve(buildVectors.size());
  for (const auto& vector : buildVectors) {
    values.emplace_back(std::make_shared<RowVector>(
        vector->pool(), spec.buildType, vector->nulls(), vector->size(), vector->children()));
  }
  if (values.empty()) {
    values.emplace_back(BaseVector::create<RowVector>(spec.buildType, 0, vectorPool.get()));
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
                      .filter(spec.filter)
                      .left(probe)
                      .right(build)
                      .outputType(fakeJoinOutputType(spec.joinType, probeType, spec.buildType))
                      .nullAware(spec.nullAware)
                      .useHashTableCache(true)
                      .cacheKey(cacheKey)
                      .build();

  std::shared_ptr<exec::Task> task;
  if (numDrivers == 1) {
    task = exec::Task::create(
        queryCtx->queryId(), core::PlanFragment{joinNode}, 0, queryCtx, exec::Task::ExecutionMode::kSerial);
    runSerial(task);
  } else {
    task = exec::Task::create(
        queryCtx->queryId(),
        core::PlanFragment{joinNode},
        0,
        queryCtx,
        exec::Task::ExecutionMode::kParallel,
        [](RowVectorPtr, bool, ContinueFuture*) { return exec::BlockingReason::kNotBlocked; });
    runParallel(task, numDrivers);
  }
  if (task->state() != exec::TaskState::kFinished) {
    std::rethrow_exception(task->error());
  }

  auto* cache = exec::HashTableCache::instance();
  if (!cache->exist(cacheKey)) {
    // HashBuild does not cache the table of a null-aware anti join without
    // filter whose build side has a null key. Such a join returns nothing
    // whatever the table holds, so cache an empty table flagged with null keys.
    VELOX_CHECK(
        core::isAntiJoin(spec.joinType) && spec.nullAware && spec.filter == nullptr,
        "Hash table build task did not cache table {}",
        cacheKey);
    auto tablePool = pool->addLeafChild(fmt::format("hash_table_build_empty_{}", buildId));
    cache->add(cacheKey, makeEmptyJoinTable(spec, tablePool.get()), true, tablePool);
    queryCtx->addReleaseCallback([cacheKey]() { exec::HashTableCache::instance()->drop(cacheKey); });
  }
  ContinueFuture future = ContinueFuture::makeEmpty();
  auto entry = cache->get(cacheKey, task->taskId(), queryCtx.get(), &future);
  VELOX_CHECK(!future.valid());
  VELOX_CHECK(entry->buildComplete);

  auto result = std::make_shared<BroadcastHashTable>();
  // The cache entry, which owns the table's pool, can be dropped while the
  // table is still in use, so the table keeps its pool alive itself.
  auto tableWithPool = std::make_shared<TableWithPool>(TableWithPool{entry->tablePool, entry->table});
  result->table = std::shared_ptr<exec::BaseHashTable>(tableWithPool, tableWithPool->table.get());
  result->joinHasNullKeys = entry->hasNullKeys;
  result->memoryUsage = entry->tablePool->usedBytes();
  result->owner = std::make_shared<BuildTaskResources>(BuildTaskResources{std::move(queryCtx), std::move(vectorPool)});
  return result;
}

} // namespace gluten

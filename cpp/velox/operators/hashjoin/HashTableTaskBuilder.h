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

#pragma once

#include <folly/Executor.h>

#include "operators/hashjoin/BroadcastHashTable.h"
#include "velox/core/PlanNode.h"

namespace gluten {

/// Describes the build side of a broadcast hash join.
struct HashTableTaskBuildSpec {
  facebook::velox::core::JoinType joinType;
  bool nullAware{false};
  /// The join filter, or nullptr. It references build columns by their names in
  /// 'buildType' and probe columns by any other name.
  facebook::velox::core::TypedExprPtr filter;
  facebook::velox::RowTypePtr buildType;
  std::vector<std::string> joinKeys;
  /// Velox query configs for the build, e.g. join build tuning knobs.
  std::unordered_map<std::string, std::string> queryConfigs;
};

/// Builds a broadcast hash table by running Velox HashBuild in a dedicated task
/// with its own QueryCtx. The task runs HashJoinNode(useHashTableCache = true,
/// cacheKey = 'cacheKey') with 'buildVectors' on the build side and an empty
/// probe side, so HashBuild publishes the table to HashTableCache. 'numDrivers'
/// > 1 builds in parallel on 'executor' and HashBuild merges the per-driver
/// tables.
///
/// The returned table owns the task's QueryCtx: releasing the last reference to
/// it drops the cache entry.
std::shared_ptr<BroadcastHashTable> buildHashTableWithTask(
    const std::string& cacheKey,
    const HashTableTaskBuildSpec& spec,
    const std::vector<facebook::velox::RowVectorPtr>& buildVectors,
    uint32_t numDrivers,
    folly::Executor* executor,
    std::shared_ptr<facebook::velox::memory::MemoryPool> queryPool);

} // namespace gluten

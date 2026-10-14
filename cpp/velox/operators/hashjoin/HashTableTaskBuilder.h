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
  /// HashBuild never evaluates the join filter. It only checks whether there is
  /// one and, for an anti join, drops build rows with a null in the build
  /// columns of a null-intolerant filter. So these properties are all it needs.
  bool withFilter{false};
  std::vector<std::string> filterBuildColumns;
  bool filterPropagatesNulls{false};
  facebook::velox::RowTypePtr buildType;
  std::vector<std::string> joinKeys;
  /// Velox query configs for the build, e.g. join build tuning knobs.
  std::unordered_map<std::string, std::string> queryConfigs;
};

/// Builds a broadcast hash table by running Velox HashBuild in a dedicated task
/// with its own QueryCtx. The task runs a fake HashJoinNode(useHashTableCache =
/// true, cacheKey = 'cacheKey') with 'buildVectors' on the build side and an
/// empty probe side, so HashBuild publishes the table to HashTableCache.
/// 'numDrivers' > 1 builds in parallel on 'executor' and HashBuild merges the
/// per-driver tables. 'vectorPool' allocates the empty inputs of the fake join.
///
/// The returned table owns the task's QueryCtx: releasing it drops the cache
/// entry.
std::shared_ptr<BroadcastHashTable> buildHashTableWithTask(
    const std::string& cacheKey,
    const HashTableTaskBuildSpec& spec,
    const std::vector<facebook::velox::RowVectorPtr>& buildVectors,
    uint32_t numDrivers,
    folly::Executor* executor,
    std::shared_ptr<facebook::velox::memory::MemoryPool> queryPool,
    facebook::velox::memory::MemoryPool* vectorPool);

} // namespace gluten

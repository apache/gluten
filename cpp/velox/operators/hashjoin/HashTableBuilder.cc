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

#include "operators/hashjoin/HashTableBuilder.h"

namespace gluten {

using namespace facebook::velox;

HashTableBuilder::HashTableBuilder(
    core::JoinType joinType,
    bool nullAware,
    bool withFilter,
    int64_t bloomFilterPushdownSize,
    const std::vector<core::FieldAccessTypedExprPtr>& joinKeys,
    const std::vector<column_index_t>& filterInputChannels,
    bool filterPropagatesNulls,
    const RowTypePtr& inputType,
    memory::MemoryPool* pool,
    uint32_t minTableRowsForParallelJoinBuild,
    uint32_t joinBuildVectorHasherMaxNumDistinct,
    uint32_t abandonHashBuildDedupMinRows,
    uint32_t abandonHashBuildDedupMinPct) {
  exec::JoinTableBuilder::Options options;
  options.joinType = joinType;
  options.nullAware = nullAware;
  options.withFilter = withFilter;
  options.inputType = inputType;
  options.joinKeys = joinKeys;
  options.minTableRowsForParallelJoinBuild = minTableRowsForParallelJoinBuild;
  options.vectorHasherMaxNumDistinct = joinBuildVectorHasherMaxNumDistinct;
  options.abandonHashBuildDedupMinRows = abandonHashBuildDedupMinRows;
  options.abandonHashBuildDedupMinPct = abandonHashBuildDedupMinPct;
  options.bloomFilterPushdownMaxSize = bloomFilterPushdownSize;

  builder_ = std::make_unique<exec::JoinTableBuilder>(std::move(options));

  // The filter columns are resolved on the Java side, hence there is no filter
  // expression to analyze here.
  exec::JoinTableBuilder::AntiJoinFilterInfo filterInfo;
  filterInfo.propagatesNulls = filterPropagatesNulls;
  filterInfo.inputChannels = filterInputChannels;

  builder_->initialize(pool, pool, filterInfo);
}

void HashTableBuilder::addInput(const RowVectorPtr& input) {
  if (!builder_->addInput(input)) {
    noMoreInput_ = true;
  }
}

} // namespace gluten

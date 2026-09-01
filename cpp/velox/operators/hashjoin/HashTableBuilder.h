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

#include <cstdint>

#include "velox/exec/JoinTableBuilder.h"

namespace gluten {
using column_index_t = uint32_t;

/// Builds a join hash table from the build side batches of a broadcast join,
/// outside of a Velox driver. This is a thin wrapper around Velox's
/// 'JoinTableBuilder' which holds all the build logic shared with the
/// 'HashBuild' operator. It only adds what the JNI layer needs on top: the
/// ownership of the merged table and the accounting of its memory usage.
class HashTableBuilder {
 public:
  HashTableBuilder(
      facebook::velox::core::JoinType joinType,
      bool nullAware,
      bool withFilter,
      int64_t bloomFilterPushdownSize,
      const std::vector<facebook::velox::core::FieldAccessTypedExprPtr>& joinKeys,
      const std::vector<column_index_t>& filterInputChannels,
      bool filterPropagatesNulls,
      const facebook::velox::RowTypePtr& inputType,
      facebook::velox::memory::MemoryPool* pool,
      uint32_t minTableRowsForParallelJoinBuild,
      uint32_t joinBuildVectorHasherMaxNumDistinct,
      uint32_t abandonHashBuildDedupMinRows,
      uint32_t abandonHashBuildDedupMinPct);

  void addInput(const facebook::velox::RowVectorPtr& input);

  void setHashTable(std::unique_ptr<facebook::velox::exec::BaseHashTable> uniqueHashTable) {
    table_ = std::move(uniqueHashTable);
  }

  std::unique_ptr<facebook::velox::exec::BaseHashTable> uniqueTable() {
    return builder_->takeTable();
  }

  std::shared_ptr<facebook::velox::exec::BaseHashTable> hashTable() {
    return table_;
  }

  void setJoinHasNullKeys(bool joinHasNullKeys) {
    builder_->setJoinHasNullKeys(joinHasNullKeys);
  }

  bool joinHasNullKeys() const {
    return builder_->joinHasNullKeys();
  }

  bool dropDuplicates() const {
    return builder_->dropDuplicates();
  }

  /// True if the build can stop early because the join returns no rows, i.e.
  /// this is a null-aware anti join without filter and a null join key was
  /// found in the build side.
  bool noMoreInput() const {
    return noMoreInput_;
  }

  void setHashTableMemoryUsage(int64_t hashTableMemoryUsage) {
    hashTableMemoryUsage_ = hashTableMemoryUsage;
  }

  int64_t hashTableMemoryUsage() const {
    return hashTableMemoryUsage_;
  }

  uint32_t joinBuildVectorHasherMaxNumDistinct() const {
    return builder_->vectorHasherMaxNumDistinct();
  }

 private:
  std::unique_ptr<facebook::velox::exec::JoinTableBuilder> builder_;

  // The table handed over to the broadcast cache. Set once the tables built by
  // all the build threads have been merged, see 'setHashTable()'.
  std::shared_ptr<facebook::velox::exec::BaseHashTable> table_;

  bool noMoreInput_{false};

  int64_t hashTableMemoryUsage_{0};
};

} // namespace gluten

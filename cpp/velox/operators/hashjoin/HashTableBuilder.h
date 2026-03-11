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

#include <thread>
#include "velox/exec/HashJoinBridge.h"
#include "velox/exec/HashTable.h"
#include "velox/exec/RowContainer.h"
#include "velox/exec/VectorHasher.h"

namespace gluten {
using column_index_t = uint32_t;
using vector_size_t = int32_t;

class HashTableBuilder {
 public:
  HashTableBuilder(
      facebook::velox::core::JoinType joinType,
      bool nullAware,
      bool withFilter,
      int64_t bloomFilterPushdownSize,
      const std::vector<facebook::velox::core::FieldAccessTypedExprPtr>& joinKeys,
      const facebook::velox::RowTypePtr& inputType,
      facebook::velox::memory::MemoryPool* pool);

  void addInput(facebook::velox::RowVectorPtr input);

  void setHashTable(std::unique_ptr<facebook::velox::exec::BaseHashTable> uniqueHashTable) {
    table_ = std::move(uniqueHashTable);
  }

  std::unique_ptr<facebook::velox::exec::BaseHashTable> uniqueTable() {
    return std::move(uniqueTable_);
  }

  std::shared_ptr<facebook::velox::exec::BaseHashTable> hashTable() {
    return table_;
  }
  void setJoinHasNullKeys(bool joinHasNullKeys) {
    joinHasNullKeys_ = joinHasNullKeys;
  }

  bool joinHasNullKeys() {
    return joinHasNullKeys_;
  }

  bool dropDuplicates() {
    return dropDuplicates_;
  }

 private:
  // Invoked to set up hash table to build.
  void setupTable();

  const facebook::velox::core::JoinType joinType_;

  const bool nullAware_;
  const bool withFilter_;

  // The row type used for hash table build and disk spilling.
  facebook::velox::RowTypePtr tableType_;

  // Container for the rows being accumulated.
  std::shared_ptr<facebook::velox::exec::BaseHashTable> table_;

  std::unique_ptr<facebook::velox::exec::BaseHashTable> uniqueTable_;

  // Key channels in 'input_'
  std::vector<column_index_t> keyChannels_;

  // Non-key channels in 'input_'.
  std::vector<column_index_t> dependentChannels_;

  // Corresponds 1:1 to 'dependentChannels_'.
  std::vector<std::unique_ptr<facebook::velox::DecodedVector>> decoders_;

  // True if we are considering use of normalized keys or array hash tables.
  // Set to false when the dataset is no longer suitable.
  bool analyzeKeys_;

  // Temporary space for hash numbers.
  facebook::velox::raw_vector<uint64_t> hashes_;

  // Set of active rows during addInput().
  facebook::velox::SelectivityVector activeRows_;

  // True if this is a build side of an anti or left semi project join and has
  // at least one entry with null join keys.
  bool joinHasNullKeys_{false};

  // Indices of key columns used by the filter in build side table.
  std::vector<column_index_t> keyFilterChannels_;
  // Indices of dependent columns used by the filter in 'decoders_'.
  std::vector<column_index_t> dependentFilterChannels_;

  // Maps key channel in 'input_' to channel in key.
  folly::F14FastMap<column_index_t, column_index_t> keyChannelMap_;

  const facebook::velox::RowTypePtr& inputType_;

  int64_t bloomFilterPushdownSize_;

  facebook::velox::memory::MemoryPool* pool_;

  bool dropDuplicates_{false};
};

} // namespace gluten

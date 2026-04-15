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

#include <algorithm>

#include <iostream>

#include "velox/exec/OperatorUtils.h"

namespace gluten {
namespace {
facebook::velox::RowTypePtr hashJoinTableType(
    const std::vector<facebook::velox::core::FieldAccessTypedExprPtr>& joinKeys,
    const facebook::velox::RowTypePtr& inputType,
    bool includeDependents) {
  const auto numKeys = joinKeys.size();

  std::vector<std::string> names;
  names.reserve(includeDependents ? inputType->size() : numKeys);
  std::vector<facebook::velox::TypePtr> types;
  types.reserve(includeDependents ? inputType->size() : numKeys);
  std::unordered_set<uint32_t> keyChannelSet;
  keyChannelSet.reserve(inputType->size());

  for (int i = 0; i < numKeys; ++i) {
    auto& key = joinKeys[i];
    auto channel = facebook::velox::exec::exprToChannel(key.get(), inputType);
    keyChannelSet.insert(channel);
    names.emplace_back(inputType->nameOf(channel));
    types.emplace_back(inputType->childAt(channel));
  }

  if (!includeDependents) {
    return ROW(std::move(names), std::move(types));
  }

  for (auto i = 0; i < inputType->size(); ++i) {
    if (keyChannelSet.find(i) == keyChannelSet.end()) {
      names.emplace_back(inputType->nameOf(i));
      types.emplace_back(inputType->childAt(i));
    }
  }

  return ROW(std::move(names), std::move(types));
}

bool isLeftNullAwareJoinWithFilter(facebook::velox::core::JoinType joinType, bool nullAware, bool withFilter) {
  return (isAntiJoin(joinType) || isLeftSemiProjectJoin(joinType) || isLeftSemiFilterJoin(joinType)) && nullAware &&
      withFilter;
}
} // namespace

HashTableBuilder::HashTableBuilder(
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
    uint32_t abandonHashBuildDedupMinPct)
    : joinType_{joinType},
      nullAware_{nullAware},
      withFilter_(withFilter),
      keyChannelMap_(joinKeys.size()),
      inputType_(inputType),
      bloomFilterPushdownSize_(bloomFilterPushdownSize),
      pool_(pool),
      minTableRowsForParallelJoinBuild_(minTableRowsForParallelJoinBuild),
      joinBuildVectorHasherMaxNumDistinct_(joinBuildVectorHasherMaxNumDistinct),
      abandonHashBuildDedupMinRows_(abandonHashBuildDedupMinRows),
      abandonHashBuildDedupMinPct_(abandonHashBuildDedupMinPct),
      filterPropagatesNulls_(filterPropagatesNulls) {
  dropDuplicates_ =
      !withFilter_ && (isLeftSemiFilterJoin(joinType_) || isLeftSemiProjectJoin(joinType_) || isAntiJoin(joinType_));
  const auto numKeys = joinKeys.size();
  keyChannels_.reserve(numKeys);

  for (int i = 0; i < numKeys; ++i) {
    auto& key = joinKeys[i];
    auto channel = facebook::velox::exec::exprToChannel(key.get(), inputType_);
    keyChannelMap_[channel] = i;
    keyChannels_.emplace_back(channel);
  }

  // Identify the non-key build side columns and make a decoder for each.
  if (!dropDuplicates_) {
    const int32_t numDependents = inputType_->size() - numKeys;
    if (numDependents > 0) {
      // Number of join keys (numKeys) may be less then number of input columns
      // (inputType->size()). In this case numDependents is negative and cannot
      // be used to call 'reserve'. This happens when we join different probe
      // side keys with the same build side key: SELECT * FROM t LEFT JOIN u ON
      // t.k1 = u.k AND t.k2 = u.k.
      dependentChannels_.reserve(numDependents);
      decoders_.reserve(numDependents);
    }
    for (auto i = 0; i < inputType->size(); ++i) {
      if (keyChannelMap_.find(i) == keyChannelMap_.end()) {
        dependentChannels_.emplace_back(i);
        decoders_.emplace_back(std::make_unique<facebook::velox::DecodedVector>());
      }
    }
  }

  tableType_ = hashJoinTableType(joinKeys, inputType, !dropDuplicates_);
  setupTable();

  if (isAntiJoin(joinType_) && withFilter_ && filterPropagatesNulls_) {
    setupFilterForAntiJoins(filterInputChannels);
  }
}

void HashTableBuilder::setupFilterForAntiJoins(const std::vector<column_index_t>& filterInputChannels) {
  VELOX_DCHECK(std::is_sorted(dependentChannels_.begin(), dependentChannels_.end()));

  for (auto channel : filterInputChannels) {
    auto keyIter = keyChannelMap_.find(channel);
    if (keyIter != keyChannelMap_.end()) {
      keyFilterChannels_.push_back(keyIter->second);
      continue;
    }

    auto dependentIter = std::lower_bound(dependentChannels_.begin(), dependentChannels_.end(), channel);
    if (dependentIter == dependentChannels_.end() || *dependentIter != channel) {
      continue;
    }
    dependentFilterChannels_.push_back(dependentIter - dependentChannels_.begin());
  }
}

void HashTableBuilder::removeInputRowsForAntiJoinFilter() {
  bool changed = false;
  auto* rawActiveRows = activeRows_.asMutableRange().bits();

  auto removeNulls = [&](facebook::velox::DecodedVector& decoded) {
    if (decoded.mayHaveNulls()) {
      changed = true;
      facebook::velox::bits::andBits(rawActiveRows, decoded.nulls(&activeRows_), 0, activeRows_.end());
    }
  };

  for (auto channel : keyFilterChannels_) {
    removeNulls(uniqueTable_->hashers()[channel]->decodedVector());
  }
  for (auto channel : dependentFilterChannels_) {
    removeNulls(*decoders_[channel]);
  }

  if (changed) {
    activeRows_.updateBounds();
  }
}

bool HashTableBuilder::abandonHashBuildDedupEarly(int64_t numDistinct) const {
  VELOX_CHECK(dropDuplicates_);
  return numHashInputRows_ > abandonHashBuildDedupMinRows_ &&
      (100 * numDistinct / numHashInputRows_) >= abandonHashBuildDedupMinPct_;
}

void HashTableBuilder::abandonHashBuildDedup() {
  abandonHashBuildDedup_ = true;
  uniqueTable_->setAllowDuplicates(true);
  lookup_.reset();
}

// Invoked to set up hash table to build.
void HashTableBuilder::setupTable() {
  VELOX_CHECK_NULL(uniqueTable_);

  const auto numKeys = keyChannels_.size();
  std::vector<std::unique_ptr<facebook::velox::exec::VectorHasher>> keyHashers;
  keyHashers.reserve(numKeys);
  for (vector_size_t i = 0; i < numKeys; ++i) {
    keyHashers.emplace_back(facebook::velox::exec::VectorHasher::create(tableType_->childAt(i), keyChannels_[i]));
  }

  const auto numDependents = tableType_->size() - numKeys;
  std::vector<facebook::velox::TypePtr> dependentTypes;
  dependentTypes.reserve(numDependents);
  for (int i = numKeys; i < tableType_->size(); ++i) {
    dependentTypes.emplace_back(tableType_->childAt(i));
  }
  if (isRightJoin(joinType_) || isFullJoin(joinType_) || isRightSemiProjectJoin(joinType_)) {
    // Do not ignore null keys.
    uniqueTable_ = facebook::velox::exec::HashTable<false>::createForJoin(
        std::move(keyHashers),
        dependentTypes,
        true, // allowDuplicates
        true, // hasProbedFlag
        false, // hasCountFlag
        minTableRowsForParallelJoinBuild_,
        pool_);
  } else {
    // Right semi join needs to tag build rows that were probed.
    const bool needProbedFlag = isRightSemiFilterJoin(joinType_);
    const bool hasCountFlag = facebook::velox::core::isCountingJoin(joinType_);
    if (isLeftNullAwareJoinWithFilter(joinType_, nullAware_, withFilter_)) {
      // We need to check null key rows in build side in case of null-aware anti
      // or left semi project join with filter set.
      uniqueTable_ = facebook::velox::exec::HashTable<false>::createForJoin(
          std::move(keyHashers),
          dependentTypes,
          !dropDuplicates_, // allowDuplicates
          needProbedFlag, // hasProbedFlag
          hasCountFlag, // hasCountFlag
          minTableRowsForParallelJoinBuild_,
          pool_);
    } else {
      // Ignore null keys
      uniqueTable_ = facebook::velox::exec::HashTable<true>::createForJoin(
          std::move(keyHashers),
          dependentTypes,
          !dropDuplicates_, // allowDuplicates
          needProbedFlag, // hasProbedFlag
          hasCountFlag, // hasCountFlag
          minTableRowsForParallelJoinBuild_,
          pool_,
          bloomFilterPushdownSize_);
    }
  }
  analyzeKeys_ = uniqueTable_->hashMode() != facebook::velox::exec::BaseHashTable::HashMode::kHash;

  if (dropDuplicates_) {
    if (!facebook::velox::core::isCountingJoin(joinType_) && abandonHashBuildDedupMinPct_ == 0) {
      abandonHashBuildDedup();
      return;
    }
    lookup_ = std::make_unique<facebook::velox::exec::HashLookup>(uniqueTable_->hashers(), pool_);
  }
}

void HashTableBuilder::addInput(facebook::velox::RowVectorPtr input) {
  activeRows_.resize(input->size());
  activeRows_.setAll();

  auto& hashers = uniqueTable_->hashers();

  for (auto i = 0; i < hashers.size(); ++i) {
    auto key = input->childAt(hashers[i]->channel())->loadedVector();
    hashers[i]->decode(*key, activeRows_);
  }

  if (!isRightJoin(joinType_) && !isFullJoin(joinType_) && !isRightSemiProjectJoin(joinType_) &&
      !isLeftNullAwareJoinWithFilter(joinType_, nullAware_, withFilter_)) {
    deselectRowsWithNulls(hashers, activeRows_);
    if (nullAware_ && !joinHasNullKeys_ && activeRows_.countSelected() < input->size()) {
      joinHasNullKeys_ = true;
    }
  } else if (nullAware_ && !joinHasNullKeys_) {
    for (auto& hasher : hashers) {
      auto& decoded = hasher->decodedVector();
      if (decoded.mayHaveNulls()) {
        auto* nulls = decoded.nulls(&activeRows_);
        if (nulls && facebook::velox::bits::countNulls(nulls, 0, activeRows_.end()) > 0) {
          joinHasNullKeys_ = true;
          break;
        }
      }
    }
  }

  if (isAntiJoin(joinType_) && nullAware_ && joinHasNullKeys_ && !withFilter_) {
    noMoreInput_ = true;
    return;
  }

  if (!activeRows_.hasSelections()) {
    return;
  }

  if (dropDuplicates_ && !abandonHashBuildDedup_) {
    // Counting joins must not abandon dedup — accurate counts are required.
    VELOX_CHECK_NOT_NULL(lookup_);
    const bool abandonEarly =
        !facebook::velox::core::isCountingJoin(joinType_) && abandonHashBuildDedupEarly(uniqueTable_->numDistinct());
    if (!abandonEarly) {
      numHashInputRows_ += activeRows_.countSelected();
      uniqueTable_->prepareForGroupProbe(
          *lookup_, input, activeRows_, facebook::velox::exec::BaseHashTable::kNoSpillInputStartPartitionBit);
      if (lookup_->rows.empty()) {
        return;
      }
      uniqueTable_->groupProbe(*lookup_, facebook::velox::exec::BaseHashTable::kNoSpillInputStartPartitionBit);

      // For counting joins, increment the count for duplicate rows.
      // New rows are initialized with count = 1 by initializeRow.
      // Increment count for all rows, then decrement for new rows to
      // correct the over-counting.
      if (facebook::velox::core::isCountingJoin(joinType_)) {
        auto* rows = uniqueTable_->rows();
        for (auto row : lookup_->rows) {
          rows->incrementCount(lookup_->hits[row]);
        }
        for (auto newRow : lookup_->newGroups) {
          rows->decrementCount(lookup_->hits[newRow]);
        }
      }
      return;
    }
    abandonHashBuildDedup();
  }

  for (auto i = 0; i < dependentChannels_.size(); ++i) {
    decoders_[i]->decode(*input->childAt(dependentChannels_[i])->loadedVector(), activeRows_);
  }

  if (isAntiJoin(joinType_) && withFilter_ && filterPropagatesNulls_) {
    removeInputRowsForAntiJoinFilter();
  }

  if (!activeRows_.hasSelections()) {
    return;
  }

  if (analyzeKeys_ && hashes_.size() < activeRows_.end()) {
    hashes_.resize(activeRows_.end());
  }

  // As long as analyzeKeys is true, we keep running the keys through
  // the Vectorhashers so that we get a possible mapping of the keys
  // to small ints for array or normalized key. When mayUseValueIds is
  // false for the first time we stop. We do not retain the value ids
  // since the final ones will only be known after all data is
  // received.
  for (auto& hasher : hashers) {
    // TODO: Load only for active rows, except if right/full outer join.
    if (analyzeKeys_) {
      hasher->computeValueIds(activeRows_, hashes_);
      analyzeKeys_ = hasher->mayUseValueIds();
    }
  }
  auto rows = uniqueTable_->rows();
  auto nextOffset = rows->nextOffset();

  activeRows_.applyToSelected([&](auto rowIndex) {
    char* newRow = rows->newRow();
    if (nextOffset) {
      *reinterpret_cast<char**>(newRow + nextOffset) = nullptr;
    }
    // Store the columns for each row in sequence. At probe time
    // strings of the row will probably be in consecutive places, so
    // reading one will prime the cache for the next.
    for (auto i = 0; i < hashers.size(); ++i) {
      rows->store(hashers[i]->decodedVector(), rowIndex, newRow, i);
    }
    for (auto i = 0; i < dependentChannels_.size(); ++i) {
      rows->store(*decoders_[i], rowIndex, newRow, i + hashers.size());
    }
  });
}

} // namespace gluten

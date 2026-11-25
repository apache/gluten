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

#include <glog/logging.h>

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "shuffle/Payload.h"
#include "shuffle/Utils.h"
#include "utils/Exception.h"

namespace gluten {

/// Coalesces the small per-partition `InMemoryPayload`s produced by the hash-based shuffle writer
/// into payloads of up to `mergeBufferSize` rows, so that the remote shuffle service - and above
/// all the reducer reading back from it - sees a few large blocks per partition instead of a long
/// tail of tiny ones.
///
/// Why this exists at all: the number of rows a hash shuffle writer can buffer for one partition
/// is `splitBufferSize / numPartitions`-ish, so with a few thousand partitions each flush carries
/// only a couple hundred rows. `LocalPartitionWriter` already compensates for that with its own
/// merger before writing to the local file; the RSS path had no equivalent and pushed every tiny
/// payload straight to the client. Every such payload costs a block header, a compression call, a
/// push request and, on the read side, one more batch to decode.
///
/// Relationship to `LocalPartitionWriter::PayloadMerger`: same idea, deliberately a separate
/// class, because the two have different contracts and this keeps the local (ESS) path untouched:
///   - Commit: the local writer caches a committed payload and reads it back later, so it must
///     copy the shuffle writer's reusable partition buffers. This writer serializes and pushes a
///     committed payload synchronously inside `hashEvict`, before the shuffle writer touches
///     those buffers again, so the copy would be pure overhead and is skipped.
///   - Accumulation: payloads are collected in a per-partition list and concatenated in a single
///     pass once the row threshold is reached, instead of being folded pairwise into a growing
///     payload. Growing an `arrow::ResizableBuffer` is never in-place here (`PoolBuffer::Reserve`
///     only rounds the capacity up to 64 bytes, and gluten's allocator implements
///     `reallocateAligned` as allocate + memcpy + free), so pairwise folding copies O(N^2) bytes
///     per output payload, and every intermediate reallocation is a reservation against the
///     payload pool that can trigger a spill.
///
/// Payloads that are *kept* for a future merge are always copied when `reuseBuffers` is set,
/// because the shuffle writer reuses its partition buffers as soon as `hashEvict` returns.
class RssPayloadMerger {
 public:
  RssPayloadMerger(arrow::MemoryPool* pool, int32_t mergeBufferSize, int32_t mergeBufferMinSize)
      : pool_(pool), mergeBufferSize_(mergeBufferSize), mergeBufferMinSize_(mergeBufferMinSize) {}

  arrow::Result<std::vector<std::unique_ptr<InMemoryPayload>>>
  merge(uint32_t partitionId, std::unique_ptr<InMemoryPayload> append, bool reuseBuffers) {
    PartitionScopeGuard mergeGuard(partitionInUse_, partitionId);

    std::vector<std::unique_ptr<InMemoryPayload>> merged{};
    if (!append->mergeable()) {
      // TODO: Merging complex type is currently not supported.
      merged.emplace_back(std::move(append));
      return merged;
    }

    // A payload that is already large enough goes straight out. Whatever has been accumulated so
    // far must be flushed first, otherwise the rows of this partition would be reordered.
    if (append->numRows() > mergeBufferMinSize_) {
      RETURN_NOT_OK(flush(partitionId, merged));
      merged.emplace_back(std::move(append));
      return merged;
    }

    if (mergedRows(partitionId) + append->numRows() > mergeBufferSize_) {
      RETURN_NOT_OK(flush(partitionId, merged));
    }

    DLOG(INFO) << "Accumulated partition: " << partitionId << ", numRows before: " << mergedRows(partitionId)
               << ", numRows appended: " << append->numRows();
    // The shuffle writer reuses its partition buffers as soon as `hashEvict` returns, so a payload
    // that stays in the group across calls has to own its memory. This keeps the invariant that the
    // group never holds a payload backed by the shuffle writer's buffers once `merge` returns.
    if (reuseBuffers) {
      RETURN_NOT_OK(append->copyBuffers(pool_));
    }
    // Looked up only after the last call that can allocate: allocating may trigger a reentrant
    // spill, and anything that inserts into `partitionMergePayload_` invalidates references to it.
    auto& group = partitionMergePayload_[partitionId];
    group.numRows += append->numRows();
    group.payloads.emplace_back(std::move(append));
    const bool reachedThreshold = group.numRows >= mergeBufferSize_;

    if (reachedThreshold) {
      RETURN_NOT_OK(flush(partitionId, merged));
    }
    return merged;
  }

  /// Hands over everything accumulated for `partitionId`.
  ///
  /// `fromSpill` must be true when called from a memory reclaim callback. In that case this
  /// function is guaranteed not to allocate: the accumulated payloads are handed back exactly as
  /// they were accumulated, without being concatenated, and the caller evicts them one by one.
  /// With `fromSpill == false` a partition is concatenated into a single payload as usual.
  ///
  /// Concatenating on the reclaim path would break it. Gluten reserves memory through
  /// `OverAcquire#borrow`, which asserts `overTarget.usedBytes() == 0` on entry; when a spill is
  /// triggered from its over-acquire step, any reservation requested from inside the spill
  /// callback trips that assertion and surfaces as an `IllegalStateException`. Both partition
  /// writers therefore keep their reclaim path free of allocations from the managed payload pool -
  /// see `LocalPartitionWriter::hashEvict`, which emits `kToBeCompressed` rather than `kCompressed`
  /// precisely so that compression, and its buffer, happen after the spill.
  arrow::Result<std::vector<std::unique_ptr<InMemoryPayload>>> finish(uint32_t partitionId, bool fromSpill) {
    // We need to check whether the spill source is from compressing/copying the merged buffers.
    if ((fromSpill && (partitionInUse_.has_value() && partitionInUse_.value() == partitionId)) ||
        !hasMerged(partitionId)) {
      return std::vector<std::unique_ptr<InMemoryPayload>>{};
    }

    if (fromSpill) {
      return drain(partitionId);
    }

    GLUTEN_DCHECK(
        !partitionInUse_.has_value(),
        "Invalid status: partitionInUse_ is set when not in spilling: " + std::to_string(partitionInUse_.value()));

    std::vector<std::unique_ptr<InMemoryPayload>> merged{};
    RETURN_NOT_OK(flush(partitionId, merged));
    return merged;
  }

  bool hasMerged(uint32_t partitionId) {
    auto iter = partitionMergePayload_.find(partitionId);
    return iter != partitionMergePayload_.end() && !iter->second.payloads.empty();
  }

 private:
  /// Payloads of one partition waiting to be concatenated, together with their total row count.
  struct MergeGroup {
    std::vector<std::unique_ptr<InMemoryPayload>> payloads{};
    uint32_t numRows{0};
  };

  uint32_t mergedRows(uint32_t partitionId) const {
    auto iter = partitionMergePayload_.find(partitionId);
    return iter == partitionMergePayload_.end() ? 0 : iter->second.numRows;
  }

  /// Moves the payloads accumulated for one partition out of the merger. Never allocates.
  std::vector<std::unique_ptr<InMemoryPayload>> drain(uint32_t partitionId) {
    auto iter = partitionMergePayload_.find(partitionId);
    if (iter == partitionMergePayload_.end()) {
      return {};
    }
    auto payloads = std::move(iter->second.payloads);
    iter->second.payloads.clear();
    iter->second.numRows = 0;
    return payloads;
  }

  /// Concatenates everything accumulated for one partition and appends the result to `out`.
  /// The group is emptied *before* the concatenation allocates, so that a reentrant spill can
  /// neither evict payloads that are already being consumed here nor invalidate the iterator.
  arrow::Status flush(uint32_t partitionId, std::vector<std::unique_ptr<InMemoryPayload>>& out) {
    auto payloads = drain(partitionId);
    if (payloads.empty()) {
      return arrow::Status::OK();
    }
    // Every payload that was accumulated across calls owns its buffers already, and concatenating
    // allocates a fresh buffer anyway. The one case where the result may still reference the
    // shuffle writer's buffers is a group of exactly one payload - the caller pushes it before
    // returning from `hashEvict`, see `RssPartitionWriter::doEvict`.
    ARROW_ASSIGN_OR_RAISE(auto payload, InMemoryPayload::mergeAll(std::move(payloads), pool_));
    out.emplace_back(std::move(payload));
    return arrow::Status::OK();
  }

  arrow::MemoryPool* pool_;
  int32_t mergeBufferSize_;
  int32_t mergeBufferMinSize_;
  std::unordered_map<uint32_t, MergeGroup> partitionMergePayload_;
  std::optional<uint32_t> partitionInUse_{std::nullopt};
};

} // namespace gluten

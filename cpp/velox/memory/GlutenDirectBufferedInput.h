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

#include <memory>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "velox/dwio/common/DirectBufferedInput.h"

namespace gluten {

/// Decides, for one scanned file, which streams are loaded only once the scan
/// needs them. Shared by a GlutenDirectBufferedInput and all its clones: the
/// Parquet reader clones the file's input once per row group, and the
/// connector sends the "rows passed the filters" hint to the file's input, so
/// the decision must be visible across all of them.
///
/// The reader names the streams of the lazily loaded columns (projected, no
/// filter) through setLazyStreams(). Their loads are planned per stream, never
/// coalesced across streams, and not scheduled ("held") until either the hint
/// marks every lazy column as needed or one of the streams is read before the
/// hint (for example by the remaining filter), which marks that stream alone
/// as needed. Parquet lays a row group's column chunks out back to back, so a
/// load spanning several lazy columns would drag the neighbours of the one
/// stream read into the synchronous load and make them eager afterwards. A
/// needed stream is prefetched like any other with every following row group.
class LazyColumnLoadState {
 public:
  using LoadPtr = std::shared_ptr<facebook::velox::dwio::common::DirectCoalescedLoad>;

  void setLazyStreams(const std::vector<int32_t>& streamIds);

  bool hasLazyStreams() const;

  /// True if the stream is lazy and no one has shown it is needed yet.
  bool isDeferred(int32_t streamId) const;

  /// Remembers a planned load of the deferred stream 'streamId' so that it can
  /// be scheduled later.
  void hold(LoadPtr load, int32_t streamId);

  /// Marks every lazy column as needed. Returns the held loads still alive, to
  /// be scheduled by the caller.
  std::vector<LoadPtr> markAllNeeded();

  /// If 'load' is held, marks its stream as needed and returns the other held
  /// loads of that stream, to be scheduled by the caller. Returns an empty
  /// vector if 'load' is not held.
  std::vector<LoadPtr> markNeededByRead(const facebook::velox::dwio::common::DirectCoalescedLoad* load);

 private:
  struct HeldLoad {
    std::weak_ptr<facebook::velox::dwio::common::DirectCoalescedLoad> load;
    int32_t streamId;
  };

  mutable std::mutex mutex_;
  std::unordered_set<int32_t> lazyStreamIds_;
  bool allNeeded_{false};
  std::unordered_set<int32_t> neededStreamIds_;
  std::vector<HeldLoad> held_;
};

/// DirectBufferedInput that (1) cancels or finishes its loads on destruction
/// so that a cancelled task does not leave IO running against a freed memory
/// pool, and (2) when asked to defer (GlutenBufferedInputBuilder decides per
/// scan), loads the streams of the lazily loaded columns only once the scan
/// shows they are needed, instead of prefetching them with their row group.
/// On a split where no row passes the filters those streams are never read.
class GlutenDirectBufferedInput : public facebook::velox::dwio::common::DirectBufferedInput {
 public:
  GlutenDirectBufferedInput(
      std::shared_ptr<facebook::velox::ReadFile> readFile,
      const facebook::velox::dwio::common::MetricsLogPtr& metricsLog,
      facebook::velox::StringIdLease fileNum,
      std::shared_ptr<facebook::velox::cache::ScanTracker> tracker,
      facebook::velox::StringIdLease groupId,
      std::shared_ptr<facebook::velox::io::IoStatistics> ioStatistics,
      std::shared_ptr<facebook::velox::IoStats> ioStats,
      folly::Executor* executor,
      const facebook::velox::io::ReaderOptions& readerOptions,
      folly::F14FastMap<std::string, std::string> fileReadOps = {},
      bool deferLazyColumnPrefetch = false);

  ~GlutenDirectBufferedInput() override;

  std::unique_ptr<facebook::velox::dwio::common::BufferedInput> clone() const override;

  void setLazyColumnStreams(const std::vector<int32_t>& streamIds) override;

  void hintLazyColumnsNeeded() override;

  /// With deferral on: prefetches every stream that is not deferred and plans,
  /// without scheduling, one load per deferred stream. Otherwise the same as
  /// DirectBufferedInput::load().
  void load(const facebook::velox::dwio::common::LogType logType) override;

  /// A held stream is being read before the hint: it is needed, so its other
  /// held loads are scheduled now.
  std::shared_ptr<facebook::velox::dwio::common::DirectCoalescedLoad> coalescedLoad(
      const facebook::velox::dwio::common::SeekableInputStream* stream) override;

  /// Testing only. Null when deferral is off for this scan.
  const LazyColumnLoadState* testingLazyState() const {
    return lazyState_.get();
  }

 private:
  // Constructor used by clone().
  GlutenDirectBufferedInput(
      std::shared_ptr<facebook::velox::dwio::common::ReadFileInputStream> input,
      facebook::velox::StringIdLease fileNum,
      std::shared_ptr<facebook::velox::cache::ScanTracker> tracker,
      facebook::velox::StringIdLease groupId,
      std::shared_ptr<facebook::velox::io::IoStatistics> ioStatistics,
      std::shared_ptr<facebook::velox::IoStats> ioStats,
      folly::Executor* executor,
      const facebook::velox::io::ReaderOptions& readerOptions,
      std::shared_ptr<LazyColumnLoadState> lazyState);

  // Makes one planned load per group of 'requests' (sorted by offset, groups
  // given by their end indices) and returns the loads made.
  std::vector<LazyColumnLoadState::LoadPtr> planLoads(
      const std::vector<facebook::velox::dwio::common::LoadRequest*>& requests,
      const std::vector<int32_t>& groupEnds);

  // Shared with all clones. Null when deferral is off for this scan.
  const std::shared_ptr<LazyColumnLoadState> lazyState_;
};

} // namespace gluten

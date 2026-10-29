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

#include "memory/GlutenDirectBufferedInput.h"

#include <folly/executors/QueuedImmediateExecutor.h>

#include <algorithm>
#include <map>

namespace gluten {

using facebook::velox::cache::CoalescedLoad;
using facebook::velox::dwio::common::BufferedInput;
using facebook::velox::dwio::common::DirectBufferedInput;
using facebook::velox::dwio::common::DirectCoalescedLoad;
using facebook::velox::dwio::common::LoadRequest;
using facebook::velox::dwio::common::LogType;
using facebook::velox::dwio::common::SeekableInputStream;

void LazyColumnLoadState::setLazyStreams(const std::vector<int32_t>& streamIds) {
  std::lock_guard<std::mutex> l(mutex_);
  lazyStreamIds_.insert(streamIds.begin(), streamIds.end());
}

bool LazyColumnLoadState::hasLazyStreams() const {
  std::lock_guard<std::mutex> l(mutex_);
  return !lazyStreamIds_.empty();
}

bool LazyColumnLoadState::isDeferred(int32_t streamId) const {
  std::lock_guard<std::mutex> l(mutex_);
  return !allNeeded_ && lazyStreamIds_.count(streamId) != 0 && neededStreamIds_.count(streamId) == 0;
}

void LazyColumnLoadState::hold(LoadPtr load, int32_t streamId) {
  std::lock_guard<std::mutex> l(mutex_);
  // Drop the loads whose input is gone (row group released or task ended).
  held_.erase(
      std::remove_if(held_.begin(), held_.end(), [](const HeldLoad& held) { return held.load.expired(); }),
      held_.end());
  held_.push_back({std::move(load), streamId});
}

std::vector<LazyColumnLoadState::LoadPtr> LazyColumnLoadState::markAllNeeded() {
  std::lock_guard<std::mutex> l(mutex_);
  allNeeded_ = true;
  std::vector<LoadPtr> loads;
  for (auto& held : held_) {
    if (auto load = held.load.lock()) {
      loads.push_back(std::move(load));
    }
  }
  held_.clear();
  return loads;
}

std::vector<LazyColumnLoadState::LoadPtr> LazyColumnLoadState::markNeededByRead(const DirectCoalescedLoad* load) {
  std::lock_guard<std::mutex> l(mutex_);
  if (allNeeded_) {
    return {};
  }
  const HeldLoad* read = nullptr;
  for (auto& held : held_) {
    auto alive = held.load.lock();
    if (alive && alive.get() == load) {
      read = &held;
      break;
    }
  }
  if (read == nullptr) {
    return {};
  }
  neededStreamIds_.insert(read->streamId);
  // Schedule the other held loads of the now needed stream; keep the rest.
  std::vector<LoadPtr> loads;
  std::vector<HeldLoad> stillHeld;
  for (auto& held : held_) {
    auto alive = held.load.lock();
    if (!alive || alive.get() == load) {
      continue;
    }
    if (neededStreamIds_.count(held.streamId) != 0) {
      loads.push_back(std::move(alive));
    } else {
      stillHeld.push_back(std::move(held));
    }
  }
  held_ = std::move(stillHeld);
  return loads;
}

GlutenDirectBufferedInput::GlutenDirectBufferedInput(
    std::shared_ptr<facebook::velox::ReadFile> readFile,
    const facebook::velox::dwio::common::MetricsLogPtr& metricsLog,
    facebook::velox::StringIdLease fileNum,
    std::shared_ptr<facebook::velox::cache::ScanTracker> tracker,
    facebook::velox::StringIdLease groupId,
    std::shared_ptr<facebook::velox::io::IoStatistics> ioStatistics,
    std::shared_ptr<facebook::velox::IoStats> ioStats,
    folly::Executor* executor,
    const facebook::velox::io::ReaderOptions& readerOptions,
    folly::F14FastMap<std::string, std::string> fileReadOps,
    bool deferLazyColumnPrefetch)
    : DirectBufferedInput(
          std::move(readFile),
          metricsLog,
          std::move(fileNum),
          std::move(tracker),
          std::move(groupId),
          std::move(ioStatistics),
          std::move(ioStats),
          executor,
          readerOptions,
          std::move(fileReadOps)),
      lazyState_(deferLazyColumnPrefetch ? std::make_shared<LazyColumnLoadState>() : nullptr) {}

GlutenDirectBufferedInput::GlutenDirectBufferedInput(
    std::shared_ptr<facebook::velox::dwio::common::ReadFileInputStream> input,
    facebook::velox::StringIdLease fileNum,
    std::shared_ptr<facebook::velox::cache::ScanTracker> tracker,
    facebook::velox::StringIdLease groupId,
    std::shared_ptr<facebook::velox::io::IoStatistics> ioStatistics,
    std::shared_ptr<facebook::velox::IoStats> ioStats,
    folly::Executor* executor,
    const facebook::velox::io::ReaderOptions& readerOptions,
    std::shared_ptr<LazyColumnLoadState> lazyState)
    : DirectBufferedInput(
          std::move(input),
          std::move(fileNum),
          std::move(tracker),
          std::move(groupId),
          std::move(ioStatistics),
          std::move(ioStats),
          executor,
          readerOptions),
      lazyState_(std::move(lazyState)) {}

GlutenDirectBufferedInput::~GlutenDirectBufferedInput() {
  requests_.clear();
  // Cancel all the planned loads as soon as possible to avoid unnecessary IO.
  for (auto& load : coalescedLoads_) {
    if (load->state() == CoalescedLoad::State::kPlanned) {
      load->cancel();
    }
  }
  // Ensure all the loadings can finish in the TableScan destructor to avoid the issue that the load is still running
  // when the VeloxMemoryManager used by the whole task is trying to destruct.
  for (auto& load : coalescedLoads_) {
    if (load->state() == CoalescedLoad::State::kLoading) {
      folly::SemiFuture<bool> waitFuture(false);
      if (!load->loadOrFuture(&waitFuture)) {
        auto& exec = folly::QueuedImmediateExecutor::instance();
        std::move(waitFuture).via(&exec).wait();
      }
    }
  }
  coalescedLoads_.clear();
}

std::unique_ptr<BufferedInput> GlutenDirectBufferedInput::clone() const {
  return std::unique_ptr<BufferedInput>(new GlutenDirectBufferedInput(
      input_, fileNum_, tracker_, groupId_, ioStatistics_, ioStats_, executor_, options_, lazyState_));
}

void GlutenDirectBufferedInput::setLazyColumnStreams(const std::vector<int32_t>& streamIds) {
  if (lazyState_) {
    lazyState_->setLazyStreams(streamIds);
  }
}

void GlutenDirectBufferedInput::hintLazyColumnsNeeded() {
  if (!lazyState_) {
    return;
  }
  for (auto& load : lazyState_->markAllNeeded()) {
    prefetchLoad(load);
  }
}

std::vector<LazyColumnLoadState::LoadPtr> GlutenDirectBufferedInput::planLoads(
    const std::vector<LoadRequest*>& requests,
    const std::vector<int32_t>& groupEnds) {
  std::vector<LazyColumnLoadState::LoadPtr> loads;
  std::vector<LoadRequest*> group;
  int32_t i = 0;
  for (auto end : groupEnds) {
    while (i < end) {
      group.push_back(requests[i++]);
    }
    const auto numLoads = coalescedLoads_.size();
    // 'prefetch' here only selects dense coalescing and makes a load even for
    // a single request; nothing is scheduled.
    readRegion(group, /*prefetch=*/true);
    if (coalescedLoads_.size() > numLoads) {
      loads.push_back(std::static_pointer_cast<DirectCoalescedLoad>(coalescedLoads_.back()));
    }
    group.clear();
  }
  return loads;
}

void GlutenDirectBufferedInput::load(const LogType logType) {
  if (!lazyState_ || !lazyState_->hasLazyStreams()) {
    DirectBufferedInput::load(logType);
    return;
  }
  // After load, new requests cannot be merged into pre-load ones.
  auto requests = std::move(requests_);
  std::vector<LoadRequest*> eager;
  std::vector<LoadRequest*> deferred;
  for (auto& request : requests) {
    if (!request.trackingId.empty() && lazyState_->isDeferred(request.trackingId.id())) {
      deferred.push_back(&request);
    } else {
      eager.push_back(&request);
    }
  }
  auto byOffset = [](const LoadRequest* left, const LoadRequest* right) { return *left < *right; };
  std::sort(eager.begin(), eager.end(), byOffset);

  // Everything the scan needs now goes to the IO threads right away. The
  // deferred streams are not in this set, so an eager read never spans a
  // deferred chunk that lies between two eager ones.
  for (auto& load : planLoads(eager, groupRequests(eager, /*prefetch=*/true))) {
    prefetchLoad(load);
  }
  // One planned load per deferred stream, never coalesced across streams: the
  // first read of a stream before the hint must load that stream only and
  // mark that stream only.
  std::map<int32_t, std::vector<LoadRequest*>> deferredByStream;
  for (auto* request : deferred) {
    deferredByStream[request->trackingId.id()].push_back(request);
  }
  for (auto& [streamId, requests] : deferredByStream) {
    std::sort(requests.begin(), requests.end(), byOffset);
    for (auto& load : planLoads(requests, groupRequests(requests, /*prefetch=*/true))) {
      lazyState_->hold(load, streamId);
    }
  }
}

std::shared_ptr<DirectCoalescedLoad> GlutenDirectBufferedInput::coalescedLoad(const SeekableInputStream* stream) {
  auto load = DirectBufferedInput::coalescedLoad(stream);
  if (load && lazyState_ && load->state() == CoalescedLoad::State::kPlanned) {
    // The caller loads 'load' synchronously now; start the other held loads of
    // the same columns in the background so later row groups do not wait.
    for (auto& other : lazyState_->markNeededByRead(load.get())) {
      prefetchLoad(other);
    }
  }
  return load;
}

} // namespace gluten

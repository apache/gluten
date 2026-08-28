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

#include "velox/dwio/common/DirectBufferedInput.h"
#include "velox/dwio/common/ExecutorBarrier.h"

namespace gluten {

namespace detail {

// Owns the ExecutorBarrier that wraps the IO executor passed to
// DirectBufferedInput. DirectBufferedInput::readRegions() enqueues an
// AsyncLoadHolder closure per planned load, and that closure keeps a
// shared_ptr on the reader's MemoryPool. Cancelling a load only flips its
// state, it does not dequeue or destroy the closure, so the pool reference
// survives until the executor happens to drain that entry. Routing all the
// enqueues through a barrier lets the destructor wait for them explicitly.
//
// This must be listed as a base before DirectBufferedInput so that it is
// constructed first (the barrier pointer is handed to the base constructor)
// and destructed last (the barrier has to outlive any closure referring to
// it).
class ExecutorBarrierHolder {
 public:
  explicit ExecutorBarrierHolder(folly::Executor* executor)
      : rawExecutor_(executor), barrier_(makeBarrier(executor)) {}

 protected:
  // The unwrapped executor, to be handed to clones instead of this object's
  // barrier.
  folly::Executor* rawExecutor() const {
    return rawExecutor_;
  }

  facebook::velox::dwio::common::ExecutorBarrier* barrier() const {
    return barrier_.get();
  }

 private:
  static std::unique_ptr<facebook::velox::dwio::common::ExecutorBarrier> makeBarrier(folly::Executor* executor) {
    if (executor == nullptr) {
      return nullptr;
    }
    return std::make_unique<facebook::velox::dwio::common::ExecutorBarrier>(folly::getKeepAliveToken(executor));
  }

  folly::Executor* const rawExecutor_;
  const std::unique_ptr<facebook::velox::dwio::common::ExecutorBarrier> barrier_;
};

} // namespace detail

class GlutenDirectBufferedInput : private detail::ExecutorBarrierHolder,
                                 public facebook::velox::dwio::common::DirectBufferedInput {
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
      folly::F14FastMap<std::string, std::string> fileReadOps = {})
      : ExecutorBarrierHolder(executor),
        DirectBufferedInput(
            std::move(readFile),
            metricsLog,
            std::move(fileNum),
            std::move(tracker),
            std::move(groupId),
            std::move(ioStatistics),
            std::move(ioStats),
            barrier(),
            readerOptions,
            std::move(fileReadOps)) {}

  ~GlutenDirectBufferedInput() override {
    requests_.clear();
    // Cancel all the planned loads as soon as possible to avoid unnecessary IO.
    // Only kPlanned loads may be cancelled: cancel() overwrites the state
    // unconditionally, so cancelling a kLoading load would hide it from the
    // wait below while its IO is still in flight.
    for (auto& load : coalescedLoads_) {
      if (load->state() == facebook::velox::cache::CoalescedLoad::State::kPlanned) {
        load->cancel();
      }
    }
    // Ensure all the loadings can finish in the TableScan destructor to avoid the issue that the load is still running
    // when the VeloxMemoryManager used by the whole task is trying to destruct.
    for (auto& load : coalescedLoads_) {
      if (load->state() == facebook::velox::cache::CoalescedLoad::State::kLoading) {
        folly::SemiFuture<bool> waitFuture(false);
        if (!load->loadOrFuture(&waitFuture)) {
          auto& exec = folly::QueuedImmediateExecutor::instance();
          std::move(waitFuture).via(&exec).wait();
        }
      }
    }
    coalescedLoads_.clear();
    // The cancelled loads above are still referenced by the AsyncLoadHolder
    // closures queued on the executor, and those closures hold a shared_ptr on
    // the memory pool. Wait until the executor has run and destroyed them so
    // that the pool reference is released before this destructor returns,
    // instead of on an IO thread after the task and its memory manager are
    // gone.
    if (barrier() != nullptr) {
      try {
        barrier()->waitAll();
      } catch (const std::exception& e) {
        // waitAll() rethrows an exception raised by any of the loads. It must
        // not escape the destructor: the loads were cancelled anyway.
        LOG(WARNING) << "Async load failed while destructing GlutenDirectBufferedInput: " << e.what();
      }
    }
  }

  std::unique_ptr<facebook::velox::dwio::common::BufferedInput> clone() const override {
    // Pass the unwrapped executor: the clone has its own lifetime and must not
    // enqueue onto this object's barrier.
    return std::unique_ptr<facebook::velox::dwio::common::BufferedInput>(new GlutenDirectBufferedInput(
        input_, fileNum_, tracker_, groupId_, ioStatistics_, ioStats_, rawExecutor(), options_));
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
      const facebook::velox::io::ReaderOptions& readerOptions)
      : ExecutorBarrierHolder(executor),
        DirectBufferedInput(
            std::move(input),
            std::move(fileNum),
            std::move(tracker),
            std::move(groupId),
            std::move(ioStatistics),
            std::move(ioStats),
            barrier(),
            readerOptions) {}
};

} // namespace gluten

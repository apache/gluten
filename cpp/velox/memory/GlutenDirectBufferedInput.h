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

#include "velox/dwio/common/DirectBufferedInput.h"

namespace gluten {

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
      folly::F14FastMap<std::string, std::string> fileReadOps = {})
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
            std::move(fileReadOps)) {}

  ~GlutenDirectBufferedInput() override {
    requests_.clear();
    for (auto& load : coalescedLoads_) {
      if (load->state() == facebook::velox::cache::CoalescedLoad::State::kLoading) {
        folly::SemiFuture<bool> waitFuture(false);
        if (!load->loadOrFuture(&waitFuture)) {
          auto& exec = folly::QueuedImmediateExecutor::instance();
          std::move(waitFuture).via(&exec).wait();
        }
      }
      load->cancel();
    }
    coalescedLoads_.clear();
  }
};

} // namespace gluten

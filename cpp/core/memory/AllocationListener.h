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

#include <algorithm>
#include <memory>
#include <mutex>

namespace gluten {

class AllocationListener {
 public:
  static std::unique_ptr<AllocationListener> noop();

  virtual ~AllocationListener() = default;

  // Value of diff can be either positive or negative
  virtual void allocationChanged(int64_t diff) = 0;

  virtual int64_t currentBytes() {
    return 0;
  }

  virtual int64_t peakBytes() {
    return 0;
  }

 protected:
  AllocationListener() = default;
};

/// Memory changes will be round to specified block size which aim to decrease delegated listener calls.
// The class must be thread safe
class BlockAllocationListener final : public AllocationListener {
 public:
  BlockAllocationListener(AllocationListener* delegated, int64_t blockSize)
      : delegated_(delegated), blockSize_(blockSize) {}

  void allocationChanged(int64_t diff) override {
    if (diff == 0) {
      return;
    }
    int64_t granted = reserve(diff);
    if (granted == 0) {
      return;
    }
    try {
      delegated_->allocationChanged(granted);
    } catch (const std::exception&) {
      reserve(-diff);
      throw;
    }
  }

  int64_t currentBytes() override {
    return reservationBytes_;
  }

  int64_t peakBytes() override {
    return peakBytes_;
  }

 private:
  inline int64_t reserve(int64_t diff) {
    std::lock_guard<std::mutex> lock(mutex_);
    usedBytes_ += diff;
    int64_t newBlockCount;
    if (usedBytes_ == 0) {
      newBlockCount = 0;
    } else {
      // ceil to get the required block number
      newBlockCount = (usedBytes_ - 1) / blockSize_ + 1;
    }
    int64_t bytesGranted = (newBlockCount - blocksReserved_) * blockSize_;
    blocksReserved_ = newBlockCount;
    peakBytes_ = std::max(peakBytes_, usedBytes_);
    return bytesGranted;
  }

  AllocationListener* const delegated_;
  const uint64_t blockSize_;
  int64_t blocksReserved_{0L};
  int64_t usedBytes_{0L};
  int64_t peakBytes_{0L};
  int64_t reservationBytes_{0L};

  mutable std::mutex mutex_;
};

} // namespace gluten

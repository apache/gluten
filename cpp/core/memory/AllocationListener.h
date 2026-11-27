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

  struct Change {
    const int64_t diff;
    const bool afterFailedReservation{false};
    // Set before propagating an error if the delegate has already applied the charge.
    bool committed{false};
  };

  virtual void applyChange(Change& change) {
    allocationChanged(change.diff);
    change.committed = true;
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
    Change change{diff};
    applyChange(change);
  }

  void applyChange(Change& change) override {
    if (change.diff == 0) {
      change.committed = true;
      return;
    }
    Change charge{reserve(change.diff), change.afterFailedReservation};
    try {
      if (charge.diff != 0) {
        delegated_->applyChange(charge);
      }
    } catch (...) {
      std::lock_guard<std::mutex> lock(mutex_);
      // Growth never reached the allocator. A free or shrink already did.
      if (change.diff > 0) {
        usedBytes_ -= change.diff;
      }
      if (!charge.committed) {
        blocksReserved_ -= charge.diff / blockSize_;
      }
      change.committed = change.diff < 0 && charge.committed;
      throw;
    }
    change.committed = true;
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
    if (diff > 0) {
      // Reuse blocks retained after a failed repayment. Growth must not initiate a refund.
      newBlockCount = std::max(newBlockCount, blocksReserved_);
    }
    int64_t bytesGranted = (newBlockCount - blocksReserved_) * blockSize_;
    blocksReserved_ = newBlockCount;
    peakBytes_ = std::max(peakBytes_, usedBytes_);
    return bytesGranted;
  }

  AllocationListener* const delegated_;
  const int64_t blockSize_;
  int64_t blocksReserved_{0L};
  int64_t usedBytes_{0L};
  int64_t peakBytes_{0L};
  int64_t reservationBytes_{0L};

  mutable std::mutex mutex_;
};

} // namespace gluten

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
#include <stdexcept>

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
    // A confirmed change, even if an error still needs to propagate. False is not proof of rejection.
    bool committed{false};
  };

  virtual void applyChange(Change& change) {
    allocationChanged(change.diff);
    change.committed = true;
  }

  // Retire disputed debits only if the owner can confirm none of this charge remains.
  // Do not change the account when returning false or throwing.
  virtual bool reconcileUnreserve(int64_t /*bytes*/) {
    return false;
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
      if (change.diff > 0) {
        usedBytes_ -= change.diff; // Growth never reached the allocator.
        if (!charge.committed) {
          blocksReserved_ -= charge.diff / blockSize_;
        }
      } else if (!charge.committed) {
        // The buffer was freed. A throw is not proof that the owner kept the charge.
        disputedBytes_ -= charge.diff;
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
    std::unique_lock<std::mutex> lock(mutex_);
    if (diff > 0 && disputedBytes_ != 0) {
      reconcile(lock);
    }
    usedBytes_ += diff;
    const auto needed = usedBytes_ == 0 ? 0 : (usedBytes_ - 1) / blockSize_ + 1;
    // A free must not initiate a reservation, or growth a repayment.
    const auto blocks = diff > 0 ? std::max(needed, blocksReserved_) : std::min(needed, blocksReserved_);
    const auto granted = (blocks - blocksReserved_) * blockSize_;
    blocksReserved_ = blocks;
    peakBytes_ = std::max(peakBytes_, usedBytes_);
    return granted;
  }

  void reconcile(std::unique_lock<std::mutex>& lock) {
    if (usedBytes_ != 0 || reconciling_) {
      throw std::runtime_error("Cannot grow an active memory pool with an unconfirmed repayment");
    }
    const auto disputed = disputedBytes_;
    reconciling_ = true;
    lock.unlock(); // Never call into Java under the block mutex, including during recovery.
    bool settled;
    try {
      settled = delegated_->reconcileUnreserve(disputed);
    } catch (...) {
      lock.lock();
      reconciling_ = false;
      throw;
    }
    lock.lock();
    reconciling_ = false;
    if (settled) {
      disputedBytes_ -= disputed;
    }
    if (disputedBytes_ != 0) {
      throw std::runtime_error("Cannot reserve memory: the owner has not confirmed the disputed repayment");
    }
  }

  AllocationListener* const delegated_;
  const int64_t blockSize_;
  int64_t blocksReserved_{0};
  int64_t disputedBytes_{0};
  bool reconciling_{false};
  int64_t usedBytes_{0L};
  int64_t peakBytes_{0L};
  int64_t reservationBytes_{0L};

  mutable std::mutex mutex_;
};

} // namespace gluten

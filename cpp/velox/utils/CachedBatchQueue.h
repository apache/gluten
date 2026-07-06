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
#include "velox/common/base/Exceptions.h"

#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>

namespace gluten {

template <typename T>
class CachedBatchQueue {
 public:
  explicit CachedBatchQueue(const int64_t capacity) : capacity_(capacity) {}

  void put(std::shared_ptr<T> batch) {
    std::unique_lock<std::mutex> lock(mtx_);

    const auto batchSize = batch->numBytes();
    VELOX_CHECK_LE(batchSize, capacity_, "Batch size exceeds queue capacity");

    notFull_.wait(lock, [&]() { return noMoreBatches_ || totalSize_ + batchSize <= capacity_; });
    if (noMoreBatches_) {
      LOG(WARNING) << "Discard batch due to calling put() after noMoreBatches().";
      return;
    }

    queue_.push(std::move(batch));
    totalSize_ += batchSize;

    notEmpty_.notify_one();
  }

  std::shared_ptr<T> get() {
    std::unique_lock<std::mutex> lock(mtx_);
    notEmpty_.wait(lock, [&]() { return noMoreBatches_ || !queue_.empty(); });

    if (exception_) {
      std::rethrow_exception(exception_);
    }

    if (queue_.empty()) {
      return nullptr;
    }
    auto batch = std::move(queue_.front());
    DLOG(INFO) << "CachedBatchQueue get(): Queue length=" << queue_.size() << ", queue size in bytes=" << totalSize_
               << ", current batch size in bytes=" << batch->numBytes();

    queue_.pop();
    totalSize_ -= batch->numBytes();

    notFull_.notify_one();
    return batch;
  }

  void noMoreBatches() {
    std::lock_guard<std::mutex> lock(mtx_);
    if (noMoreBatches_) {
      return;
    }
    noMoreBatches_ = true;
    notFull_.notify_all();
    notEmpty_.notify_all();
  }

  /// Returns true if another producer has already set an exception.
  /// Producers can poll this to abort early without waiting for put() to unblock.
  bool hasException() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return exception_ != nullptr;
  }

  /// Called by a producer thread to propagate an exception to the consumer.
  /// Wakes up any blocked get() call, which will rethrow the exception.
  void setException(std::exception_ptr e) {
    std::lock_guard<std::mutex> lock(mtx_);
    if (!exception_) {
      exception_ = std::move(e);
    }
    noMoreBatches_ = true;
    notFull_.notify_all();
    notEmpty_.notify_all();
  }

 private:
  int64_t size() const {
    return totalSize_;
  }

  bool empty() const {
    return queue_.empty();
  }

  int64_t capacity_;
  int64_t totalSize_{0};
  bool noMoreBatches_{false};
  std::exception_ptr exception_{nullptr};

  std::queue<std::shared_ptr<T>> queue_;

  mutable std::mutex mtx_;
  std::condition_variable notEmpty_;
  std::condition_variable notFull_;
};

} // namespace gluten

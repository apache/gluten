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

#include "compute/ResultIterator.h"
#include "memory/AllocationListener.h"
#include "memory/Reclaimable.h"

namespace gluten {

// Instance with limited capacity, used by tests and benchmarks.
class TestAllocationListener final : public AllocationListener {
 public:
  TestAllocationListener() = default;

  void setThrowIfOOM(bool throwIfOOM) {
    throwIfOOM_ = throwIfOOM;
  }

  void updateLimit(uint64_t limit) {
    limit_ = limit;
  }

  void setIterator(ResultIterator* iterator) {
    iterator_ = iterator;
  }

  void setShuffleWriter(Reclaimable* shuffleWriter) {
    shuffleWriter_ = shuffleWriter;
  }

  void allocationChanged(int64_t diff) override;

  int64_t currentBytes() override;

  int64_t reclaimedBytes() const;

  void reset();

 private:
  bool throwIfOOM_{false};

  uint64_t usedBytes_{0L};
  uint64_t reclaimedBytes_{0L};

  uint64_t limit_{std::numeric_limits<uint64_t>::max()};
  ResultIterator* iterator_{nullptr};
  Reclaimable* shuffleWriter_{nullptr};
};

} // namespace gluten

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
#include <vector>
#include "ColumnarBatchIterator.h"

// Forward declarations
namespace substrait {
class ReadRel_LocalFiles;
}

namespace gluten {

// Forward declaration
class ResultIterator;

/// Abstract base class for iterators that support dynamic split management.
/// Provides APIs for adding splits after iterator creation and signaling completion.
class SplitAwareColumnarBatchIterator : public ColumnarBatchIterator {
 public:
  SplitAwareColumnarBatchIterator() = default;
  virtual ~SplitAwareColumnarBatchIterator() = default;

  /// Add iterator-based splits from input iterators.
  virtual void addIteratorSplits(const std::vector<std::shared_ptr<ResultIterator>>& inputIterators) = 0;

  /// Signal that no more splits will be added to this iterator.
  /// This must be called after all splits have been added to ensure proper task completion.
  virtual void noMoreSplits() = 0;

  /// Request a barrier in task execution. This signals the task to finish processing
  /// all currently queued splits and drain all stateful operators before continuing.
  /// Enables task reuse and deterministic execution for streaming workloads.
  /// @see https://facebookincubator.github.io/velox/develop/task-barrier.html
  virtual void requestBarrier() = 0;
};

} // namespace gluten

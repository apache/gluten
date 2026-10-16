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

#include "velox/exec/HashTable.h"

namespace gluten {

/// The hash table of a broadcast hash join, as handed to Java through an
/// ObjectStore handle.
class BroadcastHashTable {
 public:
  /// 'owner' is kept alive as long as this, e.g. the QueryCtx of the task that
  /// built 'table'.
  BroadcastHashTable(
      std::shared_ptr<facebook::velox::exec::BaseHashTable> table,
      bool joinHasNullKeys,
      int64_t memoryUsage,
      std::shared_ptr<void> owner = nullptr)
      : owner_(std::move(owner)),
        table_(std::move(table)),
        joinHasNullKeys_(joinHasNullKeys),
        memoryUsage_(memoryUsage) {}

  const std::shared_ptr<facebook::velox::exec::BaseHashTable>& hashTable() const {
    return table_;
  }

  bool joinHasNullKeys() const {
    return joinHasNullKeys_;
  }

  /// Retained bytes of the table, including the tables merged by a parallel
  /// build.
  int64_t memoryUsage() const {
    return memoryUsage_;
  }

 private:
  // Declared before 'table_' so that it is released after it.
  const std::shared_ptr<void> owner_;
  const std::shared_ptr<facebook::velox::exec::BaseHashTable> table_;
  const bool joinHasNullKeys_;
  const int64_t memoryUsage_;
};

} // namespace gluten

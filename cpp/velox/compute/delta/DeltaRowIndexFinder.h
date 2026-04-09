/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "velox/common/file/FileSystems.h"
#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Task.h"
#include "velox/type/Type.h"

namespace gluten::delta {

using namespace facebook::velox;
using namespace facebook::velox::connector;

/// Finds row indices matching a filter condition in a Parquet file.
/// Uses Velox's native execution engine for vectorized filtering.
///
/// This component is critical for Delta Lake Merge-on-Read (MoR) operations,
/// enabling native row index discovery without falling back to Spark.
///
/// Usage:
///   DeltaRowIndexFinder finder(pool, config);
///   auto indices = finder.findMatchingRows(filePath, filter, schema);
///   // indices contains 0-based row indices of matching rows
class DeltaRowIndexFinder {
 public:
  /// Configuration for row index finding
  struct Config {
    /// Maximum number of rows to process in a single batch
    uint32_t batchSize = 10000;

    /// Number of threads for parallel execution
    uint32_t numThreads = 1;

    /// Whether to use dictionary encoding for filters
    bool useDictionaryFilters = true;

    /// Memory limit for the operation (bytes)
    uint64_t memoryLimit = 1ULL << 30; // 1GB default
  };

  DeltaRowIndexFinder(
      memory::MemoryPool* pool,
      const Config& config);

  /// Find row indices matching the filter condition.
  /// Returns a vector of row indices (0-based).
  ///
  /// @param filePath Path to the Parquet file
  /// @param filter Filter expression (Velox expression tree)
  /// @param schema File schema
  /// @return Vector of matching row indices
  std::vector<uint64_t> findMatchingRows(
      const std::string& filePath,
      const std::shared_ptr<const core::ITypedExpr>& filter,
      const std::shared_ptr<const RowType>& schema);

  /// Find row indices matching the filter condition (batch mode).
  /// Processes multiple files in parallel for better throughput.
  ///
  /// @param files Vector of (filePath, filter, schema) tuples
  /// @return Map of filePath -> row indices
  std::unordered_map<std::string, std::vector<uint64_t>>
  findMatchingRowsBatch(
      const std::vector<std::tuple<
          std::string,
          std::shared_ptr<const core::ITypedExpr>,
          std::shared_ptr<const RowType>>>& files);

  /// Get statistics about the last operation
  struct Stats {
    uint64_t rowsScanned = 0;
    uint64_t rowsMatched = 0;
    uint64_t bytesRead = 0;
    uint64_t executionTimeMs = 0;
  };

  const Stats& getStats() const {
    return stats_;
  }

 private:
  /// Create a Velox plan for scanning and filtering
  std::shared_ptr<const core::PlanNode> createScanPlan(
      const std::shared_ptr<const core::ITypedExpr>& filter,
      const std::shared_ptr<const RowType>& schema,
      core::PlanNodeId& scanNodeId);

  /// Execute the plan and collect row indices
  std::vector<uint64_t> executeAndCollectIndices(
      const std::shared_ptr<const core::PlanNode>& plan,
      const std::string& filePath,
      const core::PlanNodeId& scanNodeId);

  /// Create a task for executing the plan
  std::shared_ptr<exec::Task> createTask(
      const std::shared_ptr<const core::PlanNode>& plan);

  memory::MemoryPool* pool_;
  std::shared_ptr<core::QueryCtx> queryCtx_;
  Config config_;
  Stats stats_;
};

} // namespace gluten::delta

// Made with Bob

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

#include "compute/delta/DeltaRowIndexFinder.h"

#include <chrono>

#include "velox/common/base/Exceptions.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/core/Expressions.h"
#include "velox/vector/SimpleVector.h"

namespace gluten::delta {

namespace {
constexpr const char* kHiveConnectorId = "test-hive";
constexpr const char* kNativeRowIndexColumnName = "__native_row_index__";
} // namespace

DeltaRowIndexFinder::DeltaRowIndexFinder(
    memory::MemoryPool* pool,
    const Config& config)
    : pool_(pool), config_(config), stats_{} {
  VELOX_CHECK_NOT_NULL(pool_, "Memory pool cannot be null");
  queryCtx_ = core::QueryCtx::create(
      nullptr,
      core::QueryConfig{{}},
      {},
      cache::AsyncDataCache::getInstance(),
      nullptr,
      nullptr,
      "delta-row-index-finder");
}

std::vector<uint64_t> DeltaRowIndexFinder::findMatchingRows(
    const std::string& filePath,
    const std::shared_ptr<const core::ITypedExpr>& filter,
    const std::shared_ptr<const RowType>& schema) {
  auto startTime = std::chrono::steady_clock::now();
  stats_ = Stats{};

  try {
    core::PlanNodeId scanNodeId;
    auto plan = createScanPlan(filter, schema, scanNodeId);
    auto indices = executeAndCollectIndices(plan, filePath, scanNodeId);

    auto endTime = std::chrono::steady_clock::now();
    stats_.executionTimeMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            endTime - startTime)
            .count();
    stats_.rowsMatched = indices.size();
    return indices;
  } catch (const std::exception& e) {
    VELOX_FAIL(
        "Failed to find matching rows in file {}: {}",
        filePath,
        e.what());
  }
}

std::shared_ptr<const core::PlanNode> DeltaRowIndexFinder::createScanPlan(
    const std::shared_ptr<const core::ITypedExpr>& filter,
    const std::shared_ptr<const RowType>& schema,
    core::PlanNodeId& scanNodeId) {
  connector::ColumnHandleMap assignments;
  std::vector<std::string> outputNames;
  std::vector<TypePtr> outputTypes;

  outputNames.reserve(schema->size() + 1);
  outputTypes.reserve(schema->size() + 1);

  for (size_t i = 0; i < schema->size(); ++i) {
    auto columnName = schema->nameOf(i);
    auto columnType = schema->childAt(i);

    assignments[columnName] = std::make_shared<connector::hive::HiveColumnHandle>(
        columnName,
        connector::hive::HiveColumnHandle::ColumnType::kRegular,
        columnType,
        columnType);
    outputNames.emplace_back(columnName);
    outputTypes.emplace_back(columnType);
  }

  assignments[kNativeRowIndexColumnName] = std::make_shared<connector::hive::HiveColumnHandle>(
      kNativeRowIndexColumnName,
      connector::hive::HiveColumnHandle::ColumnType::kRowIndex,
      BIGINT(),
      BIGINT());
  outputNames.emplace_back(kNativeRowIndexColumnName);
  outputTypes.emplace_back(BIGINT());

  auto outputType = ROW(std::move(outputNames), std::move(outputTypes));
  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      kHiveConnectorId,
      "delta_row_index_finder",
      common::SubfieldFilters{},
      filter,
      schema);

  auto tableScanNode = core::TableScanNode::Builder()
                           .id("0")
                           .outputType(outputType)
                           .tableHandle(tableHandle)
                           .assignments(assignments)
                           .build();
  scanNodeId = tableScanNode->id();
  return tableScanNode;
}

std::vector<uint64_t> DeltaRowIndexFinder::executeAndCollectIndices(
    const std::shared_ptr<const core::PlanNode>& plan,
    const std::string& filePath,
    const core::PlanNodeId& scanNodeId) {
  auto task = createTask(plan);

  auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
      kHiveConnectorId,
      filePath,
      dwio::common::FileFormat::PARQUET);
  task->addSplit(scanNodeId, exec::Split{connectorSplit});
  task->noMoreSplits(scanNodeId);

  std::vector<uint64_t> indices;
  while (auto result = task->next()) {
    stats_.rowsScanned += result->size();
    auto* rowIndexVector =
        result->childAt(result->childrenSize() - 1)->as<SimpleVector<int64_t>>();
    for (vector_size_t i = 0; i < result->size(); ++i) {
      indices.push_back(rowIndexVector->valueAt(i));
    }
  }

  return indices;
}

std::shared_ptr<exec::Task> DeltaRowIndexFinder::createTask(
    const std::shared_ptr<const core::PlanNode>& plan) {
  // This finder consumes results via Task::next(), which is a serial-only API in
  // the current Velox task model. Keep per-file scans serial here and rely on
  // the caller to parallelize across files.
  return exec::Task::create(
      "delta-row-finder-task",
      core::PlanFragment(plan),
      0,
      queryCtx_,
      exec::Task::ExecutionMode::kSerial,
      exec::Consumer{});
}

std::unordered_map<std::string, std::vector<uint64_t>>
DeltaRowIndexFinder::findMatchingRowsBatch(
    const std::vector<std::tuple<
        std::string,
        std::shared_ptr<const core::ITypedExpr>,
        std::shared_ptr<const RowType>>>& files) {
  std::unordered_map<std::string, std::vector<uint64_t>> results;
  for (const auto& [filePath, filter, schema] : files) {
    try {
      results[filePath] = findMatchingRows(filePath, filter, schema);
    } catch (const std::exception& e) {
      LOG(ERROR) << "Failed to process file " << filePath << ": " << e.what();
      results[filePath] = {};
    }
  }
  return results;
}

} // namespace gluten::delta

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
#include "WholeStageResultIterator.h"
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/thread_factory/NamedThreadFactory.h>
#include <folly/json.h>
#include <folly/system/ThreadName.h>
#include <optional>
#include "VeloxBackend.h"
#include "VeloxPlanConverter.h"
#include "VeloxRuntime.h"
#include "compute/delta/DeltaConnector.h"
#include "compute/delta/DeltaSplit.h"
#include "compute/delta/DeltaSplitInfo.h"
#include "config/VeloxConfig.h"
#include "utils/ConfigExtractor.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/functions/sparksql/SparkQueryConfig.h"
#ifdef GLUTEN_ENABLE_GPU
#include <cudf/io/types.hpp>
#include "cudf/GpuLock.h"
#include "velox/experimental/cudf/CudfConfig.h"
#include "velox/experimental/cudf/connectors/hive/CudfHiveConnectorSplit.h"
#include "velox/experimental/cudf/exec/ToCudf.h"
#endif
#include "operators/plannodes/RowVectorStream.h"

using namespace facebook;
using facebook::velox::functions::sparksql::SparkQueryConfig;

namespace gluten {

namespace {

// others
const std::string kHiveDefaultPartition = "__HIVE_DEFAULT_PARTITION__";
const std::string kDeltaTableFormat = "delta";

const velox::core::TableScanNode* findTableScanNodeById(
    const std::shared_ptr<const velox::core::PlanNode>& planNode,
    const velox::core::PlanNodeId& nodeId) {
  if (planNode == nullptr) {
    return nullptr;
  }

  if (planNode->id() == nodeId) {
    return dynamic_cast<const velox::core::TableScanNode*>(planNode.get());
  }

  for (const auto& source : planNode->sources()) {
    if (const auto* found = findTableScanNodeById(source, nodeId)) {
      return found;
    }
  }
  return nullptr;
}

std::string connectorIdForScanNode(
    const std::shared_ptr<const velox::core::PlanNode>& planNode,
    const velox::core::PlanNodeId& nodeId) {
  const auto* tableScanNode = findTableScanNodeById(planNode, nodeId);
  if (tableScanNode == nullptr) {
    return "";
  }
  return tableScanNode->tableHandle()->connectorId();
}

} // namespace

WholeStageResultIterator::WholeStageResultIterator(
    VeloxMemoryManager* memoryManager,
    const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode,
    const std::vector<facebook::velox::core::PlanNodeId>& scanNodeIds,
    const std::vector<std::shared_ptr<SplitInfo>>& scanInfos,
    const std::vector<facebook::velox::core::PlanNodeId>& streamIds,
    folly::Executor* executor,
    folly::Executor* spillExecutor,
    VeloxConnectorIds connectorIds,
    const std::string spillDir,
    const std::shared_ptr<facebook::velox::config::ConfigBase>& veloxCfg,
    const SparkTaskInfo& taskInfo)
    : memoryManager_(memoryManager),
      veloxCfg_(veloxCfg),
#ifdef GLUTEN_ENABLE_GPU
      enableCudf_(veloxCfg_->get<bool>(kCudfEnabled, kCudfEnabledDefault)),
#endif
      taskInfo_(taskInfo),
      executor_(executor),
      veloxPlan_(planNode),
      spillExecutor_(spillExecutor),
      connectorIds_(std::move(connectorIds)),
      scanNodeIds_(scanNodeIds),
      scanInfos_(scanInfos),
      streamIds_(streamIds) {
  spillStrategy_ = veloxCfg_->get<std::string>(kSpillStrategy, kSpillStrategyDefaultValue);
  getOrderedNodeIds(veloxPlan_, orderedNodeIds_);

  auto fileSystem = velox::filesystems::getFileSystem(spillDir, nullptr);
  GLUTEN_CHECK(fileSystem != nullptr, "File System for spilling is null!");
  fileSystem->mkdir(spillDir);

  std::unordered_set<velox::core::PlanNodeId> emptySet;
  const bool serialExecution = true;

  facebook::velox::exec::CursorParameters params;
  params.planNode = planNode;
  params.destination = 0;
  params.maxDrivers = 1;
  params.queryCtx = createNewVeloxQueryCtx();
  params.executionStrategy = velox::core::ExecutionStrategy::kUngrouped;
  params.groupedExecutionLeafNodeIds = std::move(emptySet);
  params.numSplitGroups = 1;
  params.spillDirectory = spillDir;
  params.serialExecution = serialExecution;
  params.copyResult = false;
  params.outputPool = memoryManager_->getLeafMemoryPool();
  cursor_ = velox::exec::TaskCursor::create(params);
  task_ = cursor_->task().get();
  if (!task_->supportSerialExecutionMode()) {
    throw std::runtime_error("Task doesn't support single threaded execution: " + planNode->toString());
  }

  // Generate splits for all scan nodes.
  splits_.reserve(scanInfos.size());
  if (scanNodeIds.size() != scanInfos.size()) {
    throw std::runtime_error("Invalid scan information.");
  }

  for (size_t scanInfoIdx = 0; scanInfoIdx < scanInfos.size(); ++scanInfoIdx) {
    const auto& scanInfo = scanInfos[scanInfoIdx];
    // Get the information for TableScan.
    // Partition index in scan info is not used.
    const auto& paths = scanInfo->paths;
    const auto& starts = scanInfo->starts;
    const auto& lengths = scanInfo->lengths;
    const auto& properties = scanInfo->properties;
    const auto& format = scanInfo->format;
    const auto& partitionColumns = scanInfo->partitionColumns;
    const auto& metadataColumns = scanInfo->metadataColumns;
    const auto scanNodeConnectorId = connectorIdForScanNode(veloxPlan_, scanNodeIds_[scanInfoIdx]);
    const auto deltaSplitInfo = std::dynamic_pointer_cast<DeltaSplitInfo>(scanInfo);
    const bool isDeltaScan = scanNodeConnectorId == connectorIds_.delta || deltaSplitInfo != nullptr;
#ifdef GLUTEN_ENABLE_GPU
    // Under the pre-condition that all the split infos has same partition column and format.
    const auto canUseCudfConnector = scanInfo->canUseCudfConnector();
#endif
    std::vector<std::shared_ptr<velox::connector::ConnectorSplit>> connectorSplits;
    connectorSplits.reserve(paths.size());
    for (int idx = 0; idx < paths.size(); idx++) {
      auto metadataColumn = metadataColumns[idx];
      std::unordered_map<std::string, std::optional<std::string>> partitionKeys;
      if (!partitionColumns.empty()) {
        auto partitionColumn = partitionColumns[idx];
        constructPartitionColumns(partitionKeys, partitionColumn);
      }

      std::shared_ptr<velox::connector::ConnectorSplit> split;
      if (auto icebergSplitInfo = std::dynamic_pointer_cast<IcebergSplitInfo>(scanInfo)) {
        // Set Iceberg split.
        std::unordered_map<std::string, std::string> customSplitInfo{{"table_format", "hive-iceberg"}};
        auto deleteFiles = icebergSplitInfo->deleteFilesVec[idx];
        split = std::make_shared<velox::connector::hive::iceberg::HiveIcebergSplit>(
            connectorIds_.iceberg,
            paths[idx],
            format,
            starts[idx],
            lengths[idx],
            partitionKeys,
            std::nullopt,
            customSplitInfo,
            nullptr,
            true,
            deleteFiles,
            metadataColumn,
            properties[idx]);
      } else if (isDeltaScan) {
        std::unordered_map<std::string, std::string> customSplitInfo{{"table_format", kDeltaTableFormat}};
        std::optional<gluten::delta::DeltaDeletionVectorDescriptor> deletionVector = std::nullopt;
        auto rowIndexFilterType = gluten::delta::DeltaRowIndexFilterType::kKeepAll;
        if (deltaSplitInfo != nullptr) {
          VELOX_USER_CHECK_LT(idx, deltaSplitInfo->deletionVectors.size());
          VELOX_USER_CHECK_LT(idx, deltaSplitInfo->rowIndexFilterTypes.size());
          deletionVector = deltaSplitInfo->deletionVectors[idx];
          rowIndexFilterType = deltaSplitInfo->rowIndexFilterTypes[idx];
        }
        split = std::make_shared<gluten::delta::HiveDeltaSplit>(
            connectorIds_.delta,
            paths[idx],
            format,
            starts[idx],
            lengths[idx],
            partitionKeys,
            std::nullopt,
            customSplitInfo,
            nullptr,
            std::unordered_map<std::string, std::string>(),
            true,
            deletionVector,
            std::nullopt,
            rowIndexFilterType,
            metadataColumn,
            properties[idx]);
      } else {
        auto connectorId = connectorIds_.hive;
#ifdef GLUTEN_ENABLE_GPU
        if (connectorId == connectorIds_.hive && canUseCudfConnector && enableCudf_ &&
            veloxCfg_->get<bool>(kCudfEnableTableScan, kCudfEnableTableScanDefault)) {
          connectorId = connectorIds_.cudfHive;
        }
#endif
        split = std::make_shared<velox::connector::hive::HiveConnectorSplit>(
            connectorId,
            paths[idx],
            format,
            starts[idx],
            lengths[idx],
            partitionKeys,
            std::nullopt /*tableBucketName*/,
            std::unordered_map<std::string, std::string>(),
            nullptr,
            std::unordered_map<std::string, std::string>(),
            0,
            true,
            metadataColumn,
            properties[idx]);
      }
      connectorSplits.emplace_back(split);
    }

    std::vector<velox::exec::Split> scanSplits;
    scanSplits.reserve(connectorSplits.size());
    for (const auto& connectorSplit : connectorSplits) {
      // Bucketed group id (-1 means 'none').
      int32_t groupId = -1;
      scanSplits.emplace_back(velox::exec::Split(folly::copy(connectorSplit), groupId));
    }
    splits_.emplace_back(scanSplits);
  }
}

std::shared_ptr<velox::core::QueryCtx> WholeStageResultIterator::createNewVeloxQueryCtx() {
  std::unordered_map<std::string, std::shared_ptr<velox::config::ConfigBase>> connectorConfigs;
  auto hiveSessionConfig = createHiveConnectorSessionConfig(veloxCfg_);
  connectorConfigs[connectorIds_.hive] = hiveSessionConfig;
  connectorConfigs[connectorIds_.iceberg] = hiveSessionConfig;
  connectorConfigs[connectorIds_.delta] = hiveSessionConfig;
  connectorConfigs[connectorIds_.iterator] = hiveSessionConfig;
#ifdef GLUTEN_ENABLE_GPU
  if (!connectorIds_.cudfHive.empty()) {
    connectorConfigs[connectorIds_.cudfHive] = hiveSessionConfig;
  }
#endif
  std::shared_ptr<velox::core::QueryCtx> ctx = velox::core::QueryCtx::create(
      executor_,
      facebook::velox::core::QueryConfig{getQueryContextConf()},
      connectorConfigs,
      gluten::VeloxBackend::get()->getAsyncDataCache(),
      memoryManager_->getAggregateMemoryPool(),
      spillExecutor_,
      fmt::format(
          "Gluten_Stage_{}_TID_{}_VTID_{}",
          std::to_string(taskInfo_.stageId),
          std::to_string(taskInfo_.taskId),
          std::to_string(taskInfo_.vId)));
  return ctx;
}

std::shared_ptr<ColumnarBatch> WholeStageResultIterator::next() {
  while (true) {
    if (!cursor_->moveNext()) {
      return nullptr;
    }
    RowVectorPtr vector = cursor_->current();
    GLUTEN_CHECK(vector != nullptr, "Cursor returned null vector.");
    uint64_t numRows = vector->size();
    if (numRows == 0) {
      continue;
    }
    {
      ScopedTimer timer(&loadLazyVectorTime_);
      for (auto& child : vector->children()) {
        child->loadedVector();
      }
    }
    return std::make_shared<VeloxColumnarBatch>(vector);
  }
}

int64_t WholeStageResultIterator::spillFixedSize(int64_t size) {
  auto pool = memoryManager_->getAggregateMemoryPool();
  std::string poolName{pool->root()->name() + "/" + pool->name()};
  std::string logPrefix{"Spill[" + poolName + "]: "};
  int64_t shrunken = memoryManager_->shrink(size);
  if (spillStrategy_ == "auto") {
    int64_t remaining = size - shrunken;
    LOG(INFO) << fmt::format("{} trying to request spill for {}.", logPrefix, velox::succinctBytes(remaining));
    auto mm = memoryManager_->getMemoryManager();
    uint64_t spilledOut = mm->arbitrator()->shrinkCapacity(remaining); // this conducts spill
    uint64_t total = shrunken + spilledOut;
    LOG(INFO) << fmt::format(
        "{} successfully reclaimed total {} with shrunken {} and spilled {}.",
        logPrefix,
        velox::succinctBytes(total),
        velox::succinctBytes(shrunken),
        velox::succinctBytes(spilledOut));
    return total;
  }
  LOG(WARNING) << "Spill-to-disk was disabled since " << kSpillStrategy << " was not configured.";
  VLOG(2) << logPrefix << "Successfully reclaimed total " << shrunken << " bytes.";
  return shrunken;
}

void WholeStageResultIterator::getOrderedNodeIds(
    const std::shared_ptr<const velox::core::PlanNode>& planNode,
    std::vector<velox::core::PlanNodeId>& nodeIds) {
  bool isProjectNode = (std::dynamic_pointer_cast<const velox::core::ProjectNode>(planNode) != nullptr);
  bool isLocalExchangeNode = (std::dynamic_pointer_cast<const velox::core::LocalPartitionNode>(planNode) != nullptr);
  const auto& sourceNodes = planNode->sources();
  if (isProjectNode) {
    GLUTEN_CHECK(sourceNodes.size() == 1, "Illegal state");
    const auto sourceNode = sourceNodes.at(0);
    // Filter over Project are mapped into FilterProject operator in Velox.
    // Metrics are all applied on Project node, and the metrics for Filter node
    // do not exist.
    if (std::dynamic_pointer_cast<const velox::core::FilterNode>(sourceNode)) {
      omittedNodeIds_.insert(sourceNode->id());
    }
    getOrderedNodeIds(sourceNode, nodeIds);
    nodeIds.emplace_back(planNode->id());
    return;
  }

  if (isLocalExchangeNode) {
    // LocalPartition was interpreted as LocalPartition + LocalExchange + 2 fake projects (optional) as children
    // in SubstraitToVeloxPlan. So we only fetch metrics from the root node.
    for (const auto& source : planNode->sources()) {
      const auto projectedChild = std::dynamic_pointer_cast<const velox::core::ProjectNode>(source);
      if (projectedChild != nullptr) {
        const auto projectSources = projectedChild->sources();
        GLUTEN_CHECK(projectSources.size() == 1, "Illegal state");
        const auto projectSource = projectSources.at(0);
        getOrderedNodeIds(projectSource, nodeIds);
      } else {
        getOrderedNodeIds(source, nodeIds);
      }
    }
    if (planNode->sources().size() == 2) {
      // The LocalPartition maps to a concrete Spark native union transformer operator.
      nodeIds.emplace_back(planNode->id());
    }
    return;
  }

  for (const auto& sourceNode : sourceNodes) {
    // Post-order traversal.
    getOrderedNodeIds(sourceNode, nodeIds);
  }
  nodeIds.emplace_back(planNode->id());
}

void WholeStageResultIterator::constructPartitionColumns(
    std::unordered_map<std::string, std::optional<std::string>>& partitionKeys,
    const std::unordered_map<std::string, std::string>& map) {
  for (const auto& partitionColumn : map) {
    auto key = partitionColumn.first;
    const auto value = partitionColumn.second;
    if (!veloxCfg_->get<bool>(kCaseSensitive, false)) {
      folly::toLowerAscii(key);
    }
    if (value == kHiveDefaultPartition) {
      partitionKeys[key] = std::nullopt;
    } else {
      partitionKeys[key] = value;
    }
  }
}

void WholeStageResultIterator::addIteratorSplits(const std::vector<std::shared_ptr<ResultIterator>>& inputIterators) {
  GLUTEN_CHECK(
      !allSplitsAdded_,
      "Method addIteratorSplits should not be called since all splits has been added to the Velox task.");
  // Create IteratorConnectorSplit for each iterator
  for (size_t i = 0; i < streamIds_.size() && i < inputIterators.size(); ++i) {
    if (inputIterators[i] == nullptr) {
      continue;
    }
    auto connectorSplit = std::make_shared<IteratorConnectorSplit>(connectorIds_.iterator, inputIterators[i]);
    exec::Split split(folly::copy(connectorSplit), -1);
    task_->addSplit(streamIds_[i], std::move(split));
  }
}

void WholeStageResultIterator::noMoreSplits() {
  if (allSplitsAdded_) {
    return;
  }
  // Mark no more splits for all scan nodes
  for (int idx = 0; idx < scanNodeIds_.size(); idx++) {
    for (auto& split : splits_[idx]) {
      task_->addSplit(scanNodeIds_[idx], std::move(split));
    }
  }

  for (const auto& scanNodeId : scanNodeIds_) {
    task_->noMoreSplits(scanNodeId);
  }

  // Mark no more splits for all stream nodes
  for (const auto& streamId : streamIds_) {
    task_->noMoreSplits(streamId);
  }
  cursor_->setNoMoreSplits();
  allSplitsAdded_ = true;
}

void WholeStageResultIterator::requestBarrier() {
  if (task_ == nullptr) {
    throw GlutenException("Cannot request barrier: task is null");
  }
  task_->requestBarrier();
}

void WholeStageResultIterator::collectMetrics() {
  if (metrics_) {
    // The metrics has already been created.
    return;
  }

  const auto& taskStats = task_->taskStats();
  if (taskStats.executionStartTimeMs == 0) {
    LOG(INFO) << "Skip collect task metrics since task did not call next().";
    return;
  }

  // Save and print the plan with stats if debug mode is enabled or showTaskMetricsWhenFinished is true.
  if (veloxCfg_->get<bool>(kDebugModeEnabled, false) ||
      veloxCfg_->get<bool>(kShowTaskMetricsWhenFinished, kShowTaskMetricsWhenFinishedDefault)) {
    auto planWithStats = velox::exec::printPlanWithStats(*veloxPlan_.get(), taskStats, true);
    std::ostringstream oss;
    oss << "Native Plan with stats for: " << taskInfo_ << "\n";
    oss << "TaskStats: totalTime: " << taskStats.executionEndTimeMs - taskStats.executionStartTimeMs
        << "; startTime: " << taskStats.executionStartTimeMs << "; endTime: " << taskStats.executionEndTimeMs;
    oss << "\n" << planWithStats << std::endl;
    LOG(WARNING) << oss.str();
  }

  auto planStats = velox::exec::toPlanStats(taskStats);
  folly::dynamic orderedNodeIds = folly::dynamic::array();
  folly::dynamic omittedNodeIds = folly::dynamic::array();
  folly::dynamic nodeStats = folly::dynamic::object();
  unsigned int statsNum = 0;

  for (int idx = 0; idx < orderedNodeIds_.size(); idx++) {
    const auto& nodeId = orderedNodeIds_[idx];
    orderedNodeIds.push_back(nodeId);

    if (planStats.find(nodeId) == planStats.end()) {
      if (omittedNodeIds_.find(nodeId) == omittedNodeIds_.end()) {
        LOG(WARNING) << "Not found node id: " << nodeId;
        LOG(WARNING) << "Plan Node: " << std::endl << veloxPlan_->toString(true, true);
        throw std::runtime_error("Node id cannot be found in plan status.");
      }
      omittedNodeIds.push_back(nodeId);
      statsNum += 1;
      continue;
    }

    const auto& stats = planStats.at(nodeId);
    folly::dynamic operatorStats = folly::dynamic::array();
    for (const auto& entry : stats.operatorStats) {
      const auto& opStats = entry.second;
      folly::dynamic customStats = folly::dynamic::object();
      for (const auto& customMetric : opStats->customStats) {
        customStats[customMetric.first] = folly::dynamic::object("sum", customMetric.second.sum)(
            "count", customMetric.second.count)("min", customMetric.second.min)("max", customMetric.second.max);
      }

      operatorStats.push_back(folly::dynamic::object("inputRows", opStats->inputRows)(
          "inputVectors", opStats->inputVectors)("inputBytes", opStats->inputBytes)(
          "rawInputRows", opStats->rawInputRows)("rawInputBytes", opStats->rawInputBytes)(
          "outputRows", opStats->outputRows)("outputVectors", opStats->outputVectors)(
          "outputBytes", opStats->outputBytes)("cpuCount", opStats->cpuWallTiming.count)(
          "wallNanos", opStats->cpuWallTiming.wallNanos)("peakMemoryBytes", opStats->peakMemoryBytes)(
          "numMemoryAllocations", opStats->numMemoryAllocations)("spilledInputBytes", opStats->spilledInputBytes)(
          "spilledBytes", opStats->spilledBytes)("spilledRows", opStats->spilledRows)(
          "spilledPartitions", opStats->spilledPartitions)("spilledFiles", opStats->spilledFiles)(
          "physicalWrittenBytes", opStats->physicalWrittenBytes)("customStats", customStats));
    }

    statsNum += static_cast<unsigned int>(operatorStats.size());
    nodeStats[nodeId] = folly::dynamic::object("operatorStats", operatorStats);
  }

  folly::dynamic payload = folly::dynamic::object("orderedNodeIds", orderedNodeIds)("omittedNodeIds", omittedNodeIds)(
      "loadLazyVectorTime", loadLazyVectorTime_)("nodeStats", nodeStats);
  metrics_ = std::make_unique<Metrics>(statsNum, folly::toJson(payload));

  // Populate the metrics with task stats for long running tasks.
  if (const int64_t collectTaskStatsThreshold =
          veloxCfg_->get<int64_t>(kTaskMetricsToEventLogThreshold, kTaskMetricsToEventLogThresholdDefault);
      collectTaskStatsThreshold >= 0 &&
      static_cast<int64_t>(taskStats.terminationTimeMs - taskStats.executionStartTimeMs) >
          collectTaskStatsThreshold * 1'000) {
    auto jsonStats = velox::exec::toPlanStatsJson(taskStats);
    metrics_->stats = folly::toJson(jsonStats);
  }
}

std::unordered_map<std::string, std::string> WholeStageResultIterator::getQueryContextConf() {
  std::unordered_map<std::string, std::string> configs = {};
  // Find batch size from Spark confs. If found, set the preferred and max batch size.
  configs[velox::core::QueryConfig::kPreferredOutputBatchRows] =
      std::to_string(veloxCfg_->get<uint32_t>(kSparkBatchSize, 4096));
  configs[velox::core::QueryConfig::kMaxOutputBatchRows] =
      std::to_string(veloxCfg_->get<uint32_t>(kSparkBatchSize, 4096));
  configs[velox::core::QueryConfig::kPreferredOutputBatchBytes] =
      std::to_string(veloxCfg_->get<uint64_t>(kVeloxPreferredBatchBytes, 10L << 20));
  try {
    configs[SparkQueryConfig::qualify(SparkQueryConfig::kAnsiEnabled)] =
        veloxCfg_->get<std::string>(kAnsiEnabled, "false");
    configs[velox::core::QueryConfig::kSessionTimezone] =
        normalizeSessionTimezone(veloxCfg_->get<std::string>(kSessionTimezone, ""));
    // Adjust timestamp according to the above configured session timezone.
    configs[velox::core::QueryConfig::kAdjustTimestampToTimezone] = "true";

    {
      // Find offheap size from Spark confs. If found, set the max memory usage of partial aggregation.
      // Partial aggregation memory configurations.
      // TODO: Move the calculations to Java side.
      auto offHeapMemory = veloxCfg_->get<int64_t>(kSparkTaskOffHeapMemory, facebook::velox::memory::kMaxMemory);
      auto maxPartialAggregationMemory = std::max<int64_t>(
          1 << 24,
          veloxCfg_->get<int64_t>(kMaxPartialAggregationMemory).has_value()
              ? veloxCfg_->get<int64_t>(kMaxPartialAggregationMemory).value()
              : static_cast<int64_t>(veloxCfg_->get<double>(kMaxPartialAggregationMemoryRatio, 0.1) * offHeapMemory));
      auto maxExtendedPartialAggregationMemory = std::max<int64_t>(
          1 << 26,
          veloxCfg_->get<int64_t>(kMaxExtendedPartialAggregationMemory).has_value()
              ? veloxCfg_->get<int64_t>(kMaxExtendedPartialAggregationMemory).value()
              : static_cast<int64_t>(
                    veloxCfg_->get<double>(kMaxExtendedPartialAggregationMemoryRatio, 0.15) * offHeapMemory));
      configs[velox::core::QueryConfig::kMaxPartialAggregationMemory] = std::to_string(maxPartialAggregationMemory);
      configs[velox::core::QueryConfig::kMaxExtendedPartialAggregationMemory] =
          std::to_string(maxExtendedPartialAggregationMemory);
      configs[velox::core::QueryConfig::kAbandonPartialAggregationMinPct] =
          std::to_string(veloxCfg_->get<int32_t>(kAbandonPartialAggregationMinPct, 90));
      configs[velox::core::QueryConfig::kAbandonPartialAggregationMinRows] =
          std::to_string(veloxCfg_->get<int32_t>(kAbandonPartialAggregationMinRows, 100000));
    }
    // Spill configs
    if (spillStrategy_ == "none") {
      configs[velox::core::QueryConfig::kSpillEnabled] = "false";
    } else {
      configs[velox::core::QueryConfig::kSpillEnabled] = "true";
    }
    configs[velox::core::QueryConfig::kAggregationSpillEnabled] =
        std::to_string(veloxCfg_->get<bool>(kAggregationSpillEnabled, true));
    configs[velox::core::QueryConfig::kJoinSpillEnabled] =
        std::to_string(veloxCfg_->get<bool>(kJoinSpillEnabled, true));
    configs[velox::core::QueryConfig::kOrderBySpillEnabled] =
        std::to_string(veloxCfg_->get<bool>(kOrderBySpillEnabled, true));
    configs[velox::core::QueryConfig::kWindowSpillEnabled] =
        std::to_string(veloxCfg_->get<bool>(kWindowSpillEnabled, true));
    configs[velox::core::QueryConfig::kMaxSpillLevel] = std::to_string(veloxCfg_->get<int32_t>(kMaxSpillLevel, 4));
    configs[velox::core::QueryConfig::kMaxSpillFileSize] =
        std::to_string(veloxCfg_->get<uint64_t>(kMaxSpillFileSize, 1L * 1024 * 1024 * 1024));
    configs[velox::core::QueryConfig::kMaxSpillRunRows] =
        std::to_string(veloxCfg_->get<uint64_t>(kMaxSpillRunRows, 3L * 1024 * 1024));
    configs[velox::core::QueryConfig::kMaxSpillBytes] =
        std::to_string(veloxCfg_->get<uint64_t>(kMaxSpillBytes, 107374182400LL));
    configs[velox::core::QueryConfig::kSpillWriteBufferSize] =
        std::to_string(veloxCfg_->get<uint64_t>(kShuffleSpillDiskWriteBufferSize, 1L * 1024 * 1024));
    configs[velox::core::QueryConfig::kSpillReadBufferSize] =
        std::to_string(veloxCfg_->get<int32_t>(kSpillReadBufferSize, 1L * 1024 * 1024));
    configs[velox::core::QueryConfig::kSpillStartPartitionBit] =
        std::to_string(veloxCfg_->get<uint8_t>(kSpillStartPartitionBit, 48));
    configs[velox::core::QueryConfig::kSpillNumPartitionBits] =
        std::to_string(veloxCfg_->get<uint8_t>(kSpillPartitionBits, 3));
    configs[velox::core::QueryConfig::kSpillableReservationGrowthPct] =
        std::to_string(veloxCfg_->get<uint8_t>(kSpillableReservationGrowthPct, 25));
    configs[velox::core::QueryConfig::kSpillPrefixSortEnabled] =
        veloxCfg_->get<std::string>(kSpillPrefixSortEnabled, "false");
    if (veloxCfg_->get<bool>(kSparkShuffleSpillCompress, true)) {
      configs[velox::core::QueryConfig::kSpillCompressionKind] =
          veloxCfg_->get<std::string>(kSpillCompressionKind, veloxCfg_->get<std::string>(kCompressionKind, "lz4"));
    } else {
      configs[velox::core::QueryConfig::kSpillCompressionKind] = "none";
    }

    configs[velox::core::QueryConfig::kHashProbeDynamicFilterPushdownEnabled] =
        std::to_string(veloxCfg_->get<bool>(kHashProbeDynamicFilterPushdownEnabled, true));
    configs[velox::core::QueryConfig::kHashProbeBloomFilterPushdownMaxSize] =
        std::to_string(veloxCfg_->get<uint64_t>(kHashProbeBloomFilterPushdownMaxSize, 0));

    if (const auto opt = veloxCfg_->get<std::string>(kSparkBloomFilterExpectedNumItems)) {
      configs[SparkQueryConfig::qualify(SparkQueryConfig::kBloomFilterExpectedNumItems)] = opt.value();
    }
    if (const auto opt = veloxCfg_->get<std::string>(kSparkBloomFilterNumBits)) {
      configs[SparkQueryConfig::qualify(SparkQueryConfig::kBloomFilterNumBits)] = opt.value();
    }
    if (const auto opt = veloxCfg_->get<std::string>(kSparkBloomFilterMaxNumBits)) {
      // Velox will check memory cannot exceed 4194304.
      configs[SparkQueryConfig::qualify(SparkQueryConfig::kBloomFilterMaxNumBits)] = opt.value();
    }
    if (const auto opt = veloxCfg_->get<std::string>(kSparkBloomFilterMaxNumItems)) {
      configs[SparkQueryConfig::qualify(SparkQueryConfig::kBloomFilterMaxNumItems)] = opt.value();
    }
    // spark.gluten.sql.columnar.backend.velox.SplitPreloadPerDriver takes no effect if
    // spark.gluten.sql.columnar.backend.velox.IOThreads is set to 0
    configs[velox::core::QueryConfig::kMaxSplitPreloadPerDriver] =
        std::to_string(veloxCfg_->get<int32_t>(kVeloxSplitPreloadPerDriver, 2));

    // hashtable build optimizations
    configs[velox::core::QueryConfig::kAbandonDedupHashMapMinRows] =
        std::to_string(veloxCfg_->get<int32_t>(kAbandonDedupHashMapMinRows, 100000));
    configs[velox::core::QueryConfig::kAbandonDedupHashMapMinPct] =
        std::to_string(veloxCfg_->get<int32_t>(kAbandonDedupHashMapMinPct, 0));

    // Disable driver cpu time slicing.
    configs[velox::core::QueryConfig::kDriverCpuTimeSliceLimitMs] = "0";

    configs[SparkQueryConfig::qualify(SparkQueryConfig::kPartitionId)] = std::to_string(taskInfo_.partitionId);

    // Enable Spark legacy date formatter if spark.sql.legacy.timeParserPolicy is set to 'LEGACY'
    // or 'legacy'
    if (veloxCfg_->get<std::string>(kSparkLegacyTimeParserPolicy, "") == "LEGACY") {
      configs[SparkQueryConfig::qualify(SparkQueryConfig::kLegacyDateFormatter)] = "true";
    } else {
      configs[SparkQueryConfig::qualify(SparkQueryConfig::kLegacyDateFormatter)] = "false";
    }

    if (veloxCfg_->get<std::string>(kSparkMapKeyDedupPolicy, "") == "EXCEPTION") {
      configs[velox::core::QueryConfig::kThrowExceptionOnDuplicateMapKeys] = "true";
    } else {
      configs[velox::core::QueryConfig::kThrowExceptionOnDuplicateMapKeys] = "false";
    }

    configs[SparkQueryConfig::qualify(SparkQueryConfig::kLegacyStatisticalAggregate)] =
        std::to_string(veloxCfg_->get<bool>(kSparkLegacyStatisticalAggregate, false));

    configs[SparkQueryConfig::qualify(SparkQueryConfig::kJsonIgnoreNullFields)] =
        std::to_string(veloxCfg_->get<bool>(kSparkJsonIgnoreNullFields, true));

    configs[velox::core::QueryConfig::kExprMaxCompiledRegexes] =
        std::to_string(veloxCfg_->get<int32_t>(kExprMaxCompiledRegexes, 100));

#ifdef GLUTEN_ENABLE_GPU
    configs[velox::cudf_velox::CudfConfig::kCudfEnabled] = std::to_string(veloxCfg_->get<bool>(kCudfEnabled, false));
#endif

    const auto setIfExists = [&](const std::string& glutenKey, const std::string& veloxKey) {
      const auto valueOptional = veloxCfg_->get<std::string>(glutenKey);
      if (valueOptional.has_value()) {
        configs[veloxKey] = valueOptional.value();
      }
    };
    setIfExists(kQueryTraceEnabled, velox::core::QueryConfig::kQueryTraceEnabled);
    setIfExists(kQueryTraceDir, velox::core::QueryConfig::kQueryTraceDir);
    setIfExists(kQueryTraceMaxBytes, velox::core::QueryConfig::kQueryTraceMaxBytes);
    setIfExists(kQueryTraceTaskRegExp, velox::core::QueryConfig::kQueryTraceTaskRegExp);
    setIfExists(kOpTraceDirectoryCreateConfig, velox::core::QueryConfig::kOpTraceDirectoryCreateConfig);

    overwriteVeloxConf(veloxCfg_.get(), configs, kDynamicBackendConfPrefix);
  } catch (const std::invalid_argument& err) {
    std::string errDetails = err.what();
    throw std::runtime_error("Invalid conf arg: " + errDetails);
  }
  return configs;
}

} // namespace gluten

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
#include <fmt/ranges.h>
#include "WholeStageResultIterator.h"
#include <bolt/common/memory/sparksql/Spiller.h>
#include <bolt/connectors/hive/PaimonConstants.h>
#include <cstdint>
#include <memory>
#include "BoltBackend.h"
#include "BoltRuntime.h"
#include "config/BoltConfig.h"
#include "memory/BoltMemoryManager.h"
#include "memory/BoltGlutenMemoryManager.h"
#include "bolt/connectors/hive/HiveConfig.h"
#include "bolt/connectors/hive/HiveConnectorSplit.h"
#include "bolt/exec/PlanNodeStats.h"
#include "bolt/shuffle/sparksql/ShuffleWriterNode.h"
#include "connectors/hive/PaimonConnectorSplit.h"
#include "compute/paimon/PaimonPlanUtils.h"

#ifdef GLUTEN_ENABLE_GPU
#include <cudf/io/types.hpp>
#include <mutex>
#include "bolt/experimental/cudf/CudfConfig.h"
#include "bolt/experimental/cudf/connectors/hive/CudfHiveConnectorSplit.h"
#include "bolt/experimental/cudf/exec/ToCudf.h"
#endif

using namespace bytedance;

namespace gluten {

namespace {

// metrics
const std::string kDynamicFiltersProduced = "dynamicFiltersProduced";
const std::string kDynamicFiltersAccepted = "dynamicFiltersAccepted";
const std::string kReplacedWithDynamicFilterRows = "replacedWithDynamicFilterRows";
const std::string kFlushRowCount = "flushRowCount";
const std::string kLoadedToValueHook = "loadedToValueHook";
const std::string kTotalScanTime = "totalScanTime";
const std::string kSkippedSplits = "skippedSplits";
const std::string kProcessedSplits = "processedSplits";
const std::string kSkippedStrides = "skippedStrides";
const std::string kProcessedStrides = "processedStrides";
const std::string kRemainingFilterTime = "totalRemainingFilterTime";
const std::string kIoWaitTime = "ioWaitWallNanos";
const std::string kStorageReadBytes = "storageReadBytes";
const std::string kLocalReadBytes = "localReadBytes";
const std::string kRamReadBytes = "ramReadBytes";
const std::string kPreloadSplits = "readyPreloadedSplits";
const std::string kDataSourceAddSplitWallNanos = "dataSourceAddSplitWallNanos";
const std::string kDataSourceReadWallNanos = "dataSourceReadWallNanos";
const std::string kNumWrittenFiles = "numWrittenFiles";
const std::string kWriteIOTime = "writeIOWallNanos";

// others
const std::string kHiveDefaultPartition = "__HIVE_DEFAULT_PARTITION__";

// parquet decrypt related config
// Decryption toggle
const std::string kDecryptionEnabled = "parquet.encryption.decrypt.enabled";
// URL of the KMS instance.
const std::string kKmsInstanceUrlPropertyName = "parquet.encryption.kms.instance.url";
// ID of the KMS instance that will be used for encryption (if multiple KMS
// instances are available).
const std::string kKmsInstanceIdPropertyName = "parquet.encryption.kms.instance.id";
// Authorization token that will be passed to KMS.
const std::string kKeyAccessTokenPropertyName = "parquet.encryption.key.access.token";
// Lifetime of cached entities (key encryption keys, local wrapping keys, KMS
// client objects).
const std::string kCacheLifetimePropertyName = "parquet.encryption.cache.lifetime.seconds";
// Max retry times for kms requests
const std::string kKmsClientMaxRetryTimes = "parquet.encryption.kms.client.max.retry.times";
// Version of the KMS instance.
const std::string kKmsInstanceVersionPropertyName = "parquet.encryption.kms.instance.version";
const std::string kKmsClientClassPropertyName = "parquet.encryption.kms.client.class";
const std::string parquetDecryptionConfigs[]{
    kDecryptionEnabled,
    kKmsInstanceUrlPropertyName,
    kKmsInstanceIdPropertyName,
    kKeyAccessTokenPropertyName,
    kCacheLifetimePropertyName,
    kKmsClientMaxRetryTimes,
    kKmsInstanceVersionPropertyName,
    kKmsClientClassPropertyName};
} // namespace

WholeStageResultIterator::WholeStageResultIterator(
    BoltMemoryManager* memoryManager,
    const std::shared_ptr<const bytedance::bolt::core::PlanNode>& planNode,
    const std::vector<bytedance::bolt::core::PlanNodeId>& scanNodeIds,
    const std::vector<std::shared_ptr<SplitInfo>>& scanInfos,
    const std::vector<bytedance::bolt::core::PlanNodeId>& streamIds,
    const std::string spillDir,
    const std::unordered_map<std::string, std::string>& confMap,
    const SparkTaskInfo& taskInfo)
    : memoryManager_(memoryManager),
      spillDir_(spillDir),
      boltCfg_(
          std::make_shared<bytedance::bolt::config::ConfigBase>(std::unordered_map<std::string, std::string>(confMap))),
      taskInfo_(taskInfo),
      boltPlan_(planNode),
#ifdef GLUTEN_ENABLE_GPU
      lock_(mutex_, std::defer_lock),
#endif
      scanNodeIds_(scanNodeIds),
      scanInfos_(scanInfos),
      streamIds_(streamIds) {
}

void WholeStageResultIterator::addIteratorSplits(
    const std::vector<std::shared_ptr<ResultIterator>>& /* inputIterators */) {
  // Bolt consumes input iterators during plan conversion via SubstraitToBoltPlan.
  // Keep this as a no-op to align with the split-aware JNI contract without
  // double-injecting iterator-backed inputs after task creation.
}

void WholeStageResultIterator::addShuffleWriter(
    const bytedance::bolt::shuffle::sparksql::ShuffleWriterOptions& options,
    bytedance::bolt::shuffle::sparksql::ReportShuffleStatusCallback reportShuffleStatusCallback) {
  std::function<int(const bytedance::bolt::core::PlanNodePtr&)> findMaxNodeId =
      [&](const bytedance::bolt::core::PlanNodePtr& node) {
        int maxId = 0;
        try {
          maxId = folly::to<int>(node->id());
        } catch (const std::exception& e) {
          LOG(WARNING) << "Failed to convert plan node id to int: " << node->id() << ", exception: " << e.what();
        }
        for (const auto& child : node->sources()) {
          auto childMaxId = findMaxNodeId(child);
          if (childMaxId > maxId) {
            maxId = childMaxId;
          }
        }
        return maxId;
      };
  auto shuffleWriterNodeId = bytedance::bolt::core::PlanNodeId(fmt::format("{}", findMaxNodeId(boltPlan_) + 1));
  auto shuffleWriterNode = std::make_shared<bytedance::bolt::shuffle::sparksql::SparkShuffleWriterNode>(
      shuffleWriterNodeId, options, reportShuffleStatusCallback, boltPlan_);
  boltPlan_ = shuffleWriterNode;
}

void WholeStageResultIterator::initTask() {
  spillStrategy_ = boltCfg_->get<std::string>(kSpillStrategy, kSpillStrategyDefaultValue);
  auto spillThreadNum = boltCfg_->get<uint32_t>(kSpillThreadNum, kSpillThreadNumDefaultValue);
  if (spillThreadNum > 0) {
    spillExecutor_ = std::make_shared<folly::CPUThreadPoolExecutor>(spillThreadNum);
  }
  getOrderedNodeIds(boltPlan_, orderedNodeIds_);
  parallelEnabled_ = boltCfg_->get<bool>(kGlutenEnableParallel, false);
  LOG(INFO) << "WholeStageResultIterator::WholeStageResultIterator parallelEnabled=" << parallelEnabled_;
  // Check the plan ends with shuffleWriter
  isMultiThreadExecMode_ = parallelEnabled_ &&
      std::dynamic_pointer_cast<const bytedance::bolt::shuffle::sparksql::SparkShuffleWriterNode>(boltPlan_) !=
          nullptr;
  if (isMultiThreadExecMode_) {
    LOG(INFO) << "Running task-" << std::to_string(taskInfo_.taskId) << " on multi-thread mode";
  }

#ifdef GLUTEN_ENABLE_GPU
  enableCudf_ = boltCfg_->get<bool>(kCudfEnabled, kCudfEnabledDefault);
  if (enableCudf_) {
    lock_.lock();
  }
#endif

  auto fileSystem = bolt::filesystems::getFileSystem(spillDir_, nullptr);
  GLUTEN_CHECK(fileSystem != nullptr, "File System for spilling is null!");
  fileSystem->mkdir(spillDir_);
  bolt::common::SpillDiskOptions spillOpts{
      .spillDirPath = spillDir_, .spillDirCreated = true, .spillDirCreateCb = nullptr};
  dynamicConcurrencyAdjustmentEnabled_ =
      boltCfg_->get<bool>(kDynamicConcurrencyAdjustmentEnabled, kDynamicConcurrencyAdjustmentEnabledDefault);
  // Create task instance.
  std::unordered_set<bolt::core::PlanNodeId> emptySet;
  bolt::core::PlanFragment planFragment{boltPlan_, bolt::core::ExecutionStrategy::kUngrouped, 1, emptySet};
  std::shared_ptr<bolt::core::QueryCtx> queryCtx = createNewBoltQueryCtx(isMultiThreadExecMode_);
  task_ = bolt::exec::Task::create(
      fmt::format(
          "Gluten_Stage_{}_TID_{}_VTID_{}",
          std::to_string(taskInfo_.stageId),
          std::to_string(taskInfo_.taskId),
          std::to_string(taskInfo_.vId)),
      std::move(planFragment),
      0,
      queryCtx,
      bolt::exec::Task::ExecutionMode::kSerial,
      /*consumer=*/bolt::exec::Consumer{},
      /*memoryArbitrationPriority=*/0,
      /*spillDiskOpts=*/spillOpts,
      /*onError=*/nullptr);
  VLOG(1) << "After task::create pool=" << queryCtx->pool()->treeMemoryUsage(false);
  if (!task_->supportSerialExecutionMode()) {
    throw std::runtime_error("Task doesn't support single threaded execution: " + boltPlan_->toString());
  }

  // Generate splits for all scan nodes.
  splits_.reserve(scanInfos_.size());
  if (scanNodeIds_.size() != scanInfos_.size()) {
    throw std::runtime_error("Invalid scan information.");
  }

  std::unordered_map<std::string, std::string> customSplitInfo{
      {"spark_partition_id", std::to_string(taskInfo_.partitionId)}};

  for (const auto& scanInfo : scanInfos_) {
    // Get the information for TableScan.
    // Partition index in scan info is not used.
    const auto& paths = scanInfo->paths;
    const auto& starts = scanInfo->starts;
    const auto& lengths = scanInfo->lengths;
    const auto& properties = scanInfo->properties;
    const auto& format = scanInfo->format;
    const auto& partitionColumns = scanInfo->partitionColumns;
    const auto& metadataColumns = scanInfo->metadataColumns;
    // Under the pre-condition that all the split infos has same partition column and format.
    [[maybe_unused]] const auto canUseCudfConnector = scanInfo->canUseCudfConnector();

    std::unordered_map<uint32_t, std::vector<std::shared_ptr<bolt::connector::hive::HiveConnectorSplit>>> splitGroups{};

    std::vector<std::shared_ptr<bolt::connector::ConnectorSplit>> connectorSplits;
    connectorSplits.reserve(paths.size());
    auto connectorId = kHiveConnectorId;
    for (int idx = 0; idx < paths.size(); idx++) {
      const auto& metadataColumn = metadataColumns[idx];
      std::unordered_map<std::string, std::optional<std::string>> partitionKeys;
      if (!partitionColumns.empty()) {
        auto partitionColumn = partitionColumns[idx];
        constructPartitionColumns(partitionKeys, partitionColumn);
      }


      std::shared_ptr<bolt::connector::ConnectorSplit> split;
      if (auto paimonSplitInfo = std::dynamic_pointer_cast<paimon::PaimonSplitInfo>(scanInfo)) {
        const auto& splitMetadata = paimonSplitInfo->metaAt(idx);
        std::unordered_map<std::string, std::string> splitInfo = customSplitInfo;
        splitInfo[bytedance::bolt::connector::paimon::kFileMetaFirstRowID] =
        std::to_string(splitMetadata.firstRowId);
        splitInfo[bytedance::bolt::connector::paimon::kFileMetaMaxSequenceNumber] =
        std::to_string(splitMetadata.maxSequenceNumber);

        auto split = std::make_shared<bolt::connector::hive::HiveConnectorSplit>(
            kHiveConnectorId,
            paths[idx],
            splitMetadata.format,
            starts[idx],
            lengths[idx],
            partitionKeys,
            std::optional<int32_t>{splitMetadata.bucket},
            nullptr,
            std::move(splitInfo),
            std::make_shared<std::string>(),
            std::unordered_map<std::string, std::string>(),
            properties[idx]->fileSize.value_or(0),
            std::nullopt,
            metadataColumn);
        if (!splitMetadata.rawConvertible) {
          LOG(INFO) << "Split is not rawConvertible, adding to group " << splitMetadata.splitGroup << " : " << split->toString();
          splitGroups[splitMetadata.splitGroup].push_back(split);
        } else {
          LOG(INFO) << "Split is rawConvertible, adding single split: " << split->toString();
          connectorSplits.emplace_back(split);
        }
      } else {
      // } else if (auto icebergSplitInfo = std::dynamic_pointer_cast<IcebergSplitInfo>(scanInfo)) {
      //   // Set Iceberg split.
      //   std::unordered_map<std::string, std::string> customSplitInfo{{"table_format", "hive-iceberg"}};
      //   auto deleteFiles = icebergSplitInfo->deleteFilesVec[idx];
      //   split = std::make_shared<bolt::connector::hive::iceberg::HiveIcebergSplit>(
      //       kHiveConnectorId,
      //       paths[idx],
      //       format,
      //       starts[idx],
      //       lengths[idx],
      //       partitionKeys,
      //       std::nullopt,
      //       customSplitInfo,
      //       nullptr,
      //       true,
      //       deleteFiles,
      //       std::unordered_map<std::string, std::string>(),
      //       properties[idx]);
      // } else {
#ifdef GLUTEN_ENABLE_GPU
      if (canUseCudfConnector) {
        connectorId = kCudfHiveConnectorId;
        BOLT_CHECK_EQ(starts[idx], 0, "Not support split file");
        BOLT_CHECK_EQ(lengths[idx], scanInfo->properties[idx]->fileSize, "Not support split file");
      }
#endif
      // TODO sync bolt and uncomment it
      // split = std::make_shared<bolt::connector::hive::HiveConnectorSplit>(
      //     connectorId,
      //     paths[idx],
      //     format,
      //     starts[idx],
      //     lengths[idx],
      //     partitionKeys,
      //     std::nullopt /*tableBucketName*/,
      //     std::unordered_map<std::string, std::string>() /*_customSplitInfo*/,
      //     nullptr /*_extraFileInfo*/,
      //     std::unordered_map<std::string, std::string>() /*_serdeParameters*/,
      //     0 /*splitWeight*/,
      //     true /*cacheable*/,
      //     metadataColumn /*_infoColumns*/,
      //     properties[idx] /*_properties*/);
      split = std::make_shared<bolt::connector::hive::HiveConnectorSplit>(
          connectorId,
          paths[idx],
          format,
          starts[idx],
          lengths[idx],
          partitionKeys,
          std::nullopt /*tableBucketName*/,
          nullptr /*_hiveConnectorSplitCacheLimit*/,
          customSplitInfo,
          std::make_shared<std::string>() /*_extraFileInfo*/,
          std::unordered_map<std::string, std::string>() /*_serdeParameters*/,
          properties[idx]->fileSize.value_or(0) /*_fileSize*/,
          std::nullopt /*_rowIdProperties*/,
          metadataColumn);

      connectorSplits.emplace_back(split);
    }
    }
    for (const auto& [splitGroup, splits] : splitGroups) {
      LOG(INFO) << "Adding PaimonConnectorSplit group " << splitGroup << " with " << splits.size() << " splits";
      auto connectorSplit = std::make_shared<bytedance::bolt::connector::hive::PaimonConnectorSplit>(connectorId, std::move(splits));
      connectorSplits.emplace_back(connectorSplit);
    }

    std::vector<bolt::exec::Split> scanSplits;
    scanSplits.reserve(connectorSplits.size());
    LOG(INFO) << "Adding " << connectorSplits.size() << " connector splits to task";
    for (const auto& connectorSplit : connectorSplits) {
      // Bucketed group id (-1 means 'none').
      int32_t groupId = -1;
      scanSplits.emplace_back(bolt::exec::Split(folly::copy(connectorSplit), groupId));
    }
    splits_.emplace_back(scanSplits);
  }
  BOLT_CHECK_NOT_NULL(task_);
  BOLT_CHECK(!task_->isGroupedExecution(), "task-{} should be group executed", task_->taskId());
  if (isMultiThreadExecMode_) {
    // Hardcode #drivers to 2 for now
    const uint32_t maxDrivers = 2;
    LOG(INFO) << "Task-" << task_->taskId() << " starts with maxDrivers=" << maxDrivers;
    task_->start(maxDrivers);
    // Create a promise and a future
    std::promise<bytedance::bolt::exec::TaskState> taskCompletionPromise;
    auto& exec = folly::QueuedImmediateExecutor::instance();
    taskCompletionFuture_ = task_->taskCompletionFuture().via(&exec);
    tryAddSplitsToTask();
  }
}

#ifdef GLUTEN_ENABLE_GPU
std::mutex WholeStageResultIterator::mutex_;
#endif

std::shared_ptr<bolt::core::QueryCtx> WholeStageResultIterator::createNewBoltQueryCtx(bool isMultiThreaded) {
  std::unordered_map<std::string, std::shared_ptr<bolt::config::ConfigBase>> connectorConfigs;
  connectorConfigs[kHiveConnectorId] = createConnectorConfig();

  memory::sparksql::BoltMemoryPoolPtr boltPool;
  if (gluten::BoltGlutenMemoryManager::enabled()) {
    auto holder = gluten::BoltGlutenMemoryManager::getMemoryManagerHolder(
        memoryManager_->name(), taskInfo_.taskId, reinterpret_cast<int64_t>(memoryManager_));
    auto mm = holder->getManager();
    boltPool = mm->getAggregateMemoryPool();
  } else {
    boltPool = memoryManager_->getAggregateMemoryPool();
  }
  VLOG(1) << "WholeStageResultIterator::createNewBoltQueryCtx boltPool=" << boltPool->treeMemoryUsage(false);

  std::shared_ptr<bolt::core::QueryCtx> ctx = bolt::core::QueryCtx::create(
      isMultiThreaded ? gluten::BoltBackend::get()->getDriverExecutor() : nullptr,
      bytedance::bolt::core::QueryConfig{getQueryContextConf()},
      connectorConfigs,
      gluten::BoltBackend::get()->getAsyncDataCache(),
      boltPool,
      spillExecutor_.get(),
      fmt::format(
          "Gluten_Stage_{}_TID_{}_VTID_{}",
          std::to_string(taskInfo_.stageId),
          std::to_string(taskInfo_.taskId),
          std::to_string(taskInfo_.vId)));
  return ctx;
}

std::shared_ptr<ColumnarBatch> WholeStageResultIterator::next() {
  if (FOLLY_UNLIKELY(!task_)) {
    initTask();
  }
  auto result = nextInternal();
#ifdef GLUTEN_ENABLE_GPU
  if (result == nullptr && enableCudf_) {
    lock_.unlock();
  }
#endif

  return result;
}

std::shared_ptr<ColumnarBatch> WholeStageResultIterator::nextInternal() {
  tryAddSplitsToTask();
  if (task_->isFinished()) {
    return nullptr;
  }

  if (isMultiThreadExecMode_) {
    // Block the current thread until the task completes
    taskCompletionFuture_.wait();
    if (task_->error()) {
      LOG(ERROR) << "Task " << task_->taskId() << " failed with error";
      std::rethrow_exception(task_->error());
    }
    return nullptr;
  }

  bolt::RowVectorPtr vector;
  while (true) {
    auto future = bolt::ContinueFuture::makeEmpty();
    auto out = task_->next(&future);
    if (!future.valid()) {
      // Not need to wait. Break.
      vector = std::move(out);
      break;
    }
    // Bolt suggested to wait. This might be because another thread (e.g., background io thread) is spilling the task.
    GLUTEN_CHECK(out == nullptr, "Expected to wait but still got non-null output from Bolt task");
    VLOG(2) << "Bolt task " << task_->taskId()
            << " is busy when ::next() is called. Will wait and try again. Task state: "
            << taskStateString(task_->state());
    future.wait();
  }
  if (vector == nullptr) {
    return nullptr;
  }
  uint64_t numRows = vector->size();
  if (numRows == 0) {
    return nullptr;
  }

  {
    ScopedTimer timer(&loadLazyVectorTime_);
    for (auto& child : vector->children()) {
      if (child) {
        child->loadedVector();
      }
    }
  }

  return std::make_shared<BoltColumnarBatch>(vector);
}

int64_t WholeStageResultIterator::spillFixedSize(int64_t size) {
  memory::sparksql::BoltMemoryPoolPtr pool;
  memory::sparksql::BoltMemoryManagerPtr manager;
  if (gluten::BoltGlutenMemoryManager::enabled()) {
    auto holder = gluten::BoltGlutenMemoryManager::getMemoryManagerHolder(
        memoryManager_->name(), taskInfo_.taskId, reinterpret_cast<int64_t>(memoryManager_));
    manager = holder->getManager();
    pool = manager->getAggregateMemoryPool();
  } else {
    pool = memoryManager_->getAggregateMemoryPool();
  }

  std::string poolName{pool->root()->name() + "/" + pool->name()};
  std::string logPrefix{"Spill[" + poolName + "]: "};
  int64_t shrunken = 0;
  if (gluten::BoltGlutenMemoryManager::enabled()) {
    shrunken = manager->shrink(size);
  } else {
    shrunken = memoryManager_->shrink(size);
  }
  if (spillStrategy_ == "auto") {
    int64_t remaining = size - shrunken;
    LOG(INFO) << fmt::format("{} trying to request spill for {}.", logPrefix, bolt::succinctBytes(remaining));
    uint64_t spilledOut;
    if (gluten::BoltGlutenMemoryManager::enabled()) {
      auto mm = manager->getMemoryManager();
      spilledOut = mm->arbitrator()->shrinkCapacity(remaining); // this conducts spilling
    } else {
      auto mm = memoryManager_->getMemoryManager();
      spilledOut = mm->arbitrator()->shrinkCapacity(remaining); // this conducts spilling
    }
    uint64_t total = shrunken + spilledOut;
    LOG(INFO) << fmt::format(
        "{} successfully reclaimed total {} with shrunken {} and spilled {}.",
        logPrefix,
        bolt::succinctBytes(total),
        bolt::succinctBytes(shrunken),
        bolt::succinctBytes(spilledOut));
    return total;
  }
  LOG(WARNING) << "Spill-to-disk was disabled since " << kSpillStrategy << " was not configured.";
  VLOG(2) << logPrefix << "Successfully reclaimed total " << shrunken << " bytes.";
  return shrunken;
}

void WholeStageResultIterator::getOrderedNodeIds(
    const std::shared_ptr<const bolt::core::PlanNode>& planNode,
    std::vector<bolt::core::PlanNodeId>& nodeIds) {
  bool isProjectNode = (std::dynamic_pointer_cast<const bolt::core::ProjectNode>(planNode) != nullptr);
  bool isLocalExchangeNode = (std::dynamic_pointer_cast<const bolt::core::LocalPartitionNode>(planNode) != nullptr);
  bool isUnionNode = isLocalExchangeNode &&
      std::dynamic_pointer_cast<const bolt::core::LocalPartitionNode>(planNode)->type() ==
          bolt::core::LocalPartitionNode::Type::kGather;
  const auto& sourceNodes = planNode->sources();
  if (isProjectNode) {
    GLUTEN_CHECK(sourceNodes.size() == 1, "Illegal state");
    const auto sourceNode = sourceNodes.at(0);
    // Filter over Project are mapped into FilterProject operator in Bolt.
    // Metrics are all applied on Project node, and the metrics for Filter node
    // do not exist.
    if (std::dynamic_pointer_cast<const bolt::core::FilterNode>(sourceNode)) {
      omittedNodeIds_.insert(sourceNode->id());
    }
    getOrderedNodeIds(sourceNode, nodeIds);
    nodeIds.emplace_back(planNode->id());
    return;
  }

  if (isUnionNode) {
    // FIXME: The whole metrics system in gluten-substrait is magic. Passing metrics trees through JNI with a trivial
    //  array is possible but requires for a solid design. Apparently we haven't had it. All the code requires complete
    //  rework.
    // Union was interpreted as LocalPartition + LocalExchange + 2 fake projects as children in Bolt. So we only fetch
    // metrics from the root node.
    std::vector<std::shared_ptr<const bolt::core::PlanNode>> unionChildren{};
    for (const auto& source : planNode->sources()) {
      const auto projectedChild = std::dynamic_pointer_cast<const bolt::core::ProjectNode>(source);
      GLUTEN_CHECK(projectedChild != nullptr, "Illegal state");
      const auto projectSources = projectedChild->sources();
      GLUTEN_CHECK(projectSources.size() == 1, "Illegal state");
      const auto projectSource = projectSources.at(0);
      getOrderedNodeIds(projectSource, nodeIds);
    }
    nodeIds.emplace_back(planNode->id());
    return;
  }

  for (const auto& sourceNode : sourceNodes) {
    // Post-order traversal.
    getOrderedNodeIds(sourceNode, nodeIds);
  }
  // SparkShuffleWriterNode's metrics will be report by ShuffleWriterResult so it does not contains in stats
  if (std::dynamic_pointer_cast<const bytedance::bolt::shuffle::sparksql::SparkShuffleWriterNode>(planNode) !=
      nullptr) {
    omittedNodeIds_.insert(planNode->id());
  }
  nodeIds.emplace_back(planNode->id());
}

void WholeStageResultIterator::constructPartitionColumns(
    std::unordered_map<std::string, std::optional<std::string>>& partitionKeys,
    const std::unordered_map<std::string, std::string>& map) {
  for (const auto& partitionColumn : map) {
    auto key = partitionColumn.first;
    const auto value = partitionColumn.second;
    if (!boltCfg_->get<bool>(kCaseSensitive, false)) {
      folly::toLowerAscii(key);
    }
    if (value == kHiveDefaultPartition) {
      partitionKeys[key] = std::nullopt;
    } else {
      partitionKeys[key] = value;
    }
  }
}

void WholeStageResultIterator::tryAddSplitsToTask() {
  if (noMoreSplits_) {
    return;
  }
  for (int idx = 0; idx < scanNodeIds_.size(); idx++) {
    for (auto& split : splits_[idx]) {
      task_->addSplit(scanNodeIds_[idx], std::move(split));
    }
    task_->noMoreSplits(scanNodeIds_[idx]);
  }
  for (const auto& streamId : streamIds_) {
    task_->noMoreSplits(streamId);
  }
  noMoreSplits_ = true;
}

void WholeStageResultIterator::noMoreSplits() {
  if (task_ == nullptr) {
    initTask();
  }
  tryAddSplitsToTask();
}

void WholeStageResultIterator::requestBarrier() {
  throw GlutenException("Bolt backend does not support task barrier yet");
}

void WholeStageResultIterator::collectMetrics() {
  if (metrics_) {
    // The metrics has already been created.
    return;
  }

  if (!task_) {
    // Task has not been initialized (next() was never called), nothing to collect.
    LOG(INFO) << "Skip collect task metrics since task was not initialized.";
    return;
  }

  const auto& taskStats = task_->taskStats();
  if (taskStats.executionStartTimeMs == 0) {
    LOG(INFO) << "Skip collect task metrics since task did not call next().";
    return;
  }

  // Save and print the plan with stats if debug mode is enabled or showTaskMetricsWhenFinished is true.
  if (boltCfg_->get<bool>(kDebugModeEnabled, false) ||
      boltCfg_->get<bool>(kShowTaskMetricsWhenFinished, kShowTaskMetricsWhenFinishedDefault)) {
    auto planWithStats = bolt::exec::printPlanWithStats(*boltPlan_.get(), taskStats, true);
    std::ostringstream oss;
    oss << "Native Plan with stats for: " << taskInfo_ << "\n";
    oss << "TaskStats: totalTime: " << taskStats.executionEndTimeMs - taskStats.executionStartTimeMs
        << "; startTime: " << taskStats.executionStartTimeMs << "; endTime: " << taskStats.executionEndTimeMs;
    oss << "\n" << planWithStats << std::endl;
    LOG(WARNING) << oss.str();
  }

  auto planStats = bolt::exec::toPlanStats(taskStats);
  // Calculate the total number of metrics.
  int statsNum = 0;
  for (int idx = 0; idx < orderedNodeIds_.size(); idx++) {
    const auto& nodeId = orderedNodeIds_[idx];
    if (planStats.find(nodeId) == planStats.end()) {
      if (omittedNodeIds_.find(nodeId) == omittedNodeIds_.end()) {
        LOG(WARNING) << "Not found node id: " << nodeId;
        LOG(WARNING) << "Plan Node: " << std::endl << boltPlan_->toString(true, true);
        throw std::runtime_error("Node id cannot be found in plan status.");
      }
      // Special handing for Filter over Project case. Filter metrics are
      // omitted.
      statsNum += 1;
      continue;
    }
    statsNum += planStats.at(nodeId).operatorStats.size();
  }

  metrics_ = std::make_unique<Metrics>(statsNum);

  int metricIndex = 0;
  for (int idx = 0; idx < orderedNodeIds_.size(); idx++) {
    metrics_->get(Metrics::kLoadLazyVectorTime)[metricIndex] = 0;

    const auto& nodeId = orderedNodeIds_[idx];
    if (planStats.find(nodeId) == planStats.end()) {
      // Special handing for Filter over Project case. Filter metrics are
      // omitted.
      metrics_->get(Metrics::kOutputRows)[metricIndex] = 0;
      metrics_->get(Metrics::kOutputVectors)[metricIndex] = 0;
      metrics_->get(Metrics::kOutputBytes)[metricIndex] = 0;
      metrics_->get(Metrics::kCpuCount)[metricIndex] = 0;
      metrics_->get(Metrics::kWallNanos)[metricIndex] = 0;
      metrics_->get(Metrics::kPeakMemoryBytes)[metricIndex] = 0;
      metrics_->get(Metrics::kNumMemoryAllocations)[metricIndex] = 0;
      metricIndex += 1;
      continue;
    }

    const auto& stats = planStats.at(nodeId);
    // Add each operator stats into metrics.
    for (const auto& entry : stats.operatorStats) {
      const auto& second = entry.second;
      metrics_->get(Metrics::kInputRows)[metricIndex] = second->inputRows;
      metrics_->get(Metrics::kInputVectors)[metricIndex] = second->inputVectors;
      metrics_->get(Metrics::kInputBytes)[metricIndex] = second->inputBytes;
      metrics_->get(Metrics::kRawInputRows)[metricIndex] = second->rawInputRows;
      metrics_->get(Metrics::kRawInputBytes)[metricIndex] = second->rawInputBytes;
      metrics_->get(Metrics::kOutputRows)[metricIndex] = second->outputRows;
      metrics_->get(Metrics::kOutputVectors)[metricIndex] = second->outputVectors;
      metrics_->get(Metrics::kOutputBytes)[metricIndex] = second->outputBytes;
      metrics_->get(Metrics::kCpuCount)[metricIndex] = second->cpuWallTiming.count;
      metrics_->get(Metrics::kWallNanos)[metricIndex] = second->cpuWallTiming.wallNanos;
      metrics_->get(Metrics::kPeakMemoryBytes)[metricIndex] = second->peakMemoryBytes;
      metrics_->get(Metrics::kNumMemoryAllocations)[metricIndex] = second->numMemoryAllocations;
      metrics_->get(Metrics::kSpilledInputBytes)[metricIndex] = second->spilledInputBytes;
      metrics_->get(Metrics::kSpilledBytes)[metricIndex] = second->spilledBytes;
      metrics_->get(Metrics::kSpilledRows)[metricIndex] = second->spilledRows;
      metrics_->get(Metrics::kSpilledPartitions)[metricIndex] = second->spilledPartitions;
      metrics_->get(Metrics::kSpilledFiles)[metricIndex] = second->spilledFiles;
      metrics_->get(Metrics::kNumDynamicFiltersProduced)[metricIndex] =
          runtimeMetric("sum", second->customStats, kDynamicFiltersProduced);
      metrics_->get(Metrics::kNumDynamicFiltersAccepted)[metricIndex] =
          runtimeMetric("sum", second->customStats, kDynamicFiltersAccepted);
      metrics_->get(Metrics::kNumReplacedWithDynamicFilterRows)[metricIndex] =
          runtimeMetric("sum", second->customStats, kReplacedWithDynamicFilterRows);
      metrics_->get(Metrics::kFlushRowCount)[metricIndex] = runtimeMetric("sum", second->customStats, kFlushRowCount);
      metrics_->get(Metrics::kLoadedToValueHook)[metricIndex] =
          runtimeMetric("sum", second->customStats, kLoadedToValueHook);
      metrics_->get(Metrics::kScanTime)[metricIndex] = runtimeMetric("sum", second->customStats, kTotalScanTime);
      metrics_->get(Metrics::kSkippedSplits)[metricIndex] = runtimeMetric("sum", second->customStats, kSkippedSplits);
      metrics_->get(Metrics::kProcessedSplits)[metricIndex] =
          runtimeMetric("sum", second->customStats, kProcessedSplits);
      metrics_->get(Metrics::kSkippedStrides)[metricIndex] = runtimeMetric("sum", second->customStats, kSkippedStrides);
      metrics_->get(Metrics::kProcessedStrides)[metricIndex] =
          runtimeMetric("sum", second->customStats, kProcessedStrides);
      metrics_->get(Metrics::kRemainingFilterTime)[metricIndex] =
          runtimeMetric("sum", second->customStats, kRemainingFilterTime);
      metrics_->get(Metrics::kIoWaitTime)[metricIndex] = runtimeMetric("sum", second->customStats, kIoWaitTime);
      metrics_->get(Metrics::kStorageReadBytes)[metricIndex] =
          runtimeMetric("sum", second->customStats, kStorageReadBytes);
      metrics_->get(Metrics::kLocalReadBytes)[metricIndex] = runtimeMetric("sum", second->customStats, kLocalReadBytes);
      metrics_->get(Metrics::kRamReadBytes)[metricIndex] = runtimeMetric("sum", second->customStats, kRamReadBytes);
      metrics_->get(Metrics::kPreloadSplits)[metricIndex] =
          runtimeMetric("sum", entry.second->customStats, kPreloadSplits);
      metrics_->get(Metrics::kDataSourceAddSplitWallNanos)[metricIndex] =
          runtimeMetric("sum", second->customStats, kDataSourceAddSplitWallNanos);
      metrics_->get(Metrics::kDataSourceReadWallNanos)[metricIndex] =
          runtimeMetric("sum", second->customStats, kDataSourceReadWallNanos);
      metrics_->get(Metrics::kNumWrittenFiles)[metricIndex] =
          runtimeMetric("sum", entry.second->customStats, kNumWrittenFiles);
      metrics_->get(Metrics::kPhysicalWrittenBytes)[metricIndex] = second->physicalWrittenBytes;
      metrics_->get(Metrics::kWriteIOTime)[metricIndex] = runtimeMetric("sum", second->customStats, kWriteIOTime);

      metricIndex += 1;
    }
  }

  // Put the loadLazyVector time into the metrics of the last operator.
  metrics_->get(Metrics::kLoadLazyVectorTime)[orderedNodeIds_.size() - 1] = loadLazyVectorTime_;

  // Populate the metrics with task stats for long running tasks.
  if (const int64_t collectTaskStatsThreshold =
          boltCfg_->get<int64_t>(kTaskMetricsToEventLogThreshold, kTaskMetricsToEventLogThresholdDefault);
      collectTaskStatsThreshold >= 0 &&
      static_cast<int64_t>(taskStats.terminationTimeMs - taskStats.executionStartTimeMs) >
          collectTaskStatsThreshold * 1'000) {
    auto jsonStats = bolt::exec::toPlanStatsJson(taskStats);
    metrics_->stats = folly::toJson(jsonStats);
  }
}

int64_t WholeStageResultIterator::runtimeMetric(
    const std::string& type,
    const std::unordered_map<std::string, bolt::RuntimeMetric>& runtimeStats,
    const std::string& metricId) {
  if (runtimeStats.find(metricId) == runtimeStats.end()) {
    return 0;
  }

  if (type == "sum") {
    return runtimeStats.at(metricId).sum;
  } else if (type == "count") {
    return runtimeStats.at(metricId).count;
  } else if (type == "min") {
    return runtimeStats.at(metricId).min;
  } else if (type == "max") {
    return runtimeStats.at(metricId).max;
  } else {
    return 0;
  }
}

std::pair<int64_t, int64_t> WholeStageResultIterator::getExecutorConcurrency() {
  if (!dynamicConcurrencyAdjustmentEnabled_) {
    return {0, 0};
  }
  return {
      bolt::exec::ExecutorTaskScheduler::instance().getCurrentConcurrency(),
      bolt::exec::ExecutorTaskScheduler::instance().getConcurrencyVersion()};
}

std::unordered_map<std::string, std::string> WholeStageResultIterator::getQueryContextConf() {
  std::unordered_map<std::string, std::string> configs = {};
  // Find batch size from Spark confs. If found, set the preferred and max batch size.
  configs[bolt::core::QueryConfig::kPreferredOutputBatchRows] =
      std::to_string(boltCfg_->get<uint32_t>(kSparkBatchSize, 4096));
  configs[bolt::core::QueryConfig::kMaxOutputBatchRows] =
      std::to_string(boltCfg_->get<uint32_t>(kSparkBatchSize, 4096));
  configs[bolt::core::QueryConfig::kPreferredOutputBatchBytes] =
      std::to_string(boltCfg_->get<uint64_t>(kBoltPreferredBatchBytes, 10L << 20));
  configs[bolt::core::QueryConfig::kEnableEstimateRowSizeBasedOnSample] =
      boltCfg_->get<std::string>(kEstimateRowSizeBasedOnSampleEnabled, "false");
  configs[bolt::core::QueryConfig::kThrowExceptionWhenEncounterBadJson] =
      boltCfg_->get<std::string>(kThrowExceptionWhenEncounterBadJson, "false");
  configs[bolt::core::QueryConfig::kUseDOMParserInGetJsonObject] =
      boltCfg_->get<std::string>(kUseDOMParserInGetJsonObject, "false");
  configs[bolt::core::QueryConfig::kUseSonicJson] =
        boltCfg_->get<std::string>(kUseSonicJson, "true");
  configs[bolt::core::QueryConfig::kIgnoreCorruptFiles] =
      std::to_string(boltCfg_->get<bool>(kIgnoreCorruptFiles, false));
  configs[bolt::core::QueryConfig::kThrowExceptionWhenEncounterBadTimestamp] =
      boltCfg_->get<std::string>(kThrowExceptionWhenEncounterBadTimestamp, "false");
  configs[bolt::core::QueryConfig::kRegexMatchDanglingRightBrackets] =
      boltCfg_->get<std::string>(kRegexMatchDanglingRightBrackets, "true");
  configs[bolt::core::QueryConfig::kSparkLegacyCastComplexTypesToStringEnabled] =
      boltCfg_->get<std::string>(kLegacyCastComplexTypesToStringEnabled, "false");
  configs[bolt::core::QueryConfig::kRowBasedSpillMode] =
        boltCfg_->get<std::string>(kRowBasedSpillMode, bolt::core::QueryConfig::kDefaultRowBasedSpillMode);
  configs[bolt::core::QueryConfig::kBoltJitEnabled] =
        std::to_string(boltCfg_->get<bool>(kBoltJitEnabled, true));
  try {
    // configs[bolt::core::QueryConfig::kSparkAnsiEnabled] = boltCfg_->get<std::string>(kAnsiEnabled, "false");
    configs[bolt::core::QueryConfig::kSessionTimezone] = boltCfg_->get<std::string>(kSessionTimezone, "");
    // Adjust timestamp according to the above configured session timezone.
    configs[bolt::core::QueryConfig::kAdjustTimestampToTimezone] = "true";
    configs[bolt::core::QueryConfig::kTimeParserPolicy] = boltCfg_->get<std::string>(kSparkLegacyTimeParserPolicy, "exception");
    {
      // Find offheap size from Spark confs. If found, set the max memory usage of partial aggregation.
      // Partial aggregation memory configurations.
      // TODO: Move the calculations to Java side.
      auto offHeapMemory = boltCfg_->get<int64_t>(kSparkTaskOffHeapMemory, bytedance::bolt::memory::kMaxMemory);
      auto maxPartialAggregationMemory = std::max<int64_t>(
          1 << 24,
          boltCfg_->get<int64_t>(kMaxPartialAggregationMemory).has_value()
              ? boltCfg_->get<int64_t>(kMaxPartialAggregationMemory).value()
              : static_cast<int64_t>(boltCfg_->get<double>(kMaxPartialAggregationMemoryRatio, 0.1) * offHeapMemory));
      auto maxExtendedPartialAggregationMemory = std::max<int64_t>(
          1 << 26,
          static_cast<long>(boltCfg_->get<double>(kMaxExtendedPartialAggregationMemoryRatio, 0.15) * offHeapMemory));
      configs[bolt::core::QueryConfig::kMaxPartialAggregationMemory] = std::to_string(maxPartialAggregationMemory);
      configs[bolt::core::QueryConfig::kMaxExtendedPartialAggregationMemory] =
          std::to_string(maxExtendedPartialAggregationMemory);
      configs[bolt::core::QueryConfig::kAbandonPartialAggregationMinPct] =
          std::to_string(boltCfg_->get<int32_t>(kAbandonPartialAggregationMinPct, 90));
      configs[bolt::core::QueryConfig::kAbandonPartialAggregationMinRows] =
          std::to_string(boltCfg_->get<int32_t>(kAbandonPartialAggregationMinRows, 100000));
      configs[bolt::core::QueryConfig::kPartialAggregationSpillMaxPct] =
          std::to_string(boltCfg_->get<int32_t>(kPartialAggregationSpillMaxPct, 50));
      configs[bolt::core::QueryConfig::kAbandonPartialAggregationMinFinalPct] =
          std::to_string(boltCfg_->get<int32_t>(kAbandonPartialAggregationMinFinalPct, 75));
      configs[bolt::core::QueryConfig::kSpilledAggregationBypassHTRatio] =
          std::to_string(boltCfg_->get<double>(kSpilledAggregationBypassHTRatio, 0.95));
      configs[bolt::core::QueryConfig::kPreferPartialAggregationSpill] =
          std::to_string(boltCfg_->get<bool>(kPreferPartialAggregationSpill, false));
      configs[bolt::core::QueryConfig::kAdaptiveSkippedDataSizeThreshold] =
          std::to_string(boltCfg_->get<uint64_t>(kAdaptiveSkippedDataSizeThreshold, 20UL << 30));
      configs[bolt::core::QueryConfig::kMaxHashTableSize] =
          std::to_string(boltCfg_->get<int32_t>(kMaxHashTableSize, 50L << 20));
      configs[bolt::core::QueryConfig::kHashAggregationCompositeOutputEnabled] =
          std::to_string(boltCfg_->get<bool>(kHashAggregationCompositeOutputEnabled, true));
      configs[bolt::core::QueryConfig::kHashAggregationUniqueRowOpt] =
          std::to_string(boltCfg_->get<bool>(kHashAggregationUniqueRowOpt, true));
      configs[bolt::core::QueryConfig::kHashAggregationCompositeOutputAccumulatorRatio] =
          std::to_string(boltCfg_->get<int32_t>(kHashAggregationCompositeOutputAccumulatorRatio, 5));
      configs[bolt::core::QueryConfig::kSpillUringEnabled] =
          boltCfg_->get<std::string>(kSpillUringEnabled, "false");
      configs[bolt::core::QueryConfig::kTestingSpillPct] =
          std::to_string(boltCfg_->get<int32_t>(kTestingSpillPct, 0));

      configs[bolt::core::QueryConfig::kEnableSonicIsJsonScalar] =
        std::to_string(boltCfg_->get<bool>(kEnableSonicIsJsonScalar, true));
      configs[bolt::core::QueryConfig::kEnableSonicJSsonArrayContains] =
        std::to_string(boltCfg_->get<bool>(kEnableSonicJsonArrayContains, true));
      configs[bolt::core::QueryConfig::kEnableSonicJsonArrayLength] =
        std::to_string(boltCfg_->get<bool>(kEnableSonicJsonArrayLength, true));
      configs[bolt::core::QueryConfig::kEnableSonicJsonExtractScalar] =
        std::to_string(boltCfg_->get<bool>(kEnableSonicJsonExtractScalar, true));
      configs[bolt::core::QueryConfig::kEnableSonicJsonExtract] =
        std::to_string(boltCfg_->get<bool>(kEnableSonicJsonExtract, true));
      configs[bolt::core::QueryConfig::kEnableSonicJsonSize] =
        std::to_string(boltCfg_->get<bool>(kEnableSonicJsonSize, true));
      configs[bolt::core::QueryConfig::kEnableSonicJsonSplit] =
        std::to_string(boltCfg_->get<bool>(kEnableSonicJsonSplit, true));
      configs[bolt::core::QueryConfig::kEnableSonicJsonParse] =
        std::to_string(boltCfg_->get<bool>(kEnableSonicJsonParse, true));
      configs[bolt::core::QueryConfig::kEnableSonicJsonToMap] =
        std::to_string(boltCfg_->get<bool>(kEnableSonicJsonToMap, true));

      int32_t maxParquetRepDefMemoryLimit =
          (int32_t)(boltCfg_->get<double>(kParquetRepDefMemoryRatio, 0.1) * offHeapMemory);
      maxParquetRepDefMemoryLimit = maxParquetRepDefMemoryLimit < 0 ? 1UL << 30 : maxParquetRepDefMemoryLimit;
      configs[bolt::core::QueryConfig::kParquetRepDefMemoryLimit] = std::to_string(maxParquetRepDefMemoryLimit);
    }
    // Spill configs
    if (spillStrategy_ == "none") {
      configs[bolt::core::QueryConfig::kSpillEnabled] = "false";
    } else {
      configs[bolt::core::QueryConfig::kSpillEnabled] = "true";
    }
    configs[bolt::core::QueryConfig::kAggregationSpillEnabled] =
        std::to_string(boltCfg_->get<bool>(kAggregationSpillEnabled, true));
    configs[bolt::core::QueryConfig::kJoinSpillEnabled] =
        std::to_string(boltCfg_->get<bool>(kJoinSpillEnabled, true));
    configs[bolt::core::QueryConfig::kOrderBySpillEnabled] =
        std::to_string(boltCfg_->get<bool>(kOrderBySpillEnabled, true));
    configs[bolt::core::QueryConfig::kOrderBySpillInOutputStageEnabled] =
        std::to_string(boltCfg_->get<bool>(kOrderBySpillInOutputStageEnabled, true));
    configs[bolt::core::QueryConfig::kWindowSpillEnabled] =
        std::to_string(boltCfg_->get<bool>(kWindowSpillEnabled, true));
    configs[bolt::core::QueryConfig::kMaxSpillLevel] = std::to_string(boltCfg_->get<int32_t>(kMaxSpillLevel, 4));
    configs[bolt::core::QueryConfig::kMaxSpillFileSize] =
        std::to_string(boltCfg_->get<uint64_t>(kMaxSpillFileSize, 1L * 1024 * 1024 * 1024));
    configs[bolt::core::QueryConfig::kMaxSpillRunRows] =
        std::to_string(boltCfg_->get<uint64_t>(kMaxSpillRunRows, 3L * 1024 * 1024));
    configs[bolt::core::QueryConfig::kMaxSpillBytes] =
        std::to_string(boltCfg_->get<uint64_t>(kMaxSpillBytes, 107374182400LL));
    configs[bolt::core::QueryConfig::kSpillWriteBufferSize] =
        std::to_string(boltCfg_->get<uint64_t>(kShuffleSpillDiskWriteBufferSize, 1L * 1024 * 1024));
    configs[kSpillReadBufferSize] = std::to_string(boltCfg_->get<int32_t>(kSpillReadBufferSize, 1L * 1024 * 1024));
    configs[bolt::core::QueryConfig::kSpillStartPartitionBit] =
        std::to_string(boltCfg_->get<uint8_t>(kSpillStartPartitionBit, 48));
    configs[bolt::core::QueryConfig::kSpillNumPartitionBits] =
        std::to_string(boltCfg_->get<uint8_t>(kSpillPartitionBits, 3));
    configs[bolt::core::QueryConfig::kSpillableReservationGrowthPct] =
        std::to_string(boltCfg_->get<uint8_t>(kSpillableReservationGrowthPct, 25));
    configs[kSpillPrefixSortEnabled] = boltCfg_->get<std::string>(kSpillPrefixSortEnabled, "false");
    if (boltCfg_->get<bool>(kSparkShuffleSpillCompress, true)) {
      configs[bolt::core::QueryConfig::kSpillCompressionKind] =
          boltCfg_->get<std::string>(kSpillCompressionKind, boltCfg_->get<std::string>(kCompressionKind, "lz4"));
    } else {
      configs[bolt::core::QueryConfig::kSpillCompressionKind] = "none";
    }
    configs[bolt::core::QueryConfig::kSparkBloomFilterExpectedNumItems] =
        std::to_string(boltCfg_->get<int64_t>(kBloomFilterExpectedNumItems, 1000000));
    configs[bolt::core::QueryConfig::kSparkBloomFilterNumBits] =
        std::to_string(boltCfg_->get<int64_t>(kBloomFilterNumBits, 8388608));
    configs[bolt::core::QueryConfig::kSparkBloomFilterMaxNumBits] =
        std::to_string(boltCfg_->get<int64_t>(kBloomFilterMaxNumBits, 4194304));
    // spark.gluten.sql.columnar.backend.bolt.SplitPreloadPerDriver takes no effect if
    // spark.gluten.sql.columnar.backend.bolt.IOThreads is set to 0
    auto [_, __, preloadSplitPerDriver, avgTaskMemory, preloadEnabled] =
        BoltBackend::getScanPreloadAdaptiveParam(BoltBackend::getCombinedConf(boltCfg_), false);
    configs[bolt::core::QueryConfig::kMaxSplitPreloadPerDriver] = std::to_string(preloadSplitPerDriver);
    configs[bolt::core::QueryConfig::kPreloadBytesLimit] = std::to_string(avgTaskMemory);
    configs[bolt::core::QueryConfig::kPreloadAdaptive] =
        std::to_string(preloadEnabled == 1 ? true : false);

    LOG(INFO) << "Split preload configs: "
              << " maxSplitPreloadPerDriver=" << configs[bolt::core::QueryConfig::kMaxSplitPreloadPerDriver]
              << ", preloadBytesLimit=" << configs[bolt::core::QueryConfig::kPreloadBytesLimit]
              << ", preloadAdaptive=" << configs[bolt::core::QueryConfig::kPreloadAdaptive];

    // hashtable build optimizations
    configs[bolt::core::QueryConfig::kAbandonBuildNoDupHashMinRows] =
        std::to_string(boltCfg_->get<int32_t>(kAbandonBuildNoDupHashMinRows, 100000));
    configs[bolt::core::QueryConfig::kAbandonBuildNoDupHashMinPct] =
        std::to_string(boltCfg_->get<int32_t>(kAbandonBuildNoDupHashMinPct, 80));

    // Disable driver cpu time slicing.
    configs[bolt::core::QueryConfig::kDriverCpuTimeSliceLimitMs] = "0";

    configs[bolt::core::QueryConfig::kSparkPartitionId] = std::to_string(taskInfo_.partitionId);

    if (boltCfg_->get<std::string>(kSparkMapKeyDedupPolicy, "") == "EXCEPTION") {
      configs[bolt::core::QueryConfig::kThrowExceptionOnDuplicateMapKeys] = "true";
    } else {
      configs[bolt::core::QueryConfig::kThrowExceptionOnDuplicateMapKeys] = "false";
    }
    // To align with Spark's behavior, set the policy to deduplicate map keys in builtin functions
    configs[bolt::core::QueryConfig::kSparkMapKeyDedupPolicy] =
      boltCfg_->get<std::string>(kSparkMapKeyDedupPolicy, "EXCEPTION");

    configs[bolt::core::QueryConfig::kSparkLegacyStatisticalAggregate] =
        std::to_string(boltCfg_->get<bool>(kSparkLegacyStatisticalAggregate, false));

    // configs[bolt::core::QueryConfig::kSparkJsonIgnoreNullFields] =
    //     std::to_string(boltCfg_->get<bool>(kSparkJsonIgnoreNullFields, true));
    configs[kSparkJsonIgnoreNullFields] = std::to_string(boltCfg_->get<bool>(kSparkJsonIgnoreNullFields, true));

    // configs[bolt::core::QueryConfig::kExprMaxCompiledRegexes] =
    //     std::to_string(boltCfg_->get<int32_t>(kExprMaxCompiledRegexes, 100));
    configs[kExprMaxCompiledRegexes] = std::to_string(boltCfg_->get<int32_t>(kExprMaxCompiledRegexes, 100));

#ifdef GLUTEN_ENABLE_GPU
    configs[bolt::cudf_bolt::CudfConfig::kCudfEnabled] = std::to_string(boltCfg_->get<bool>(kCudfEnabled, false));
#endif

    configs[bolt::core::QueryConfig::kDynamicConcurrencyAdjustmentEnabled] = std::to_string(
        boltCfg_->get<bool>(kDynamicConcurrencyAdjustmentEnabled, kDynamicConcurrencyAdjustmentEnabledDefault));

    configs[bolt::core::QueryConfig::kBoltTaskSchedulingEnabled] =
        std::to_string(boltCfg_->get<bool>(kBoltTaskSchedulingEnabled, false));

    {
      // parquet encryption related config
      for (auto cfgKey : parquetDecryptionConfigs) {
        auto val = boltCfg_->get<std::string>("spark.hadoop." + cfgKey);
        if (val.has_value()) {
          configs[cfgKey] = val.value();
        }
      }
    }

    const auto setIfExists = [&](const std::string& glutenKey, const std::string& boltKey) {
      const auto valueOptional = boltCfg_->get<std::string>(glutenKey);
      if (valueOptional.has_value()) {
        configs[boltKey] = valueOptional.value();
      }
    };
    setIfExists(kQueryTraceEnabled, bolt::core::QueryConfig::kQueryTraceEnabled);
    setIfExists(kQueryTraceDir, bolt::core::QueryConfig::kQueryTraceDir);
    setIfExists(kQueryTraceMaxBytes, bolt::core::QueryConfig::kQueryTraceMaxBytes);
    setIfExists(kQueryTraceTaskRegExp, bolt::core::QueryConfig::kQueryTraceTaskRegExp);
    setIfExists(kOpTraceDirectoryCreateConfig, bolt::core::QueryConfig::kOpTraceDirectoryCreateConfig);
  } catch (const std::invalid_argument& err) {
    std::string errDetails = err.what();
    throw std::runtime_error("Invalid conf arg: " + errDetails);
  }
  return configs;
}

std::shared_ptr<bolt::config::ConfigBase> WholeStageResultIterator::createConnectorConfig() {
  // The configs below are used at session level.
  std::unordered_map<std::string, std::string> configs = {};
  // The semantics of reading as lower case is opposite with case-sensitive.
  configs[bolt::connector::hive::HiveConfig::kFileColumnNamesReadAsLowerCaseSession] =
      !boltCfg_->get<bool>(kCaseSensitive, false) ? "true" : "false";
  configs[bolt::connector::hive::HiveConfig::kPartitionPathAsLowerCaseSession] = "false";
  configs[bolt::connector::hive::HiveConfig::kArrowBridgeTimestampUnit] = "6";
  configs[bolt::connector::hive::HiveConfig::kReadTimestampUnitSession] =
      "6";
  configs[bolt::connector::hive::HiveConfig::kMaxPartitionsPerWritersSession] =
      std::to_string(boltCfg_->get<int32_t>(kMaxPartitions, 10000));
  configs[bolt::connector::hive::HiveConfig::kIgnoreMissingFilesSession] =
      std::to_string(boltCfg_->get<bool>(kIgnoreMissingFiles, false));
  configs[bolt::connector::hive::HiveConfig::kParquetUseColumnNamesSession] =
      std::to_string(boltCfg_->get<bool>(kParquetUseColumnNames, true));
  configs[bolt::connector::hive::HiveConfig::kOrcUseColumnNamesSession] =
        std::to_string(boltCfg_->get<bool>(kOrcUseColumnNames, true));
  return std::make_shared<bolt::config::ConfigBase>(std::move(configs));
}

} // namespace gluten

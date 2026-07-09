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

#include <gflags/gflags.h>

#include <bolt/common/memory/sparksql/MemoryTarget.h>
#include "BoltRuntime.h"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <utility>

#include "BoltBackend.h"
#include "compute/BoltPlanConverter.h"
#include "compute/ResultIterator.h"
#include "compute/Runtime.h"
#include "config/BoltConfig.h"
#include "config/GlutenConfig.h"
#include "memory/BoltGlutenMemoryManager.h"
#include "operators/serializer/BoltRowToColumnarConverter.h"
#include "shuffle/BoltShuffleReaderWrapper.h"
#include "shuffle/BoltShuffleWriterWrapper.h"
#include "utils/BoltArrowUtils.h"
#include "utils/BoltWholeStageDumper.h"
#include "utils/ConfigExtractor.h"

DECLARE_bool(bolt_exception_user_stacktrace_enabled);
DECLARE_bool(bolt_memory_use_hugepages);
DECLARE_bool(bolt_memory_pool_capacity_transfer_across_tasks);

#ifdef ENABLE_HDFS
#include "operators/writer/BoltParquetDataSourceHDFS.h"
#endif

#ifdef ENABLE_S3
#include "operators/writer/BoltParquetDataSourceS3.h"
#endif

#ifdef ENABLE_GCS
#include "operators/writer/BoltParquetDataSourceGCS.h"
#endif

#ifdef ENABLE_ABFS
#include "operators/writer/BoltParquetDataSourceABFS.h"
#endif

using namespace bytedance;

namespace gluten {

BoltRuntime::BoltRuntime(
    const std::string& kind,
    BoltMemoryManager* vmm,
    ThreadManager* threadManager,
    const std::unordered_map<std::string, std::string>& confMap,
    RuntimeOptions options)
    : Runtime(kind, vmm, threadManager, confMap, std::move(options)) {
  // Refresh session config.
  boltCfg_ =
      std::make_shared<bytedance::bolt::config::ConfigBase>(std::unordered_map<std::string, std::string>(confMap_));

  gluten::BoltGlutenMemoryManager::init(BoltBackend::getCombinedConf(boltCfg_)->rawConfigs());

  if (gluten::BoltGlutenMemoryManager::enabled()) {
    auto holder = gluten::BoltGlutenMemoryManager::getMemoryManagerHolder(
        memoryManager()->name(), runtimeOptions().taskAttemptId, reinterpret_cast<int64_t>(memoryManager()));
    auto mm = holder->getManager();
    leafPool_ = mm->getLeafMemoryPool();
    aggregatePool_ = mm->getAggregateMemoryPool();
    arrowPool_ = mm->getArrowMemoryPool();
  } else {
    leafPool_ = memoryManager()->getLeafMemoryPool();
    aggregatePool_ = memoryManager()->getAggregateMemoryPool();
    arrowPool_ = memoryManager()->defaultArrowMemoryPool();
  }

  debugModeEnabled_ = boltCfg_->get<bool>(kDebugModeEnabled, false);
  FLAGS_minloglevel = boltCfg_->get<uint32_t>(kGlogSeverityLevel, FLAGS_minloglevel);
  FLAGS_v = boltCfg_->get<uint32_t>(kGlogVerboseLevel, FLAGS_v);
  FLAGS_bolt_exception_user_stacktrace_enabled =
      boltCfg_->get<bool>(kEnableUserExceptionStacktrace, FLAGS_bolt_exception_user_stacktrace_enabled);
  FLAGS_bolt_exception_system_stacktrace_enabled =
      boltCfg_->get<bool>(kEnableSystemExceptionStacktrace, FLAGS_bolt_exception_system_stacktrace_enabled);
  FLAGS_bolt_memory_use_hugepages = boltCfg_->get<bool>(kMemoryUseHugePages, FLAGS_bolt_memory_use_hugepages);
  FLAGS_bolt_memory_pool_capacity_transfer_across_tasks = boltCfg_->get<bool>(
      kMemoryPoolCapacityTransferAcrossTasks, FLAGS_bolt_memory_pool_capacity_transfer_across_tasks);
}

void BoltRuntime::parsePlan(const uint8_t* data, int32_t size) {
  if (debugModeEnabled_ || dumper_ != nullptr) {
    try {
      auto planJson = substraitFromPbToJson("Plan", data, size);
      if (dumper_ != nullptr) {
        dumper_->dumpPlan(planJson);
      }

      LOG_IF(INFO, debugModeEnabled_ && taskInfo_.has_value())
          << std::string(50, '#') << " received substrait::Plan: " << taskInfo_.value() << std::endl
          << planJson;
    } catch (const std::exception& e) {
      LOG(WARNING) << "Error converting substrait::Plan to JSON: " << e.what();
    }
  }

  GLUTEN_CHECK(parseProtobuf(data, size, &substraitPlan_) == true, "Parse substrait plan failed");
}

void BoltRuntime::parseSplitInfo(const uint8_t* data, int32_t size, int32_t splitIndex) {
  if (debugModeEnabled_ || dumper_ != nullptr) {
    try {
      auto splitJson = substraitFromPbToJson("ReadRel.LocalFiles", data, size);
      if (dumper_ != nullptr) {
        dumper_->dumpInputSplit(splitIndex, splitJson);
      }
      LOG_IF(INFO, debugModeEnabled_ && taskInfo_.has_value())
          << std::string(50, '#') << " received substrait::ReadRel.LocalFiles: " << taskInfo_.value() << std::endl
          << splitJson;
    } catch (const std::exception& e) {
      LOG(WARNING) << "Error converting substrait::ReadRel.LocalFiles to JSON: " << e.what();
    }
  }
  ::substrait::ReadRel_LocalFiles localFile;
  GLUTEN_CHECK(parseProtobuf(data, size, &localFile) == true, "Parse substrait plan failed");
  localFiles_.push_back(localFile);
}

void BoltRuntime::getInfoAndIds(
    const std::unordered_map<bolt::core::PlanNodeId, std::shared_ptr<SplitInfo>>& splitInfoMap,
    const std::unordered_set<bolt::core::PlanNodeId>& leafPlanNodeIds,
    std::vector<std::shared_ptr<SplitInfo>>& scanInfos,
    std::vector<bolt::core::PlanNodeId>& scanIds,
    std::vector<bolt::core::PlanNodeId>& streamIds) {
  for (const auto& leafPlanNodeId : leafPlanNodeIds) {
    auto it = splitInfoMap.find(leafPlanNodeId);
    if (it == splitInfoMap.end()) {
      throw std::runtime_error("Could not find leafPlanNodeId.");
    }
    auto splitInfo = it->second;
    switch (splitInfo->leafType) {
      case SplitInfo::LeafType::SPLIT_AWARE_STREAM:
        streamIds.emplace_back(leafPlanNodeId);
        break;
      case SplitInfo::LeafType::TABLE_SCAN:
        scanInfos.emplace_back(splitInfo);
        scanIds.emplace_back(leafPlanNodeId);
        break;
      case SplitInfo::LeafType::TRIVIAL_LEAF:
        break;
    }
  }
}

std::string BoltRuntime::planString(bool details, const std::unordered_map<std::string, std::string>& sessionConf) {
  std::vector<std::shared_ptr<ResultIterator>> inputs;
  auto boltMemoryPool = gluten::defaultLeafBoltMemoryPool();
  BoltPlanConverter boltPlanConverter(inputs, boltMemoryPool.get(), boltCfg_.get(), std::nullopt, std::nullopt, true);
  auto boltPlan = boltPlanConverter.toBoltPlan(substraitPlan_, localFiles_);
  return boltPlan->toString(details, true);
}

BoltMemoryManager* BoltRuntime::memoryManager() {
  auto vmm = dynamic_cast<BoltMemoryManager*>(memoryManager_);
  GLUTEN_CHECK(vmm != nullptr, "Not a Bolt memory manager");
  return vmm;
}

std::shared_ptr<ResultIterator> BoltRuntime::createResultIterator(
    const std::string& spillDir,
    const std::vector<std::shared_ptr<ResultIterator>>& inputs) {
  LOG_IF(INFO, debugModeEnabled_) << "BoltRuntime session config:" << printConfig(confMap_);

  BoltPlanConverter boltPlanConverter(
      inputs, leafPool_.get(), boltCfg_.get(), *localWriteFilesTempPath(), *localWriteFileName());
  boltPlan_ = boltPlanConverter.toBoltPlan(substraitPlan_, std::move(localFiles_));
  LOG_IF(INFO, debugModeEnabled_ && taskInfo_.has_value())
      << "############### Bolt plan for task " << taskInfo_.value() << " ###############" << std::endl
      << boltPlan_->toString(true, true);

  // Scan node can be required.
  std::vector<std::shared_ptr<SplitInfo>> scanInfos;
  std::vector<bolt::core::PlanNodeId> scanIds;
  std::vector<bolt::core::PlanNodeId> streamIds;

  // Separate the scan ids and stream ids, and get the scan infos.
  getInfoAndIds(boltPlanConverter.splitInfos(), boltPlan_->leafPlanNodeIds(), scanInfos, scanIds, streamIds);

  auto wholeStageIter = std::make_unique<WholeStageResultIterator>(
      memoryManager(),
      boltPlan_,
      scanIds,
      scanInfos,
      streamIds,
      spillDir,
      confMap_,
      taskInfo_.has_value() ? taskInfo_.value() : SparkTaskInfo{});
  auto ans = std::make_shared<ResultIterator>(std::move(wholeStageIter), this);
  if (gluten::BoltGlutenMemoryManager::enabled()) {
    std::weak_ptr<ResultIterator> weakAns = ans;
    auto spiller = std::make_shared<OperatorSpiller>(weakAns);
    auto genericSpiller = std::dynamic_pointer_cast<bytedance::bolt::memory::sparksql::Spiller>(spiller);
    auto holder = gluten::BoltGlutenMemoryManager::getMemoryManagerHolder(
        memoryManager()->name(), runtimeOptions().taskAttemptId, reinterpret_cast<int64_t>(memoryManager()));
    holder->appendSpiller(genericSpiller);
  }

  return ans;
}

void BoltRuntime::noMoreSplits(ResultIterator* iter) {
  auto* splitAwareIter = dynamic_cast<gluten::SplitAwareColumnarBatchIterator*>(iter->getInputIter());
  if (splitAwareIter == nullptr) {
    throw GlutenException("Iterator does not support split management");
  }
  splitAwareIter->noMoreSplits();
}

void BoltRuntime::requestBarrier(ResultIterator* iter) {
  auto* splitAwareIter = dynamic_cast<gluten::SplitAwareColumnarBatchIterator*>(iter->getInputIter());
  if (splitAwareIter == nullptr) {
    throw GlutenException("Iterator does not support split management");
  }
  splitAwareIter->requestBarrier();
}

std::shared_ptr<ColumnarToRowConverter> BoltRuntime::createColumnar2RowConverter(int64_t column2RowMemThreshold) {
  return std::make_shared<BoltColumnarToRowConverter>(leafPool_, column2RowMemThreshold);
}

std::shared_ptr<ColumnarBatch> BoltRuntime::createOrGetEmptySchemaBatch(int32_t numRows) {
  auto& lookup = emptySchemaBatchLoopUp_;
  if (lookup.find(numRows) == lookup.end()) {
    const std::shared_ptr<BoltColumnarBatch>& batch =
        BoltColumnarBatch::from(leafPool_.get(), gluten::createZeroColumnBatch(numRows));
    lookup.emplace(numRows, batch); // the batch will be released after Spark task ends
  }
  return lookup.at(numRows);
}

std::shared_ptr<ColumnarBatch> BoltRuntime::select(
    std::shared_ptr<ColumnarBatch> batch,
    const std::vector<int32_t>& columnIndices) {
  auto boltBatch = gluten::BoltColumnarBatch::from(leafPool_.get(), batch);
  auto outputBatch = boltBatch->select(leafPool_.get(), std::move(columnIndices));
  return outputBatch;
}

std::shared_ptr<RowToColumnarConverter> BoltRuntime::createRow2ColumnarConverter(struct ArrowSchema* cSchema) {
  return std::make_shared<BoltRowToColumnarConverter>(cSchema, leafPool_);
}

#ifdef GLUTEN_ENABLE_ENHANCED_FEATURES
std::shared_ptr<IcebergWriter> BoltRuntime::createIcebergWriter(
    RowTypePtr rowType,
    int32_t format,
    const std::string& outputDirectory,
    bytedance::bolt::common::CompressionKind compressionKind,
    std::shared_ptr<const bytedance::bolt::connector::hive::iceberg::IcebergPartitionSpec> spec,
    const gluten::IcebergNestedField& protoField,
    const std::unordered_map<std::string, std::string>& sparkConfs) {
  return std::make_shared<IcebergWriter>(
      rowType,
      format,
      outputDirectory,
      compressionKind,
      spec,
      protoField,
      sparkConfs,
      leafPool_.get(),
      aggregatePool_.get());
}
#endif

std::shared_ptr<BoltDataSource> BoltRuntime::createDataSource(
    const std::string& filePath,
    std::shared_ptr<arrow::Schema> schema) {
  static std::atomic_uint32_t id{0UL};
  auto boltPool = aggregatePool_.get()->addAggregateChild("datasource." + std::to_string(id++));
  // Pass a dedicate pool for S3 and GCS sinks as can't share boltPool
  // with parquet writer.
  // FIXME: Check file formats?
  auto sinkPool = leafPool_;
  if (isSupportedHDFSPath(filePath)) {
#ifdef ENABLE_HDFS
    return std::make_shared<BoltParquetDataSourceHDFS>(filePath, boltPool, sinkPool, schema);
#else
    throw std::runtime_error(
        "The write path is hdfs path but the HDFS haven't been enabled when writing parquet data in bolt runtime!");
#endif
  } else if (isSupportedS3SdkPath(filePath)) {
#ifdef ENABLE_S3
    return std::make_shared<BoltParquetDataSourceS3>(filePath, boltPool, sinkPool, schema);
#else
    throw std::runtime_error(
        "The write path is S3 path but the S3 haven't been enabled when writing parquet data in bolt runtime!");
#endif
  } else if (isSupportedGCSPath(filePath)) {
#ifdef ENABLE_GCS
    return std::make_shared<BoltParquetDataSourceGCS>(filePath, boltPool, sinkPool, schema);
#else
    throw std::runtime_error(
        "The write path is GCS path but the GCS haven't been enabled when writing parquet data in bolt runtime!");
#endif
  } else if (isSupportedABFSPath(filePath)) {
#ifdef ENABLE_ABFS
    return std::make_shared<BoltParquetDataSourceABFS>(filePath, boltPool, sinkPool, schema);
#else
    throw std::runtime_error(
        "The write path is ABFS path but the ABFS haven't been enabled when writing parquet data in bolt runtime!");
#endif
  }
  return std::make_shared<BoltParquetDataSource>(filePath, boltPool, sinkPool, schema);
}

std::unique_ptr<ColumnarBatchSerializer> BoltRuntime::createColumnarBatchSerializer(struct ArrowSchema* cSchema) {
  return std::make_unique<BoltColumnarBatchSerializer>(arrowPool_, leafPool_, cSchema);
}

void BoltRuntime::enableDumping() {
  auto saveDir = boltCfg_->get<std::string>(kGlutenSaveDir);
  GLUTEN_CHECK(saveDir.has_value(), kGlutenSaveDir + " is not set");

  auto taskInfo = getSparkTaskInfo();
  GLUTEN_CHECK(taskInfo.has_value(), "Task info is not set. Please set task info before enabling dumping.");

  dumper_ = std::make_shared<BoltWholeStageDumper>(
      taskInfo.value(), saveDir.value(), boltCfg_->get<int64_t>(kSparkBatchSize, 4096), aggregatePool_.get());

  dumper_->dumpConf(getConfMap());
}

std::shared_ptr<ShuffleWriterBase> BoltRuntime::createShuffleWriter(
    const ShuffleWriterInfo& info,
    std::shared_ptr<RssClient> rssClient,
    std::shared_ptr<ColumnarBatch> cb) {
  int32_t numColumnsExludePid = cb ? cb->numColumns() - 1 : 0;
  // used to calculate prealloc row size, only used for hash partitioning
  int32_t firstBatchRowNumber = 0, firstBatchFlatSize = 0;
  using bytedance::bolt::shuffle::sparksql::supportAdaptiveShuffleWriter;
  using bytedance::bolt::shuffle::sparksql::toPartitioning;
  // for partitioning support adaptive shuffle writer, should pass first batch information to compute preAllocSize
  if (info.forced_writer_type() == 0 && supportAdaptiveShuffleWriter(toPartitioning(info.partitioning_name()))) {
    auto boltColumnBatch = BoltColumnarBatch::from(leafPool_.get(), cb);
    BOLT_CHECK_NOT_NULL(boltColumnBatch);
    const auto& rv = boltColumnBatch->getRowVector();
    firstBatchRowNumber = rv->size();
    firstBatchFlatSize = rv->estimateFlatSize();
  }

  auto shuffleWriter = std::make_shared<BoltShuffleWriterWrapper>(
      info, rssClient, numColumnsExludePid, firstBatchRowNumber, firstBatchFlatSize, leafPool_.get(), arrowPool_);
  GLUTEN_CHECK(shuffleWriter != nullptr, "Failed to create BoltShuffleWriter");

  if (gluten::BoltGlutenMemoryManager::enabled()) {
    auto weakShuffleWriter = std::weak_ptr<ShuffleWriterBase>(shuffleWriter);
    auto spiller = std::make_shared<ShuffleSpiller>(weakShuffleWriter);
    auto genericSpiller = std::dynamic_pointer_cast<bytedance::bolt::memory::sparksql::Spiller>(spiller);
    auto holder = gluten::BoltGlutenMemoryManager::getMemoryManagerHolder(
        memoryManager()->name(), runtimeOptions().taskAttemptId, reinterpret_cast<int64_t>(memoryManager()));
    holder->appendSpiller(genericSpiller);
  }
  return shuffleWriter;
}

std::shared_ptr<ShuffleReaderBase> BoltRuntime::createShuffleReader(
    std::shared_ptr<arrow::Schema> schema,
    const ShuffleReaderInfo& info) {
  auto rowType = bytedance::bolt::asRowType(gluten::fromArrowSchema(schema));

  return std::make_shared<BoltShuffleReaderWrapper>(schema, info, arrowPool_, leafPool_.get());
}

} // namespace gluten

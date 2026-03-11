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

#include "VeloxRuntime.h"

#include <operators/plannodes/RowVectorStream.h>

#include <algorithm>
#include <filesystem>

#include "VeloxBackend.h"
#include "compute/ResultIterator.h"
#include "compute/Runtime.h"
#include "compute/VeloxPlanConverter.h"
#include "config/VeloxConfig.h"
#include "operators/serializer/VeloxRowToColumnarConverter.h"
#include "shuffle/VeloxShuffleReader.h"
#include "shuffle/VeloxShuffleWriter.h"
#include "utils/ConfigExtractor.h"
#include "utils/VeloxArrowUtils.h"
#include "utils/VeloxWholeStageDumper.h"

DECLARE_bool(velox_exception_user_stacktrace_enabled);
DECLARE_bool(velox_memory_use_hugepages);
DECLARE_bool(velox_memory_pool_capacity_transfer_across_tasks);

#ifdef ENABLE_HDFS
#include "operators/writer/VeloxParquetDataSourceHDFS.h"
#endif

#ifdef ENABLE_S3
#include "operators/writer/VeloxParquetDataSourceS3.h"
#endif

#ifdef ENABLE_GCS
#include "operators/writer/VeloxParquetDataSourceGCS.h"
#endif

#ifdef ENABLE_ABFS
#include "operators/writer/VeloxParquetDataSourceABFS.h"
#endif

#ifdef GLUTEN_ENABLE_GPU
#include "operators/serializer/VeloxGpuColumnarBatchSerializer.h"
#endif

using namespace facebook;

namespace gluten {

VeloxRuntime::VeloxRuntime(
    const std::string& kind,
    VeloxMemoryManager* vmm,
    const std::unordered_map<std::string, std::string>& confMap)
    : Runtime(kind, vmm, confMap) {
  // Refresh session config.
  veloxCfg_ =
      std::make_shared<facebook::velox::config::ConfigBase>(std::unordered_map<std::string, std::string>(confMap_));
  debugModeEnabled_ = veloxCfg_->get<bool>(kDebugModeEnabled, false);
  FLAGS_minloglevel = veloxCfg_->get<uint32_t>(kGlogSeverityLevel, FLAGS_minloglevel);
  FLAGS_v = veloxCfg_->get<uint32_t>(kGlogVerboseLevel, FLAGS_v);
  FLAGS_velox_exception_user_stacktrace_enabled =
      veloxCfg_->get<bool>(kEnableUserExceptionStacktrace, FLAGS_velox_exception_user_stacktrace_enabled);
  FLAGS_velox_exception_system_stacktrace_enabled =
      veloxCfg_->get<bool>(kEnableSystemExceptionStacktrace, FLAGS_velox_exception_system_stacktrace_enabled);
  FLAGS_velox_memory_use_hugepages = veloxCfg_->get<bool>(kMemoryUseHugePages, FLAGS_velox_memory_use_hugepages);
  FLAGS_velox_memory_pool_capacity_transfer_across_tasks = veloxCfg_->get<bool>(
      kMemoryPoolCapacityTransferAcrossTasks, FLAGS_velox_memory_pool_capacity_transfer_across_tasks);
}

void VeloxRuntime::parsePlan(const uint8_t* data, int32_t size) {
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

void VeloxRuntime::parseSplitInfo(const uint8_t* data, int32_t size, int32_t splitIndex) {
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

void VeloxRuntime::getInfoAndIds(
    const std::unordered_map<velox::core::PlanNodeId, std::shared_ptr<SplitInfo>>& splitInfoMap,
    const std::unordered_set<velox::core::PlanNodeId>& leafPlanNodeIds,
    std::vector<std::shared_ptr<SplitInfo>>& scanInfos,
    std::vector<velox::core::PlanNodeId>& scanIds,
    std::vector<velox::core::PlanNodeId>& streamIds) {
  int32_t streamIdx = 0;
  for (const auto& leafPlanNodeId : leafPlanNodeIds) {
    auto it = splitInfoMap.find(leafPlanNodeId);
    if (it == splitInfoMap.end()) {
      throw std::runtime_error("Could not find leafPlanNodeId.");
    }
    auto splitInfo = it->second;
    // Based on the current code, indexing of streams and files follow different orders:
    // 1. Streams follow "iterator:<idx>" in the substrait plan;
    // 2. Files follow the traversal order in the plan node tree.
    // FIXME: Why we didn't have a unified design?
    switch (splitInfo->leafType) {
    case SplitInfo::LeafType::SPLIT_AWARE_STREAM:
      streamIds.emplace_back(ValueStreamConnectorFactory::nodeIdOf(streamIdx++));
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

std::string VeloxRuntime::planString(bool details, const std::unordered_map<std::string, std::string>& sessionConf) {
  auto veloxMemoryPool = gluten::defaultLeafVeloxMemoryPool();
  VeloxPlanConverter veloxPlanConverter(veloxMemoryPool.get(), veloxCfg_.get(), {}, std::nullopt, std::nullopt, true);
  auto veloxPlan = veloxPlanConverter.toVeloxPlan(substraitPlan_, localFiles_);
  return veloxPlan->toString(details, true);
}

VeloxMemoryManager* VeloxRuntime::memoryManager() {
  auto vmm = dynamic_cast<VeloxMemoryManager*>(memoryManager_);
  GLUTEN_CHECK(vmm != nullptr, "Not a Velox memory manager");
  return vmm;
}

std::shared_ptr<ResultIterator> VeloxRuntime::createResultIterator(
    const std::string& spillDir,
    const std::vector<std::shared_ptr<ResultIterator>>& inputs) {
  LOG_IF(INFO, debugModeEnabled_) << "VeloxRuntime session config:" << printConfig(confMap_);

  VeloxPlanConverter veloxPlanConverter(
      memoryManager()->getLeafMemoryPool().get(),
      veloxCfg_.get(),
      inputs,
      *localWriteFilesTempPath(),
      *localWriteFileName());
  veloxPlan_ = veloxPlanConverter.toVeloxPlan(substraitPlan_, std::move(localFiles_));
  LOG_IF(INFO, debugModeEnabled_ && taskInfo_.has_value())
      << "############### Velox plan for task " << taskInfo_.value() << " ###############" << std::endl
      << veloxPlan_->toString(true, true);

  // Scan node can be required.
  std::vector<std::shared_ptr<SplitInfo>> scanInfos;
  std::vector<velox::core::PlanNodeId> scanIds;
  std::vector<velox::core::PlanNodeId> streamIds;

  // Separate the scan ids and stream ids, and get the scan infos.
  getInfoAndIds(veloxPlanConverter.splitInfos(), veloxPlan_->leafPlanNodeIds(), scanInfos, scanIds, streamIds);

  auto wholeStageIter = std::make_unique<WholeStageResultIterator>(
      memoryManager(),
      veloxPlan_,
      scanIds,
      scanInfos,
      streamIds,
      spillDir,
      veloxCfg_,
      taskInfo_.has_value() ? taskInfo_.value() : SparkTaskInfo{});

  auto remainingInputIterators = veloxPlanConverter.remainingInputIterators();
  if (!remainingInputIterators.empty()) {
  // Converts remaining input iterators to splits and add them to the task.
    wholeStageIter->addIteratorSplits(remainingInputIterators);
  }

  return std::make_shared<ResultIterator>(std::move(wholeStageIter), this);
}

void VeloxRuntime::noMoreSplits(ResultIterator* iter){
    auto* splitAwareIter = dynamic_cast<gluten::SplitAwareColumnarBatchIterator*>(iter->getInputIter());
    if (splitAwareIter == nullptr) {
      throw GlutenException("Iterator does not support split management");
    }
    splitAwareIter->noMoreSplits();
}

void VeloxRuntime::requestBarrier(ResultIterator* iter){
    auto* splitAwareIter = dynamic_cast<gluten::SplitAwareColumnarBatchIterator*>(iter->getInputIter());
    if (splitAwareIter == nullptr) {
      throw GlutenException("Iterator does not support split management");
    }
    splitAwareIter->requestBarrier();
}

std::shared_ptr<ColumnarToRowConverter> VeloxRuntime::createColumnar2RowConverter(int64_t column2RowMemThreshold) {
  auto veloxPool = memoryManager()->getLeafMemoryPool();
  return std::make_shared<VeloxColumnarToRowConverter>(veloxPool, column2RowMemThreshold);
}

std::shared_ptr<ColumnarBatch> VeloxRuntime::createOrGetEmptySchemaBatch(int32_t numRows) {
  auto& lookup = emptySchemaBatchLoopUp_;
  if (lookup.find(numRows) == lookup.end()) {
    auto veloxPool = memoryManager()->getLeafMemoryPool();
    const std::shared_ptr<VeloxColumnarBatch>& batch =
        VeloxColumnarBatch::from(veloxPool.get(), gluten::createZeroColumnBatch(numRows));
    lookup.emplace(numRows, batch); // the batch will be released after Spark task ends
  }
  return lookup.at(numRows);
}

std::shared_ptr<ColumnarBatch> VeloxRuntime::select(
    std::shared_ptr<ColumnarBatch> batch,
    const std::vector<int32_t>& columnIndices) {
  auto veloxPool = memoryManager()->getLeafMemoryPool();
  auto veloxBatch = gluten::VeloxColumnarBatch::from(veloxPool.get(), batch);
  auto outputBatch = veloxBatch->select(veloxPool.get(), std::move(columnIndices));
  return outputBatch;
}

std::shared_ptr<RowToColumnarConverter> VeloxRuntime::createRow2ColumnarConverter(struct ArrowSchema* cSchema) {
  auto veloxPool = memoryManager()->getLeafMemoryPool();
  return std::make_shared<VeloxRowToColumnarConverter>(cSchema, veloxPool);
}

#ifdef GLUTEN_ENABLE_ENHANCED_FEATURES
std::shared_ptr<IcebergWriter> VeloxRuntime::createIcebergWriter(
    RowTypePtr rowType,
    int32_t format,
    const std::string& outputDirectory,
    facebook::velox::common::CompressionKind compressionKind,
    int32_t partitionId,
    int64_t taskId,
    const std::string& operationId,
    std::shared_ptr<const facebook::velox::connector::hive::iceberg::IcebergPartitionSpec> spec,
    const gluten::IcebergNestedField& protoField,
    const std::unordered_map<std::string, std::string>& sparkConfs) {
  auto veloxPool = memoryManager()->getLeafMemoryPool();
  auto connectorPool = memoryManager()->getAggregateMemoryPool();
  return std::make_shared<IcebergWriter>(
      rowType, format, outputDirectory, compressionKind, partitionId, taskId, operationId, spec, protoField, sparkConfs, veloxPool, connectorPool);
}
#endif

std::shared_ptr<ShuffleWriter> VeloxRuntime::createShuffleWriter(
    int32_t numPartitions,
    const std::shared_ptr<PartitionWriter>& partitionWriter,
    const std::shared_ptr<ShuffleWriterOptions>& options) {
  GLUTEN_ASSIGN_OR_THROW(
      std::shared_ptr<ShuffleWriter> shuffleWriter,
      VeloxShuffleWriter::create(options->shuffleWriterType, numPartitions, partitionWriter, options, memoryManager()));
  return shuffleWriter;
}

std::shared_ptr<VeloxDataSource> VeloxRuntime::createDataSource(
    const std::string& filePath,
    std::shared_ptr<arrow::Schema> schema) {
  static std::atomic_uint32_t id{0UL};
  auto veloxPool = memoryManager()->getAggregateMemoryPool()->addAggregateChild("datasource." + std::to_string(id++));
  // Pass a dedicate pool for S3 and GCS sinks as can't share veloxPool
  // with parquet writer.
  // FIXME: Check file formats?
  auto sinkPool = memoryManager()->getLeafMemoryPool();
  if (isSupportedHDFSPath(filePath)) {
#ifdef ENABLE_HDFS
    return std::make_shared<VeloxParquetDataSourceHDFS>(filePath, veloxPool, sinkPool, schema);
#else
    throw std::runtime_error(
        "The write path is hdfs path but the HDFS haven't been enabled when writing parquet data in velox runtime!");
#endif
  } else if (isSupportedS3SdkPath(filePath)) {
#ifdef ENABLE_S3
    return std::make_shared<VeloxParquetDataSourceS3>(filePath, veloxPool, sinkPool, schema);
#else
    throw std::runtime_error(
        "The write path is S3 path but the S3 haven't been enabled when writing parquet data in velox runtime!");
#endif
  } else if (isSupportedGCSPath(filePath)) {
#ifdef ENABLE_GCS
    return std::make_shared<VeloxParquetDataSourceGCS>(filePath, veloxPool, sinkPool, schema);
#else
    throw std::runtime_error(
        "The write path is GCS path but the GCS haven't been enabled when writing parquet data in velox runtime!");
#endif
  } else if (isSupportedABFSPath(filePath)) {
#ifdef ENABLE_ABFS
    return std::make_shared<VeloxParquetDataSourceABFS>(filePath, veloxPool, sinkPool, schema);
#else
    throw std::runtime_error(
        "The write path is ABFS path but the ABFS haven't been enabled when writing parquet data in velox runtime!");
#endif
  }
  return std::make_shared<VeloxParquetDataSource>(filePath, veloxPool, sinkPool, schema);
}

std::shared_ptr<ShuffleReader> VeloxRuntime::createShuffleReader(
    std::shared_ptr<arrow::Schema> schema,
    ShuffleReaderOptions options) {
  auto codec = gluten::createCompressionCodec(options.compressionType, options.codecBackend);
  const auto veloxCompressionKind = arrowCompressionTypeToVelox(options.compressionType);
  const auto rowType = facebook::velox::asRowType(gluten::fromArrowSchema(schema));

  auto deserializerFactory = std::make_unique<gluten::VeloxShuffleReaderDeserializerFactory>(
      schema,
      std::move(codec),
      veloxCompressionKind,
      rowType,
      options.batchSize,
      options.readerBufferSize,
      options.deserializerBufferSize,
      memoryManager(),
      options.shuffleWriterType);

  return std::make_shared<VeloxShuffleReader>(std::move(deserializerFactory));
}

std::unique_ptr<ColumnarBatchSerializer> VeloxRuntime::createColumnarBatchSerializer(struct ArrowSchema* cSchema) {
  auto arrowPool = memoryManager()->defaultArrowMemoryPool();
  auto veloxPool = memoryManager()->getLeafMemoryPool();
#ifdef GLUTEN_ENABLE_GPU
  if (veloxCfg_->get<bool>(kCudfEnabled, kCudfEnabledDefault)) {
    return std::make_unique<VeloxGpuColumnarBatchSerializer>(arrowPool, veloxPool, cSchema);
  }
#endif
  return std::make_unique<VeloxColumnarBatchSerializer>(arrowPool, veloxPool, cSchema);
}

void VeloxRuntime::enableDumping() {
  auto saveDir = veloxCfg_->get<std::string>(kGlutenSaveDir);
  GLUTEN_CHECK(saveDir.has_value(), kGlutenSaveDir + " is not set");

  auto taskInfo = getSparkTaskInfo();
  GLUTEN_CHECK(taskInfo.has_value(), "Task info is not set. Please set task info before enabling dumping.");

  dumper_ = std::make_shared<VeloxWholeStageDumper>(
      taskInfo.value(),
      saveDir.value(),
      veloxCfg_->get<int64_t>(kSparkBatchSize, 4096),
      memoryManager()->getAggregateMemoryPool().get());

  dumper_->dumpConf(getConfMap());
}
} // namespace gluten

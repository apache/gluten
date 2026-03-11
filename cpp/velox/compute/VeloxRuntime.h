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

#include "WholeStageResultIterator.h"
#include "compute/Runtime.h"
#ifdef GLUTEN_ENABLE_ENHANCED_FEATURES
#include "iceberg/IcebergWriter.h"
#endif
#include "memory/VeloxMemoryManager.h"
#include "operators/serializer/VeloxColumnarBatchSerializer.h"
#include "operators/serializer/VeloxColumnarToRowConverter.h"
#include "operators/writer/VeloxParquetDataSource.h"
#include "shuffle/ShuffleReader.h"
#include "shuffle/ShuffleWriter.h"

#ifdef GLUTEN_ENABLE_ENHANCED_FEATURES
#include "IcebergNestedField.pb.h"
#endif

namespace gluten {

class VeloxRuntime final : public Runtime {
 public:
  explicit VeloxRuntime(
      const std::string& kind,
      VeloxMemoryManager* vmm,
      const std::unordered_map<std::string, std::string>& confMap);

  void setSparkTaskInfo(SparkTaskInfo taskInfo) override {
    static std::atomic<uint32_t> vtId{0};
    taskInfo.vId = vtId++;
    taskInfo_ = taskInfo;
  }

  void parsePlan(const uint8_t* data, int32_t size) override;

  void parseSplitInfo(const uint8_t* data, int32_t size, int32_t splitIndex) override;

  VeloxMemoryManager* memoryManager() override;

  // FIXME This is not thread-safe?
  std::shared_ptr<ResultIterator> createResultIterator(
      const std::string& spillDir,
      const std::vector<std::shared_ptr<ResultIterator>>& inputs = {}) override;

  void noMoreSplits(ResultIterator* iter) override;

  void requestBarrier(ResultIterator* iter) override;

  std::shared_ptr<ColumnarToRowConverter> createColumnar2RowConverter(int64_t column2RowMemThreshold) override;

  std::shared_ptr<ColumnarBatch> createOrGetEmptySchemaBatch(int32_t numRows) override;

  std::shared_ptr<ColumnarBatch> select(std::shared_ptr<ColumnarBatch> batch, const std::vector<int32_t>& columnIndices)
      override;

  std::shared_ptr<RowToColumnarConverter> createRow2ColumnarConverter(struct ArrowSchema* cSchema) override;

#ifdef GLUTEN_ENABLE_ENHANCED_FEATURES
  std::shared_ptr<IcebergWriter> createIcebergWriter(
      RowTypePtr rowType,
      int32_t format,
      const std::string& outputDirectory,
      facebook::velox::common::CompressionKind compressionKind,
      int32_t partitionId,
      int64_t taskId,
      const std::string& operationId,
      std::shared_ptr<const facebook::velox::connector::hive::iceberg::IcebergPartitionSpec> spec,
      const gluten::IcebergNestedField& protoField,
      const std::unordered_map<std::string, std::string>& sparkConfs);
#endif

  std::shared_ptr<ShuffleWriter> createShuffleWriter(
      int numPartitions,
      const std::shared_ptr<PartitionWriter>& partitionWriter,
      const std::shared_ptr<ShuffleWriterOptions>& options) override;

  Metrics* getMetrics(ColumnarBatchIterator* rawIter, int64_t exportNanos) override {
    auto iter = static_cast<WholeStageResultIterator*>(rawIter);
    return iter->getMetrics(exportNanos);
  }

  std::shared_ptr<ShuffleReader> createShuffleReader(
      std::shared_ptr<arrow::Schema> schema,
      ShuffleReaderOptions options) override;

  std::unique_ptr<ColumnarBatchSerializer> createColumnarBatchSerializer(struct ArrowSchema* cSchema) override;

  std::string planString(bool details, const std::unordered_map<std::string, std::string>& sessionConf) override;

  void enableDumping() override;

  std::shared_ptr<VeloxDataSource> createDataSource(const std::string& filePath, std::shared_ptr<arrow::Schema> schema);

  std::shared_ptr<const facebook::velox::core::PlanNode> getVeloxPlan() {
    return veloxPlan_;
  }

  bool debugModeEnabled() const {
    return debugModeEnabled_;
  }

  static void getInfoAndIds(
      const std::unordered_map<facebook::velox::core::PlanNodeId, std::shared_ptr<SplitInfo>>& splitInfoMap,
      const std::unordered_set<facebook::velox::core::PlanNodeId>& leafPlanNodeIds,
      std::vector<std::shared_ptr<SplitInfo>>& scanInfos,
      std::vector<facebook::velox::core::PlanNodeId>& scanIds,
      std::vector<facebook::velox::core::PlanNodeId>& streamIds);

 private:
  std::shared_ptr<const facebook::velox::core::PlanNode> veloxPlan_;
  std::shared_ptr<facebook::velox::config::ConfigBase> veloxCfg_;
  bool debugModeEnabled_{false};

  std::unordered_map<int32_t, std::shared_ptr<VeloxColumnarBatch>> emptySchemaBatchLoopUp_;
};

} // namespace gluten

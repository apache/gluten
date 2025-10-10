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
// #ifdef GLUTEN_ENABLE_ENHANCED_FEATURES
// #include "iceberg/IcebergWriter.h"
// #endif
#include "memory/BoltMemoryManager.h"
#include "operators/serializer/BoltColumnarBatchSerializer.h"
#include "operators/serializer/BoltColumnarToRowConverter.h"
#include "operators/writer/BoltParquetDataSource.h"
#include "shuffle/ShuffleReader.h"
#include "shuffle/ShuffleWriter.h"
#include "shuffle/ShuffleReaderBase.h"
#include "shuffle/ShuffleWriterBase.h"
#include "shuffle_reader_info.pb.h"
#include "shuffle_writer_info.pb.h"
#include "shuffle/rss/RssClient.h"

#ifdef GLUTEN_ENABLE_ENHANCED_FEATURES
#include "IcebergNestedField.pb.h"
#endif

namespace gluten {

class BoltRuntime final : public Runtime {
 public:
  explicit BoltRuntime(
      const std::string& kind,
      BoltMemoryManager* vmm,
      ThreadManager* threadManager,
      const std::unordered_map<std::string, std::string>& confMap,
      int64_t taskId);

  void setSparkTaskInfo(SparkTaskInfo taskInfo) override {
    static std::atomic<uint32_t> vtId{0};
    taskInfo.vId = vtId++;
    taskInfo_ = taskInfo;
  }

  void parsePlan(const uint8_t* data, int32_t size) override;

  void parseSplitInfo(const uint8_t* data, int32_t size, int32_t splitIndex) override;

  BoltMemoryManager* memoryManager() override;

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
      bytedance::bolt::common::CompressionKind compressionKind,
      std::shared_ptr<const bytedance::bolt::connector::hive::iceberg::IcebergPartitionSpec> spec,
      const gluten::IcebergNestedField& protoField,
      const std::unordered_map<std::string, std::string>& sparkConfs);
#endif

  Metrics* getMetrics(ColumnarBatchIterator* rawIter, int64_t exportNanos) override {
    auto iter = static_cast<WholeStageResultIterator*>(rawIter);
    return iter->getMetrics(exportNanos);
  }

  std::unique_ptr<ColumnarBatchSerializer> createColumnarBatchSerializer(struct ArrowSchema* cSchema) override;

  std::string planString(bool details, const std::unordered_map<std::string, std::string>& sessionConf) override;

  void enableDumping() override;

  std::shared_ptr<BoltDataSource> createDataSource(const std::string& filePath, std::shared_ptr<arrow::Schema> schema);

  std::shared_ptr<const bytedance::bolt::core::PlanNode> getBoltPlan() {
    return boltPlan_;
  }

  bool debugModeEnabled() const {
    return debugModeEnabled_;
  }

  static void getInfoAndIds(
      const std::unordered_map<bytedance::bolt::core::PlanNodeId, std::shared_ptr<SplitInfo>>& splitInfoMap,
      const std::unordered_set<bytedance::bolt::core::PlanNodeId>& leafPlanNodeIds,
      std::vector<std::shared_ptr<SplitInfo>>& scanInfos,
      std::vector<bytedance::bolt::core::PlanNodeId>& scanIds,
      std::vector<bytedance::bolt::core::PlanNodeId>& streamIds);

  std::shared_ptr<ShuffleWriterBase> createShuffleWriter(
      const ShuffleWriterInfo& info,
      std::shared_ptr<RssClient> rssClient,
      std::shared_ptr<ColumnarBatch> cb);

  std::shared_ptr<ShuffleReaderBase> createShuffleReader(
      std::shared_ptr<arrow::Schema> schema,
      const ShuffleReaderInfo& options);

  void setShuffleWriterResult(const ShuffleWriterResult& shuffleWriterResult) {
    shuffleWriterResult_ = shuffleWriterResult;
  }

  const std::optional<ShuffleWriterResult>& getShuffleWriterResult() {
    return shuffleWriterResult_;
  }

 private:
  std::shared_ptr<bytedance::bolt::memory::MemoryPool> aggregatePool_{nullptr};
  std::shared_ptr<bytedance::bolt::memory::MemoryPool> leafPool_{nullptr};
  arrow::MemoryPool* arrowPool_{nullptr};
  std::shared_ptr<const bytedance::bolt::core::PlanNode> boltPlan_;
  std::shared_ptr<bytedance::bolt::config::ConfigBase> boltCfg_;
  bool debugModeEnabled_{false};
  // save shuffle result, include metrics and partition length info
  std::optional<ShuffleWriterResult> shuffleWriterResult_;

  std::unordered_map<int32_t, std::shared_ptr<BoltColumnarBatch>> emptySchemaBatchLoopUp_;
};

} // namespace gluten

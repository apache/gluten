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

#include "compute/Runtime.h"
// #include "iceberg/IcebergPlanConverter.h"
#include "compute/TaskStatusListener.h"
#include "memory/ColumnarBatchIterator.h"
#include "memory/SplitAwareColumnarBatchIterator.h"
#include "memory/BoltColumnarBatch.h"
#include "substrait/SubstraitToBoltPlan.h"
#include "substrait/plan.pb.h"
#include "utils/Metrics.h"
#include "bolt/common/config/Config.h"
// #include "bolt/connectors/hive/iceberg/IcebergSplit.h"
#include "bolt/core/PlanNode.h"
#include "bolt/exec/Task.h"
#include "bolt/shuffle/sparksql/ShuffleWriterNode.h"

namespace gluten {

class WholeStageResultIterator : public SplitAwareColumnarBatchIterator {
 public:
  WholeStageResultIterator(
      BoltMemoryManager* memoryManager,
      const std::shared_ptr<const bytedance::bolt::core::PlanNode>& planNode,
      const std::vector<bytedance::bolt::core::PlanNodeId>& scanNodeIds,
      const std::vector<std::shared_ptr<SplitInfo>>& scanInfos,
      const std::vector<bytedance::bolt::core::PlanNodeId>& streamIds,
      const std::string spillDir,
      const std::unordered_map<std::string, std::string>& confMap,
      const SparkTaskInfo& taskInfo);

  virtual ~WholeStageResultIterator() {
    if (task_ != nullptr && task_->isRunning()) {
      // calling .wait() may take no effect in single thread execution mode
      task_->requestCancel().wait();
    }
#ifdef GLUTEN_ENABLE_GPU
    if (enableCudf_ && lock_.owns_lock()) {
      lock_.unlock();
    }
#endif
  }

  // Add shuffle writer to task, this should called before initTask()
  void addShuffleWriter(
    const bytedance::bolt::shuffle::sparksql::ShuffleWriterOptions& options, bytedance::bolt::shuffle::sparksql::ReportShuffleStatusCallback reportShuffleStatusCallback);
  
  void initTask();

  void addIteratorSplits(const std::vector<std::shared_ptr<ResultIterator>>& inputIterators) override;

  void noMoreSplits() override;

  void requestBarrier() override;

  std::shared_ptr<ColumnarBatch> next() override;

  int64_t spillFixedSize(int64_t size) override;

  Metrics* getMetrics(int64_t exportNanos) {
    collectMetrics();
    if (metrics_) {
      metrics_->veloxToArrow = exportNanos;
    }
    return metrics_.get();
  }

  const bytedance::bolt::exec::Task* task() const {
    return task_.get();
  }

  const bytedance::bolt::core::PlanNode* boltPlan() const {
    return boltPlan_.get();
  }

 private:
  std::shared_ptr<ColumnarBatch> nextInternal();

  /// Get the Spark confs to Bolt query context.
  std::unordered_map<std::string, std::string> getQueryContextConf();

  /// Create QueryCtx.
  std::shared_ptr<bytedance::bolt::core::QueryCtx> createNewBoltQueryCtx(bool isMultiThreaded = false);

  /// Get all the children plan node ids with postorder traversal.
  void getOrderedNodeIds(
      const std::shared_ptr<const bytedance::bolt::core::PlanNode>&,
      std::vector<bytedance::bolt::core::PlanNodeId>& nodeIds);

  /// Create connector config.
  std::shared_ptr<bytedance::bolt::config::ConfigBase> createConnectorConfig();

  /// Construct partition columns.
  void constructPartitionColumns(
      std::unordered_map<std::string, std::optional<std::string>>&,
      const std::unordered_map<std::string, std::string>&);

  /// Add splits to task. Skip if already added.
  void tryAddSplitsToTask();

  /// Collect Bolt metrics.
  void collectMetrics();

  /// Return a certain type of runtime metric. Supported metric types are: sum, count, min, max.
  static int64_t runtimeMetric(
      const std::string& type,
      const std::unordered_map<std::string, bytedance::bolt::RuntimeMetric>& runtimeStats,
      const std::string& metricId);

  std::pair<int64_t, int64_t> getExecutorConcurrency();

  /// Memory.
  BoltMemoryManager* memoryManager_;

  std::string spillDir_;
  /// Config, task and plan.
  std::shared_ptr<config::ConfigBase> boltCfg_;
  const SparkTaskInfo taskInfo_;
  std::shared_ptr<bytedance::bolt::exec::Task> task_;
  std::shared_ptr<const bytedance::bolt::core::PlanNode> boltPlan_;

  /// Spill.
  std::string spillStrategy_;
  std::shared_ptr<folly::Executor> spillExecutor_ = nullptr;

  /// Metrics
  std::unique_ptr<Metrics> metrics_{};

#ifdef GLUTEN_ENABLE_GPU
  // Mutex for thread safety.
  static std::mutex mutex_;
  std::unique_lock<std::mutex> lock_;
  bool enableCudf_;
#endif

  /// All the children plan node ids with postorder traversal.
  std::vector<bytedance::bolt::core::PlanNodeId> orderedNodeIds_;

  /// Node ids should be omitted in metrics.
  std::unordered_set<bytedance::bolt::core::PlanNodeId> omittedNodeIds_;
  std::vector<bytedance::bolt::core::PlanNodeId> scanNodeIds_;
  std::vector<std::shared_ptr<SplitInfo>> scanInfos_;
  std::vector<bytedance::bolt::core::PlanNodeId> streamIds_;
  std::vector<std::vector<bytedance::bolt::exec::Split>> splits_;
  bool noMoreSplits_ = false;

  int64_t loadLazyVectorTime_ = 0;

  /// For multi-threaded execution
  ContinueFuture taskCompletionFuture_;
  bool isMultiThreadExecMode_{false};
  bool parallelEnabled_{false};

  bool dynamicConcurrencyAdjustmentEnabled_ = false;
};

} // namespace gluten

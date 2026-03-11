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
#include "iceberg/IcebergPlanConverter.h"
#include "memory/SplitAwareColumnarBatchIterator.h"
#include "memory/VeloxColumnarBatch.h"
#include "substrait/SubstraitToVeloxPlan.h"
#include "substrait/plan.pb.h"
#include "utils/Metrics.h"
#include "velox/common/config/Config.h"
#include "velox/connectors/hive/iceberg/IcebergSplit.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Task.h"
#ifdef GLUTEN_ENABLE_GPU
#include "cudf/GpuLock.h"
#endif

namespace gluten {

class WholeStageResultIterator : public SplitAwareColumnarBatchIterator {
 public:
  WholeStageResultIterator(
      VeloxMemoryManager* memoryManager,
      const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode,
      const std::vector<facebook::velox::core::PlanNodeId>& scanNodeIds,
      const std::vector<std::shared_ptr<SplitInfo>>& scanInfos,
      const std::vector<facebook::velox::core::PlanNodeId>& streamIds,
      const std::string spillDir,
      const std::shared_ptr<facebook::velox::config::ConfigBase>& veloxCfg,
      const SparkTaskInfo& taskInfo);

  virtual ~WholeStageResultIterator() {
    if (task_ != nullptr && task_->isRunning()) {
      // calling .wait() may take no effect in single thread execution mode
      task_->requestCancel().wait();
    }
#ifdef GLUTEN_ENABLE_GPU
    if (enableCudf_) {
      unlockGpu();
    }
#endif
  }

  std::shared_ptr<ColumnarBatch> next() override;

  int64_t spillFixedSize(int64_t size) override;

  Metrics* getMetrics(int64_t exportNanos) {
    collectMetrics();
    if (metrics_) {
      metrics_->veloxToArrow = exportNanos;
    }
    return metrics_.get();
  }

  const facebook::velox::core::PlanNode* veloxPlan() const {
    return veloxPlan_.get();
  }

  /// Get the underlying Velox task for direct manipulation
  facebook::velox::exec::Task* task() {
    return task_.get();
  }

  /// Add iterator-based splits from input iterators
  void addIteratorSplits(const std::vector<std::shared_ptr<ResultIterator>>& inputIterators) override;

  /// Signal that no more splits will be added.
  /// This is required for proper task completion and enables future barrier support.
  void noMoreSplits() override;

  /// Request a barrier in the Velox task execution.
  /// This signals the task to finish processing all currently queued splits
  /// and drain all stateful operators before continuing.
  /// @see https://facebookincubator.github.io/velox/develop/task-barrier.html
  void requestBarrier() override;

 private:
  /// Get the Spark confs to Velox query context.
  std::unordered_map<std::string, std::string> getQueryContextConf();

  /// Create QueryCtx.
  std::shared_ptr<facebook::velox::core::QueryCtx> createNewVeloxQueryCtx();

  /// Get all the children plan node ids with postorder traversal.
  void getOrderedNodeIds(
      const std::shared_ptr<const facebook::velox::core::PlanNode>&,
      std::vector<facebook::velox::core::PlanNodeId>& nodeIds);

  /// Construct partition columns.
  void constructPartitionColumns(
      std::unordered_map<std::string, std::optional<std::string>>&,
      const std::unordered_map<std::string, std::string>&);

  /// Collect Velox metrics.
  void collectMetrics();

  /// Return a certain type of runtime metric. Supported metric types are: sum, count, min, max.
  static int64_t runtimeMetric(
      const std::string& type,
      const std::unordered_map<std::string, facebook::velox::RuntimeMetric>& runtimeStats,
      const std::string& metricId);

  /// Memory.
  VeloxMemoryManager* memoryManager_;

  /// Config, task and plan.
  const std::shared_ptr<facebook::velox::config::ConfigBase> veloxCfg_;
#ifdef GLUTEN_ENABLE_GPU
  const bool enableCudf_;
#endif
  const SparkTaskInfo taskInfo_;
  std::shared_ptr<facebook::velox::exec::Task> task_;
  std::shared_ptr<const facebook::velox::core::PlanNode> veloxPlan_;

  /// Spill.
  std::string spillStrategy_;
  std::shared_ptr<folly::Executor> spillExecutor_ = nullptr;

  /// Metrics
  std::unique_ptr<Metrics> metrics_{};

  /// All the children plan node ids with postorder traversal.
  std::vector<facebook::velox::core::PlanNodeId> orderedNodeIds_;

  /// Node ids should be omitted in metrics.
  std::unordered_set<facebook::velox::core::PlanNodeId> omittedNodeIds_;
  std::vector<facebook::velox::core::PlanNodeId> scanNodeIds_;
  std::vector<std::shared_ptr<SplitInfo>> scanInfos_;
  std::vector<facebook::velox::core::PlanNodeId> streamIds_;
  std::vector<std::vector<facebook::velox::exec::Split>> splits_;
  bool allSplitsAdded_ = false;

  int64_t loadLazyVectorTime_ = 0;
};

} // namespace gluten

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

#include "compute/VeloxPlanEvaluator.h"
#include <atomic>
#include "compute/VeloxExpressionEvaluator.h"
#include "compute/VeloxPlanConverter.h"
#include "utils/ConfigExtractor.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregateInfo.h"
#include "velox/exec/Driver.h"
#include "velox/exec/GroupingSet.h"
#include "velox/exec/Operator.h"
#include "velox/exec/OrderBy.h"
#include "velox/exec/Task.h"
#include "velox/exec/Unnest.h"
#include "velox/exec/VectorHasher.h"
#include "velox/exec/Window.h"

namespace gluten {
using namespace facebook::velox;
namespace {
std::atomic<uint64_t> nextAggregateId{0};
std::atomic<uint64_t> nextUnnestId{0};

class ProjectOutput final : public ColumnarBatchIterator {
 public:
  ProjectOutput(
      std::shared_ptr<ResultIterator> input,
      std::shared_ptr<memory::MemoryPool> pool,
      std::shared_ptr<core::QueryCtx> query,
      const core::ProjectNode& project)
      : input_(std::move(input)),
        evaluator_(
            std::move(pool),
            std::move(query),
            project.sources().front()->outputType(),
            project.outputType(),
            project.projections()) {}

  std::shared_ptr<ColumnarBatch> next() override {
    return input_->hasNext() ? evaluator_.evaluate(input_->next()) : nullptr;
  }

 private:
  std::shared_ptr<ResultIterator> input_;
  VeloxExpressionEvaluator evaluator_;
};

class WindowStageOutput final : public ColumnarBatchIterator {
 public:
  WindowStageOutput(
      std::shared_ptr<ResultIterator> input,
      const core::PlanNodePtr& plan,
      const std::shared_ptr<core::QueryCtx>& query)
      : input_(std::move(input)),
        task_(exec::Task::create(
            "expression-window-" + std::to_string(nextAggregateId++),
            core::PlanFragment{plan},
            0,
            query,
            exec::Task::ExecutionMode::kParallel)),
        driver_(exec::Driver::testingCreate(std::make_unique<exec::DriverCtx>(task_, 0, 0, 0, 0))) {
    if (auto window = std::dynamic_pointer_cast<const core::WindowNode>(plan)) {
      operator_ = std::make_unique<exec::Window>(0, driver_->driverCtx(), window);
    } else {
      operator_ = std::make_unique<exec::OrderBy>(
          0, driver_->driverCtx(), std::dynamic_pointer_cast<const core::OrderByNode>(plan));
    }
    try {
      operator_->initialize();
    } catch (...) {
      try {
        operator_->close();
      } catch (const std::exception& error) {
        LOG(WARNING) << "Closing uninitialized benchmark window stage: " << error.what();
      }
      throw;
    }
  }
  ~WindowStageOutput() override {
    try {
      operator_->close();
    } catch (const std::exception& error) {
      LOG(WARNING) << "Closing benchmark window stage: " << error.what();
    }
  }
  std::shared_ptr<ColumnarBatch> next() override {
    while (!operator_->isFinished()) {
      if (auto result = operator_->getOutput()) {
        return std::make_shared<VeloxColumnarBatch>(std::move(result));
      }
      if (operator_->isFinished())
        break;
      GLUTEN_CHECK(operator_->needsInput(), "Window stage made no progress");
      if (input_->hasNext()) {
        auto batch = std::dynamic_pointer_cast<VeloxColumnarBatch>(input_->next());
        GLUTEN_CHECK(batch != nullptr, "Expected native window input");
        operator_->addInput(batch->getRowVector());
      } else {
        operator_->noMoreInput();
      }
    }
    return nullptr;
  }

 private:
  std::shared_ptr<ResultIterator> input_;
  std::shared_ptr<exec::Task> task_;
  // Only anchors the operator reclaimer; no Driver pipeline is initialized or run.
  std::shared_ptr<exec::Driver> driver_;
  std::unique_ptr<exec::Operator> operator_;
};

class UnnestOutput final : public ColumnarBatchIterator {
 public:
  UnnestOutput(
      std::shared_ptr<ResultIterator> input,
      const std::shared_ptr<const core::UnnestNode>& plan,
      const std::shared_ptr<core::QueryCtx>& query)
      : input_(std::move(input)),
        task_(exec::Task::create(
            "expression-unnest-" + std::to_string(nextUnnestId++),
            core::PlanFragment{plan},
            0,
            query,
            exec::Task::ExecutionMode::kParallel)) {
    // Initialize the operator's driver context and reclaimers without scheduling a driver.
    exec::DriverFactory factory{};
    factory.planNodes = {plan};
    factory.maxDrivers = 1;
    factory.numDrivers = 1;
    factory.numTotalDrivers = 1;
    driver_ = factory.createDriver(
        std::make_unique<exec::DriverCtx>(task_, 0, 0, 0, 0),
        nullptr,
        nullptr,
        nullptr,
        std::make_shared<exec::PipelinePushdownFilters>(),
        [](int) { return 1; });
    unnest_ = dynamic_cast<exec::Unnest*>(driver_->findOperator(plan->id()));
    GLUTEN_CHECK(unnest_ != nullptr, "Expected native Unnest operator for plan node " + plan->id());
    driver_->initializeOperators();
    VLOG(1) << "Expression benchmark Unnest stage prepared: planNodeId=" << plan->id();
  }

  ~UnnestOutput() override {
    unnest_->close();
  }

  std::shared_ptr<ColumnarBatch> next() override {
    while (true) {
      if (auto output = unnest_->getOutput()) {
        return std::make_shared<VeloxColumnarBatch>(std::move(output));
      }
      if (unnest_->isFinished()) {
        return nullptr;
      }
      GLUTEN_CHECK(unnest_->needsInput(), "Native Unnest neither produced output nor requested input");
      if (!input_->hasNext()) {
        unnest_->noMoreInput();
        continue;
      }
      auto batch = std::dynamic_pointer_cast<VeloxColumnarBatch>(input_->next());
      GLUTEN_CHECK(batch != nullptr, "Expected native Unnest input");
      if (batch->numRows() > 0) {
        unnest_->addInput(batch->getRowVector());
      }
    }
  }

 private:
  std::shared_ptr<ResultIterator> input_;
  std::shared_ptr<exec::Task> task_;
  std::shared_ptr<exec::Driver> driver_;
  exec::Unnest* unnest_{nullptr}; // Owned by driver_.
};

class AggregateOutput final : public ColumnarBatchIterator {
 public:
  AggregateOutput(
      std::shared_ptr<ResultIterator> input,
      const std::shared_ptr<const core::AggregationNode>& plan,
      const std::shared_ptr<core::QueryCtx>& query)
      : input_(std::move(input)),
        plan_(plan),
        task_(exec::Task::create(
            "expression-aggregate-" + std::to_string(nextAggregateId++),
            core::PlanFragment{plan},
            0,
            query,
            exec::Task::ExecutionMode::kParallel)),
        driver_(task_, 0, 0, 0, 0),
        context_(&driver_, plan->id(), 0, "Aggregation") {
    GLUTEN_CHECK(plan->globalGroupingSets().empty() && !plan->groupId(), "Grouping sets are unsupported");
    auto aggregates = exec::toAggregateInfo(*plan, context_, plan->groupingKeys().size(), expressionEvaluator_);
    grouping_ = std::make_unique<exec::GroupingSet>(
        plan->sources().front()->outputType(),
        exec::createVectorHashers(plan->sources().front()->outputType(), plan->groupingKeys()),
        std::vector<column_index_t>{},
        std::vector<column_index_t>{},
        std::move(aggregates),
        plan->ignoreNullKeys(),
        exec::isPartialOutput(plan->step()),
        exec::isRawInput(plan->step()),
        std::vector<vector_size_t>{},
        std::nullopt,
        nullptr,
        &nonReclaimable_,
        &query->queryConfig(),
        context_.pool(),
        &spillStats_);
    batchSize_ = query->queryConfig().preferredOutputBatchRows();
    partialLimit_ = query->queryConfig().maxPartialAggregationMemoryUsage();
  }

  std::shared_ptr<ColumnarBatch> next() override {
    while (true) {
      if (draining_) {
        auto result = BaseVector::create<RowVector>(
            plan_->outputType(), plan_->groupingKeys().empty() ? 1 : batchSize_, context_.pool());
        if (grouping_->getOutput(batchSize_, std::numeric_limits<int32_t>::max(), position_, result)) {
          return std::make_shared<VeloxColumnarBatch>(std::move(result));
        }
        if (finished_)
          return nullptr;
        grouping_->resetTable(false);
        position_ = exec::RowContainerIterator{};
        draining_ = false;
      }
      while (input_->hasNext()) {
        auto batch = std::dynamic_pointer_cast<VeloxColumnarBatch>(input_->next());
        GLUTEN_CHECK(batch != nullptr, "Expected native aggregate input");
        grouping_->addInput(batch->getRowVector(), false);
        if (exec::isPartialOutput(plan_->step()) && grouping_->isPartialFull(partialLimit_)) {
          draining_ = true;
          break;
        }
      }
      if (!draining_) {
        grouping_->noMoreInput();
        finished_ = true;
        draining_ = true;
      }
    }
  }

 private:
  std::shared_ptr<ResultIterator> input_;
  std::shared_ptr<const core::AggregationNode> plan_;
  // Only supplies OperatorCtx to Velox's aggregate preparation; no Driver is created or run.
  std::shared_ptr<exec::Task> task_;
  exec::DriverCtx driver_;
  exec::OperatorCtx context_;
  std::shared_ptr<core::ExpressionEvaluator> expressionEvaluator_;
  tsan_atomic<bool> nonReclaimable_{true};
  exec::SpillStats spillStats_;
  std::unique_ptr<exec::GroupingSet> grouping_;
  exec::RowContainerIterator position_;
  int32_t batchSize_;
  int64_t partialLimit_;
  bool draining_{false};
  bool finished_{false};
};
} // namespace

VeloxPlanEvaluator::VeloxPlanEvaluator(VeloxRuntime* runtime, const uint8_t* data, int32_t size) : runtime_(runtime) {
  GLUTEN_CHECK(runtime != nullptr && data != nullptr && size > 0, "Invalid plan evaluator input");
  ::substrait::Plan substrait;
  GLUTEN_CHECK(substrait.ParseFromArray(data, size), "Invalid Substrait");
  VeloxPlanConverter converter(
      runtime->memoryManager()->getLeafMemoryPool().get(), runtime->veloxCfg().get(), {}, runtime->connectorIds());
  plan_ = converter.toVeloxPlan(substrait, {});
  std::vector<std::shared_ptr<SplitInfo>> scanInfos;
  std::vector<core::PlanNodeId> scanIds;
  std::vector<core::PlanNodeId> streamIds;
  VeloxRuntime::getInfoAndIds(converter.splitInfos(), plan_->leafPlanNodeIds(), scanInfos, scanIds, streamIds);
  GLUTEN_CHECK(scanIds.empty() && streamIds.size() == 1, "Expected one iterator input");
  auto node = plan_;
  while (!node->sources().empty()) {
    GLUTEN_CHECK(node->sources().size() == 1, "Expected unary expression stage");
    GLUTEN_CHECK(
        std::dynamic_pointer_cast<const core::ProjectNode>(node) ||
            std::dynamic_pointer_cast<const core::AggregationNode>(node) ||
            std::dynamic_pointer_cast<const core::OrderByNode>(node) ||
            std::dynamic_pointer_cast<const core::WindowNode>(node) ||
            std::dynamic_pointer_cast<const core::UnnestNode>(node),
        "Unsupported expression stage: " + node->toString());
    nodes_.push_back(node);
    node = node->sources().front();
  }
  std::reverse(nodes_.begin(), nodes_.end());
}

std::shared_ptr<ResultIterator> VeloxPlanEvaluator::evaluate(
    std::shared_ptr<ResultIterator> input,
    SparkTaskInfo taskInfo) {
  GLUTEN_CHECK(input != nullptr, "Plan input must not be null");
  auto* memory = runtime_->memoryManager();
  auto query = core::QueryCtx::create(
      nullptr,
      core::QueryConfig{getQueryContextConf(runtime_->veloxCfg().get(), taskInfo.partitionId)},
      {},
      nullptr,
      memory->getAggregateMemoryPool());
  for (const auto& node : nodes_) {
    if (auto project = std::dynamic_pointer_cast<const core::ProjectNode>(node)) {
      input = std::make_shared<ResultIterator>(
          std::make_unique<ProjectOutput>(std::move(input), memory->getLeafMemoryPool(), query, *project), runtime_);
    } else if (
        std::dynamic_pointer_cast<const core::WindowNode>(node) ||
        std::dynamic_pointer_cast<const core::OrderByNode>(node)) {
      input = std::make_shared<ResultIterator>(
          std::make_unique<WindowStageOutput>(std::move(input), node, query), runtime_);
    } else if (auto unnest = std::dynamic_pointer_cast<const core::UnnestNode>(node)) {
      input =
          std::make_shared<ResultIterator>(std::make_unique<UnnestOutput>(std::move(input), unnest, query), runtime_);
    } else {
      input = std::make_shared<ResultIterator>(
          std::make_unique<AggregateOutput>(
              std::move(input), std::dynamic_pointer_cast<const core::AggregationNode>(node), query),
          runtime_);
    }
  }
  return input;
}
} // namespace gluten

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

#include <cstdint>
#include "compute/ResultIterator.h"
#include "memory/BoltColumnarBatch.h"
#include "bolt/exec/Driver.h"
#include "bolt/exec/Operator.h"
#include "bolt/exec/Task.h"

namespace {

class SuspendedSection {
 public:
  explicit SuspendedSection(bytedance::bolt::exec::Driver* driver) : driver_(driver) {
    if (driver_->task()->enterSuspended(driver->state()) != bytedance::bolt::exec::StopReason::kNone) {
      BOLT_FAIL("Terminate detected when entering suspended section");
    }
  }

  virtual ~SuspendedSection() {
    if (driver_->task()->leaveSuspended(driver_->state()) != bytedance::bolt::exec::StopReason::kNone) {
      LOG(WARNING) << "Terminate detected when leaving suspended section for driver " << driver_->driverCtx()->driverId
                   << " from task " << driver_->task()->taskId();
    }
  }

 private:
  bytedance::bolt::exec::Driver* const driver_;
};

} // namespace

namespace gluten {

class RowVectorStream {
 public:
  explicit RowVectorStream(
      bytedance::bolt::exec::DriverCtx* driverCtx,
      bytedance::bolt::memory::MemoryPool* pool,
      ResultIterator* iterator,
      const bytedance::bolt::RowTypePtr& outputType)
      : driverCtx_(driverCtx), pool_(pool), outputType_(outputType), iterator_(iterator) {}

  bool hasNext() {
    if (finished_) {
      return false;
    }
    BOLT_DCHECK_NOT_NULL(iterator_);

    bool hasNext;
    {
      // We are leaving Bolt task execution and are probably entering Spark code through JNI. Suspend the current
      // driver to make the current task open to spilling.
      //
      // When a task is getting spilled, it should have been suspended so has zero running threads, otherwise there's
      // possibility that this spill call hangs. See https://github.com/apache/incubator-gluten/issues/7243.
      // As of now, non-zero running threads usually happens when:
      // 1. Task A spills task B;
      // 2. Task A tries to grow buffers created by task B, during which spill is requested on task A again.
      SuspendedSection ss(driverCtx_->driver);
      hasNext = iterator_->hasNext();
    }
    if (!hasNext) {
      finished_ = true;
    }
    return hasNext;
  }

  // Convert arrow batch to row vector and use new output columns
  bytedance::bolt::RowVectorPtr next() {
    if (finished_) {
      return nullptr;
    }
    std::shared_ptr<ColumnarBatch> cb;
    {
      // We are leaving Bolt task execution and are probably entering Spark code through JNI. Suspend the current
      // driver to make the current task open to spilling.
      SuspendedSection ss(driverCtx_->driver);
      cb = iterator_->next();
    }
    const std::shared_ptr<BoltColumnarBatch>& vb = BoltColumnarBatch::from(pool_, cb);
    bool isComposite = vb->isCompositeLayout();
    auto vp = vb->getRowVector();
    BOLT_DCHECK(vp != nullptr);
    if (isComposite) {
      auto newvp = std::make_shared<bytedance::bolt::CompositeRowVector>(
          outputType_, vp->size(), vp->pool(), nullptr, std::move(vp->children()));
      std::dynamic_pointer_cast<bytedance::bolt::CompositeRowVector>(vp)->moveto(newvp);
      return newvp;
    } else {
      return std::make_shared<bytedance::bolt::RowVector>(
          vp->pool(), outputType_, bytedance::bolt::BufferPtr(0), vp->size(), vp->children());
    }
  }

 private:
  bytedance::bolt::exec::DriverCtx* driverCtx_;
  bytedance::bolt::memory::MemoryPool* pool_;
  const bytedance::bolt::RowTypePtr outputType_;
  ResultIterator* iterator_;

  bool finished_{false};
};

class ValueStreamNode final : public bytedance::bolt::core::PlanNode {
 public:
  ValueStreamNode(
      const bytedance::bolt::core::PlanNodeId& id,
      const bytedance::bolt::RowTypePtr& outputType,
      std::shared_ptr<ResultIterator> iterator)
      : bytedance::bolt::core::PlanNode(id), outputType_(outputType), iterator_(std::move(iterator)) {}

  const bytedance::bolt::RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<bytedance::bolt::core::PlanNodePtr>& sources() const override {
    return kEmptySources_;
  };

  bool requiresSplits() const override {
    return true;
  }

  ResultIterator* iterator() const {
    return iterator_.get();
  }

  std::string_view name() const override {
    return "ValueStream";
  }

  void setRowCount(int64_t rowCount) {
    rowCount_ = rowCount;
    existRowCount_ = true;
  }
  int64_t getRowCount() const {
    return rowCount_;
  }
  bool existRowCount() const {
    return existRowCount_;
  }

  folly::dynamic serialize() const override {
    folly::dynamic result = folly::dynamic::object();
    result["name"] = name();
    result["id"] = id();
    result["outputType"] = outputType_ ? outputType_->toString() : "";
    return result;
  }

 private:
  void addDetails(std::stringstream& stream) const override {};

  const bytedance::bolt::RowTypePtr outputType_;
  std::shared_ptr<ResultIterator> iterator_;
  const std::vector<bytedance::bolt::core::PlanNodePtr> kEmptySources_;

  int64_t rowCount_;
  bool existRowCount_ = false;
};

class ValueStream : public bytedance::bolt::exec::SourceOperator {
 public:
  ValueStream(
      int32_t operatorId,
      bytedance::bolt::exec::DriverCtx* driverCtx,
      std::shared_ptr<const ValueStreamNode> valueStreamNode)
      : bytedance::bolt::exec::SourceOperator(
            driverCtx,
            valueStreamNode->outputType(),
            operatorId,
            valueStreamNode->id(),
            valueStreamNode->name().data()) {
    ResultIterator* itr = valueStreamNode->iterator();
    rvStream_ = std::make_unique<RowVectorStream>(driverCtx, pool(), itr, outputType_);

    if (valueStreamNode->existRowCount()) {
      LOG(INFO) << "ValueStreamNode rowCount=" << valueStreamNode->getRowCount();
      this->setRuntimeMetric(bytedance::bolt::exec::kTotalRowCount, std::to_string(valueStreamNode->getRowCount()));
      this->setRuntimeMetric(bytedance::bolt::exec::kCanUsedToEstimateHashBuildPartitionNum, "true");
    }
  }

  bytedance::bolt::RowVectorPtr getOutput() override {
    if (finished_) {
      return nullptr;
    }
    if (rvStream_->hasNext()) {
      auto data = rvStream_->next();
      outputRows_ += data->size();
      this->setRuntimeMetric(bytedance::bolt::exec::kHasBeenProcessedRowCount, folly::to<std::string>(outputRows_));
      return data;
    } else {
      finished_ = true;
      auto lockedStats = stats_.wlock();
      lockedStats->addRuntimeStat("dynamicConcurrency", bytedance::bolt::RuntimeCounter(this->getConcurrency()));
      return nullptr;
    }
  }

  bytedance::bolt::exec::BlockingReason isBlocked(bytedance::bolt::ContinueFuture* /* unused */) override {
    return bytedance::bolt::exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return finished_;
  }

 private:
  bool finished_ = false;
  std::unique_ptr<RowVectorStream> rvStream_;
  int64_t outputRows_{0};
};

class RowVectorStreamOperatorTranslator : public bytedance::bolt::exec::Operator::PlanNodeTranslator {
  std::unique_ptr<bytedance::bolt::exec::Operator> toOperator(
      bytedance::bolt::exec::DriverCtx* ctx,
      int32_t id,
      const bytedance::bolt::core::PlanNodePtr& node) override {
    if (auto valueStreamNode = std::dynamic_pointer_cast<const ValueStreamNode>(node)) {
      return std::make_unique<ValueStream>(id, ctx, valueStreamNode);
    }
    return nullptr;
  }
};
} // namespace gluten

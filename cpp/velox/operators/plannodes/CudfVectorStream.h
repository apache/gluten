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

#include "CudfVectorStream.h"
#include "compute/ResultIterator.h"
#include "memory/VeloxColumnarBatch.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"
#include "velox/exec/Task.h"
#include "velox/experimental/cudf/exec/CudfOperator.h"
#include "velox/experimental/cudf/exec/Utilities.h"
#include "velox/experimental/cudf/exec/VeloxCudfInterop.h"
#include "velox/experimental/cudf/vector/CudfVector.h"

namespace gluten {

class CudfVectorStreamBase {
 public:
  virtual ~CudfVectorStreamBase() = default;

  explicit CudfVectorStreamBase(
      facebook::velox::exec::DriverCtx* driverCtx,
      facebook::velox::memory::MemoryPool* pool,
      ResultIterator* iterator,
      const facebook::velox::RowTypePtr& outputType)
      : driverCtx_(driverCtx), pool_(pool), outputType_(outputType), iterator_(iterator) {}

  bool hasNext();

  // Convert arrow batch to row vector, construct the new Rowvector with new outputType.
  virtual facebook::velox::RowVectorPtr next();

 protected:
  // Get the next batch from iterator_.
  std::shared_ptr<ColumnarBatch> nextInternal();

  facebook::velox::exec::DriverCtx* driverCtx_;
  facebook::velox::memory::MemoryPool* pool_;
  const facebook::velox::RowTypePtr outputType_;
  ResultIterator* iterator_;

  bool finished_{false};
};

class ValueStreamNode final : public facebook::velox::core::PlanNode {
 public:
  ValueStreamNode(
      const facebook::velox::core::PlanNodeId& id,
      const facebook::velox::RowTypePtr& outputType,
      std::shared_ptr<ResultIterator> iterator)
      : facebook::velox::core::PlanNode(id), outputType_(outputType), iterator_(std::move(iterator)) {}

  const facebook::velox::RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<facebook::velox::core::PlanNodePtr>& sources() const override {
    return kEmptySources_;
  };

  ResultIterator* iterator() const {
    return iterator_.get();
  }

  std::string_view name() const override {
    return "ValueStream";
  }

  folly::dynamic serialize() const override {
    VELOX_UNSUPPORTED("ValueStream plan node is not serializable");
  }

 private:
  void addDetails(std::stringstream& stream) const override{};

  const facebook::velox::RowTypePtr outputType_;
  std::shared_ptr<ResultIterator> iterator_;
  const std::vector<facebook::velox::core::PlanNodePtr> kEmptySources_;
};

class CudfVectorStream : public CudfVectorStreamBase {
 public:
  CudfVectorStream(
      facebook::velox::exec::DriverCtx* driverCtx,
      facebook::velox::memory::MemoryPool* pool,
      ResultIterator* iterator,
      const facebook::velox::RowTypePtr& outputType)
      : CudfVectorStreamBase(driverCtx, pool, iterator, outputType) {}

  // Convert arrow batch to row vector and use new output columns
  facebook::velox::RowVectorPtr next() override {
    auto cb = nextInternal();
    if (cb == nullptr) {
      return nullptr;
    }
    auto vb = std::dynamic_pointer_cast<VeloxColumnarBatch>(cb);
    VELOX_CHECK_NOT_NULL(vb);
    auto vp = vb->getRowVector();
    VELOX_DCHECK(vp != nullptr);
    auto cudfVector = std::dynamic_pointer_cast<facebook::velox::cudf_velox::CudfVector>(vp);
    if (cudfVector == nullptr) {
      // The vector may comes from BroadcastExchange, in this case, it's not a CudfVector.
      vp->setType(outputType_);
      return vp;
    }
    VELOX_CHECK_NOT_NULL(cudfVector);
    return std::make_shared<facebook::velox::cudf_velox::CudfVector>(
        vp->pool(), outputType_, vp->size(), cudfVector->release(), cudfVector->stream());
  }
};

// To avoid plan translator uses false node, this one cannot inherit ValueStreamNode.
class CudfValueStreamNode final : public facebook::velox::core::PlanNode {
 public:
  CudfValueStreamNode(
      const facebook::velox::core::PlanNodeId& id,
      const facebook::velox::RowTypePtr& outputType,
      std::shared_ptr<ResultIterator> iterator)
      : facebook::velox::core::PlanNode(id), outputType_(outputType), iterator_(std::move(iterator)) {}

  const facebook::velox::RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<facebook::velox::core::PlanNodePtr>& sources() const override {
    return kEmptySources_;
  };

  ResultIterator* iterator() const {
    return iterator_.get();
  }

  std::string_view name() const override {
    return "CudfValueStream";
  }

  folly::dynamic serialize() const override {
    VELOX_UNSUPPORTED("CudfValueStream plan node is not serializable");
  }

 private:
  void addDetails(std::stringstream& stream) const override{};

  const facebook::velox::RowTypePtr outputType_;
  std::shared_ptr<ResultIterator> iterator_;
  const std::vector<facebook::velox::core::PlanNodePtr> kEmptySources_;
};

// Extends CudfOperator to identify it as GPU node, so not add CudfFormVelox operator.
class CudfValueStream : public facebook::velox::exec::SourceOperator, public facebook::velox::cudf_velox::CudfOperator {
 public:
  CudfValueStream(
      int32_t operatorId,
      facebook::velox::exec::DriverCtx* driverCtx,
      std::shared_ptr<const CudfValueStreamNode> valueStreamNode)
      : facebook::velox::exec::SourceOperator(
            driverCtx,
            valueStreamNode->outputType(),
            operatorId,
            valueStreamNode->id(),
            valueStreamNode->name().data()),
        facebook::velox::cudf_velox::CudfOperator(operatorId, valueStreamNode->id()) {
    ResultIterator* itr = valueStreamNode->iterator();
    rvStream_ = std::make_unique<CudfVectorStream>(driverCtx, pool(), itr, outputType_);
  }

  facebook::velox::RowVectorPtr getOutput() override {
    if (finished_) {
      return nullptr;
    }
    if (rvStream_->hasNext()) {
      return rvStream_->next();
    } else {
      finished_ = true;
      return nullptr;
    }
  }

  facebook::velox::exec::BlockingReason isBlocked(facebook::velox::ContinueFuture* /* unused */) override {
    return facebook::velox::exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return finished_;
  }

 private:
  bool finished_ = false;
  std::unique_ptr<CudfVectorStream> rvStream_;
};

class CudfVectorStreamOperatorTranslator : public facebook::velox::exec::Operator::PlanNodeTranslator {
  std::unique_ptr<facebook::velox::exec::Operator> toOperator(
      facebook::velox::exec::DriverCtx* ctx,
      int32_t id,
      const facebook::velox::core::PlanNodePtr& node) override {
    if (auto valueStreamNode = std::dynamic_pointer_cast<const CudfValueStreamNode>(node)) {
      return std::make_unique<CudfValueStream>(id, ctx, valueStreamNode);
    }
    return nullptr;
  }
};
} // namespace gluten

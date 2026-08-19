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

#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"

namespace gluten {

/// ---------------------------------------------------------------------------
/// GlutenStrideNode — a Gluten-native "every-Nth-row" sampler.
///
/// Outputs exactly the rows at positions 0, stride, 2*stride, … within each
/// input batch (0-based within the batch, not globally across batches).
/// This is a deterministic, state-free operator that demonstrates the full
/// Gluten custom-operator mechanism without requiring any Velox upstream change.
///
/// Encoding on the wire (reuses FetchRel to avoid a new proto type):
///   FetchRel.offset = stride
///   FetchRel.count  = 0   (unused)
///   AdvancedExtension.optimization[0] = "isGlutenStride=1"
/// ---------------------------------------------------------------------------
class GlutenStrideNode final : public facebook::velox::core::PlanNode {
 public:
  GlutenStrideNode(
      const facebook::velox::core::PlanNodeId& id,
      int64_t stride,
      facebook::velox::core::PlanNodePtr child)
      : PlanNode(id), stride_(stride), sources_({std::move(child)}) {
    VELOX_USER_CHECK_GT(stride_, 0, "GlutenStrideNode: stride must be > 0");
  }

  const facebook::velox::RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<facebook::velox::core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  int64_t stride() const {
    return stride_;
  }

  std::string_view name() const override {
    return "GlutenStride";
  }

  folly::dynamic serialize() const override {
    VELOX_UNSUPPORTED("GlutenStrideNode serialization is not supported");
  }

 private:
  void addDetails(std::stringstream& stream) const override {
    stream << "stride=" << stride_;
  }

  const int64_t stride_;
  std::vector<facebook::velox::core::PlanNodePtr> sources_;
};

/// Outputs every stride-th row from each input batch.
/// Row indices within the batch are 0-based; the first row (index 0) is always
/// included.  The counter resets at the start of every batch, keeping the
/// operator stateless and parallelism-friendly.
class GlutenStrideOperator : public facebook::velox::exec::Operator {
 public:
  GlutenStrideOperator(
      int32_t operatorId,
      facebook::velox::exec::DriverCtx* driverCtx,
      std::shared_ptr<const GlutenStrideNode> node);

  bool needsInput() const override {
    return !noMoreInput_ && !input_;
  }

  void addInput(facebook::velox::RowVectorPtr input) override {
    input_ = std::move(input);
  }

  facebook::velox::RowVectorPtr getOutput() override;

  facebook::velox::exec::BlockingReason isBlocked(facebook::velox::ContinueFuture*) override {
    return facebook::velox::exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_ && !input_;
  }

 private:
  const int64_t stride_;
};

/// Registers GlutenStrideNode → GlutenStrideOperator.
/// Call once at backend startup via Operator::registerOperator().
class GlutenStrideTranslator : public facebook::velox::exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<facebook::velox::exec::Operator> toOperator(
      facebook::velox::exec::DriverCtx* ctx,
      int32_t id,
      const facebook::velox::core::PlanNodePtr& node) override {
    if (auto n = std::dynamic_pointer_cast<const GlutenStrideNode>(node)) {
      return std::make_unique<GlutenStrideOperator>(id, ctx, n);
    }
    return nullptr;
  }
};

} // namespace gluten

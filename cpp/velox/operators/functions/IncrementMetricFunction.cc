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

#include "operators/functions/IncrementMetricFunction.h"

#include <memory>
#include <vector>

#include "velox/core/Expressions.h"
#include "velox/expression/EvalCtx.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/expression/VectorFunction.h"

namespace gluten {
namespace {

using namespace facebook::velox;

/// Returns its only argument. See the header for why the name, not the body, carries the meaning.
class IncrementMetricFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 1);
    context.moveOrCopyResult(args[0], rows, result);
  }
};

bool callsIncrementMetric(const core::ITypedExpr& expr) {
  if (const auto* call = dynamic_cast<const core::CallTypedExpr*>(&expr)) {
    if (isIncrementMetricFunction(call->name())) {
      return true;
    }
  }
  if (const auto* lambda = dynamic_cast<const core::LambdaTypedExpr*>(&expr)) {
    return lambda->body() != nullptr && callsIncrementMetric(*lambda->body());
  }
  for (const auto& input : expr.inputs()) {
    if (input != nullptr && callsIncrementMetric(*input)) {
      return true;
    }
  }
  return false;
}

} // namespace

std::string incrementMetricFunctionName(int32_t slot) {
  return std::string(kIncrementMetricFunctionPrefix) + std::to_string(slot);
}

bool isIncrementMetricFunction(const std::string& functionName) {
  return functionName.rfind(kIncrementMetricFunctionPrefix, 0) == 0;
}

void registerIncrementMetricFunctions() {
  // Not deterministic: keeps Velox from constant-folding, sharing or dictionary-peeling the call,
  // any of which would change how many rows it is evaluated on. No default null behavior: a null
  // input still counts, as it does on Spark.
  const auto metadata = exec::VectorFunctionMetadataBuilder().deterministic(false).defaultNullBehavior(false).build();
  for (int32_t slot = 0; slot < kIncrementMetricFunctionSlots; ++slot) {
    exec::registerVectorFunction(
        incrementMetricFunctionName(slot),
        {exec::FunctionSignatureBuilder().typeVariable("T").returnType("T").argumentType("T").build()},
        std::make_unique<IncrementMetricFunction>(),
        metadata);
  }
}

bool usesIncrementMetricFunctions(const core::PlanNode& plan) {
  if (const auto* project = dynamic_cast<const core::ProjectNode*>(&plan)) {
    for (const auto& projection : project->projections()) {
      if (projection != nullptr && callsIncrementMetric(*projection)) {
        return true;
      }
    }
  } else if (const auto* filter = dynamic_cast<const core::FilterNode*>(&plan)) {
    if (filter->filter() != nullptr && callsIncrementMetric(*filter->filter())) {
      return true;
    }
  }
  for (const auto& source : plan.sources()) {
    if (source != nullptr && usesIncrementMetricFunctions(*source)) {
      return true;
    }
  }
  return false;
}

} // namespace gluten

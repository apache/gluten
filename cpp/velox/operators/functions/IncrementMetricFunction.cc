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

#include <charconv>
#include <memory>
#include <optional>
#include <string_view>
#include <system_error>
#include <unordered_set>
#include <vector>

#include "velox/core/Expressions.h"
#include "velox/core/QueryConfig.h"
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

void collectIncrementMetricCalls(const core::ITypedExpr& expr, std::unordered_set<std::string>& names) {
  if (const auto* call = dynamic_cast<const core::CallTypedExpr*>(&expr)) {
    if (isIncrementMetricFunction(call->name())) {
      names.insert(call->name());
    }
  }
  if (const auto* lambda = dynamic_cast<const core::LambdaTypedExpr*>(&expr)) {
    if (lambda->body() != nullptr) {
      collectIncrementMetricCalls(*lambda->body(), names);
    }
    return;
  }
  for (const auto& input : expr.inputs()) {
    if (input != nullptr) {
      collectIncrementMetricCalls(*input, names);
    }
  }
}

void collectIncrementMetricCallsInProjects(const core::PlanNode& plan, std::unordered_set<std::string>& names) {
  if (const auto* project = dynamic_cast<const core::ProjectNode*>(&plan)) {
    for (const auto& projection : project->projections()) {
      if (projection != nullptr) {
        collectIncrementMetricCalls(*projection, names);
      }
    }
  }
  for (const auto& source : plan.sources()) {
    if (source != nullptr) {
      collectIncrementMetricCallsInProjects(*source, names);
    }
  }
}

} // namespace

std::string incrementMetricFunctionName(int32_t slot) {
  return std::string(kIncrementMetricFunctionPrefix) + std::to_string(slot);
}

bool isIncrementMetricFunction(const std::string& functionName) {
  const std::string_view prefix{kIncrementMetricFunctionPrefix};
  if (functionName.size() <= prefix.size() || functionName.compare(0, prefix.size(), prefix) != 0) {
    return false;
  }
  int32_t slot = -1;
  const char* const end = functionName.data() + functionName.size();
  const auto [parsedEnd, error] = std::from_chars(functionName.data() + prefix.size(), end, slot);
  // The last check rejects spellings that parse to a slot but are not its name, such as a leading
  // zero.
  return error == std::errc() && parsedEnd == end && slot >= 0 && slot < kIncrementMetricFunctionSlots &&
      functionName == incrementMetricFunctionName(slot);
}

void registerIncrementMetricFunctions() {
  // Not deterministic: keeps Velox from constant-folding or dictionary-peeling the call, either of
  // which would change how many rows it is evaluated on. No default null behavior: a null input
  // still counts, as it does on Spark.
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
  std::unordered_set<std::string> names;
  collectIncrementMetricCallsInProjects(plan, names);
  return !names.empty();
}

void checkIncrementMetricFunctionsInProjects(
    const std::unordered_map<uint64_t, std::string>& functionMap,
    const core::PlanNode& plan) {
  std::optional<std::unordered_set<std::string>> calledInProjects;
  for (const auto& entry : functionMap) {
    // Declarations are spelled `<name>:<argument types>`.
    const auto name = entry.second.substr(0, entry.second.find(':'));
    if (!isIncrementMetricFunction(name)) {
      continue;
    }
    if (!calledInProjects.has_value()) {
      calledInProjects.emplace();
      collectIncrementMetricCallsInProjects(plan, *calledInProjects);
    }
    VELOX_CHECK(
        calledInProjects->count(name) > 0,
        "Delta counter {} is called outside a projection, where the JVM does not credit it",
        name);
  }
}

void setIncrementMetricQueryConfigs(std::unordered_map<std::string, std::string>& configs) {
  configs[core::QueryConfig::kOperatorTrackExpressionStats] = "true";
  configs[core::QueryConfig::kExprEvalSimplified] = "false";
}

} // namespace gluten

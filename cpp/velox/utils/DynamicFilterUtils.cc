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

#include "utils/DynamicFilterUtils.h"

#include "velox/connectors/common/FileTableHandle.h"
#include "velox/functions/FunctionRegistry.h"

using namespace facebook::velox;

namespace gluten {

namespace {

bool callsNonDeterministicFunction(const core::ITypedExpr& expr) {
  if (const auto* call = dynamic_cast<const core::CallTypedExpr*>(&expr)) {
    // Special forms such as if, switch, and, or and cast are not registered functions; they are
    // deterministic when their inputs are.
    if (isDeterministic(call->name()) == false) {
      return true;
    }
  }
  if (const auto* lambda = dynamic_cast<const core::LambdaTypedExpr*>(&expr)) {
    if (lambda->body() != nullptr && callsNonDeterministicFunction(*lambda->body())) {
      return true;
    }
  }
  for (const auto& input : expr.inputs()) {
    if (input != nullptr && callsNonDeterministicFunction(*input)) {
      return true;
    }
  }
  return false;
}

bool evaluatesNonDeterministicExpr(const core::PlanNode& node) {
  if (const auto* project = dynamic_cast<const core::AbstractProjectNode*>(&node)) {
    for (const auto& projection : project->projections()) {
      if (projection != nullptr && callsNonDeterministicFunction(*projection)) {
        return true;
      }
    }
  } else if (const auto* filter = dynamic_cast<const core::FilterNode*>(&node)) {
    return filter->filter() != nullptr && callsNonDeterministicFunction(*filter->filter());
  } else if (const auto* scan = dynamic_cast<const core::TableScanNode*>(&node)) {
    // A filter pushed into the scan, such as `rand() < 0.5`, runs after the scan applies the
    // dynamic filters it accepted.
    const auto* fileTable = dynamic_cast<const connector::FileTableHandle*>(scan->tableHandle().get());
    return fileTable != nullptr && fileTable->remainingFilter() != nullptr &&
        callsNonDeterministicFunction(*fileTable->remainingFilter());
  }
  return false;
}

// The nodes a dynamic filter from the probe can reach: the probe input and, below it, the first
// input of each node down to the leaf. Other inputs, such as a build side, run in other pipelines.
bool hasNonDeterministicExprOnProbePath(const core::PlanNode& probeInput) {
  for (const core::PlanNode* node = &probeInput; node != nullptr;) {
    if (evaluatesNonDeterministicExpr(*node)) {
      return true;
    }
    node = node->sources().empty() ? nullptr : node->sources()[0].get();
  }
  return false;
}

} // namespace

bool hasNonDeterministicExprBelowHashProbe(const core::PlanNode& plan) {
  if (const auto* join = dynamic_cast<const core::HashJoinNode*>(&plan)) {
    if (!join->sources().empty() && join->sources()[0] != nullptr &&
        hasNonDeterministicExprOnProbePath(*join->sources()[0])) {
      return true;
    }
  }
  for (const auto& source : plan.sources()) {
    if (source != nullptr && hasNonDeterministicExprBelowHashProbe(*source)) {
      return true;
    }
  }
  return false;
}

} // namespace gluten

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

// Whether a hash join of this type pushes dynamic filters into its probe side, as in
// HashProbe::asyncWaitForHashTable(). A left or full outer join, for example, never does.
bool canPushDynamicFilters(const core::HashJoinNode& join) {
  const auto type = join.joinType();
  return core::isInnerJoin(type) || core::isLeftSemiFilterJoin(type) || core::isCountingLeftSemiFilterJoin(type) ||
      core::isRightSemiFilterJoin(type) || (core::isRightSemiProjectJoin(type) && !join.isNullAware()) ||
      core::isRightJoin(type) || core::isRightAntiJoin(type);
}

struct ProbePathInfo {
  // Whether this node or one below it on the path of first inputs down to the leaf evaluates a
  // non-deterministic expression. A dynamic filter from a hash probe reaches only that path: other
  // inputs, such as a build side, run in other pipelines.
  bool nonDeterministicOnPath{false};
  // Whether this node or one below it is a hash join that can push a dynamic filter below a
  // non-deterministic expression.
  bool affectedJoin{false};
};

// Computes both in one bottom-up pass, so each node and expression is checked once.
ProbePathInfo checkProbePaths(const core::PlanNode& node) {
  ProbePathInfo info;
  const auto& sources = node.sources();
  for (size_t i = 0; i < sources.size(); ++i) {
    if (sources[i] == nullptr) {
      continue;
    }
    const auto source = checkProbePaths(*sources[i]);
    info.affectedJoin = info.affectedJoin || source.affectedJoin;
    if (i == 0) {
      info.nonDeterministicOnPath = source.nonDeterministicOnPath;
    }
  }
  if (const auto* join = dynamic_cast<const core::HashJoinNode*>(&node)) {
    info.affectedJoin = info.affectedJoin || (info.nonDeterministicOnPath && canPushDynamicFilters(*join));
  }
  info.nonDeterministicOnPath = info.nonDeterministicOnPath || evaluatesNonDeterministicExpr(node);
  return info;
}

} // namespace

bool hasNonDeterministicExprBelowHashProbe(const core::PlanNode& plan) {
  return checkProbePaths(plan).affectedJoin;
}

} // namespace gluten

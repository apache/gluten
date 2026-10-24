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
#include <string>
#include <unordered_map>

#include "velox/core/PlanNode.h"

namespace gluten {

/// Delta's `IncrementMetric` counts how many rows an expression is evaluated on. Velox already
/// keeps that count per expression (`ExprStats::numProcessedRows`) on its regular evaluation path
/// and project and filter operators export it per function name when asked to, so a counter is a
/// pass-through function whose name is the key the JVM credits the metric from. Inside a CASE WHEN
/// branch that is the number of rows that took the branch, at the top of a projection it is every
/// row, which is exactly how Spark evaluates `IncrementMetric`.
///
/// The names are fixed slots, `increment_metric_<slot>`, registered once at startup. The JVM
/// assigns one slot per distinct metric of a projection, so a metric needs no name of its own and
/// nothing is registered while queries run.
constexpr const char* kIncrementMetricFunctionPrefix = "increment_metric_";

/// Number of counter slots registered. Mirrored by `IncrementMetricCall.maxCounters` on the JVM.
constexpr int32_t kIncrementMetricFunctionSlots = 32;

/// Name of the counter function for `slot`.
std::string incrementMetricFunctionName(int32_t slot);

/// True if `functionName` is one of the registered slots, `increment_metric_0` to
/// `increment_metric_<kIncrementMetricFunctionSlots - 1>`.
bool isIncrementMetricFunction(const std::string& functionName);

/// Registers every counter slot. Called from registerAllFunctions().
void registerIncrementMetricFunctions();

/// True if a project or filter in `plan` calls a counter, which is when the query needs
/// setIncrementMetricQueryConfigs(). Only those nodes are inspected because they are the only ones
/// Gluten puts counters in: the JVM emits them from Delta's project and filter transformers alone.
bool usesIncrementMetricFunctions(const facebook::velox::core::PlanNode& plan);

/// Sets the query configs a plan that calls a counter depends on, over any user override: project
/// and filter operators export their expression statistics, and expressions use the regular
/// evaluation path, since simplified evaluation (`expression.eval_simplified`) applies functions
/// without counting their rows.
void setIncrementMetricQueryConfigs(std::unordered_map<std::string, std::string>& configs);

} // namespace gluten

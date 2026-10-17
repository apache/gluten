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

#include "velox/core/PlanNode.h"

namespace gluten {

/// Delta's `IncrementMetric` counts how many rows an expression is evaluated on. Velox already
/// keeps that count per expression (`ExprStats::numProcessedRows`) and project and filter operators
/// export it per function name, so a counter is a pass-through function whose name is the key the
/// JVM credits the metric from. Inside a CASE WHEN branch that is the number of rows that took the
/// branch, at the top of a projection it is every row, which is exactly how Spark evaluates
/// `IncrementMetric`.
///
/// The names are fixed slots, `increment_metric_<slot>`, registered once at startup. The JVM
/// assigns one slot per distinct metric of a projection, so a metric needs no name of its own and
/// nothing is registered while queries run.
constexpr const char* kIncrementMetricFunctionPrefix = "increment_metric_";

/// Number of counter slots registered. Mirrored by `IncrementMetricCall.maxCounters` on the JVM.
constexpr int32_t kIncrementMetricFunctionSlots = 32;

/// Name of the counter function for `slot`.
std::string incrementMetricFunctionName(int32_t slot);

/// True if `functionName` is a counter slot.
bool isIncrementMetricFunction(const std::string& functionName);

/// Registers every counter slot. Called from registerAllFunctions().
void registerIncrementMetricFunctions();

/// True if a project or filter in `plan` calls a counter, which is when the query must export
/// per-expression statistics.
bool usesIncrementMetricFunctions(const facebook::velox::core::PlanNode& plan);

} // namespace gluten

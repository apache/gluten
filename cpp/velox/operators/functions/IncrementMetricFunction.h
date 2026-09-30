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

#include <string>

namespace gluten {

/// Prefix of the Velox function names that carry a Delta `IncrementMetric` counter. Every Delta
/// metric gets its own name (`increment_metric_<metric>`), because Velox keys its per-expression
/// statistics by function name. The function returns its argument unchanged; what the query wants
/// is Velox's `numProcessedRows` for that name, read back through the operator's expression stats.
/// Inside a CASE WHEN branch that is the number of rows that took the branch, at the top of a
/// projection it is every row, which is exactly how Spark evaluates `IncrementMetric`.
constexpr const char* kIncrementMetricFunctionPrefix = "increment_metric_";

/// True if `functionName` is a Delta counter that must be registered before use.
bool isIncrementMetricFunction(const std::string& functionName);

/// Registers `functionName` as a counter pass-through if it is not registered yet. Idempotent and
/// safe to call from plan conversion on any thread; the set of names is bounded by the distinct
/// Delta metrics the process has seen.
void ensureIncrementMetricFunctionRegistered(const std::string& functionName);

} // namespace gluten

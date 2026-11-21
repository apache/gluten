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

namespace gluten {

/// True if a projection or filter on the probe side of a hash join that can push dynamic filters,
/// between the join and the leaf that feeds it, calls a non-deterministic function. That includes a
/// filter pushed into the scan. Joins that never push one, such as a left outer join, are skipped.
///
/// A hash probe pushes a filter on its join keys to the operators that feed it, through every
/// operator that passes the key column through unchanged, down to the scan. Velox does not check
/// whether those operators evaluate non-deterministic expressions, so with the filter in place
/// rand() and monotonically_increasing_id() see fewer rows than on Spark and return different
/// values, a filter such as `rand() < 0.5` keeps different rows, and a Delta counter undercounts.
/// Spark never moves a filter below a non-deterministic expression, so such plans must not push
/// dynamic filters.
bool hasNonDeterministicExprBelowHashProbe(const facebook::velox::core::PlanNode& plan);

} // namespace gluten

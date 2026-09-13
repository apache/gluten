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
#include "operators/functions/overlay/ElementAt.h"

#include "velox/functions/lib/SubscriptUtil.h"
#include "velox/functions/sparksql/SparkQueryConfig.h"

using namespace facebook::velox;

namespace gluten {
namespace {

/// Spark's element_at over an array is 1-based, negative indices count from the
/// end of the array, and an index of 0 is an error. Those match Velox's
/// element_at; 'allowOutOfBound' is the one thing ANSI mode changes, an index
/// past the end of the array giving NULL rather than an error.
///
/// The map side of SubscriptImpl does not look at 'allowOutOfBound', so a key
/// that the map does not contain keeps giving NULL, which is what Spark does in
/// ANSI mode as well.
template <bool allowOutOfBound>
using ElementAtFunction = functions::SubscriptImpl<
    /*allowNegativeIndices=*/true,
    /*nullOnNegativeIndices=*/false,
    allowOutOfBound,
    /*indexStartsAtOne=*/true>;

} // namespace

std::vector<exec::FunctionSignaturePtr> elementAtSignatures() {
  // The signatures do not depend on the out-of-bound behavior.
  return ElementAtFunction</*allowOutOfBound=*/true>::signatures();
}

std::shared_ptr<exec::VectorFunction> makeElementAt(
    const std::string& /*name*/,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  VELOX_CHECK_EQ(inputArgs.size(), 2);
  if (!inputArgs[0].type->isArray()) {
    // Same as Velox's element_at over a map, which may cache a materialized
    // version of the map when it is reused across batches.
    return std::make_shared<ElementAtFunction</*allowOutOfBound=*/true>>(config.isExpressionEvaluationCacheEnabled());
  }
  // The array side holds no state, so one shared instance per behavior is
  // enough. Caching is a map-only optimization.
  if (functions::sparksql::SparkQueryConfig{config}.ansiEnabled()) {
    static const auto kFailOnOutOfBound =
        std::make_shared<ElementAtFunction</*allowOutOfBound=*/false>>(/*allowCaching=*/false);
    return kFailOnOutOfBound;
  }
  static const auto kNullOnOutOfBound =
      std::make_shared<ElementAtFunction</*allowOutOfBound=*/true>>(/*allowCaching=*/false);
  return kNullOnOutOfBound;
}

} // namespace gluten

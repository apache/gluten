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

#include <folly/Synchronized.h>

#include <memory>
#include <unordered_set>
#include <vector>

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

std::vector<std::shared_ptr<exec::FunctionSignature>> incrementMetricSignatures() {
  return {exec::FunctionSignatureBuilder().typeVariable("T").returnType("T").argumentType("T").build()};
}

exec::VectorFunctionMetadata incrementMetricMetadata() {
  // Not deterministic: keeps Velox from constant-folding, sharing or dictionary-peeling the call,
  // any of which would change how many rows it is evaluated on. No default null behavior: a null
  // input still counts, as it does on Spark.
  return exec::VectorFunctionMetadataBuilder().deterministic(false).defaultNullBehavior(false).build();
}

folly::Synchronized<std::unordered_set<std::string>>& registeredIncrementMetricFunctions() {
  static folly::Synchronized<std::unordered_set<std::string>> names;
  return names;
}

} // namespace

bool isIncrementMetricFunction(const std::string& functionName) {
  return functionName.rfind(kIncrementMetricFunctionPrefix, 0) == 0;
}

void ensureIncrementMetricFunctionRegistered(const std::string& functionName) {
  {
    auto names = registeredIncrementMetricFunctions().rlock();
    if (names->count(functionName) != 0) {
      return;
    }
  }
  auto names = registeredIncrementMetricFunctions().wlock();
  if (names->count(functionName) != 0) {
    return;
  }
  exec::registerVectorFunction(
      functionName,
      incrementMetricSignatures(),
      std::make_unique<IncrementMetricFunction>(),
      incrementMetricMetadata(),
      /*overwrite=*/false);
  names->insert(functionName);
}

} // namespace gluten

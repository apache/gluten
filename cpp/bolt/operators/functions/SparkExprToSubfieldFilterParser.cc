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
#include "operators/functions/SparkExprToSubfieldFilterParser.h"

namespace gluten {

using namespace bytedance::bolt;

namespace {
std::optional<std::pair<bytedance::bolt::common::Subfield, std::unique_ptr<bytedance::bolt::common::Filter>>> combine(
    bytedance::bolt::common::Subfield& subfield,
    std::unique_ptr<bytedance::bolt::common::Filter>& filter) {
  if (filter != nullptr) {
    return std::make_pair(std::move(subfield), std::move(filter));
  }

  return std::nullopt;
}
} // namespace

std::optional<std::pair<bytedance::bolt::common::Subfield, std::unique_ptr<bytedance::bolt::common::Filter>>>
SparkExprToSubfieldFilterParser::leafCallToSubfieldFilter(
    const core::CallTypedExpr& call,
    core::ExpressionEvaluator* evaluator,
    bool negated) {
  if (call.inputs().empty()) {
    return std::nullopt;
  }

  const auto* leftSide = call.inputs()[0].get();

  common::Subfield subfield;
  if (call.name() == "equalto") {
    if (toSubfield(leftSide, subfield)) {
      auto filter =
          negated ? makeNotEqualFilter(call.inputs()[1], evaluator) : makeEqualFilter(call.inputs()[1], evaluator);
      return combine(subfield, filter);
    }
  } else if (call.name() == "lessthanorequal") {
    if (toSubfield(leftSide, subfield)) {
      auto filter = negated ? makeGreaterThanFilter(call.inputs()[1], evaluator)
                            : makeLessThanOrEqualFilter(call.inputs()[1], evaluator);
      return combine(subfield, filter);
    }
  } else if (call.name() == "lessthan") {
    if (toSubfield(leftSide, subfield)) {
      auto filter = negated ? makeGreaterThanOrEqualFilter(call.inputs()[1], evaluator)
                            : makeLessThanFilter(call.inputs()[1], evaluator);
      return combine(subfield, filter);
    }
  } else if (call.name() == "greaterthanorequal") {
    if (toSubfield(leftSide, subfield)) {
      auto filter = negated ? makeLessThanFilter(call.inputs()[1], evaluator)
                            : makeGreaterThanOrEqualFilter(call.inputs()[1], evaluator);
      return combine(subfield, filter);
    }
  } else if (call.name() == "greaterthan") {
    if (toSubfield(leftSide, subfield)) {
      auto filter = negated ? makeLessThanOrEqualFilter(call.inputs()[1], evaluator)
                            : makeGreaterThanFilter(call.inputs()[1], evaluator);
      return combine(subfield, filter);
    }
  } else if (call.name() == "in") {
    if (toSubfield(leftSide, subfield)) {
      auto filter = makeInFilter(call.inputs()[1], evaluator, negated);
      return combine(subfield, filter);
    }
  } else if (call.name() == "isnull") {
    if (toSubfield(leftSide, subfield)) {
      if (negated) {
        return std::make_pair(std::move(subfield), bytedance::bolt::exec::isNotNull());
      }
      return std::make_pair(std::move(subfield), bytedance::bolt::exec::isNull());
    }
  } else if (call.name() == "isnotnull") {
    if (toSubfield(leftSide, subfield)) {
      if (negated) {
        return std::make_pair(std::move(subfield), bytedance::bolt::exec::isNull());
      }
      return std::make_pair(std::move(subfield), bytedance::bolt::exec::isNotNull());
    }
  }
  return std::nullopt;
}

} // namespace gluten

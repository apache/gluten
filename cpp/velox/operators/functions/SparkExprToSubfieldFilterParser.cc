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

#include "utils/Exception.h"
#include "velox/common/base/BloomFilter.h"
#include "velox/expression/Expr.h"
#include "velox/functions/sparksql/XxHash64.h"
#include "velox/vector/ComplexVector.h"

namespace gluten {

using namespace facebook::velox;

namespace {

// Evaluates an expression as a constant. Returns nullptr if the expression is
// not constant or evaluation fails. Errors are intentionally swallowed because
// a non-evaluable expression simply means the filter cannot be pushed down.
VectorPtr toConstant(const core::TypedExprPtr& expr, core::ExpressionEvaluator* evaluator) {
  auto exprSet = evaluator->compile(expr);
  if (!exprSet->exprs()[0]->isConstantExpr()) {
    return nullptr;
  }
  RowVector input(evaluator->pool(), ROW({}, {}), nullptr, 1, std::vector<VectorPtr>{});
  SelectivityVector rows(1);
  VectorPtr result;
  try {
    evaluator->evaluate(exprSet.get(), rows, input, result);
  } catch (const VeloxUserError& error) {
    VLOG(1) << "Failed to evaluate constant expression for scan filter pushdown: " << error.what();
    return nullptr;
  }
  return result;
}

/// Subfield filter backed by Velox's BloomFilter from bloom_filter_agg / might_contain.
/// Values are hashed with Spark-compatible XXH64 using the seed extracted from
/// xxhash64_with_seed, then re-hashed with folly hasher for bloom filter bucket
/// selection, matching bloom_filter_agg's insertion path.
template <bool kIsInt32>
class SparkMightContain final : public common::BigintValuesUsingBloomFilter {
 public:
  SparkMightContain(VectorPtr constantVector, bool nullAllowed, int64_t seed)
      : common::BigintValuesUsingBloomFilter(0, nullAllowed), constantVector_(std::move(constantVector)), seed_(seed) {
    auto sv = constantVector_->as<SimpleVector<StringView>>()->valueAt(0);
    view_ = std::make_unique<BloomFilterView>(sv.data());
  }

  bool testInt64(int64_t value) const override {
    uint64_t hash;
    if constexpr (kIsInt32) {
      hash = functions::sparksql::XxHash64::hashInt32(static_cast<int32_t>(value), seed_);
    } else {
      hash = functions::sparksql::XxHash64::hashInt64(value, seed_);
    }
    return view_->mayContain(folly::hasher<int64_t>()(hash));
  }

  bool testInt64Range(int64_t /*min*/, int64_t /*max*/, bool /*hasNull*/) const override {
    return true;
  }

  std::unique_ptr<Filter> clone(std::optional<bool> nullAllowed) const override {
    return std::make_unique<SparkMightContain<kIsInt32>>(constantVector_, nullAllowed.value_or(nullAllowed_), seed_);
  }

  bool testingEquals(const Filter& other) const override {
    return dynamic_cast<const SparkMightContain<kIsInt32>*>(&other) != nullptr;
  }

  folly::dynamic serialize() const override {
    VELOX_UNSUPPORTED("Serialization is not supported for SparkMightContain");
  }

 private:
  VectorPtr constantVector_;
  std::unique_ptr<BloomFilterView> view_;
  int64_t seed_;
};

std::optional<std::pair<facebook::velox::common::Subfield, std::unique_ptr<facebook::velox::common::Filter>>> combine(
    facebook::velox::common::Subfield& subfield,
    std::unique_ptr<facebook::velox::common::Filter>& filter) {
  if (filter != nullptr) {
    return std::make_pair(std::move(subfield), std::move(filter));
  }

  return std::nullopt;
}

} // namespace

std::optional<std::pair<facebook::velox::common::Subfield, std::unique_ptr<facebook::velox::common::Filter>>>
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
        return std::make_pair(std::move(subfield), facebook::velox::exec::isNotNull());
      }
      return std::make_pair(std::move(subfield), facebook::velox::exec::isNull());
    }
  } else if (call.name() == "isnotnull") {
    if (toSubfield(leftSide, subfield)) {
      if (negated) {
        return std::make_pair(std::move(subfield), facebook::velox::exec::isNull());
      }
      return std::make_pair(std::move(subfield), facebook::velox::exec::isNotNull());
    }
  } else if (scanBloomFilterPushdownEnabled_ && call.name() == "might_contain" && !negated) {
    // Matches: might_contain(bloomFilter, xxhash64_with_seed(seed, field)).
    GLUTEN_CHECK(
        call.inputs().size() == 2,
        "might_contain expects 2 arguments: bloomFilter and xxhash64_with_seed(seed, field)");
    const auto* hashCall = dynamic_cast<const core::CallTypedExpr*>(call.inputs()[1].get());
    if (hashCall && hashCall->name() == "xxhash64_with_seed") {
      GLUTEN_CHECK(hashCall->inputs().size() == 2, "xxhash64_with_seed expects 2 arguments");
      const auto inputTypeKind = hashCall->inputs()[1]->type()->kind();
      if (inputTypeKind != TypeKind::INTEGER && inputTypeKind != TypeKind::BIGINT) {
        return std::nullopt;
      }
      auto seedValue = toConstant(hashCall->inputs()[0], evaluator);
      if (!seedValue || seedValue->isNullAt(0)) {
        LOG(WARNING) << "might_contain: seed value is null or not constant, "
                     << "cannot push down to subfield filter";
        return std::nullopt;
      }
      auto seed = seedValue->as<SimpleVector<int64_t>>()->valueAt(0);
      if (!toSubfield(hashCall->inputs()[1].get(), subfield)) {
        LOG(WARNING) << "might_contain: second argument to xxhash64_with_seed "
                     << "is not a subfield, cannot push down to subfield filter";
        return std::nullopt;
      }
      auto bloomFilterValue = toConstant(call.inputs()[0], evaluator);
      if (bloomFilterValue && !bloomFilterValue->isNullAt(0)) {
        std::unique_ptr<common::Filter> filter;
        if (inputTypeKind == TypeKind::INTEGER) {
          filter = std::make_unique<SparkMightContain<true>>(bloomFilterValue, false /*nullAllowed*/, seed);
        } else {
          filter = std::make_unique<SparkMightContain<false>>(bloomFilterValue, false /*nullAllowed*/, seed);
        }
        return combine(subfield, filter);
      }
    }
    LOG(WARNING) << "might_contain could not be converted to a subfield filter";
  }
  return std::nullopt;
}

} // namespace gluten

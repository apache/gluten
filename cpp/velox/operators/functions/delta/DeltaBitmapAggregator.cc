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

#include "operators/functions/delta/DeltaBitmapAggregator.h"

#include <memory>
#include <optional>
#include <tuple>
#include <vector>

#include "compute/delta/RoaringBitmapArray.h"
#include "velox/exec/SimpleAggregateAdapter.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/type/SimpleFunctionApi.h"

using namespace facebook::velox;

namespace gluten {
namespace {

class DeltaBitmapAggregatorFunction {
 public:
  using InputType = Row<int64_t>;
  using IntermediateType = Varbinary;
  using OutputType = Row<int64_t, int64_t, Varbinary>;

  static constexpr bool default_null_behavior_ = false;

  static void addRowIndex(delta::RoaringBitmapArray& bitmap, int64_t value) {
    VELOX_CHECK_GE(value, 0, "Delta bitmap row index cannot be negative: {}", value);
    bitmap.addSafe(static_cast<uint64_t>(value));
  }

  static bool toIntermediate(exec::out_type<IntermediateType>& out, exec::optional_arg_type<int64_t> value) {
    delta::RoaringBitmapArray bitmap;
    if (value.has_value()) {
      addRowIndex(bitmap, value.value());
    }
    const auto serialized = bitmap.serializeToString();
    out.copy_from(StringView(serialized.data(), serialized.size()));
    return true;
  }

  struct AccumulatorType {
    static constexpr bool use_external_memory_ = true;

    explicit AccumulatorType(HashStringAllocator* /* allocator */, DeltaBitmapAggregatorFunction* /* fn */) {}

    bool addInput(HashStringAllocator* /* allocator */, exec::optional_arg_type<int64_t> value) {
      if (!value.has_value()) {
        return false;
      }
      addRowIndex(bitmap, value.value());
      return true;
    }

    bool combine(HashStringAllocator* /* allocator */, exec::optional_arg_type<IntermediateType> other) {
      if (!other.has_value()) {
        return false;
      }
      const auto serialized = other.value();
      delta::RoaringBitmapArray otherBitmap;
      otherBitmap.deserialize(serialized.data(), serialized.size());
      bitmap.merge(otherBitmap);
      return true;
    }

    bool writeIntermediateResult(bool /* nonNullGroup */, exec::out_type<IntermediateType>& out) {
      const auto serialized = bitmap.serializeToString();
      out.copy_from(StringView(serialized.data(), serialized.size()));
      return true;
    }

    bool writeFinalResult(bool /* nonNullGroup */, exec::out_type<OutputType>& out) {
      const auto serialized = bitmap.serializeToString(true);
      const auto last = bitmap.last();
      out.copy_from(std::make_tuple(
          static_cast<int64_t>(bitmap.cardinality()),
          last.has_value() ? std::optional<int64_t>(static_cast<int64_t>(*last)) : std::nullopt,
          StringView(serialized.data(), serialized.size())));
      return true;
    }

    delta::RoaringBitmapArray bitmap;
  };
};

} // namespace

void registerDeltaBitmapAggregator(const std::string& prefix, bool withCompanionFunctions, bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .argumentType("bigint")
          .intermediateType("varbinary")
          .returnType("row(bigint,bigint,varbinary)")
          .build()};

  exec::registerAggregateFunction(
      prefix + "bitmapaggregator",
      std::move(signatures),
      [](core::AggregationNode::Step step,
         const std::vector<TypePtr>& argTypes,
         const TypePtr& resultType,
         const core::QueryConfig& /* config */) -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(argTypes.size(), 1, "bitmapaggregator takes one argument");
        VELOX_CHECK_EQ(argTypes[0]->kind(), exec::isRawInput(step) ? TypeKind::BIGINT : TypeKind::VARBINARY);
        return std::make_unique<exec::SimpleAggregateAdapter<DeltaBitmapAggregatorFunction>>(
            step, argTypes, resultType);
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace gluten

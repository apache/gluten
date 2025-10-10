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

#include <bolt/exec/SimpleAggregateAdapter.h>
#include <bolt/expression/VectorFunction.h>
#include <bolt/functions/Macros.h>
#include <bolt/functions/Registerer.h>
#include <bolt/functions/lib/aggregates/AverageAggregateBase.h>

#include "udf/Udaf.h"
#include "udf/examples/UdfCommon.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;

namespace {

static const char* kBoolean = "boolean";
static const char* kInteger = "int";
static const char* kBigInt = "bigint";
static const char* kFloat = "float";
static const char* kDouble = "double";

namespace myavg {
// Copied from bolt/exec/tests/SimpleAverageAggregate.cpp

// Implementation of the average aggregation function through the
// SimpleAggregateAdapter.
template <typename T>
class AverageAggregate {
 public:
  // Type(s) of input vector(s) wrapped in Row.
  using InputType = Row<T>;

  // Type of intermediate result vector wrapped in Row.
  using IntermediateType =
      Row</*sum*/ double,
          /*count*/ int64_t>;

  // Type of output vector.
  using OutputType = std::conditional_t<std::is_same_v<T, float>, float, double>;

  static bool toIntermediate(exec::out_type<Row<double, int64_t>>& out, exec::arg_type<T> in) {
    out.copy_from(std::make_tuple(static_cast<double>(in), 1));
    return true;
  }

  struct AccumulatorType {
    double sum_;
    int64_t count_;

    AccumulatorType() = delete;

    // Constructor used in initializeNewGroups().
    explicit AccumulatorType(HashStringAllocator* /*allocator*/) {
      sum_ = 0;
      count_ = 0;
    }

    // addInput expects one parameter of exec::arg_type<T> for each child-type T
    // wrapped in InputType.
    void addInput(HashStringAllocator* /*allocator*/, exec::arg_type<T> data) {
      sum_ += data;
      count_ = checkedPlus<int64_t>(count_, 1);
    }

    // combine expects one parameter of exec::arg_type<IntermediateType>.
    void combine(HashStringAllocator* /*allocator*/, exec::arg_type<Row<double, int64_t>> other) {
      // Both field of an intermediate result should be non-null because
      // writeIntermediateResult() never make an intermediate result with a
      // single null.
      BOLT_CHECK(other.at<0>().has_value());
      BOLT_CHECK(other.at<1>().has_value());
      sum_ += other.at<0>().value();
      count_ = checkedPlus<int64_t>(count_, other.at<1>().value());
    }

    bool writeFinalResult(exec::out_type<OutputType>& out) {
      out = sum_ / count_ + 100.0;
      return true;
    }

    bool writeIntermediateResult(exec::out_type<IntermediateType>& out) {
      out = std::make_tuple(sum_, count_);
      return true;
    }
  };
};

class MyAvgRegisterer final : public gluten::UdafRegisterer {
  int getNumUdaf() override {
    return 2;
  }

  void populateUdafEntries(int& index, gluten::UdafEntry* udafEntries) override {
    for (const auto& argTypes : {myAvgArgFloat_, myAvgArgDouble_}) {
      udafEntries[index++] = {name_.c_str(), kDouble, 1, argTypes, myAvgIntermediateType_, false, true};
    }
  }

  void registerSignatures() override {
    registerSimpleAverageAggregate();
  }

 private:
  exec::AggregateRegistrationResult registerSimpleAverageAggregate() {
    std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;

    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType("double")
                             .intermediateType("row(double,bigint)")
                             .argumentType("double")
                             .build());

    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType("real")
                             .intermediateType("row(double,bigint)")
                             .argumentType("real")
                             .build());

    return exec::registerAggregateFunction(
        name_,
        std::move(signatures),
        [this](
            core::AggregationNode::Step step,
            const std::vector<TypePtr>& argTypes,
            const TypePtr& resultType,
            const core::QueryConfig& /*config*/) -> std::unique_ptr<exec::Aggregate> {
          BOLT_CHECK_LE(argTypes.size(), 1, "{} takes at most one argument", name_);
          auto inputType = argTypes[0];
          if (exec::isRawInput(step)) {
            switch (inputType->kind()) {
              case TypeKind::REAL:
                return std::make_unique<SimpleAggregateAdapter<AverageAggregate<float>>>(resultType);
              case TypeKind::DOUBLE:
                return std::make_unique<SimpleAggregateAdapter<AverageAggregate<double>>>(resultType);
              default:
                BOLT_FAIL("Unknown input type for {} aggregation {}", name_, inputType->kindName());
            }
          } else {
            switch (resultType->kind()) {
              case TypeKind::REAL:
                return std::make_unique<SimpleAggregateAdapter<AverageAggregate<float>>>(resultType);
              case TypeKind::DOUBLE:
              case TypeKind::ROW:
                return std::make_unique<SimpleAggregateAdapter<AverageAggregate<double>>>(resultType);
              default:
                BOLT_FAIL("Unsupported result type for final aggregation: {}", resultType->kindName());
            }
          }
        },
        true /*registerCompanionFunctions*/,
        true /*overwrite*/);
  }

  const std::string name_ = "test.org.apache.spark.sql.MyDoubleAvg";
  const char* myAvgArgFloat_[1] = {kFloat};
  const char* myAvgArgDouble_[1] = {kDouble};

  const char* myAvgIntermediateType_ = "struct<a:double,b:bigint>";
};

} // namespace myavg

std::vector<std::shared_ptr<gluten::UdafRegisterer>>& globalRegisters() {
  static std::vector<std::shared_ptr<gluten::UdafRegisterer>> registerers;
  return registerers;
}

void setupRegisterers() {
  static bool inited = false;
  if (inited) {
    return;
  }
  auto& registerers = globalRegisters();
  registerers.push_back(std::make_shared<myavg::MyAvgRegisterer>());
  inited = true;
}
} // namespace

DEFINE_GET_NUM_UDAF {
  setupRegisterers();

  int numUdf = 0;
  for (const auto& registerer : globalRegisters()) {
    numUdf += registerer->getNumUdaf();
  }
  return numUdf;
}

DEFINE_GET_UDAF_ENTRIES {
  setupRegisterers();

  int index = 0;
  for (const auto& registerer : globalRegisters()) {
    registerer->populateUdafEntries(index, udafEntries);
  }
}

DEFINE_REGISTER_UDAF {
  setupRegisterers();

  for (const auto& registerer : globalRegisters()) {
    registerer->registerSignatures();
  }
}

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

#include <velox/exec/SimpleAggregateAdapter.h>
#include <velox/expression/VectorFunction.h>
#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>
#include <velox/functions/lib/aggregates/AverageAggregateBase.h>
#include <velox/functions/prestosql/aggregates/ArbitraryAggregate.h>

#include "udf/Udaf.h"
#include "udf/examples/UdfCommon.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

static const char* kBoolean = "boolean";
static const char* kInteger = "int";
static const char* kBigInt = "bigint";
static const char* kFloat = "float";
static const char* kDouble = "double";

namespace myavg {
// Copied from velox/exec/tests/SimpleAverageAggregate.cpp

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
    explicit AccumulatorType(HashStringAllocator* /*allocator*/, AverageAggregate* /* fn*/) {
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
      VELOX_CHECK(other.at<0>().has_value());
      VELOX_CHECK(other.at<1>().has_value());
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
          VELOX_CHECK_LE(argTypes.size(), 1, "{} takes at most one argument", name_);
          auto inputType = argTypes[0];
          if (exec::isRawInput(step)) {
            switch (inputType->kind()) {
              case TypeKind::REAL:
                return std::make_unique<SimpleAggregateAdapter<AverageAggregate<float>>>(step, argTypes, resultType);
              case TypeKind::DOUBLE:
                return std::make_unique<SimpleAggregateAdapter<AverageAggregate<double>>>(step, argTypes, resultType);
              default:
                VELOX_FAIL("Unknown input type for {} aggregation {}", name_, inputType->kindName());
            }
          } else {
            switch (resultType->kind()) {
              case TypeKind::REAL:
                return std::make_unique<SimpleAggregateAdapter<AverageAggregate<float>>>(step, argTypes, resultType);
              case TypeKind::DOUBLE:
              case TypeKind::ROW:
                return std::make_unique<SimpleAggregateAdapter<AverageAggregate<double>>>(step, argTypes, resultType);
              default:
                VELOX_FAIL("Unsupported result type for final aggregation: {}", resultType->kindName());
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

namespace myarbitrary {

// name: myudaf_arbitrary
// signatures:
//    T -> T, intermediate T
// type: RegistryUdafEntry
//
// Nothing restates this signature for Gluten. A UdafEntry would, and since T
// is a type variable that means one entry per supported type with the return
// and intermediate types written out in each. A RegistryUdafEntry names the
// aggregate and Gluten resolves both types from the Velox aggregate registry
// per call site.
const std::string kMyArbitraryName = "myudaf_arbitrary";

// The same aggregate under a hive UDAF class name. An aggregate is only reachable from SQL
// through one, so this is what lets a query call it and exercise the partial / final split
// across a shuffle.
const std::string kMyArbitraryHiveName = "test.org.apache.spark.sql.MyDoubleSum";

void registerMyArbitrary() {
  // Companion functions are required, not optional: a grouped aggregation splits into partial
  // and final stages, and the plan validator looks for <name>_partial and <name>_merge_extract
  // in the Velox registry. Without them the aggregate falls back to the JVM.
  facebook::velox::aggregate::prestosql::registerArbitraryAggregate(
      {kMyArbitraryName, kMyArbitraryHiveName},
      /*withCompanionFunctions=*/true,
      /*overwrite=*/true);
}

} // namespace myarbitrary

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

DEFINE_GET_NUM_REGISTRY_UDAF {
  return 2;
}

DEFINE_GET_REGISTRY_UDAF_ENTRIES {
  registryUdafEntries[0] = {myarbitrary::kMyArbitraryName.c_str()};
  registryUdafEntries[1] = {myarbitrary::kMyArbitraryHiveName.c_str()};
}

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

  myarbitrary::registerMyArbitrary();
}

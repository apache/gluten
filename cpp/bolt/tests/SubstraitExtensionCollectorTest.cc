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

#include "substrait/SubstraitExtensionCollector.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/core/PlanNode.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"

using namespace bytedance::bolt;

namespace gluten {

class SubstraitExtensionCollectorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    Test::SetUp();
    functions::prestosql::registerAllScalarFunctions();
  }

  int getReferenceNumber(const std::string& functionName, std::vector<TypePtr>&& arguments) {
    int referenceNumber1 = extensionCollector_->getReferenceNumber(functionName, arguments);
    // Repeat the call to make sure properly de-duplicated.
    int referenceNumber2 = extensionCollector_->getReferenceNumber(functionName, arguments);
    EXPECT_EQ(referenceNumber1, referenceNumber2);
    return referenceNumber1;
  }

  int getReferenceNumber(
      const std::string& functionName,
      std::vector<TypePtr>&& arguments,
      core::AggregationNode::Step step) {
    int referenceNumber1 = extensionCollector_->getReferenceNumber(functionName, arguments);
    // Repeat the call to make sure properly de-duplicated.
    int referenceNumber2 = extensionCollector_->getReferenceNumber(functionName, arguments);
    EXPECT_EQ(referenceNumber1, referenceNumber2);
    return referenceNumber2;
  }

  /// Given a substrait plan, return the sorted extension functions by the
  /// function anchor.
  ::google::protobuf::RepeatedPtrField<::substrait::extensions::SimpleExtensionDeclaration> getSortedSubstraitExtension(
      const ::substrait::Plan* substraitPlan) {
    auto substraitExtensions = substraitPlan->extensions();
    std::sort(substraitExtensions.begin(), substraitExtensions.end(), [](const auto& a, const auto& b) {
      return a.extension_function().function_anchor() < b.extension_function().function_anchor();
    });

    return substraitExtensions;
  }

  SubstraitExtensionCollectorPtr extensionCollector_ = std::make_shared<SubstraitExtensionCollector>();
};

TEST_F(SubstraitExtensionCollectorTest, getReferenceNumberForScalarFunction) {
  ASSERT_EQ(getReferenceNumber("plus", {INTEGER(), INTEGER()}), 0);
  ASSERT_EQ(getReferenceNumber("divide", {INTEGER(), INTEGER()}), 1);
  ASSERT_EQ(getReferenceNumber("cardinality", {ARRAY(INTEGER())}), 2);
  ASSERT_EQ(getReferenceNumber("array_sum", {ARRAY(INTEGER())}), 3);

  auto functionType = std::make_shared<const FunctionType>(std::vector<TypePtr>{INTEGER(), VARCHAR()}, BIGINT());
  std::vector<TypePtr> types = {MAP(INTEGER(), VARCHAR()), functionType};
  ASSERT_ANY_THROW(getReferenceNumber("transform_keys", std::move(types)));
}

TEST_F(SubstraitExtensionCollectorTest, getReferenceNumberForAggregateFunction) {
  // Sum aggregate function have same argument type for each aggregation step.
  ASSERT_EQ(getReferenceNumber("sum", {INTEGER()}, core::AggregationNode::Step::kSingle), 0);

  // Partial avg aggregate function should use primitive integral type.
  ASSERT_EQ(getReferenceNumber("avg", {INTEGER()}, core::AggregationNode::Step::kPartial), 1);

  // Final avg aggregate function should use struct type, like
  // 'ROW<DOUBLE,BIGINT>'
  ASSERT_EQ(getReferenceNumber("avg", {ROW({DOUBLE(), BIGINT()})}, core::AggregationNode::Step::kFinal), 2);

  // Count aggregate function have same argument type for each aggregation step.
  ASSERT_EQ(getReferenceNumber("count", {INTEGER()}, core::AggregationNode::Step::kFinal), 3);
}

TEST_F(SubstraitExtensionCollectorTest, addExtensionsToPlan) {
  getReferenceNumber("plus", {INTEGER(), INTEGER()});
  getReferenceNumber("divide", {INTEGER(), INTEGER()});
  getReferenceNumber("cardinality", {ARRAY(INTEGER())});
  getReferenceNumber("array_sum", {ARRAY(INTEGER())});
  getReferenceNumber("sum", {INTEGER()});
  getReferenceNumber("avg", {INTEGER()});
  getReferenceNumber("avg", {ROW({DOUBLE(), BIGINT()})});
  getReferenceNumber("count", {INTEGER()});

  google::protobuf::Arena arena;
  auto* substraitPlan = google::protobuf::Arena::CreateMessage<::substrait::Plan>(&arena);

  extensionCollector_->addExtensionsToPlan(substraitPlan);

  const auto& substraitExtensions = getSortedSubstraitExtension(substraitPlan);
  auto getFunctionName = [&](auto id) { return substraitExtensions[id].extension_function().name(); };

  ASSERT_EQ(substraitPlan->extensions().size(), 8);
  ASSERT_EQ(getFunctionName(0), "plus:i32_i32");
  ASSERT_EQ(getFunctionName(1), "divide:i32_i32");
  ASSERT_EQ(getFunctionName(2), "cardinality:list");
  ASSERT_EQ(getFunctionName(3), "array_sum:list");
  ASSERT_EQ(getFunctionName(4), "sum:i32");
  ASSERT_EQ(getFunctionName(5), "avg:i32");
  ASSERT_EQ(getFunctionName(6), "avg:struct<fp64,i64>");
  ASSERT_EQ(getFunctionName(7), "count:i32");
}

} // namespace gluten

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
#include "compute/VeloxExpressionEvaluator.h"

#include "substrait/extended_expression.pb.h"
#include "velox/expression/RegisterSpecialForm.h"
#include "velox/functions/sparksql/registration/Register.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace gluten {
using namespace facebook::velox;

class VeloxExpressionEvaluatorTest : public testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    functions::sparksql::registerFunctions("");
    exec::registerFunctionCallToSpecialForms();
  }

  static ::substrait::ExtendedExpression trim(const std::string& name = "rtrim", int32_t columns = 1) {
    ::substrait::ExtendedExpression expression;
    auto* schema = expression.mutable_base_schema();
    for (int32_t i = 0; i < columns; ++i) {
      schema->add_names("input" + std::to_string(i));
      schema->mutable_struct_()->add_types()->mutable_string();
    }
    auto* mapping = expression.add_extensions()->mutable_extension_function();
    mapping->set_function_anchor(1);
    mapping->set_name(name + (columns == 1 ? ":str" : ":str_str"));
    auto* reference = expression.add_referred_expr();
    reference->add_output_names("result");
    auto* function = reference->mutable_expression()->mutable_scalar_function();
    function->set_function_reference(1);
    function->mutable_output_type()->mutable_string();
    // ExpressionConverter places custom trim characters before the source string.
    for (int32_t i = columns - 1; i >= 0; --i) {
      auto* selection = function->add_arguments()->mutable_value()->mutable_selection();
      selection->mutable_root_reference();
      selection->mutable_direct_reference()->mutable_struct_field()->set_field(i);
    }
    return expression;
  }

  std::unique_ptr<VeloxExpressionEvaluator> compile(const ::substrait::ExtendedExpression& expression) {
    const auto bytes = expression.SerializeAsString();
    return std::make_unique<VeloxExpressionEvaluator>(
        pool_, core::QueryCtx::create(), reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
  }

  std::shared_ptr<VeloxColumnarBatch> batch(const VectorPtr& column) {
    return std::make_shared<VeloxColumnarBatch>(makeRowVector({"different_input_name"}, {column}));
  }
};

TEST_F(VeloxExpressionEvaluatorTest, reusesColumnarInputsAcrossBatches) {
  auto evaluator = compile(trim());
  auto values = makeNullableFlatVector<std::string>(
      {"0123456789", "data  ", "", "   ", "\t", "\xC2\xA0", std::nullopt, std::string(64, 'x')});
  auto input = batch(values);
  auto output = evaluator->evaluate(input);
  EXPECT_EQ(output->numColumns(), 1);
  EXPECT_EQ(output->numRows(), values->size());
  EXPECT_EQ(output->getRowVector()->type()->asRow().nameOf(0), "result");
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>(
          {"0123456789", "data", "", "", "\t", "\xC2\xA0", std::nullopt, std::string(64, 'x')}),
      output->getRowVector()->childAt(0));
  auto smaller = evaluator->evaluate(batch(makeFlatVector<std::string>({"next "})));
  EXPECT_EQ(smaller->numRows(), 1);
  test::assertEqualVectors(makeFlatVector<std::string>({"next"}), smaller->getRowVector()->childAt(0));
  auto empty = evaluator->evaluate(batch(makeFlatVector<std::string>(std::vector<std::string>{})));
  EXPECT_EQ(empty->numRows(), 0);
  EXPECT_EQ(empty->numColumns(), 1);
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>(
          {"0123456789", "data  ", "", "   ", "\t", "\xC2\xA0", std::nullopt, std::string(64, 'x')}),
      input->getRowVector()->childAt(0));
  EXPECT_EQ(output->numRows(), 8);
}

TEST_F(VeloxExpressionEvaluatorTest, leftTrimPreservesTrailingBytes) {
  auto evaluator = compile(trim("ltrim"));
  auto input = batch(makeNullableFlatVector<std::string>(
      {"  value  ", "", "   ", "\tvalue", "\xE4\xB8\xAD ", std::nullopt, "  " + std::string(13, 'x')}));
  auto output = evaluator->evaluate(input);
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>(
          {"value  ", "", "", "\tvalue", "\xE4\xB8\xAD ", std::nullopt, std::string(13, 'x')}),
      output->getRowVector()->childAt(0));
}

TEST_F(VeloxExpressionEvaluatorTest, customTrimUsesBothInputColumns) {
  auto input = std::make_shared<VeloxColumnarBatch>(makeRowVector(
      {"value", "trim"},
      {makeNullableFlatVector<std::string>({"xyvalueyx", "  value  ", "keep", std::nullopt, "x"}),
       makeNullableFlatVector<std::string>({"xy", " ", "", "xy", std::nullopt})}));
  auto right = compile(trim("rtrim", 2))->evaluate(input);
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"xyvalue", "  value", "keep", std::nullopt, std::nullopt}),
      right->getRowVector()->childAt(0));
  auto left = compile(trim("ltrim", 2))->evaluate(input);
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"valueyx", "value  ", "keep", std::nullopt, std::nullopt}),
      left->getRowVector()->childAt(0));
}

TEST_F(VeloxExpressionEvaluatorTest, outputSurvivesEvaluatorAndInputRelease) {
  const std::string value(64, 'x');
  std::shared_ptr<VeloxColumnarBatch> output;
  {
    auto evaluator = compile(trim());
    auto input = batch(makeFlatVector<std::string>({value, value}));
    output = evaluator->evaluate(input);
    auto next = evaluator->evaluate(batch(makeFlatVector<std::string>({"changed ", "other "})));
    test::assertEqualVectors(makeFlatVector<std::string>({"changed", "other"}), next->getRowVector()->childAt(0));
  }
  test::assertEqualVectors(makeFlatVector<std::string>({value, value}), output->getRowVector()->childAt(0));
}

TEST_F(VeloxExpressionEvaluatorTest, nestedIdentitySurvivesEvaluatorAndInputRelease) {
  auto expected =
      makeRowVector({"payload"}, {makeNullableFlatVector<std::string>({std::string(64, 'x'), std::nullopt, ""})});
  std::shared_ptr<VeloxColumnarBatch> output;
  {
    auto expression = trim();
    expression.clear_extensions();
    auto* nested = expression.mutable_base_schema()->mutable_struct_()->mutable_types(0);
    nested->clear_string();
    nested->mutable_struct_()->add_types()->mutable_string();
    auto* value = expression.mutable_referred_expr(0)->mutable_expression();
    value->clear_scalar_function();
    value->mutable_selection()->mutable_root_reference();
    value->mutable_selection()->mutable_direct_reference()->mutable_struct_field()->set_field(0);
    auto evaluator = compile(expression);
    auto input = batch(
        makeRowVector({"payload"}, {makeNullableFlatVector<std::string>({std::string(64, 'x'), std::nullopt, ""})}));
    auto child = input->getRowVector()->childAt(0);
    const auto originalType = child->type();
    output = evaluator->evaluate(input);
    EXPECT_EQ(child->type(), originalType);
    auto next = evaluator->evaluate(batch(makeRowVector({"other"}, {makeFlatVector<std::string>({"next"})})));
    EXPECT_EQ(next->numRows(), 1);
  }
  EXPECT_EQ(output->numRows(), 3);
  test::assertEqualVectors(expected, output->getRowVector()->childAt(0));
}

TEST_F(VeloxExpressionEvaluatorTest, constantDictionaryAndNullBatches) {
  auto evaluator = compile(trim());
  auto base = makeNullableFlatVector<std::string>({" x ", std::nullopt, "keep"});
  auto dictionary = wrapInDictionary(makeIndices({2, 0, 1, 0, 2}), base);
  auto output = evaluator->evaluate(batch(dictionary));
  test::assertEqualVectors(
      makeNullableFlatVector<std::string>({"keep", " x", std::nullopt, " x", "keep"}),
      output->getRowVector()->childAt(0));
  for (vector_size_t index = 0; index < base->size(); ++index) {
    auto result = evaluator->evaluate(batch(BaseVector::wrapInConstant(9, index, base)));
    auto expected = makeNullableFlatVector<std::string>(
        index == 1 ? std::vector<std::optional<std::string>>(9, std::nullopt)
                   : std::vector<std::optional<std::string>>(9, index == 0 ? " x" : "keep"));
    test::assertEqualVectors(expected, result->getRowVector()->childAt(0));
  }
}

TEST_F(VeloxExpressionEvaluatorTest, constantExpressionsPreserveEarlierOutputs) {
  auto expression = trim();
  expression.clear_extensions();
  expression.mutable_referred_expr(0)->mutable_expression()->mutable_literal()->set_string("constant");
  auto evaluator = compile(expression);
  auto first = evaluator->evaluate(batch(makeFlatVector<std::string>({"a", "b", "c"})));
  auto second = evaluator->evaluate(batch(makeFlatVector<std::string>({"d"})));
  evaluator.reset();
  test::assertEqualVectors(
      makeFlatVector<std::string>({"constant", "constant", "constant"}), first->getRowVector()->childAt(0));
  test::assertEqualVectors(makeFlatVector<std::string>({"constant"}), second->getRowVector()->childAt(0));
}

TEST_F(VeloxExpressionEvaluatorTest, validatesSchemaAndProtocol) {
  auto valid = trim();
  auto evaluator = compile(valid);
  EXPECT_ANY_THROW(evaluator->evaluate(nullptr));
  EXPECT_ANY_THROW(evaluator->evaluate(createZeroColumnBatch(1)));
  EXPECT_ANY_THROW(evaluator->evaluate(batch(makeFlatVector<int32_t>({1, 2}))));
  EXPECT_ANY_THROW(evaluator->evaluate(std::make_shared<VeloxColumnarBatch>(
      makeRowVector({makeFlatVector<std::string>({"x"}), makeFlatVector<std::string>({"y"})}))));

  auto invalid = valid;
  invalid.clear_base_schema();
  EXPECT_ANY_THROW(compile(invalid));
  invalid = valid;
  invalid.clear_referred_expr();
  EXPECT_ANY_THROW(compile(invalid));
  invalid = valid;
  invalid.add_referred_expr()->CopyFrom(valid.referred_expr(0));
  EXPECT_ANY_THROW(compile(invalid));
  invalid = valid;
  invalid.mutable_referred_expr(0)->clear_output_names();
  EXPECT_ANY_THROW(compile(invalid));
  invalid = valid;
  invalid.mutable_extensions(0)->mutable_extension_function()->set_name("missing_function:str");
  EXPECT_ANY_THROW(compile(invalid));
  EXPECT_ANY_THROW(VeloxExpressionEvaluator(pool_, core::QueryCtx::create(), nullptr, 0));
}
} // namespace gluten

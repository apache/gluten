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

#include <string>
#include <vector>

#include "operators/functions/RegistrationAllFunctions.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/Expressions.h"
#include "velox/functions/sparksql/SparkQueryConfig.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace facebook::velox::functions::sparksql::test;
using namespace facebook::velox;

namespace {
constexpr const char* kSparkAnsiCast = "spark_ansi_cast";
constexpr const char* kSparkLegacyCast = "spark_legacy_cast";

std::string sparkAnsiEnabledConfigKey() {
  return functions::sparksql::SparkQueryConfig::qualify(functions::sparksql::SparkQueryConfig::kAnsiEnabled);
}
} // namespace

class SparkFunctionTest : public SparkFunctionBaseTest {
 public:
  SparkFunctionTest() {
    gluten::registerAllFunctions();
  }

 protected:
  std::optional<std::string> conv(
      const std::optional<std::string>& num,
      const std::optional<int32_t>& fromBase,
      const std::optional<int32_t>& toBase) {
    return evaluateOnce<std::string>("conv(c0, c1, c2)", num, fromBase, toBase);
  }

  template <typename T>
  void runRoundTest(const std::vector<std::tuple<T, T>>& data) {
    auto result = evaluate<SimpleVector<T>>("round(c0)", makeRowVector({makeFlatVector<T, 0>(data)}));
    for (int32_t i = 0; i < data.size(); ++i) {
      ASSERT_EQ(result->valueAt(i), std::get<1>(data[i]));
    }
  }

  template <typename T>
  void runRoundWithDecimalTest(const std::vector<std::tuple<T, int32_t, T>>& data) {
    auto result = evaluate<SimpleVector<T>>(
        "round(c0, c1)", makeRowVector({makeFlatVector<T, 0>(data), makeFlatVector<int32_t, 1>(data)}));
    for (int32_t i = 0; i < data.size(); ++i) {
      ASSERT_EQ(result->valueAt(i), std::get<2>(data[i]));
    }
  }

  template <typename T>
  std::vector<std::tuple<T, T>> testRoundFloatData() {
    return {
        {1.0, 1.0},
        {1.9, 2.0},
        {1.3, 1.0},
        {0.0, 0.0},
        {0.9999, 1.0},
        {-0.9999, -1.0},
        {1.0 / 9999999, 0},
        {123123123.0 / 9999999, 12.0}};
  }

  template <typename T>
  std::vector<std::tuple<T, T>> testRoundIntegralData() {
    return {{1, 1}, {0, 0}, {-1, -1}};
  }

  template <typename T>
  std::vector<std::tuple<T, int32_t, T>> testRoundWithDecFloatAndDoubleData() {
    return {{1.122112, 0, 1},       {1.129, 1, 1.1},        {1.129, 2, 1.13},         {1.0 / 3, 0, 0.0},
            {1.0 / 3, 1, 0.3},      {1.0 / 3, 2, 0.33},     {1.0 / 3, 6, 0.333333},   {-1.122112, 0, -1},
            {-1.129, 1, -1.1},      {-1.129, 2, -1.13},     {-1.129, 2, -1.13},       {-1.0 / 3, 0, 0.0},
            {-1.0 / 3, 1, -0.3},    {-1.0 / 3, 2, -0.33},   {-1.0 / 3, 6, -0.333333}, {1.0, -1, 0.0},
            {0.0, -2, 0.0},         {-1.0, -3, 0.0},        {11111.0, -1, 11110.0},   {11111.0, -2, 11100.0},
            {11111.0, -3, 11000.0}, {11111.0, -4, 10000.0}, {0.575, 2, 0.58},         {0.574, 2, 0.57},
            {-0.575, 2, -0.58},     {-0.574, 2, -0.57}};
  }

  template <typename T>
  std::vector<std::tuple<T, int32_t, T>> testRoundWithDecIntegralData() {
    return {
        {1, 0, 1},
        {0, 0, 0},
        {-1, 0, -1},
        {1, 1, 1},
        {0, 1, 0},
        {-1, 1, -1},
        {1, 10, 1},
        {0, 10, 0},
        {-1, 10, -1},
        {1, -1, 0},
        {0, -2, 0},
        {-1, -3, 0}};
  }
};

TEST_F(SparkFunctionTest, round) {
  runRoundTest<float>(testRoundFloatData<float>());
  runRoundTest<double>(testRoundFloatData<double>());
  runRoundTest<int64_t>(testRoundIntegralData<int64_t>());
  runRoundTest<int32_t>(testRoundIntegralData<int32_t>());
  runRoundTest<int16_t>(testRoundIntegralData<int16_t>());
  runRoundTest<int8_t>(testRoundIntegralData<int8_t>());
}

TEST_F(SparkFunctionTest, roundWithDecimal) {
  runRoundWithDecimalTest<float>(testRoundWithDecFloatAndDoubleData<float>());
  runRoundWithDecimalTest<double>(testRoundWithDecFloatAndDoubleData<double>());
  runRoundWithDecimalTest<int64_t>(testRoundWithDecIntegralData<int64_t>());
  runRoundWithDecimalTest<int32_t>(testRoundWithDecIntegralData<int32_t>());
  runRoundWithDecimalTest<int16_t>(testRoundWithDecIntegralData<int16_t>());
  runRoundWithDecimalTest<int8_t>(testRoundWithDecIntegralData<int8_t>());
}

TEST_F(SparkFunctionTest, expressionLevelAnsiCastIgnoresSessionAnsiOff) {
  queryCtx_->testingOverrideConfigUnsafe({{sparkAnsiEnabledConfigKey(), "false"}});
  auto input = makeRowVector({makeFlatVector<std::string>({"2147483648"})});
  core::TypedExprPtr field = std::make_shared<const core::FieldAccessTypedExpr>(VARCHAR(), "c0");
  auto ansiCast =
      std::make_shared<const core::CallTypedExpr>(INTEGER(), std::vector<core::TypedExprPtr>{field}, kSparkAnsiCast);

  VELOX_ASSERT_THROW(evaluate(ansiCast, input), "Cannot cast");
}

TEST_F(SparkFunctionTest, expressionLevelLegacyCastIgnoresSessionAnsiOn) {
  queryCtx_->testingOverrideConfigUnsafe({{sparkAnsiEnabledConfigKey(), "true"}});
  auto input = makeRowVector({makeFlatVector<int32_t>({1234567})});
  core::TypedExprPtr field = std::make_shared<const core::FieldAccessTypedExpr>(INTEGER(), "c0");
  auto legacyCast =
      std::make_shared<const core::CallTypedExpr>(TINYINT(), std::vector<core::TypedExprPtr>{field}, kSparkLegacyCast);

  facebook::velox::test::assertEqualVectors(makeFlatVector<int8_t>({-121}), evaluate(legacyCast, input));
}

TEST_F(SparkFunctionTest, elt) {
  // The index picks a different input per row. A NULL in an input that is not
  // picked does not affect the result, while a NULL in the picked one does.
  auto index = makeFlatVector<int32_t>({1, 2, 3, 3});
  auto first = makeNullableFlatVector<StringView>({"a0", std::nullopt, "a2", "a3"});
  auto second = makeNullableFlatVector<StringView>({std::nullopt, "b1", "b2", "b3"});
  auto third = makeNullableFlatVector<StringView>({"c0", "c1", "c2", std::nullopt});

  facebook::velox::test::assertEqualVectors(
      makeNullableFlatVector<StringView>({"a0", "b1", "c2", std::nullopt}),
      evaluate("elt(c0, c1, c2, c3)", makeRowVector({index, first, second, third})));
}

TEST_F(SparkFunctionTest, eltNullIndex) {
  // A NULL index returns NULL, with ANSI mode on as well.
  queryCtx_->testingOverrideConfigUnsafe({{sparkAnsiEnabledConfigKey(), "true"}});
  auto index = makeNullableFlatVector<int32_t>({std::nullopt, 2});
  auto first = makeFlatVector<StringView>({"a0", "a1"});
  auto second = makeFlatVector<StringView>({"b0", "b1"});

  facebook::velox::test::assertEqualVectors(
      makeNullableFlatVector<StringView>({std::nullopt, "b1"}),
      evaluate("elt(c0, c1, c2)", makeRowVector({index, first, second})));
}

TEST_F(SparkFunctionTest, eltIndexOutOfRangeAnsiOff) {
  queryCtx_->testingOverrideConfigUnsafe({{sparkAnsiEnabledConfigKey(), "false"}});
  auto index = makeFlatVector<int32_t>({0, -1, 3, 2});
  auto first = makeFlatVector<StringView>({"a0", "a1", "a2", "a3"});
  auto second = makeFlatVector<StringView>({"b0", "b1", "b2", "b3"});

  facebook::velox::test::assertEqualVectors(
      makeNullableFlatVector<StringView>({std::nullopt, std::nullopt, std::nullopt, "b3"}),
      evaluate("elt(c0, c1, c2)", makeRowVector({index, first, second})));
}

TEST_F(SparkFunctionTest, eltIndexOutOfRangeAnsiOn) {
  queryCtx_->testingOverrideConfigUnsafe({{sparkAnsiEnabledConfigKey(), "true"}});
  auto first = makeFlatVector<StringView>({"a0", "a1"});
  auto second = makeFlatVector<StringView>({"b0", "b1"});

  auto evaluateWithIndex = [&](const std::vector<int32_t>& indexes) {
    return evaluate("elt(c0, c1, c2)", makeRowVector({makeFlatVector<int32_t>(indexes), first, second}));
  };

  VELOX_ASSERT_THROW(evaluateWithIndex({1, 0}), "The index is out of bounds for elt. index: 0, number of inputs: 2");
  VELOX_ASSERT_THROW(evaluateWithIndex({-1, 1}), "The index is out of bounds for elt. index: -1, number of inputs: 2");
  VELOX_ASSERT_THROW(evaluateWithIndex({1, 3}), "The index is out of bounds for elt. index: 3, number of inputs: 2");

  // In-range indexes are unaffected by ANSI mode.
  facebook::velox::test::assertEqualVectors(makeFlatVector<StringView>({"a0", "b1"}), evaluateWithIndex({1, 2}));
}

TEST_F(SparkFunctionTest, eltVarbinary) {
  auto index = makeFlatVector<int32_t>({2, 1});
  auto first = makeNullableFlatVector<StringView>({"a0", "a1"}, VARBINARY());
  auto second = makeNullableFlatVector<StringView>({"b0", std::nullopt}, VARBINARY());

  auto result = evaluate("elt(c0, c1, c2)", makeRowVector({index, first, second}));
  ASSERT_EQ(result->type()->kind(), TypeKind::VARBINARY);
  facebook::velox::test::assertEqualVectors(makeNullableFlatVector<StringView>({"b0", "a1"}, VARBINARY()), result);
}

TEST_F(SparkFunctionTest, eltConstantIndexOverDictionaryInput) {
  // A constant index selects the same input for all rows, and the selected
  // input may carry an encoding.
  auto index = makeConstant<int32_t>(2, 3);
  auto first = makeFlatVector<StringView>({"a0", "a1", "a2"});
  auto second = makeFlatVector<StringView>({"b0", "b1", "b2"});
  auto dictionary = BaseVector::wrapInDictionary(nullptr, makeIndices({2, 0, 1}), 3, second);

  facebook::velox::test::assertEqualVectors(
      makeFlatVector<StringView>({"b2", "b0", "b1"}),
      evaluate("elt(c0, c1, c2)", makeRowVector({index, first, dictionary})));
}

TEST_F(SparkFunctionTest, convOverflowAnsiOff) {
  queryCtx_->testingOverrideConfigUnsafe({{sparkAnsiEnabledConfigKey(), "false"}});

  // Digits that do not fit in an unsigned 64-bit integer saturate, which is
  // what Spark does with ANSI mode off.
  EXPECT_EQ(conv("9223372036854775807", 36, 16), "FFFFFFFFFFFFFFFF");
  EXPECT_EQ(conv("10000000000000000", 16, 10), "18446744073709551615");
  EXPECT_EQ(conv("-10000000000000000", 16, -10), "-1");
}

TEST_F(SparkFunctionTest, convOverflowAnsiOn) {
  queryCtx_->testingOverrideConfigUnsafe({{sparkAnsiEnabledConfigKey(), "true"}});

  VELOX_ASSERT_THROW(conv("9223372036854775807", 36, 16), "Overflow in function conv()");
  VELOX_ASSERT_THROW(conv("10000000000000000", 16, 10), "Overflow in function conv()");
  VELOX_ASSERT_THROW(conv("-10000000000000000", 16, -10), "Overflow in function conv()");
}

TEST_F(SparkFunctionTest, convNoOverflowAnsiOn) {
  queryCtx_->testingOverrideConfigUnsafe({{sparkAnsiEnabledConfigKey(), "true"}});

  EXPECT_EQ(conv("4", 10, 2), "100");
  EXPECT_EQ(conv("big", 36, 16), "3A48");
  // 2^64 - 1 is the largest input that does not overflow.
  EXPECT_EQ(conv("18446744073709551615", 10, 10), "18446744073709551615");
  EXPECT_EQ(conv("FFFFFFFFFFFFFFFF", 16, -10), "-1");
  // The sign is applied after the digits are accumulated, so wrapping around
  // through a negative input is not an overflow.
  EXPECT_EQ(conv("-1", 10, 16), "FFFFFFFFFFFFFFFF");
  EXPECT_EQ(conv("-15", 10, 16), "FFFFFFFFFFFFFFF1");
  // The digits stop at the first character that is invalid for the base, so a
  // long tail of invalid characters is not an overflow either.
  EXPECT_EQ(conv("11abcabcabcabcabcabcabcabc", 10, 16), "B");
}

TEST_F(SparkFunctionTest, convInvalidInputAnsiOn) {
  queryCtx_->testingOverrideConfigUnsafe({{sparkAnsiEnabledConfigKey(), "true"}});

  // Invalid input gives NULL, with ANSI mode on as well.
  EXPECT_EQ(conv("15", 1, 10), std::nullopt);
  EXPECT_EQ(conv("15", 37, 10), std::nullopt);
  EXPECT_EQ(conv("15", 10, 1), std::nullopt);
  EXPECT_EQ(conv("15", 10, -37), std::nullopt);
  EXPECT_EQ(conv("", 10, 16), std::nullopt);
  EXPECT_EQ(conv("   ", 10, 16), std::nullopt);
  EXPECT_EQ(conv(std::nullopt, 10, 16), std::nullopt);
}

TEST_F(SparkFunctionTest, elementAtArrayOutOfBoundAnsiOff) {
  queryCtx_->testingOverrideConfigUnsafe({{sparkAnsiEnabledConfigKey(), "false"}});
  auto array = makeArrayVector<int32_t>({{1, 2, 3}, {1, 2, 3}, {1, 2, 3}, {1, 2, 3}, {1, 2, 3}});
  auto index = makeFlatVector<int32_t>({1, 3, -1, 4, -4});

  // An index past either end of the array gives NULL with ANSI mode off.
  facebook::velox::test::assertEqualVectors(
      makeNullableFlatVector<int32_t>({1, 3, 3, std::nullopt, std::nullopt}),
      evaluate("element_at(c0, c1)", makeRowVector({array, index})));
}

TEST_F(SparkFunctionTest, elementAtArrayOutOfBoundAnsiOn) {
  queryCtx_->testingOverrideConfigUnsafe({{sparkAnsiEnabledConfigKey(), "true"}});
  auto array = makeArrayVector<int32_t>({{1, 2, 3}, {1, 2, 3}});

  // In-bound indices, including the negative ones counting from the end, are
  // unaffected by ANSI mode.
  facebook::velox::test::assertEqualVectors(
      makeFlatVector<int32_t>({1, 3}),
      evaluate("element_at(c0, c1)", makeRowVector({array, makeFlatVector<int32_t>({1, -1})})));

  VELOX_ASSERT_THROW(
      evaluate("element_at(c0, c1)", makeRowVector({array, makeFlatVector<int32_t>({1, 4})})),
      "Array subscript index out of bounds");
  VELOX_ASSERT_THROW(
      evaluate("element_at(c0, c1)", makeRowVector({array, makeFlatVector<int32_t>({-4, 1})})),
      "Array subscript index out of bounds");
}

TEST_F(SparkFunctionTest, elementAtArrayZeroIndex) {
  auto data = makeRowVector({makeArrayVector<int32_t>({{1, 2, 3}}), makeFlatVector<int32_t>({0})});

  // An index of 0 is an error whatever the ANSI mode is.
  queryCtx_->testingOverrideConfigUnsafe({{sparkAnsiEnabledConfigKey(), "false"}});
  VELOX_ASSERT_THROW(evaluate("element_at(c0, c1)", data), "SQL array indices start at 1");

  queryCtx_->testingOverrideConfigUnsafe({{sparkAnsiEnabledConfigKey(), "true"}});
  VELOX_ASSERT_THROW(evaluate("element_at(c0, c1)", data), "SQL array indices start at 1");
}

TEST_F(SparkFunctionTest, elementAtMapMissingKeyAnsiOn) {
  queryCtx_->testingOverrideConfigUnsafe({{sparkAnsiEnabledConfigKey(), "true"}});
  auto map = makeMapVector<int32_t, int32_t>({{{1, 10}, {2, 20}}, {{1, 10}, {2, 20}}});
  auto key = makeFlatVector<int32_t>({2, 3});

  // A key the map does not contain gives NULL, with ANSI mode on as well.
  facebook::velox::test::assertEqualVectors(
      makeNullableFlatVector<int32_t>({20, std::nullopt}), evaluate("element_at(c0, c1)", makeRowVector({map, key})));
}

TEST_F(SparkFunctionTest, sizeOfNull) {
  // Gluten passes Spark's Size.legacySizeOfNull as the second argument, and
  // Spark derives it from 'spark.sql.legacy.sizeOfNull' AND NOT ANSI mode, so
  // both behaviors have to work regardless of the session's ANSI mode.
  queryCtx_->testingOverrideConfigUnsafe({{sparkAnsiEnabledConfigKey(), "true"}});
  auto data = makeRowVector({makeArrayVectorFromJson<int32_t>({"[1, 2, 3]", "null"})});

  facebook::velox::test::assertEqualVectors(
      makeNullableFlatVector<int32_t>({3, std::nullopt}), evaluate("size(c0, false)", data));
  facebook::velox::test::assertEqualVectors(makeFlatVector<int32_t>({3, -1}), evaluate("size(c0, true)", data));
}

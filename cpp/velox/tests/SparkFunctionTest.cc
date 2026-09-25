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

#include <limits>
#include <string>
#include <tuple>
#include <vector>

#include "jni/JniCastException.h"
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

TEST_F(SparkFunctionTest, nativeCastExceptionAttribution) {
  queryCtx_->testingOverrideConfigUnsafe({{sparkAnsiEnabledConfigKey(), "true"}});
  const auto maxInt = std::numeric_limits<int32_t>::max();
  const auto maxLong = std::numeric_limits<int64_t>::max();
  const std::vector<std::tuple<VectorPtr, TypePtr, std::string>> cases = {
      {makeFlatVector<int32_t>({maxInt}),
       TINYINT(),
       "Cannot cast INTEGER '2147483647' to TINYINT. Overflow during arithmetic conversion: "},
      {makeFlatVector<int32_t>({maxInt}),
       SMALLINT(),
       "Cannot cast INTEGER '2147483647' to SMALLINT. Overflow during arithmetic conversion: "},
      {makeFlatVector<int64_t>({maxLong}),
       INTEGER(),
       "Cannot cast BIGINT '9223372036854775807' to INTEGER. Overflow during arithmetic conversion: "},
      {makeFlatVector<double>({1.2345678901234567e19}),
       BIGINT(),
       "Cannot cast DOUBLE '12345678901234567000' to BIGINT. "
       "Cannot cast floating-point value to an integral value due to overflow."},
      {makeFlatVector<int64_t>({maxLong}), DECIMAL(7, 2), "Cannot cast BIGINT '9223372036854775807' to DECIMAL(7, 2)"},
      {makeFlatVector<std::string>({"9223372036854775807"}),
       INTEGER(),
       "Cannot cast VARCHAR '9223372036854775807' to INTEGER. Overflow during conversion: \"\""},
      {makeFlatVector<int64_t>({123}, DECIMAL(3, 1)), DECIMAL(3, 2), "Cannot cast DECIMAL '12.3' to DECIMAL(3, 2)"}};
  for (const auto& [values, target, reason] : cases) {
    SCOPED_TRACE(reason);
    core::TypedExprPtr field = std::make_shared<const core::FieldAccessTypedExpr>(values->type(), "c0");
    auto cast =
        std::make_shared<const core::CallTypedExpr>(target, std::vector<core::TypedExprPtr>{field}, kSparkAnsiCast);
    try {
      evaluate(cast, makeRowVector({values}));
      FAIL() << "Expected a native cast failure";
    } catch (const VeloxException& error) {
      EXPECT_TRUE(gluten::isNativeCastException(error)) << error.what();
      EXPECT_EQ(error.message(), reason);
    }
  }

  auto input = makeRowVector({makeRowVector({"value"}, {makeFlatVector<int64_t>({maxLong})})});
  try {
    evaluate("cast(c0.value as integer)", input);
    FAIL() << "Expected a scalar member cast failure";
  } catch (const VeloxException& error) {
    EXPECT_TRUE(gluten::isNativeCastException(error)) << error.what();
    EXPECT_EQ(
        error.message(),
        "Cannot cast BIGINT '9223372036854775807' to INTEGER. Overflow during arithmetic conversion: ");
  }
}

TEST_F(SparkFunctionTest, nativeCastAttributionRejectsUnrelatedErrors) {
  const std::string reason = "Cannot cast INTEGER '2147483647' to TINYINT. Overflow during arithmetic conversion: ";
  auto error = [&](const std::string& source, const std::string& code) {
    return VeloxException(__FILE__, __LINE__, __FUNCTION__, "", reason, source, code, false);
  };
  EXPECT_FALSE(gluten::isNativeCastException(error("USER", "INVALID_ARGUMENT")));

  ExpressionExceptionProperties properties;
  properties.functionName = "cast";
  ExceptionContext context;
  context.arg = &properties;
  context.propertiesFunc = [](VeloxException::Type, void* arg) -> std::shared_ptr<const ExceptionContextProperties> {
    return std::make_shared<ExpressionExceptionProperties>(*static_cast<ExpressionExceptionProperties*>(arg));
  };
  ExceptionContextSetter scopedContext(context);
  EXPECT_TRUE(gluten::isNativeCastException(error("USER", "INVALID_ARGUMENT")));
  EXPECT_FALSE(gluten::isNativeCastException(error("SYSTEM", "INVALID_ARGUMENT")));
  EXPECT_FALSE(gluten::isNativeCastException(error("USER", "UNSUPPORTED")));
  properties.functionName = "plus";
  EXPECT_FALSE(gluten::isNativeCastException(error("USER", "INVALID_ARGUMENT")));
  properties.functionName = "try_cast";
  EXPECT_FALSE(gluten::isNativeCastException(error("USER", "INVALID_ARGUMENT")));
  properties.functionName = "cast";
  properties.owner = "user-defined-function";
  EXPECT_FALSE(gluten::isNativeCastException(error("USER", "INVALID_ARGUMENT")));
}

TEST_F(SparkFunctionTest, nativeCastErrorsDoNotChangeLegacyOrTryCast) {
  auto input = makeRowVector({makeFlatVector<std::string>({"9223372036854775807", "invalid"})});
  auto expected = makeNullableFlatVector<int32_t>({std::nullopt, std::nullopt});
  queryCtx_->testingOverrideConfigUnsafe({{sparkAnsiEnabledConfigKey(), "false"}});
  facebook::velox::test::assertEqualVectors(expected, evaluate("cast(c0 as integer)", input));
  facebook::velox::test::assertEqualVectors(expected, evaluate("try_cast(c0 as integer)", input));
  queryCtx_->testingOverrideConfigUnsafe({{sparkAnsiEnabledConfigKey(), "true"}});
  facebook::velox::test::assertEqualVectors(expected, evaluate("try_cast(c0 as integer)", input));
}

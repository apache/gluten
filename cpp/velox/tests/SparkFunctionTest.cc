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

// Spark GetMapValue is emitted as get_map_value and registered through the
// function overlay as a map-only, pushdown-capable subscript, so that
// Expr::extractSubfields() yields m["k"] instead of the bare column path. This
// keeps map-key pruning declared via HiveColumnHandle::requiredSubfields
// effective when the remaining filter references the map.
TEST_F(SparkFunctionTest, getMapValueSubfieldExtraction) {
  // Spark function names throughout: the test base registers only the Spark
  // functions, so the parser's `>` (gt) does not resolve.
  auto mapRow = ROW({"m"}, {MAP(VARCHAR(), BIGINT())});
  auto exprSet = compileExpression("greaterthan(get_map_value(m, 'k'), 0)", mapRow);
  auto subfields = exprSet->expr(0)->extractSubfields();
  ASSERT_EQ(subfields.size(), 1);
  EXPECT_EQ(subfields[0].toString(), "m[\"k\"]");

  // Velox's element_at stays non-pushdown: it also serves arrays, and a
  // Subfield path cannot express a negative array index.
  exprSet = compileExpression("greaterthan(element_at(m, 'k'), 0)", mapRow);
  subfields = exprSet->expr(0)->extractSubfields();
  ASSERT_EQ(subfields.size(), 1);
  EXPECT_EQ(subfields[0].toString(), "m");
}

TEST_F(SparkFunctionTest, getMapValueSemantics) {
  auto maps = makeMapVector<std::string, int64_t>({{{"k", 1}}, {{"x", 2}}, {}});
  auto result = evaluate("get_map_value(c0, 'k')", makeRowVector({maps}));
  facebook::velox::test::assertEqualVectors(makeNullableFlatVector<int64_t>({1, std::nullopt, std::nullopt}), result);

  // Integer literals parse as BIGINT, and the key must match the map's key type exactly.
  auto intKeyed = makeMapVector<int64_t, std::string>({{{1, "a"}, {2, "b"}}, {{3, "c"}}});
  result = evaluate("get_map_value(c0, 2)", makeRowVector({intKeyed}));
  facebook::velox::test::assertEqualVectors(makeNullableFlatVector<std::string>({"b", std::nullopt}), result);
}

// checked_get_timestamp is get_timestamp's failOnError = true sibling (see
// CheckedGetTimestamp.h). Exercise both names directly.
TEST_F(SparkFunctionTest, checkedGetTimestampMatchesGetTimestampOnValidInput) {
  auto input = makeRowVector({
      makeFlatVector<std::string>({"2024-01-01", "2024-06-15 10:30:00"}),
      makeFlatVector<std::string>({"yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss"}),
  });
  auto getTimestampResult = evaluate("get_timestamp(c0, c1)", input);
  auto checkedResult = evaluate("checked_get_timestamp(c0, c1)", input);
  facebook::velox::test::assertEqualVectors(getTimestampResult, checkedResult);
}

TEST_F(SparkFunctionTest, checkedGetTimestampThrowsOnInvalidData) {
  // get_timestamp stays NULL on the exact same input.
  auto input = makeRowVector({
      makeFlatVector<std::string>({"not a date"}),
      makeFlatVector<std::string>({"yyyy-MM-dd"}),
  });
  VELOX_ASSERT_THROW(evaluate("checked_get_timestamp(c0, c1)", input), "Invalid date format");
  facebook::velox::test::assertEqualVectors(
      makeNullableFlatVector<Timestamp>({std::nullopt}), evaluate("get_timestamp(c0, c1)", input));
}

TEST_F(SparkFunctionTest, checkedGetTimestampNullInputStaysNull) {
  // Not an error, even though checked_get_timestamp throws for other invalid input.
  auto input = makeRowVector({
      makeNullableFlatVector<std::string>({std::nullopt}),
      makeFlatVector<std::string>({"yyyy-MM-dd"}),
  });
  facebook::velox::test::assertEqualVectors(
      makeNullableFlatVector<Timestamp>({std::nullopt}), evaluate("checked_get_timestamp(c0, c1)", input));
}

TEST_F(SparkFunctionTest, checkedGetTimestampInvalidPatternPreservesLegacyVsJodaDistinction) {
  // An invalid format pattern is never ANSI-gated: legacy stays NULL, Joda still throws,
  // unchanged from get_timestamp for both names.
  auto input = makeRowVector({
      makeFlatVector<std::string>({"2024-01-01"}),
      makeFlatVector<std::string>({"yyyy-MM-dd bb"}), // 'b' is not a supported Joda specifier.
  });

  queryCtx_->testingOverrideConfigUnsafe(
      {{functions::sparksql::SparkQueryConfig::qualify(functions::sparksql::SparkQueryConfig::kLegacyDateFormatter),
        "true"}});
  facebook::velox::test::assertEqualVectors(
      makeNullableFlatVector<Timestamp>({std::nullopt}), evaluate("checked_get_timestamp(c0, c1)", input));
  facebook::velox::test::assertEqualVectors(
      makeNullableFlatVector<Timestamp>({std::nullopt}), evaluate("get_timestamp(c0, c1)", input));

  queryCtx_->testingOverrideConfigUnsafe(
      {{functions::sparksql::SparkQueryConfig::qualify(functions::sparksql::SparkQueryConfig::kLegacyDateFormatter),
        "false"}});
  VELOX_ASSERT_THROW(evaluate("checked_get_timestamp(c0, c1)", input), "Specifier b is not supported");
  VELOX_ASSERT_THROW(evaluate("get_timestamp(c0, c1)", input), "Specifier b is not supported");
}

TEST_F(SparkFunctionTest, checkedGetTimestampConstantFormat) {
  // A literal format takes the isConstantTimeFormat_ path (formatter built once in initialize()).
  auto columnFormatInput =
      makeRowVector({makeFlatVector<std::string>({"2024-01-01"}), makeFlatVector<std::string>({"yyyy-MM-dd"})});
  facebook::velox::test::assertEqualVectors(
      evaluate("checked_get_timestamp(c0, 'yyyy-MM-dd')", columnFormatInput),
      evaluate("checked_get_timestamp(c0, c1)", columnFormatInput));

  auto invalidDataConstantFormat = makeRowVector({makeFlatVector<std::string>({"not a date"})});
  VELOX_ASSERT_THROW(
      evaluate("checked_get_timestamp(c0, 'yyyy-MM-dd')", invalidDataConstantFormat), "Invalid date format");
}

TEST_F(SparkFunctionTest, checkedGetTimestampNullFormatStaysNull) {
  // A NULL format, not just a NULL input string, also propagates to NULL.
  auto input = makeRowVector({
      makeFlatVector<std::string>({"2024-01-01"}),
      makeNullableFlatVector<std::string>({std::nullopt}),
  });
  facebook::velox::test::assertEqualVectors(
      makeNullableFlatVector<Timestamp>({std::nullopt}), evaluate("checked_get_timestamp(c0, c1)", input));
  facebook::velox::test::assertEqualVectors(
      makeNullableFlatVector<Timestamp>({std::nullopt}), evaluate("get_timestamp(c0, c1)", input));
}

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

#include <optional>
#include <unordered_map>

#include "operators/functions/IncrementMetricFunction.h"
#include "operators/functions/RegistrationAllFunctions.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/QueryConfig.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::functions::sparksql::test;

namespace gluten {

class IncrementMetricFunctionTest : public SparkFunctionBaseTest {
 public:
  IncrementMetricFunctionTest() {
    registerAllFunctions();
  }
};

TEST_F(IncrementMetricFunctionTest, slotNames) {
  ASSERT_EQ(incrementMetricFunctionName(0), "increment_metric_0");
  ASSERT_EQ(incrementMetricFunctionName(31), "increment_metric_31");
  ASSERT_TRUE(isIncrementMetricFunction("increment_metric_7"));
  ASSERT_FALSE(isIncrementMetricFunction("plus"));
  ASSERT_FALSE(isIncrementMetricFunction("my_increment_metric_7"));
  ASSERT_TRUE(isIncrementMetricFunction("increment_metric_0"));
  ASSERT_TRUE(isIncrementMetricFunction(incrementMetricFunctionName(kIncrementMetricFunctionSlots - 1)));
  // Only the registered slots count.
  ASSERT_FALSE(isIncrementMetricFunction("increment_metric_"));
  ASSERT_FALSE(isIncrementMetricFunction(incrementMetricFunctionName(kIncrementMetricFunctionSlots)));
  ASSERT_FALSE(isIncrementMetricFunction("increment_metric_-1"));
  ASSERT_FALSE(isIncrementMetricFunction("increment_metric_07"));
  ASSERT_FALSE(isIncrementMetricFunction("increment_metric_7x"));
  ASSERT_FALSE(isIncrementMetricFunction("increment_metric_foo"));
}

TEST_F(IncrementMetricFunctionTest, passesInputThroughIncludingNulls) {
  auto input = makeNullableFlatVector<int64_t>({1, std::nullopt, 3});
  auto result = evaluate("increment_metric_0(c0)", makeRowVector({input}));
  facebook::velox::test::assertEqualVectors(input, result);

  auto last = incrementMetricFunctionName(kIncrementMetricFunctionSlots - 1);
  auto strings = makeFlatVector<StringView>({"a", "bb", "ccc"});
  auto stringResult = evaluate(last + "(c0)", makeRowVector({strings}));
  facebook::velox::test::assertEqualVectors(strings, stringResult);
}

TEST_F(IncrementMetricFunctionTest, countsOnlyRowsThatReachTheCall) {
  // The shape of a MERGE clause: rows 2, 3 and 4 take the branch, row 1 does not, so each counter
  // sees the rows of its own branch, exactly as Spark evaluates IncrementMetric. The literals are
  // what Delta wraps; they must not be folded into constants, which would skip the count.
  // Spark function names: the parser's `>` maps to a Presto name that Gluten does not register.
  auto data = makeRowVector({makeFlatVector<int32_t>({1, 2, 3, 4})});
  auto exprSet = compileExpression(
      "if(greaterthan(c0, 1), increment_metric_1(true), increment_metric_2(false))", asRowType(data->type()));
  auto result = evaluate(*exprSet, data);
  facebook::velox::test::assertEqualVectors(makeFlatVector<bool>({false, true, true, true}), result);
  auto stats = exprSet->stats();
  ASSERT_EQ(stats.at("increment_metric_1").numProcessedRows, 3);
  ASSERT_EQ(stats.at("increment_metric_2").numProcessedRows, 1);

  // A counter over a literal at the root of a projection counts every row.
  exprSet = compileExpression("increment_metric_3(true)", asRowType(data->type()));
  evaluate(*exprSet, data);
  ASSERT_EQ(exprSet->stats().at("increment_metric_3").numProcessedRows, 4);
}

TEST_F(IncrementMetricFunctionTest, countsEveryRowOfAnEncodedInput) {
  // Peeling the dictionary would evaluate the counter on its 2 distinct values instead of 6 rows.
  auto base = makeFlatVector<int32_t>({10, 20});
  auto input = wrapInDictionary(makeIndices({0, 1, 0, 1, 0, 1}), 6, base);
  auto data = makeRowVector({input});
  auto exprSet = compileExpression("increment_metric_5(c0)", asRowType(data->type()));
  auto result = evaluate(*exprSet, data);
  facebook::velox::test::assertEqualVectors(input, result);
  ASSERT_EQ(exprSet->stats().at("increment_metric_5").numProcessedRows, 6);
}

TEST_F(IncrementMetricFunctionTest, nullInputStillCounts) {
  auto data = makeRowVector({makeNullableFlatVector<int64_t>({1, std::nullopt, std::nullopt})});
  auto exprSet = compileExpression("increment_metric_4(c0)", asRowType(data->type()));
  evaluate(*exprSet, data);
  ASSERT_EQ(exprSet->stats().at("increment_metric_4").numProcessedRows, 3);
}

TEST_F(IncrementMetricFunctionTest, detectsCountersInAPlan) {
  auto data = makeRowVector({makeFlatVector<int32_t>({1, 2})});
  auto withCounter = exec::test::PlanBuilder().values({data}).project({"increment_metric_5(c0) AS c0"}).planNode();
  ASSERT_TRUE(usesIncrementMetricFunctions(*withCounter));

  auto nestedCounter = exec::test::PlanBuilder()
                           .values({data})
                           .project({"if(greaterthan(c0, 1), increment_metric_6(c0), c0) AS c0"})
                           .filter("greaterthan(c0, 0)")
                           .planNode();
  ASSERT_TRUE(usesIncrementMetricFunctions(*nestedCounter));

  auto plain =
      exec::test::PlanBuilder().values({data}).project({"add(c0, 1) AS c0"}).filter("greaterthan(c0, 0)").planNode();
  ASSERT_FALSE(usesIncrementMetricFunctions(*plain));
}

TEST_F(IncrementMetricFunctionTest, rejectsCountersOutsideProjections) {
  auto data = makeRowVector({makeFlatVector<int32_t>({1, 2})});
  // Substrait declarations, `<name>:<argument types>`, as the plan converter receives them.
  const std::unordered_map<uint64_t, std::string> functionMap{
      {0, "increment_metric_5:i32"}, {1, "greaterthan:i32_i32"}};

  auto inProject = exec::test::PlanBuilder()
                       .values({data})
                       .project({"increment_metric_5(c0) AS c0"})
                       .filter("greaterthan(c0, 0)")
                       .planNode();
  checkIncrementMetricFunctionsInProjects(functionMap, *inProject);

  // The JVM credits counters from projections only, so a counter in a filter is rejected.
  auto inFilter = exec::test::PlanBuilder().values({data}).filter("greaterthan(increment_metric_5(c0), 0)").planNode();
  ASSERT_FALSE(usesIncrementMetricFunctions(*inFilter));
  VELOX_ASSERT_THROW(
      checkIncrementMetricFunctionsInProjects(functionMap, *inFilter),
      "Delta counter increment_metric_5 is called outside a projection");

  // Plans without counters are not inspected.
  checkIncrementMetricFunctionsInProjects({{1, "greaterthan:i32_i32"}}, *inFilter);
}

TEST_F(IncrementMetricFunctionTest, queryConfigsKeepCountingUnderUserOverrides) {
  // A counter at the root of a projection, like the single clause of an insert-only MERGE.
  auto data = makeRowVector({makeFlatVector<int32_t>({1, 2, 3, 4})});
  // Overrides a user can pass through spark.gluten.sql.columnar.backend.velox.*: simplified
  // evaluation applies the counter without counting the rows it processes, and without the export
  // FilterProject operators report no counts.
  std::unordered_map<std::string, std::string> configs{
      {core::QueryConfig::kExprEvalSimplified, "true"}, {core::QueryConfig::kOperatorTrackExpressionStats, "false"}};
  setIncrementMetricQueryConfigs(configs);
  queryCtx_->testingOverrideConfigUnsafe(std::move(configs));
  ASSERT_FALSE(queryCtx_->queryConfig().exprEvalSimplified());
  ASSERT_TRUE(queryCtx_->queryConfig().operatorTrackExpressionStats());

  // FilterProject operators pick their evaluator from the query config the same way.
  auto exprSet =
      exec::makeExprSetFromFlag({makeTypedExpr("increment_metric_0(c0)", asRowType(data->type()))}, &execCtx_);
  auto result = evaluate(*exprSet, data);
  facebook::velox::test::assertEqualVectors(data->childAt(0), result);
  const auto stats = exprSet->stats();
  ASSERT_EQ(stats.count("increment_metric_0"), 1);
  ASSERT_EQ(stats.at("increment_metric_0").numProcessedRows, 4);
}

} // namespace gluten

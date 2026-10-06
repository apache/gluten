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

#include "operators/functions/IncrementMetricFunction.h"
#include "operators/functions/RegistrationAllFunctions.h"
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
  // Rows 2, 3 and 4 take the branch; row 1 does not. The counter must see three rows, and the
  // unconditional counter all four, exactly as Spark evaluates IncrementMetric.
  // Spark function names: the parser's `>` and `+` map to Presto names that Gluten does not register.
  auto data = makeRowVector({makeFlatVector<int32_t>({1, 2, 3, 4})});
  auto exprSet = compileExpression(
      "add(if(greaterthan(c0, 1), increment_metric_1(c0), increment_metric_2(c0)), increment_metric_3(c0))",
      asRowType(data->type()));
  evaluate(*exprSet, data);
  auto stats = exprSet->stats();
  ASSERT_EQ(stats.at("increment_metric_1").numProcessedRows, 3);
  ASSERT_EQ(stats.at("increment_metric_2").numProcessedRows, 1);
  ASSERT_EQ(stats.at("increment_metric_3").numProcessedRows, 4);
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

} // namespace gluten

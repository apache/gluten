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

#include "utils/DynamicFilterUtils.h"
#include "operators/functions/RegistrationAllFunctions.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::functions::sparksql::test;

namespace gluten {

class DynamicFilterUtilsTest : public SparkFunctionBaseTest {
 protected:
  DynamicFilterUtilsTest() {
    registerAllFunctions();
  }

  PlanBuilder probe() {
    return PlanBuilder(ids_, pool()).values({makeRowVector({"c0"}, {makeFlatVector<int64_t>({1, 2, 3})})});
  }

  PlanBuilder build() {
    return PlanBuilder(ids_, pool()).values({makeRowVector({"u0"}, {makeFlatVector<int64_t>({1})})});
  }

  const std::shared_ptr<core::PlanNodeIdGenerator> ids_ = std::make_shared<core::PlanNodeIdGenerator>();
};

TEST_F(DynamicFilterUtilsTest, nonDeterministicProjectionOnProbeSide) {
  auto plan =
      probe().project({"c0", "rand() AS r"}).hashJoin({"c0"}, {"u0"}, build().planNode(), "", {"c0", "r"}).planNode();
  ASSERT_TRUE(hasNonDeterministicExprBelowHashProbe(*plan));

  plan = probe()
             .project({"c0", "monotonically_increasing_id() AS m"})
             .hashJoin({"c0"}, {"u0"}, build().planNode(), "", {"c0", "m"})
             .planNode();
  ASSERT_TRUE(hasNonDeterministicExprBelowHashProbe(*plan));
}

TEST_F(DynamicFilterUtilsTest, nonDeterministicFilterOnProbeSide) {
  auto plan =
      probe().filter("lessthan(rand(), 0.5)").hashJoin({"c0"}, {"u0"}, build().planNode(), "", {"c0"}).planNode();
  ASSERT_TRUE(hasNonDeterministicExprBelowHashProbe(*plan));
}

TEST_F(DynamicFilterUtilsTest, nonDeterministicFilterInProbeScan) {
  // Gluten pushes a filter directly over a scan into the scan, where it runs after the dynamic
  // filters the scan accepted.
  auto plan = PlanBuilder(ids_, pool())
                  .tableScan(ROW({"c0"}, {BIGINT()}), {}, "lessthan(rand(), 0.5)")
                  .hashJoin({"c0"}, {"u0"}, build().planNode(), "", {"c0"})
                  .planNode();
  ASSERT_TRUE(hasNonDeterministicExprBelowHashProbe(*plan));

  plan = PlanBuilder(ids_, pool())
             .tableScan(ROW({"c0"}, {BIGINT()}), {}, "greaterthan(c0, cast(0 as bigint))")
             .hashJoin({"c0"}, {"u0"}, build().planNode(), "", {"c0"})
             .planNode();
  ASSERT_FALSE(hasNonDeterministicExprBelowHashProbe(*plan));
}

TEST_F(DynamicFilterUtilsTest, deltaCounterOnProbeSide) {
  // Delta's numSourceRows counter sits in a projection over the MERGE source, below the join.
  auto plan = probe()
                  .project({"c0", "increment_metric_0(true) AS present"})
                  .hashJoin({"c0"}, {"u0"}, build().planNode(), "", {"c0"})
                  .planNode();
  ASSERT_TRUE(hasNonDeterministicExprBelowHashProbe(*plan));
}

TEST_F(DynamicFilterUtilsTest, nonDeterministicBelowAnotherJoinOnProbeSide) {
  // The probe of the upper join reads the lower join, which reads the projection: one pipeline.
  auto plan = probe()
                  .project({"c0", "rand() AS r"})
                  .hashJoin({"c0"}, {"u0"}, build().planNode(), "", {"c0", "r"})
                  .hashJoin({"c0"}, {"u0"}, build().planNode(), "", {"c0", "r"})
                  .planNode();
  ASSERT_TRUE(hasNonDeterministicExprBelowHashProbe(*plan));
}

TEST_F(DynamicFilterUtilsTest, deterministicProbeSide) {
  auto plan = probe()
                  .project({"c0", "add(c0, c0) AS d"})
                  .filter("greaterthan(d, cast(0 as bigint))")
                  .hashJoin({"c0"}, {"u0"}, build().planNode(), "", {"c0", "d"})
                  .planNode();
  ASSERT_FALSE(hasNonDeterministicExprBelowHashProbe(*plan));
}

TEST_F(DynamicFilterUtilsTest, nonDeterministicOffProbeSide) {
  // The build side runs in its own pipeline, so the probe's filter never reaches it.
  auto plan =
      probe().hashJoin({"c0"}, {"u0"}, build().project({"u0", "rand() AS r"}).planNode(), "", {"c0", "r"}).planNode();
  ASSERT_FALSE(hasNonDeterministicExprBelowHashProbe(*plan));

  // Above the join the expression only sees the rows the join produces anyway.
  plan = probe().hashJoin({"c0"}, {"u0"}, build().planNode(), "", {"c0"}).project({"c0", "rand() AS r"}).planNode();
  ASSERT_FALSE(hasNonDeterministicExprBelowHashProbe(*plan));

  // No join, no dynamic filter.
  plan = probe().project({"c0", "rand() AS r"}).planNode();
  ASSERT_FALSE(hasNonDeterministicExprBelowHashProbe(*plan));
}

} // namespace gluten

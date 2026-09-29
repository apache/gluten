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

#include "FilePathGenerator.h"
#include "JsonToProtoConverter.h"

#include "bolt/common/base/Fs.h"
#include "bolt/dwio/common/tests/utils/DataFiles.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

#include "substrait/SubstraitToBoltPlan.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::test;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;

namespace gluten {

class Substrait2BoltValuesNodeConversionTest : public OperatorTestBase {};

// SELECT * FROM tmp
TEST_F(Substrait2BoltValuesNodeConversionTest, valuesNode) {
  auto planPath = FilePathGenerator::getDataFilePath("substrait_virtualTable.json");

  ::substrait::Plan substraitPlan;
  JsonToProtoConverter::readFromFile(planPath, substraitPlan);
  auto boltCfg = std::make_shared<bytedance::bolt::config::ConfigBase>(std::unordered_map<std::string, std::string>());
  std::shared_ptr<SubstraitToBoltPlanConverter> planConverter_ =
      std::make_shared<SubstraitToBoltPlanConverter>(pool_.get(), boltCfg.get(), std::nullopt, std::nullopt, true);
  auto boltPlan = planConverter_->toBoltPlan(substraitPlan);

  RowVectorPtr expectedData = makeRowVector(
      {makeFlatVector<int64_t>({2499109626526694126, 2342493223442167775, 4077358421272316858}),
       makeFlatVector<int32_t>({581869302, -708632711, -133711905}),
       makeFlatVector<double>({0.90579193414549275, 0.96886777112423139, 0.63235925003444637}),
       makeFlatVector<bool>({true, false, false}),
       makeFlatVector<int32_t>(3, nullptr, nullEvery(1))

      });

  createDuckDbTable({expectedData});
  assertQuery(boltPlan, "SELECT * FROM tmp");
}

} // namespace gluten

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

#include "memory/BoltMemoryManager.h"
#include "substrait/SubstraitToBoltPlan.h"
#include "substrait/SubstraitToBoltPlanValidator.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/dwio/common/tests/utils/DataFiles.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/type/Type.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::test;
using namespace bytedance::bolt::connector::hive;
using namespace bytedance::bolt::exec;

namespace gluten {

class Substrait2BoltPlanValidatorTest : public exec::test::HiveConnectorTestBase {
 protected:
  bool validatePlan(std::string file) {
    std::string subPlanPath = FilePathGenerator::getDataFilePath(file);

    ::substrait::Plan substraitPlan;
    JsonToProtoConverter::readFromFile(subPlanPath, substraitPlan);
    return validatePlan(substraitPlan);
  }

  bool validatePlan(::substrait::Plan& plan) {
    auto planValidator =
        std::make_shared<SubstraitToBoltPlanValidator>(pool_.get(), std::unordered_map<std::string, std::string>{});
    return planValidator->validate(plan);
  }
};

TEST_F(Substrait2BoltPlanValidatorTest, group) {
  std::string subPlanPath = FilePathGenerator::getDataFilePath("group.json");

  ::substrait::Plan substraitPlan;
  JsonToProtoConverter::readFromFile(subPlanPath, substraitPlan);

  ASSERT_FALSE(validatePlan(substraitPlan));
}

} // namespace gluten

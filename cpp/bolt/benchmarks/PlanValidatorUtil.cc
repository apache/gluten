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

#include "benchmarks/common/BenchmarkUtils.h"
#include "compute/BoltBackend.h"
#include "compute/BoltRuntime.h"
#include "config/GlutenConfig.h"
#include "memory/BoltMemoryManager.h"
#include "substrait/SubstraitToBoltPlanValidator.h"

using namespace bytedance::bolt;
using namespace gluten;

/// Set spark.gluten.sql.debug=true to get validation plan and dump it into a json file,
/// then use this util debug validate process easily in native side.
int main(int argc, char** argv) {
  if (argc != 2) {
    LOG(WARNING) << "PlanValidatorUtil usage: \n"
                 << "./plan_validator_util <path>/substrait_json_plan";
    return -1;
  }
  std::string planPath = argv[1];

  std::ifstream msgJson(planPath);
  std::stringstream buffer;
  buffer << msgJson.rdbuf();
  std::string msgData = buffer.str();
  auto plan = substraitFromJsonToPb("Plan", msgData);

  std::unordered_map<std::string, std::string> conf;
  conf.insert({kDebugModeEnabled, "true"});
  initBoltBackend(conf);
  auto pool = defaultLeafBoltMemoryPool().get();
  SubstraitToBoltPlanValidator planValidator(pool);

  ::substrait::Plan subPlan;
  parseProtobuf(reinterpret_cast<uint8_t*>(plan.data()), plan.size(), &subPlan);
  try {
    if (!planValidator.validate(subPlan)) {
      auto reason = planValidator.getValidateLog();
      for (auto& msg : reason) {
        LOG(INFO) << msg;
      }
    } else {
      LOG(INFO) << planPath << " is valid.";
    }
  } catch (std::invalid_argument& e) {
    LOG(INFO) << "Failed to validate substrait plan because " << e.what();
  }

  return 0;
}

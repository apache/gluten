
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

#include "CudfPlanValidator.h"
#include "compute/ResultIterator.h"
#include "compute/VeloxBackend.h"
#include "compute/VeloxPlanConverter.h"
#include "config/GlutenConfig.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/TableScan.h"
#include "velox/exec/Task.h"
#include "velox/experimental/cudf/exec/NvtxHelper.h"
#include "velox/experimental/cudf/exec/ToCudf.h"

using namespace facebook;

namespace gluten {

namespace {

bool isCudfOperator(const exec::Operator* op) {
  return dynamic_cast<const velox::cudf_velox::NvtxHelper*>(op) != nullptr;
}

} // namespace

bool CudfPlanValidator::validate(const ::substrait::Plan& substraitPlan) {
  auto veloxMemoryPool = gluten::defaultLeafVeloxMemoryPool();
  std::vector<::substrait::ReadRel_LocalFiles> localFiles;
  std::unordered_map<std::string, std::string> configValues;
  std::vector<std::shared_ptr<ResultIterator>> inputs;
  std::shared_ptr<facebook::velox::config::ConfigBase> veloxCfg = std::make_shared<facebook::velox::config::ConfigBase>(
      std::unordered_map<std::string, std::string>{{kCudfEnabled, "true"}});
  VeloxPlanConverter veloxPlanConverter(veloxMemoryPool.get(), veloxCfg.get(),
      inputs, std::nullopt, std::nullopt, true);
  auto planNode = veloxPlanConverter.toVeloxPlan(substraitPlan, localFiles);
  std::unordered_set<velox::core::PlanNodeId> emptySet;
  velox::core::PlanFragment planFragment{planNode, velox::core::ExecutionStrategy::kUngrouped, 1, emptySet};

  std::unordered_map<std::string, std::shared_ptr<velox::config::ConfigBase>> connectorConfigs;
  static std::atomic<uint32_t> vtId{0};
  std::shared_ptr<velox::core::QueryCtx> queryCtx = velox::core::QueryCtx::create(
      nullptr,
      facebook::velox::core::QueryConfig{configValues},
      connectorConfigs,
      gluten::VeloxBackend::get()->getAsyncDataCache(),
      getDefaultMemoryManager()->getAggregateMemoryPool(),
      nullptr,
      fmt::format("Gluten_Cudf_Validation_VTID_{}", std::to_string(vtId++)));
  std::shared_ptr<facebook::velox::exec::Task> task = velox::exec::Task::create(
      fmt::format("Gluten_Cudf_Validation_VTID_{}", std::to_string(vtId++)),
      std::move(planFragment),
      0,
      std::move(queryCtx),
      velox::exec::Task::ExecutionMode::kSerial);
  std::vector<velox::exec::Operator*> operators;
  task->testingVisitDrivers([&](velox::exec::Driver* driver) { operators = driver->operators(); });
  for (const auto* op : operators) {
    if (dynamic_cast<const velox::exec::TableScan*>(op) != nullptr) {
      continue;
    }
    if (isCudfOperator(op)) {
      continue;
    }
    LOG(INFO) << "Operator " << op->operatorType() << " is not supported in cudf";
    task->requestCancel().wait();
    return false;
  }
  task->requestCancel().wait();
  LOG(INFO) << "Cudf Operator validation success";
  return true;
}

} // namespace gluten

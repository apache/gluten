
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
#include "compute/BoltBackend.h"
#include "compute/BoltPlanConverter.h"
#include "operators/plannodes/RowVectorStream.h"
#include "bolt/core/PlanNode.h"
#include "bolt/exec/Task.h"
#include "bolt/exec/TableScan.h"
#include "bolt/experimental/cudf/exec/ToCudf.h"

using namespace bytedance;

namespace gluten {
bool CudfPlanValidator::validate(const ::substrait::Plan& substraitPlan) {
  auto boltMemoryPool = gluten::defaultLeafBoltMemoryPool();
  std::vector<::substrait::ReadRel_LocalFiles> localFiles;
  std::unordered_map<std::string, std::string> configValues;
  std::vector<std::shared_ptr<ResultIterator>> inputs;
  std::shared_ptr<bytedance::bolt::config::ConfigBase> boltCfg =
      std::make_shared<bytedance::bolt::config::ConfigBase>(std::unordered_map<std::string, std::string>());
  BoltPlanConverter boltPlanConverter(
      inputs, boltMemoryPool.get(), boltCfg.get(), std::nullopt, std::nullopt, true);
  auto planNode = boltPlanConverter.toBoltPlan(substraitPlan, localFiles);
  std::unordered_set<bolt::core::PlanNodeId> emptySet;
  bolt::core::PlanFragment planFragment{planNode, bolt::core::ExecutionStrategy::kUngrouped, 1, emptySet};

  std::unordered_map<std::string, std::shared_ptr<bolt::config::ConfigBase>> connectorConfigs;
  static std::atomic<uint32_t> vtId{0};
  std::shared_ptr<bolt::core::QueryCtx> queryCtx = bolt::core::QueryCtx::create(
      nullptr,
      bytedance::bolt::core::QueryConfig{configValues},
      connectorConfigs,
      gluten::BoltBackend::get()->getAsyncDataCache(),
      getDefaultMemoryManager()->getAggregateMemoryPool(),
      nullptr,
      fmt::format("Gluten_Cudf_Validation_VTID_{}", std::to_string(vtId++)));
  std::shared_ptr<bytedance::bolt::exec::Task> task = bolt::exec::Task::create(
      fmt::format("Gluten_Cudf_Validation_VTID_{}", std::to_string(vtId++)),
      std::move(planFragment),
      0,
      std::move(queryCtx),
      bolt::exec::Task::ExecutionMode::kSerial);
  std::vector<bolt::exec::Operator*> operators;
  task->testingVisitDrivers([&](bolt::exec::Driver* driver) { operators = driver->operators(); });
  for (const auto* op : operators) {
    if (dynamic_cast<const bolt::exec::TableScan*>(op) != nullptr) {
      continue;
    }
    if (dynamic_cast<const ValueStream*>(op) != nullptr) {
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

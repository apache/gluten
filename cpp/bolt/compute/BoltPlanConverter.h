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

#pragma once

#include <bolt/common/memory/MemoryPool.h>
#include "compute/ResultIterator.h"
#include "memory/BoltMemoryManager.h"
#include "substrait/SubstraitToBoltPlan.h"
#include "substrait/plan.pb.h"
#include "bolt/core/PlanNode.h"

namespace gluten {

// This class is used to convert the Substrait plan into Bolt plan.
class BoltPlanConverter {
 public:
  explicit BoltPlanConverter(
      const std::vector<std::shared_ptr<ResultIterator>>& inputIters,
      bytedance::bolt::memory::MemoryPool* boltPool,
      const bytedance::bolt::config::ConfigBase* boltCfg,
      const std::optional<std::string> writeFilesTempPath = std::nullopt,
      const std::optional<std::string> writeFileName = std::nullopt,
      bool validationMode = false);

  std::shared_ptr<const bytedance::bolt::core::PlanNode> toBoltPlan(
      const ::substrait::Plan& substraitPlan,
      std::vector<::substrait::ReadRel_LocalFiles> localFiles);

  const std::unordered_map<bytedance::bolt::core::PlanNodeId, std::shared_ptr<SplitInfo>>& splitInfos() {
    return substraitBoltPlanConverter_.splitInfos();
  }

 private:
  bool validationMode_;

  const bytedance::bolt::config::ConfigBase* boltCfg_;
  SubstraitToBoltPlanConverter substraitBoltPlanConverter_;
};

} // namespace gluten

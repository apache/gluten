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

#include "compute/VeloxRuntime.h"

namespace gluten {
// One prepared iterator-input plan. The creating runtime must outlive this evaluator.
class VeloxPlanEvaluator final {
 public:
  VeloxPlanEvaluator(VeloxRuntime* runtime, const uint8_t* data, int32_t size);
  std::shared_ptr<ResultIterator> evaluate(std::shared_ptr<ResultIterator> input, SparkTaskInfo taskInfo);
  const facebook::velox::RowTypePtr& outputType() const {
    return plan_->outputType();
  }

 private:
  VeloxRuntime* runtime_;
  facebook::velox::core::PlanNodePtr plan_;
  std::vector<facebook::velox::core::PlanNodePtr> nodes_;
};
} // namespace gluten

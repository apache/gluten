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

#include "RowConstructorWithNull.h"
#include "bolt/expression/VectorFunction.h"

namespace gluten {

bytedance::bolt::TypePtr RowConstructorWithNullCallToSpecialForm::resolveType(
    const std::vector<bytedance::bolt::TypePtr>& argTypes) {
  auto numInput = argTypes.size();
  std::vector<std::string> names(numInput);
  std::vector<bytedance::bolt::TypePtr> types(numInput);
  for (auto i = 0; i < numInput; i++) {
    types[i] = argTypes[i];
    names[i] = fmt::format("c{}", i + 1);
  }
  return bytedance::bolt::ROW(std::move(names), std::move(types));
}

bytedance::bolt::exec::ExprPtr RowConstructorWithNullCallToSpecialForm::constructSpecialForm(
    const bytedance::bolt::TypePtr& type,
    std::vector<bytedance::bolt::exec::ExprPtr>&& compiledChildren,
    bool trackCpuUsage,
    const bytedance::bolt::core::QueryConfig& config) {
  auto name = this->rowFunctionName;
  auto [function, metadata] = bytedance::bolt::exec::vectorFunctionFactories().withRLock(
      [&config, &name](auto& functionMap) -> std::pair<
                                              std::shared_ptr<bytedance::bolt::exec::VectorFunction>,
                                              bytedance::bolt::exec::VectorFunctionMetadata> {
        auto functionIterator = functionMap.find(name);
        if (functionIterator != functionMap.end()) {
          return {functionIterator->second.factory(name, {}, config), functionIterator->second.metadata};
        } else {
          BOLT_FAIL("Function {} is not registered.", name);
        }
      });

  return std::make_shared<bytedance::bolt::exec::Expr>(
      type, std::move(compiledChildren), function, name, trackCpuUsage);
}

} // namespace gluten

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

#include "bolt/expression/FunctionCallToSpecialForm.h"
#include "bolt/expression/SpecialForm.h"

namespace gluten {

class RowConstructorWithNullCallToSpecialForm : public bytedance::bolt::exec::FunctionCallToSpecialForm {
 public:
  RowConstructorWithNullCallToSpecialForm(const std::string& rowFunctionName) {
    this->rowFunctionName = rowFunctionName;
  }

  bytedance::bolt::TypePtr resolveType(const std::vector<bytedance::bolt::TypePtr>& argTypes) override;

  bytedance::bolt::exec::ExprPtr constructSpecialForm(
      const bytedance::bolt::TypePtr& type,
      std::vector<bytedance::bolt::exec::ExprPtr>&& compiledChildren,
      bool trackCpuUsage,
      const bytedance::bolt::core::QueryConfig& config) override;

  static constexpr const char* kRowConstructorWithNull = "row_constructor_with_null";
  static constexpr const char* kRowConstructorWithAllNull = "row_constructor_with_all_null";

 protected:
  bytedance::bolt::exec::ExprPtr constructSpecialForm(
      const std::string& name,
      const bytedance::bolt::TypePtr& type,
      std::vector<bytedance::bolt::exec::ExprPtr>&& compiledChildren,
      bool trackCpuUsage,
      const bytedance::bolt::core::QueryConfig& config);

 private:
  std::string rowFunctionName;
};

} // namespace gluten

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
#include "operators/functions/overlay/GetMapValue.h"

#include "velox/expression/VectorFunction.h"

using namespace facebook;

namespace gluten {

void registerSparkGetMapValueFunction(const std::string& name) {
  velox::exec::registerStatefulVectorFunction(
      name,
      SparkGetMapValueFunction::signatures(),
      [](const std::string&,
         const std::vector<velox::exec::VectorFunctionArg>& inputArgs,
         const velox::core::QueryConfig& config) -> std::shared_ptr<velox::exec::VectorFunction> {
        VELOX_CHECK_EQ(inputArgs.size(), 2);
        VELOX_CHECK(inputArgs[0].type->isMap(), "get_map_value expects a map input");
        // A per-call instance so the map subscript cache stays private to the
        // expression, as in velox::functions::registerElementAtFunction.
        return std::make_shared<SparkGetMapValueFunction>(config.isExpressionEvaluationCacheEnabled());
      });
}

} // namespace gluten

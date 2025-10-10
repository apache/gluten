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
#include "bolt/expression/ExprToSubfieldFilter.h"

namespace gluten {
/// Parses Spark expression into subfield filter. Differences from Presto's parser include:
/// 1) Some Spark functions are registered under different names.
/// 2) The supported functions vary.
class SparkExprToSubfieldFilterParser : public bytedance::bolt::exec::ExprToSubfieldFilterParser {
public:
std::optional<std::pair<bytedance::bolt::common::Subfield, std::unique_ptr<bytedance::bolt::common::Filter>>>
leafCallToSubfieldFilter(
    const bytedance::bolt::core::CallTypedExpr& call,
    bytedance::bolt::core::ExpressionEvaluator* evaluator,
    bool negated) override;
};

} // namespace gluten

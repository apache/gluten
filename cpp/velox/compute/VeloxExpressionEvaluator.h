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

#include "memory/VeloxColumnarBatch.h"
#include "velox/core/QueryCtx.h"
#include "velox/expression/Expr.h"

namespace gluten {

// Not thread-safe. Runtime memory pools must outlive the evaluator and its batches.
class VeloxExpressionEvaluator final {
 public:
  VeloxExpressionEvaluator(
      std::shared_ptr<facebook::velox::memory::MemoryPool> pool,
      std::shared_ptr<facebook::velox::core::QueryCtx> queryCtx,
      const uint8_t* data,
      int32_t size);

  VeloxExpressionEvaluator(
      std::shared_ptr<facebook::velox::memory::MemoryPool> pool,
      std::shared_ptr<facebook::velox::core::QueryCtx> queryCtx,
      facebook::velox::RowTypePtr inputType,
      facebook::velox::RowTypePtr outputType,
      std::vector<facebook::velox::core::TypedExprPtr> expressions);

  std::shared_ptr<VeloxColumnarBatch> evaluate(const std::shared_ptr<ColumnarBatch>& input);

 private:
  const std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  const std::shared_ptr<facebook::velox::core::QueryCtx> queryCtx_;
  facebook::velox::core::ExecCtx execCtx_;
  facebook::velox::RowTypePtr inputType_;
  facebook::velox::RowTypePtr outputType_;
  std::unique_ptr<facebook::velox::exec::ExprSet> expressions_;
};

} // namespace gluten

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

#include "GlutenStrideNode.h"

#include "velox/exec/OperatorUtils.h"

namespace gluten {

GlutenStrideOperator::GlutenStrideOperator(
    int32_t operatorId,
    facebook::velox::exec::DriverCtx* driverCtx,
    std::shared_ptr<const GlutenStrideNode> node)
    : facebook::velox::exec::Operator(driverCtx, node->outputType(), operatorId, node->id(), "GlutenStride"),
      stride_(node->stride()) {}

facebook::velox::RowVectorPtr GlutenStrideOperator::getOutput() {
  if (!input_) {
    return nullptr;
  }
  auto inputBatch = std::move(input_);
  const auto numRows = static_cast<int64_t>(inputBatch->size());

  // Count how many rows we will keep: rows at indices 0, stride_, 2*stride_, ...
  int64_t numSelected = 0;
  for (int64_t i = 0; i < numRows; i += stride_) {
    ++numSelected;
  }

  if (numSelected == numRows) {
    // stride == 1: pass every row through unchanged.
    return inputBatch;
  }
  if (numSelected == 0) {
    return inputBatch; // empty batch — nothing to do
  }

  // Build an index buffer selecting rows 0, stride_, 2*stride_, ...
  facebook::velox::BufferPtr indices =
      facebook::velox::allocateIndices(static_cast<facebook::velox::vector_size_t>(numSelected), pool());
  auto* rawIndices = indices->asMutable<facebook::velox::vector_size_t>();
  facebook::velox::vector_size_t idx = 0;
  for (int64_t i = 0; i < numRows; i += stride_) {
    rawIndices[idx++] = static_cast<facebook::velox::vector_size_t>(i);
  }

  return facebook::velox::exec::wrap(
      static_cast<facebook::velox::vector_size_t>(numSelected), std::move(indices), inputBatch);
}

} // namespace gluten

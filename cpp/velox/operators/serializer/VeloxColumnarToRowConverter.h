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

#include <arrow/memory_pool.h>
#include <arrow/type.h>

#include "operators/c2r/ColumnarToRow.h"
#include "velox/buffer/Buffer.h"
#include "velox/row/UnsafeRowFast.h"
#include "velox/vector/ComplexVector.h"

namespace gluten {

class VeloxColumnarToRowConverter final : public ColumnarToRowConverter {
 public:
  explicit VeloxColumnarToRowConverter(
      std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool,
      int64_t memThreshold)
      : ColumnarToRowConverter(), veloxPool_(veloxPool), memThreshold_(memThreshold) {}

  void convert(std::shared_ptr<ColumnarBatch> cb, int64_t startRow = 0) override;

 private:
  void refreshStates(facebook::velox::RowVectorPtr rowVector, int64_t startRow);

  std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool_;
  std::shared_ptr<facebook::velox::row::UnsafeRowFast> fast_;
  facebook::velox::BufferPtr veloxBuffers_;
  facebook::velox::RowVectorPtr rowVectorForUnsafeRow_;
  int64_t memThreshold_;
};

} // namespace gluten

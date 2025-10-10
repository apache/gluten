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
#include "bolt/buffer/Buffer.h"
#include "bolt/row/UnsafeRowFast.h"
#include "bolt/vector/ComplexVector.h"

namespace gluten {

class BoltColumnarToRowConverter final : public ColumnarToRowConverter {
 public:
  explicit BoltColumnarToRowConverter(
      std::shared_ptr<bytedance::bolt::memory::MemoryPool> boltPool,
      int64_t memThreshold)
      : ColumnarToRowConverter(), boltPool_(boltPool), memThreshold_(memThreshold) {}

  void convert(std::shared_ptr<ColumnarBatch> cb, int64_t startRow = 0) override;

 private:
  void refreshStates(bytedance::bolt::RowVectorPtr rowVector, int64_t startRow);

  std::shared_ptr<bytedance::bolt::memory::MemoryPool> boltPool_;
  std::shared_ptr<bytedance::bolt::row::UnsafeRowFast> fast_;
  bytedance::bolt::BufferPtr boltBuffers_;
  int64_t memThreshold_;
};

} // namespace gluten

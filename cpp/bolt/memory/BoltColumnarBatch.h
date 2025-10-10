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

#include "memory/ColumnarBatch.h"
#include "memory/BoltMemoryManager.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/arrow/Bridge.h"

namespace gluten {

class BoltColumnarBatch final : public ColumnarBatch {
 public:
  BoltColumnarBatch(bytedance::bolt::RowVectorPtr rowVector)
      : ColumnarBatch(rowVector->childrenSize(), rowVector->size()), rowVector_(rowVector) {}

  std::string getType() const override {
    return kType;
  }

  static std::shared_ptr<BoltColumnarBatch> from(
      bytedance::bolt::memory::MemoryPool* pool,
      std::shared_ptr<ColumnarBatch> cb);

  static std::shared_ptr<BoltColumnarBatch> compose(
      bytedance::bolt::memory::MemoryPool* pool,
      const std::vector<std::shared_ptr<ColumnarBatch>>& batches);

  int64_t numBytes() override;

  std::shared_ptr<ArrowSchema> exportArrowSchema() override;
  std::shared_ptr<ArrowArray> exportArrowArray() override;
  std::vector<char> toUnsafeRow(int32_t rowId) const override;
  std::shared_ptr<BoltColumnarBatch> select(
      bytedance::bolt::memory::MemoryPool* pool,
      const std::vector<int32_t>& columnIndices);
  bytedance::bolt::RowVectorPtr getRowVector() const;
  bytedance::bolt::RowVectorPtr getFlattenedRowVector();

  FLATTEN bool isCompositeLayout() const {
    return (rowVector_ && dynamic_cast<bytedance::bolt::CompositeRowVector*>(rowVector_.get()) != nullptr);
  }

 private:
  void ensureFlattened();

  bytedance::bolt::RowVectorPtr rowVector_ = nullptr;
  bool flattened_ = false;

  inline static const std::string kType{"bolt"};
};

} // namespace gluten

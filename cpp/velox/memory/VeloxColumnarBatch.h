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
#include "memory/VeloxMemoryManager.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/arrow/Bridge.h"

namespace gluten {

class VeloxColumnarBatch final : public ColumnarBatch {
 public:
  VeloxColumnarBatch(facebook::velox::RowVectorPtr rowVector)
      : ColumnarBatch(rowVector->childrenSize(), rowVector->size()), rowVector_(rowVector) {}

#ifdef GLUTEN_ENABLE_GPU
  // The batch may be CudfVector, it's childrenSize is not correct, many tests failed if correcting the value
  // https://github.com/facebookincubator/velox/pull/15629#discussion_r2581655771
  VeloxColumnarBatch(facebook::velox::RowVectorPtr rowVector, int32_t numColumns)
      : ColumnarBatch(numColumns, rowVector->size()), rowVector_(rowVector) {}
#endif

  std::string getType() const override {
    return kType;
  }

  static std::shared_ptr<VeloxColumnarBatch> from(
      facebook::velox::memory::MemoryPool* pool,
      std::shared_ptr<ColumnarBatch> cb);

  static std::shared_ptr<VeloxColumnarBatch> compose(
      facebook::velox::memory::MemoryPool* pool,
      const std::vector<std::shared_ptr<ColumnarBatch>>& batches);

  int64_t numBytes() override;

  std::shared_ptr<ArrowSchema> exportArrowSchema() override;
  std::shared_ptr<ArrowArray> exportArrowArray() override;
  std::vector<char> toUnsafeRow(int32_t rowId) const override;
  std::shared_ptr<VeloxColumnarBatch> select(
      facebook::velox::memory::MemoryPool* pool,
      const std::vector<int32_t>& columnIndices);
  facebook::velox::RowVectorPtr getRowVector() const;
  facebook::velox::RowVectorPtr getFlattenedRowVector();

 private:
  void ensureFlattened();

  facebook::velox::RowVectorPtr rowVector_ = nullptr;
  bool flattened_ = false;

  inline static const std::string kType{"velox"};
};

} // namespace gluten

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

#include <execinfo.h>
#include <signal.h>
#include <cstdlib>
#include <iostream>

namespace gluten {

class GpuBufferColumnarBatch final : public ColumnarBatch {
 public:
  GpuBufferColumnarBatch(
      facebook::velox::RowTypePtr rowType,
      std::vector<std::shared_ptr<arrow::Buffer>>&& buffers,
      int32_t numRows)
      : ColumnarBatch(rowType->children().size(), numRows), rowType_(rowType), buffers_(std::move(buffers)) {}

  std::string getType() const override {
    return kType;
  }

  const facebook::velox::RowTypePtr& getRowType() const {
    return rowType_;
  }

  const std::vector<std::shared_ptr<arrow::Buffer>>& buffers() const {
    return buffers_;
  }

  const std::shared_ptr<arrow::Buffer>& bufferAt(size_t i) const {
    return buffers_[i];
  }

  static std::shared_ptr<GpuBufferColumnarBatch> compose(
      arrow::MemoryPool* pool,
      const std::vector<std::shared_ptr<GpuBufferColumnarBatch>>& batches,
      int32_t numRows);

  int64_t numBytes() override;

  std::shared_ptr<ArrowSchema> exportArrowSchema(bool exportToLargeVarTypes = false) override;
  std::shared_ptr<ArrowArray> exportArrowArray(bool exportToLargeVarTypes = false) override;
  std::vector<char> toUnsafeRow(int32_t rowId) const override;

 private:
  inline static const std::string kType{"gpu"};
  facebook::velox::RowTypePtr rowType_;
  std::vector<std::shared_ptr<arrow::Buffer>> buffers_;
};

} // namespace gluten

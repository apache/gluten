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

#include "memory/ColumnarBatchIterator.h"
#include "memory/VeloxColumnarBatch.h"
#include "utils/Exception.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/vector/ComplexVector.h"

namespace gluten {

struct VeloxBatchResizeStats {
  int64_t copyRangesBatches{0};
  int64_t copyRangesOutputBatches{0};
  // Counts generic copies: RowVector::append when copyRanges is disabled and
  // RowVector::copy fallbacks when copyRanges is enabled.
  int64_t appendCopyBatches{0};
  int64_t copyRangesFallbackBatches{0};
};

class VeloxBatchResizer : public ColumnarBatchIterator {
 public:
  VeloxBatchResizer(
      facebook::velox::memory::MemoryPool* pool,
      int32_t minOutputBatchSize,
      int32_t maxOutputBatchSize,
      int64_t preferredBatchBytes,
      std::unique_ptr<ColumnarBatchIterator> in,
      bool enableCopyRanges = true,
      VeloxBatchResizeStats* stats = nullptr);

  std::shared_ptr<ColumnarBatch> next() override;

  int64_t spillFixedSize(int64_t size) override;

 private:
  facebook::velox::memory::MemoryPool* pool_;
  const int32_t minOutputBatchSize_;
  const int32_t maxOutputBatchSize_;
  const uint64_t preferredBatchBytes_;
  const bool enableCopyRanges_;
  std::unique_ptr<ColumnarBatchIterator> in_;
  VeloxBatchResizeStats* stats_;

  std::unique_ptr<ColumnarBatchIterator> next_ = nullptr;

  void appendToBuffer(facebook::velox::RowVectorPtr& buffer, const facebook::velox::RowVectorPtr& input);

  facebook::velox::RowVectorPtr copyBufferedInputs(const std::vector<facebook::velox::RowVectorPtr>& inputs);

  std::shared_ptr<ColumnarBatch> collectAndCopy(facebook::velox::RowVectorPtr firstInput, uint64_t numBytes);
};

} // namespace gluten

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

#include "BoltColumnarToRowConverter.h"
#include <bolt/common/base/SuccinctPrinter.h>
#include <cstdint>

#include "memory/BoltColumnarBatch.h"
#include "utils/Exception.h"
#include "bolt/row/UnsafeRowFast.h"

using namespace bytedance;

namespace gluten {

void BoltColumnarToRowConverter::refreshStates(bytedance::bolt::RowVectorPtr rowVector, int64_t startRow) {
  auto vectorLength = rowVector->size();
  numCols_ = rowVector->childrenSize();

  fast_ = std::make_unique<bolt::row::UnsafeRowFast>(rowVector);

  int64_t totalMemorySize;

  if (auto fixedRowSize = bolt::row::UnsafeRowFast::fixedRowSize(bolt::asRowType(rowVector->type()))) {
    auto rowSize = fixedRowSize.value();
    // make sure it has at least one row
    numRows_ = std::max<int32_t>(1, std::min<int64_t>(memThreshold_ / rowSize, vectorLength - startRow));
    totalMemorySize = numRows_ * rowSize;
  } else {
    // Calculate the first row size
    totalMemorySize = fast_->rowSize(startRow);

    auto endRow = startRow + 1;
    for (; endRow < vectorLength; ++endRow) {
      auto rowSize = fast_->rowSize(endRow);
      if (UNLIKELY(totalMemorySize + rowSize > memThreshold_)) {
        break;
      } else {
        totalMemorySize += rowSize;
      }
    }
    // Make sure the threshold is larger than the first row size
    numRows_ = endRow - startRow;
  }

  if (nullptr == boltBuffers_) {
    boltBuffers_ = bolt::AlignedBuffer::allocate<uint8_t>(totalMemorySize, boltPool_.get());
  } else if (boltBuffers_->capacity() < totalMemorySize) {
    bolt::AlignedBuffer::reallocate<uint8_t>(&boltBuffers_, totalMemorySize);
  }

  bufferAddress_ = boltBuffers_->asMutable<uint8_t>();
  memset(bufferAddress_, 0, sizeof(int8_t) * totalMemorySize);
}

void BoltColumnarToRowConverter::convert(std::shared_ptr<ColumnarBatch> cb, int64_t startRow) {
  auto boltBatch = BoltColumnarBatch::from(boltPool_.get(), cb);
  refreshStates(boltBatch->getFlattenedRowVector(), startRow);

  // Initialize the offsets_ , lengths_
  lengths_.clear();
  offsets_.clear();
  lengths_.resize(numRows_, 0);
  offsets_.resize(numRows_, 0);

  size_t offset = 0;
  for (auto i = 0; i < numRows_; ++i) {
    auto rowSize = fast_->serialize(startRow + i, reinterpret_cast<char*>(bufferAddress_ + offset));
    lengths_[i] = rowSize;
    if (i > 0) {
      offsets_[i] = offsets_[i - 1] + lengths_[i - 1];
    }
    offset += rowSize;
  }
}

} // namespace gluten

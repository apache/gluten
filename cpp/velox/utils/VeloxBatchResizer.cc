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

#include "VeloxBatchResizer.h"

namespace gluten {
namespace {

bool supportsCopyRanges(const facebook::velox::VectorPtr& vector) {
  if (vector == nullptr) {
    return false;
  }

  if (vector->isFlatEncoding()) {
    return true;
  }

  if (vector->encoding() != facebook::velox::VectorEncoding::Simple::ROW &&
      vector->encoding() != facebook::velox::VectorEncoding::Simple::ARRAY &&
      vector->encoding() != facebook::velox::VectorEncoding::Simple::MAP) {
    return false;
  }

  switch (vector->typeKind()) {
    case facebook::velox::TypeKind::ROW: {
      const auto* row = vector->as<facebook::velox::RowVector>();
      for (const auto& child : row->children()) {
        if (!supportsCopyRanges(child)) {
          return false;
        }
      }
      return true;
    }
    case facebook::velox::TypeKind::ARRAY: {
      const auto* array = vector->as<facebook::velox::ArrayVector>();
      return supportsCopyRanges(array->elements());
    }
    case facebook::velox::TypeKind::MAP: {
      const auto* map = vector->as<facebook::velox::MapVector>();
      return supportsCopyRanges(map->mapKeys()) && supportsCopyRanges(map->mapValues());
    }
    default:
      return false;
  }
}

bool supportsCopyRanges(const facebook::velox::RowVectorPtr& rowVector) {
  if (rowVector == nullptr || rowVector->encoding() != facebook::velox::VectorEncoding::Simple::ROW ||
      rowVector->mayHaveNulls()) {
    return false;
  }
  return supportsCopyRanges(std::static_pointer_cast<facebook::velox::BaseVector>(rowVector));
}

class SliceRowVector : public ColumnarBatchIterator {
 public:
  SliceRowVector(int32_t maxOutputBatchSize, facebook::velox::RowVectorPtr in)
      : maxOutputBatchSize_(maxOutputBatchSize), in_(in) {}

  std::shared_ptr<ColumnarBatch> next() override {
    int32_t remainingLength = in_->size() - cursor_;
    GLUTEN_CHECK(remainingLength >= 0, "Invalid state");
    if (remainingLength == 0) {
      return nullptr;
    }
    int32_t sliceLength = std::min(maxOutputBatchSize_, remainingLength);
    auto out = std::dynamic_pointer_cast<facebook::velox::RowVector>(in_->slice(cursor_, sliceLength));
    cursor_ += sliceLength;
    GLUTEN_CHECK(out != nullptr, "Invalid state");
    return std::make_shared<VeloxColumnarBatch>(out);
  }

 private:
  int32_t maxOutputBatchSize_;
  facebook::velox::RowVectorPtr in_;
  int32_t cursor_ = 0;
};
} // namespace

gluten::VeloxBatchResizer::VeloxBatchResizer(
    facebook::velox::memory::MemoryPool* pool,
    int32_t minOutputBatchSize,
    int32_t maxOutputBatchSize,
    int64_t preferredBatchBytes,
    std::unique_ptr<ColumnarBatchIterator> in,
    bool enableCopyRanges,
    VeloxBatchResizeStats* stats)
    : pool_(pool),
      minOutputBatchSize_(minOutputBatchSize),
      maxOutputBatchSize_(maxOutputBatchSize),
      preferredBatchBytes_(static_cast<uint64_t>(preferredBatchBytes)),
      enableCopyRanges_(enableCopyRanges),
      in_(std::move(in)),
      stats_(stats) {
  GLUTEN_CHECK(
      minOutputBatchSize_ > 0 && maxOutputBatchSize_ > 0,
      "Either minOutputBatchSize or maxOutputBatchSize should be larger than 0");
}

void VeloxBatchResizer::appendToBuffer(
    facebook::velox::RowVectorPtr& buffer,
    const facebook::velox::RowVectorPtr& input) {
  buffer->append(input.get());
  if (stats_ != nullptr) {
    ++stats_->appendCopyBatches;
  }
}

facebook::velox::RowVectorPtr VeloxBatchResizer::copyBufferedInputs(
    const std::vector<facebook::velox::RowVectorPtr>& inputs) {
  GLUTEN_CHECK(!inputs.empty(), "Cannot copy empty inputs");

  facebook::velox::vector_size_t totalRows = 0;
  for (const auto& input : inputs) {
    totalRows += input->size();
  }

  auto buffer = facebook::velox::RowVector::createEmpty(inputs[0]->type(), pool_);
  buffer->resize(totalRows);

  bool usedCopyRanges = false;
  facebook::velox::vector_size_t offset = 0;
  for (const auto& input : inputs) {
    if (supportsCopyRanges(input)) {
      const facebook::velox::BaseVector::CopyRange range{0, offset, input->size()};
      for (auto channel = 0; channel < input->children().size(); ++channel) {
        buffer->childAt(channel)->copyRanges(input->childAt(channel)->loadedVector(), folly::Range(&range, 1));
      }
      usedCopyRanges = true;
      if (stats_ != nullptr) {
        ++stats_->copyRangesBatches;
      }
    } else {
      buffer->copy(input.get(), offset, 0, input->size());
      if (stats_ != nullptr) {
        ++stats_->copyRangesFallbackBatches;
        ++stats_->appendCopyBatches;
      }
    }
    offset += input->size();
  }

  if (usedCopyRanges && stats_ != nullptr) {
    ++stats_->copyRangesOutputBatches;
  }
  return buffer;
}

std::shared_ptr<ColumnarBatch> VeloxBatchResizer::collectAndCopy(
    facebook::velox::RowVectorPtr firstInput,
    uint64_t numBytes) {
  std::vector<facebook::velox::RowVectorPtr> inputs;
  inputs.push_back(std::move(firstInput));
  facebook::velox::vector_size_t bufferedRows = inputs.back()->size();

  std::shared_ptr<ColumnarBatch> cb;
  for (cb = in_->next(); cb != nullptr; cb = in_->next()) {
    auto vb = VeloxColumnarBatch::from(pool_, cb);
    auto rv = vb->getRowVector();
    uint64_t addedBytes = cb->numBytes();
    if (bufferedRows + rv->size() > maxOutputBatchSize_ ||
        numBytes + addedBytes > static_cast<uint64_t>(preferredBatchBytes_)) {
      GLUTEN_CHECK(next_ == nullptr, "Invalid state");
      next_ = std::make_unique<SliceRowVector>(maxOutputBatchSize_, rv);
      return std::make_shared<VeloxColumnarBatch>(copyBufferedInputs(inputs));
    }

    numBytes += addedBytes;
    bufferedRows += rv->size();
    inputs.push_back(std::move(rv));
    if (bufferedRows >= minOutputBatchSize_) {
      break;
    }
  }

  return std::make_shared<VeloxColumnarBatch>(copyBufferedInputs(inputs));
}

std::shared_ptr<ColumnarBatch> VeloxBatchResizer::next() {
  if (next_) {
    auto next = next_->next();
    if (next != nullptr) {
      return next;
    }
    // Cached output was drained. Continue reading data from input iterator.
    next_ = nullptr;
  }

  auto cb = in_->next();
  if (cb == nullptr) {
    // Input iterator was drained.
    return nullptr;
  }

  uint64_t numBytes = cb->numBytes();
  if (cb->numRows() < minOutputBatchSize_ && numBytes <= preferredBatchBytes_) {
    auto vb = VeloxColumnarBatch::from(pool_, cb);
    auto rv = vb->getRowVector();
    if (enableCopyRanges_) {
      return collectAndCopy(std::move(rv), numBytes);
    }

    auto buffer = facebook::velox::RowVector::createEmpty(rv->type(), pool_);
    appendToBuffer(buffer, rv);

    for (cb = in_->next(); cb != nullptr; cb = in_->next()) {
      vb = VeloxColumnarBatch::from(pool_, cb);
      rv = vb->getRowVector();
      uint64_t addedBytes = cb->numBytes();
      if (buffer->size() + rv->size() > maxOutputBatchSize_ ||
          numBytes + addedBytes > static_cast<uint64_t>(preferredBatchBytes_)) {
        GLUTEN_CHECK(next_ == nullptr, "Invalid state");
        next_ = std::make_unique<SliceRowVector>(maxOutputBatchSize_, rv);
        return std::make_shared<VeloxColumnarBatch>(buffer);
      }
      numBytes += addedBytes;
      appendToBuffer(buffer, rv);
      if (buffer->size() >= minOutputBatchSize_) {
        // Buffer is full.
        break;
      }
      // Call reset manully to potentially release memory
      rv.reset();
      vb.reset();
      cb.reset();
    }
    return std::make_shared<VeloxColumnarBatch>(buffer);
  }

  if (cb->numRows() > maxOutputBatchSize_) {
    auto vb = VeloxColumnarBatch::from(pool_, cb);
    auto rv = vb->getRowVector();
    GLUTEN_CHECK(next_ == nullptr, "Invalid state");
    next_ = std::make_unique<SliceRowVector>(maxOutputBatchSize_, rv);
    auto next = next_->next();
    GLUTEN_CHECK(next != nullptr, "Invalid state");
    return next;
  }

  // Fast flush path.
  return cb;
}

int64_t VeloxBatchResizer::spillFixedSize(int64_t size) {
  return in_->spillFixedSize(size);
}

} // namespace gluten

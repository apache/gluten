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

#include "BoltColumnarBatchSerializer.h"

#include <arrow/buffer.h>

#include <cstring>
#include <utility>

#include "memory/ArrowMemory.h"
#include "memory/BoltColumnarBatch.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/arrow/Bridge.h"
#include "bolt/common/memory/ByteStream.h"

#include <iostream>

using namespace bytedance::bolt;

namespace gluten {
namespace {

std::unique_ptr<ByteInputStream> toByteStream(uint8_t* data, int32_t size) {
  std::vector<ByteRange> byteRanges;
  byteRanges.push_back(ByteRange{data, size, 0});
  auto byteStream = std::make_unique<ByteInputStream>(byteRanges);
  return byteStream;
}

} // namespace

BoltColumnarBatchSerializer::BoltColumnarBatchSerializer(
    arrow::MemoryPool* arrowPool,
    std::shared_ptr<memory::MemoryPool> boltPool,
    struct ArrowSchema* cSchema)
    : ColumnarBatchSerializer(arrowPool), boltPool_(std::move(boltPool)) {
  // serializeColumnarBatches don't need rowType_
  if (cSchema != nullptr) {
    rowType_ = asRowType(importFromArrow(*cSchema));
    ArrowSchemaRelease(cSchema); // otherwise the c schema leaks memory
  }
  serde_ = std::make_unique<serializer::presto::PrestoVectorSerde>();
  options_.useLosslessTimestamp = true;
}

void BoltColumnarBatchSerializer::append(const std::shared_ptr<ColumnarBatch>& batch) {
  batches_.push_back(batch);
  serializedBuffer_.reset();
}

int64_t BoltColumnarBatchSerializer::maxSerializedSize() {
  if (serializedBuffer_ == nullptr) {
    serializedBuffer_ = serializeColumnarBatches(batches_);
  }
  return serializedBuffer_->size();
}

void BoltColumnarBatchSerializer::serializeTo(uint8_t* address, int64_t size) {
  if (serializedBuffer_ == nullptr) {
    serializedBuffer_ = serializeColumnarBatches(batches_);
  }
  GLUTEN_CHECK(
      size >= serializedBuffer_->size(),
      "The target buffer size is insufficient: " + std::to_string(size) + " vs." +
          std::to_string(serializedBuffer_->size()));
  std::memcpy(address, serializedBuffer_->data(), serializedBuffer_->size());
}

std::shared_ptr<arrow::Buffer> BoltColumnarBatchSerializer::serializeColumnarBatches(
    const std::vector<std::shared_ptr<ColumnarBatch>>& batches) {
  BOLT_DCHECK(batches.size() != 0, "Should serialize at least 1 vector");
  const std::shared_ptr<BoltColumnarBatch>& vb = BoltColumnarBatch::from(boltPool_.get(), batches[0]);
  auto firstRowVector = vb->getRowVector();
  auto numRows = firstRowVector->size();
  auto arena = std::make_unique<StreamArena>(boltPool_.get());
  auto rowType = asRowType(firstRowVector->type());
  auto serializer = serde_->createSerializer(rowType, numRows, arena.get(), &options_);
  for (auto& batch : batches) {
    auto rowVector = BoltColumnarBatch::from(boltPool_.get(), batch)->getRowVector();
    const IndexRange allRows{0, rowVector->size()};
    serializer->append(rowVector, folly::Range(&allRows, 1));
  }

  std::shared_ptr<arrow::Buffer> valueBuffer;
  GLUTEN_ASSIGN_OR_THROW(valueBuffer, arrow::AllocateResizableBuffer(serializer->maxSerializedSize(), arrowPool_));
  auto output = std::make_shared<arrow::io::FixedSizeBufferWriter>(valueBuffer);
  serializer::presto::PrestoOutputStreamListener listener;
  ArrowFixedSizeBufferOutputStream out(output, &listener);
  serializer->flush(&out);
  GLUTEN_THROW_NOT_OK(output->Close());
  return valueBuffer;
}

std::shared_ptr<ColumnarBatch> BoltColumnarBatchSerializer::deserialize(uint8_t* data, int32_t size) {
  RowVectorPtr result;
  auto byteStream = toByteStream(data, size);
  serde_->deserialize(byteStream.get(), boltPool_.get(), rowType_, &result, &options_);
  return std::make_shared<BoltColumnarBatch>(result);
}

} // namespace gluten

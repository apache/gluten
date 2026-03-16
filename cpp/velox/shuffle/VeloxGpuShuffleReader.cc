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

#include "VeloxGpuShuffleReader.h"

#include <arrow/array/array_binary.h>
#include <arrow/io/buffered.h>

#include "memory/GpuBufferColumnarBatch.h"
#include "memory/VeloxColumnarBatch.h"
#include "shuffle/Payload.h"
#include "shuffle/Utils.h"
#include "utils/CachedBatchQueue.h"
#include "utils/Common.h"
#include "utils/Macros.h"
#include "utils/Timer.h"

#include <algorithm>

using namespace facebook::velox;

namespace gluten {

namespace {

arrow::Result<BlockType> readBlockType(arrow::io::InputStream* inputStream) {
  BlockType type;
  ARROW_ASSIGN_OR_RAISE(auto bytes, inputStream->Read(sizeof(BlockType), &type));
  if (bytes == 0) {
    // Reach EOS.
    return BlockType::kEndOfStream;
  }
  return type;
}

} // namespace

VeloxGpuHashShuffleReaderDeserializer::VeloxGpuHashShuffleReaderDeserializer(
    const std::shared_ptr<StreamReader>& streamReader,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::util::Codec>& codec,
    const facebook::velox::RowTypePtr& rowType,
    int64_t readerBufferSize,
    VeloxMemoryManager* memoryManager,
    int64_t& deserializeTime,
    int64_t& decompressTime)
    : streamReader_(streamReader),
      schema_(schema),
      codec_(codec),
      rowType_(rowType),
      readerBufferSize_(readerBufferSize),
      memoryManager_(memoryManager),
      deserializeTime_(deserializeTime),
      decompressTime_(decompressTime) {
  batchQueue_ = std::make_unique<CachedBatchQueue<GpuBufferColumnarBatch>>(1L << 30);

  const size_t numThreads = std::max(1u, std::thread::hardware_concurrency());
  activeReaders_.store(numThreads);
  LOG(WARNING) << "Using " << numThreads << " threads for deserialization";

  // Create multiple reader threads
  readerThreads_.reserve(numThreads);
  for (size_t i = 0; i < numThreads; ++i) {
    readerThreads_.emplace_back([this]() { read(); });
  }
}

VeloxGpuHashShuffleReaderDeserializer::~VeloxGpuHashShuffleReaderDeserializer() {
  decompressTime_ += decompressTimeCounter_.load(std::memory_order_relaxed);
  deserializeTime_ += deserializeTimeCounter_.load(std::memory_order_relaxed);
  stopReaders_.store(true, std::memory_order_release);

  for (auto& thread : readerThreads_) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

void VeloxGpuHashShuffleReaderDeserializer::read() {
  std::shared_ptr<arrow::io::InputStream> inputStream = nullptr;

  while (!stopReaders_.load(std::memory_order_acquire)) {
    if (inputStream == nullptr) {
      std::lock_guard<std::mutex> lockGuard(mtx_);
      auto rawStream = streamReader_->readNextStream(memoryManager_->defaultArrowMemoryPool());
      if (rawStream == nullptr) {
        // No more streams available.
        break;
      }

      GLUTEN_ASSIGN_OR_THROW(
          inputStream,
          arrow::io::BufferedInputStream::Create(
              readerBufferSize_, memoryManager_->defaultArrowMemoryPool(), std::move(rawStream)));
    }

    GLUTEN_ASSIGN_OR_THROW(auto blockType, readBlockType(inputStream.get()));

    if (blockType == BlockType::kEndOfStream) {
      GLUTEN_THROW_NOT_OK(inputStream->Close());
      inputStream = nullptr;
      continue;
    }

    if (blockType != BlockType::kPlainPayload) {
      throw GlutenException(fmt::format("Unsupported block type: {}", static_cast<int32_t>(blockType)));
    }

    uint32_t numRows = 0;
    int64_t localDeserializeTime = 0;
    int64_t localDecompressTime = 0;

    GLUTEN_ASSIGN_OR_THROW(
        auto arrowBuffers,
        BlockPayload::deserialize(
            inputStream.get(),
            codec_,
            memoryManager_->defaultArrowMemoryPool(),
            numRows,
            localDeserializeTime,
            localDecompressTime));

    deserializeTimeCounter_.fetch_add(localDeserializeTime, std::memory_order_relaxed);
    decompressTimeCounter_.fetch_add(localDecompressTime, std::memory_order_relaxed);

    auto batch =
        std::make_shared<GpuBufferColumnarBatch>(rowType_, std::move(arrowBuffers), static_cast<int32_t>(numRows));

    // Put batch into queue.
    batchQueue_->put(batch);
  }

  // Close input stream if it's still open.
  if (inputStream != nullptr) {
    GLUTEN_THROW_NOT_OK(inputStream->Close());
  }

  // Decrement active reader count.
  if (activeReaders_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    batchQueue_->noMoreBatches();
  }
}

std::shared_ptr<ColumnarBatch> VeloxGpuHashShuffleReaderDeserializer::next() {
  return batchQueue_->get();
}

} // namespace gluten

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

#include "shuffle/VeloxGpuAsyncShuffleReader.h"
#include "compute/VeloxBackend.h"
#include "shuffle/Payload.h"

#include <arrow/io/buffered.h>
#include <algorithm>

namespace gluten {

namespace {

template <typename T>
class AsyncShuffleReaderIterator : public ColumnarBatchIterator {
 public:
  explicit AsyncShuffleReaderIterator(CachedBatchQueue<T>* batchQueue) : batchQueue_(batchQueue) {}

  std::shared_ptr<ColumnarBatch> next() override {
    return batchQueue_->get();
  }

 private:
  CachedBatchQueue<T>* batchQueue_;
};

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

VeloxGpuAsyncHashShuffleReaderDeserializer::VeloxGpuAsyncHashShuffleReaderDeserializer(
    const std::shared_ptr<StreamReader>& streamReader,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::util::Codec>& codec,
    const facebook::velox::RowTypePtr& rowType,
    int64_t readerBufferSize,
    int64_t maxPrefetchBytes,
    VeloxMemoryManager* memoryManager,
    int64_t& deserializeTime,
    int64_t& decompressTime)
    : streamReader_(streamReader),
      schema_(schema),
      codec_(codec),
      rowType_(rowType),
      readerBufferSize_(readerBufferSize),
      maxPrefetchBytes_(maxPrefetchBytes),
      memoryManager_(memoryManager),
      deserializeTime_(deserializeTime),
      decompressTime_(decompressTime),
      threadPool_(VeloxBackend::get()->getReaderThreadPool()) {}

VeloxGpuAsyncHashShuffleReaderDeserializer::~VeloxGpuAsyncHashShuffleReaderDeserializer() {
  // Wait for all reader threads to complete before destroying
  if (!isStopped()) {
    stop();
  }

  decompressTime_ += decompressTimeCounter_.load(std::memory_order_relaxed);
  deserializeTime_ += deserializeTimeCounter_.load(std::memory_order_relaxed);
}

std::unique_ptr<ColumnarBatchIterator> VeloxGpuAsyncHashShuffleReaderDeserializer::deserializeStreams() {
  batchQueue_ = std::make_unique<CachedBatchQueue<GpuBufferColumnarBatch>>(maxPrefetchBytes_);

  if (!threadPool_) {
    throw GlutenException("Thread pool must be provided to VeloxGpuHashShuffleReaderDeserializer");
  }

  const size_t numThreads = threadPool_->getNumThreads();
  activeReaders_.store(numThreads);

  // Submit reader tasks to the thread pool.
  std::vector<ReaderThreadPool::Task> tasks;
  tasks.reserve(numThreads);
  for (size_t i = 0; i < numThreads; ++i) {
    tasks.emplace_back([this]() { read(); });
  }
  threadPool_->submitBatch(std::move(tasks), priority_);

  if (priority_ == 0) {
    threadPool_->start();
  }

  return std::make_unique<AsyncShuffleReaderIterator<GpuBufferColumnarBatch>>(batchQueue_.get());
}

void VeloxGpuAsyncHashShuffleReaderDeserializer::stop() {
  // Signal threads to stop if not already stopped.
  stop_.store(true, std::memory_order_release);
  // Unblock any reader threads that might be waiting in CachedBatchQueue::put().
  if (batchQueue_) {
    batchQueue_->noMoreBatches();
  }
  // Wait for all reader threads to complete.
  std::unique_lock<std::mutex> lock(completionMtx_);
  completionCV_.wait(lock, [this] { return activeReaders_.load(std::memory_order_acquire) == 0; });
}

void VeloxGpuAsyncHashShuffleReaderDeserializer::read() {
  try {
    std::shared_ptr<arrow::io::InputStream> inputStream = nullptr;

    while (true) {
      // Check if stop has been called, or if a sibling producer encountered an error.
      if (stop_.load(std::memory_order_acquire) || batchQueue_->hasException()) {
        break;
      }

      if (inputStream == nullptr) {
        std::lock_guard<std::mutex> lockGuard(readStreamMtx_);
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

      // Put batch into queue. Blocked if queue is full.
      batchQueue_->put(batch);
    }

    if (inputStream != nullptr) {
      GLUTEN_THROW_NOT_OK(inputStream->Close());
    }
  } catch (...) {
    // Forward the exception to the consumer thread via the queue.
    if (batchQueue_) {
      batchQueue_->setException(std::current_exception());
    }
  }

  // Decrement activeReaders_; the last reader signals noMoreBatches() and wakes the completion waiter.
  if (activeReaders_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    if (batchQueue_) {
      batchQueue_->noMoreBatches();
    }
    completionCV_.notify_all();
  }
}

bool VeloxGpuAsyncHashShuffleReaderDeserializer::isStopped() const {
  return stop_.load(std::memory_order_acquire);
}

} // namespace gluten

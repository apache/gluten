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

#include "memory/GpuBufferColumnarBatch.h"
#include "memory/VeloxMemoryManager.h"
#include "shuffle/ReaderThreadPool.h"
#include "shuffle/VeloxShuffleReader.h"
#include "utils/CachedBatchQueue.h"

#include "velox/type/Type.h"

#include <atomic>
#include <mutex>

namespace gluten {

/// Read and convert the serialized buffers to cudf table.
/// Multi-threaded deserializer that uses producer threads to fetch and deserialize batches.
class VeloxGpuAsyncHashShuffleReaderDeserializer final : public ShuffleReaderDeserializer {
 public:
  VeloxGpuAsyncHashShuffleReaderDeserializer(
      const std::shared_ptr<StreamReader>& streamReader,
      const std::shared_ptr<arrow::Schema>& schema,
      const std::shared_ptr<arrow::util::Codec>& codec,
      const facebook::velox::RowTypePtr& rowType,
      int64_t readerBufferSize,
      int64_t maxPrefetchBytes,
      VeloxMemoryManager* memoryManager,
      int64_t& deserializeTime,
      int64_t& decompressTime);

  ~VeloxGpuAsyncHashShuffleReaderDeserializer() override;

  std::unique_ptr<ColumnarBatchIterator> deserializeStreams() override;

  void stop() override;

 private:
  // Reader thread function that deserializes batches.
  void read();

  bool isStopped() const;

  std::shared_ptr<StreamReader> streamReader_;
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<arrow::util::Codec> codec_;
  facebook::velox::RowTypePtr rowType_;
  int64_t readerBufferSize_;
  int64_t maxPrefetchBytes_;
  VeloxMemoryManager* memoryManager_;

  int32_t priority_{0};

  int64_t& deserializeTime_;
  int64_t& decompressTime_;

  ReaderThreadPool* threadPool_;

  std::unique_ptr<CachedBatchQueue<GpuBufferColumnarBatch>> batchQueue_;
  std::atomic<int> activeReaders_{0};

  std::mutex readStreamMtx_;

  std::atomic<bool> stop_{false};

  std::mutex completionMtx_;
  std::condition_variable completionCV_;

  std::atomic<int64_t> deserializeTimeCounter_{0};
  std::atomic<int64_t> decompressTimeCounter_{0};
};
} // namespace gluten

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
#include "shuffle/Payload.h"
#include "shuffle/ShuffleReader.h"
#include "utils/CachedBatchQueue.h"

#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

#include <atomic>
#include <mutex>
#include <thread>
#include <vector>

namespace gluten {

/// Convert the buffers to cudf table.
/// Multi-threaded deserializer that uses producer threads to pre-fetch and deserialize batches.
class VeloxGpuHashShuffleReaderDeserializer final : public ColumnarBatchIterator {
 public:
  VeloxGpuHashShuffleReaderDeserializer(
      const std::shared_ptr<StreamReader>& streamReader,
      const std::shared_ptr<arrow::Schema>& schema,
      const std::shared_ptr<arrow::util::Codec>& codec,
      const facebook::velox::RowTypePtr& rowType,
      int64_t readerBufferSize,
      VeloxMemoryManager* memoryManager,
      int64_t& deserializeTime,
      int64_t& decompressTime);

  ~VeloxGpuHashShuffleReaderDeserializer() override;

  std::shared_ptr<ColumnarBatch> next() override;

 private:
  // Reader thread function that deserializes batches.
  void read();

  std::shared_ptr<StreamReader> streamReader_;
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<arrow::util::Codec> codec_;
  facebook::velox::RowTypePtr rowType_;
  int64_t readerBufferSize_;
  VeloxMemoryManager* memoryManager_;

  int64_t& deserializeTime_;
  int64_t& decompressTime_;

  std::atomic<int64_t> deserializeTimeCounter_{0};
  std::atomic<int64_t> decompressTimeCounter_{0};

  std::vector<std::thread> readerThreads_;
  std::unique_ptr<CachedBatchQueue<GpuBufferColumnarBatch>> batchQueue_;
  std::atomic<bool> stopReaders_{false};
  std::atomic<int> activeReaders_{0};

  std::mutex mtx_;
};
} // namespace gluten

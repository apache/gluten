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

#include "memory/MemoryManager.h"
#include "memory/Reclaimable.h"
#include "shuffle/Options.h"
#include "shuffle/Payload.h"
#include "shuffle/Spill.h"

namespace gluten {

struct Evict {
  enum type { kCache, kSpill };
};

class PartitionWriter : public Reclaimable {
 public:
  PartitionWriter(uint32_t numPartitions, std::unique_ptr<arrow::util::Codec> codec, MemoryManager* memoryManager)
      : numPartitions_(numPartitions), codec_(std::move(codec)), memoryManager_(memoryManager) {
    payloadPool_ = memoryManager->getOrCreateArrowMemoryPool("PartitionWriter.cached_payload");
  }

  static inline std::string typeToString(PartitionWriterType type) {
    switch (type) {
      case PartitionWriterType::kLocal:
        return "LocalPartitionWriter";
      case PartitionWriterType::kRss:
        return "RssPartitionWriter";
    }
    GLUTEN_UNREACHABLE();
  }

  ~PartitionWriter() override = default;

  virtual arrow::Status stop(ShuffleWriterMetrics* metrics, int64_t& evictBytes) = 0;

  /// Evict buffers for `partitionId` partition.
  virtual arrow::Status hashEvict(
      uint32_t partitionId,
      std::unique_ptr<InMemoryPayload> inMemoryPayload,
      Evict::type evictType,
      bool reuseBuffers,
      int64_t& evictBytes) = 0;

  virtual arrow::Status sortEvict(
      uint32_t partitionId,
      std::unique_ptr<InMemoryPayload> inMemoryPayload,
      bool isFinal,
      int64_t& evictBytes) = 0;

  virtual arrow::Status
  evict(uint32_t partitionId, std::unique_ptr<BlockPayload> blockPayload, bool stop, int64_t& evictBytes) = 0;

  uint64_t cachedPayloadSize() {
    return payloadPool_->bytes_allocated();
  }

  virtual bool enableTypeAwareCompress() const {
    return false;
  }

 protected:
  uint32_t numPartitions_;
  std::unique_ptr<arrow::util::Codec> codec_;
  MemoryManager* memoryManager_;

  // Memory Pool used to track memory allocation of partition payloads.
  // The actual allocation is delegated to options_.memoryPool.
  std::shared_ptr<arrow::MemoryPool> payloadPool_;

  int64_t compressTime_{0};
  int64_t spillTime_{0};
  int64_t writeTime_{0};
};

} // namespace gluten

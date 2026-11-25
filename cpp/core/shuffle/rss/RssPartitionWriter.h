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

#include <arrow/io/api.h>
#include <arrow/memory_pool.h>

#include "shuffle/PartitionWriter.h"
#include "shuffle/rss/RssClient.h"
#include "utils/Macros.h"

namespace gluten {

class RssPartitionWriter final : public PartitionWriter {
 public:
  RssPartitionWriter(
      const uint32_t numPartitions,
      std::unique_ptr<arrow::util::Codec> codec,
      MemoryManager* memoryManager,
      const std::shared_ptr<RssPartitionWriterOptions>& options,
      const std::shared_ptr<RssClient>& rssClient)
      : PartitionWriter(numPartitions, std::move(codec), memoryManager), options_(options), rssClient_(rssClient) {
    init();
  }

  arrow::Status hashEvict(
      uint32_t partitionId,
      std::unique_ptr<InMemoryPayload> inMemoryPayload,
      Evict::type evictType,
      bool reuseBuffers,
      int64_t& evictBytes) override;

  arrow::Status sortEvict(
      uint32_t partitionId,
      std::unique_ptr<InMemoryPayload> inMemoryPayload,
      bool isFinal,
      int64_t& evictBytes) override;

  arrow::Status evict(uint32_t partitionId, std::unique_ptr<BlockPayload> blockPayload, bool stop, int64_t& evictBytes)
      override;

  arrow::Status reclaimFixedSize(int64_t size, int64_t* actual) override;

  arrow::Status stop(ShuffleWriterMetrics* metrics, int64_t& evictBytes) override;

 private:
  void init();

  arrow::Status doEvict(uint32_t partitionId, std::unique_ptr<InMemoryPayload> inMemoryPayload, int64_t& evictBytes);

  // Makes sure `pushBuffer_` can hold exactly `size` bytes, growing it when needed.
  arrow::Status reservePushBuffer(int64_t size);

  // Allocates `buffer` from the unmanaged pool on first use. See `doEvict` for why the scratch
  // buffers must not come from `payloadPool_`.
  arrow::Status ensureScratchBuffer(std::shared_ptr<arrow::ResizableBuffer>& buffer);

  // Lower bound for `pushBuffer_`, chosen to stay above the minimum capacity
  // `arrow::io::BufferOutputStream` would round up to on its own.
  static constexpr int64_t kMinPushBufferSize = 1024;

  std::shared_ptr<RssPartitionWriterOptions> options_;
  std::shared_ptr<RssClient> rssClient_;

  // Staging buffer that every hash-evicted payload is serialized into before being handed to the
  // RSS client. It is reused across payloads and only ever grows: allocating one buffer per
  // payload would both cost an allocation per push and tie the allocation size to
  // `celeborn.client.push.buffer.max.size`, making that (purely client side) knob expensive to
  // raise.
  std::shared_ptr<arrow::ResizableBuffer> pushBuffer_{nullptr};

  // Staging buffer that compression writes into, reused across payloads for the same reason as
  // `pushBuffer_`. Both are scratch space that lives only until the push below returns, both are
  // taken from the unmanaged pool because `doEvict` is reachable from the memory reclaim callback,
  // and both only ever grow - so what they cost the executor is one buffer per writer sized to its
  // largest payload, not an allocation per push. See `doEvict`.
  std::shared_ptr<arrow::ResizableBuffer> compressBuffer_{nullptr};

  // Part of `compressTime_` that was measured inside the `spillTime_` timer and therefore has to
  // be subtracted from it. Only the sort path does that.
  int64_t compressTimeInSpill_{0};

  std::vector<int64_t> bytesEvicted_;
  std::vector<int64_t> rawPartitionLengths_;

  bool shouldInitializeOs_{true};
  std::shared_ptr<arrow::io::BufferOutputStream> rssOs_{nullptr};
  std::shared_ptr<ShuffleCompressedOutputStream> compressedOs_{nullptr};
};

} // namespace gluten

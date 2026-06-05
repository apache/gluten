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

#include <numeric>

#include "shuffle/Payload.h"
#include "shuffle/Utils.h"
#include "shuffle/rss/RssPartitionWriter.h"
#include "utils/Timer.h"

namespace gluten {

void RssPartitionWriter::init() {
  bytesEvicted_.resize(numPartitions_, 0);
  rawPartitionLengths_.resize(numPartitions_, 0);
}

arrow::Status RssPartitionWriter::stop(ShuffleWriterMetrics* metrics, int64_t& /* evictBytes */) {
  spillTime_ -= compressTime_;
  rssClient_->stop();

  auto totalBytesEvicted = std::accumulate(bytesEvicted_.begin(), bytesEvicted_.end(), 0LL);
  // Populate metrics.
  metrics->totalCompressTime += compressTime_;
  metrics->totalEvictTime += spillTime_;
  metrics->totalBytesEvicted += totalBytesEvicted;
  metrics->totalBytesWritten += totalBytesEvicted;
  metrics->partitionLengths = std::move(bytesEvicted_);
  metrics->rawPartitionLengths = std::move(rawPartitionLengths_);
  return arrow::Status::OK();
}

arrow::Status RssPartitionWriter::reclaimFixedSize(int64_t size, int64_t* actual) {
  *actual = 0;
  return arrow::Status::OK();
}

arrow::Status RssPartitionWriter::hashEvict(
    uint32_t partitionId,
    std::unique_ptr<InMemoryPayload> inMemoryPayload,
    Evict::type evictType,
    bool reuseBuffers,
    int64_t& evictBytes) {
  return doEvict(partitionId, std::move(inMemoryPayload), evictBytes);
}

arrow::Status RssPartitionWriter::sortEvict(
    uint32_t partitionId,
    std::unique_ptr<InMemoryPayload> inMemoryPayload,
    bool isFinal,
    int64_t& evictBytes) {
  ScopedTimer timer(&spillTime_);
  rawPartitionLengths_[partitionId] += inMemoryPayload->rawSize();
  if (shouldInitializeOs_) {
    ARROW_ASSIGN_OR_RAISE(
        rssOs_, arrow::io::BufferOutputStream::Create(options_->pushBufferMaxSize, arrow::default_memory_pool()));
    if (codec_ != nullptr) {
      ARROW_ASSIGN_OR_RAISE(
          compressedOs_,
          ShuffleCompressedOutputStream::Make(
              codec_.get(), options_->compressionBufferSize, rssOs_, arrow::default_memory_pool()));
    }
    shouldInitializeOs_ = false;
  }
  if (compressedOs_ != nullptr) {
    RETURN_NOT_OK(inMemoryPayload->serialize(compressedOs_.get()));
  } else {
    RETURN_NOT_OK(inMemoryPayload->serialize(rssOs_.get()));
  }
  if (inMemoryPayload->numRows() > 0) {
    // Push data to rss only when there are complete rows.
    if (compressedOs_ != nullptr) {
      RETURN_NOT_OK(compressedOs_->Flush());
      RETURN_NOT_OK(compressedOs_->Close());
      compressTime_ += compressedOs_->compressTime();
    }
    ARROW_ASSIGN_OR_RAISE(const auto buffer, rssOs_->Finish());
    auto delta = rssClient_->pushPartitionData(partitionId, buffer->data_as<char>(), buffer->size());
    bytesEvicted_[partitionId] += delta;
    evictBytes += delta;
    shouldInitializeOs_ = true;
  }

  return arrow::Status::OK();
}

arrow::Status
RssPartitionWriter::evict(uint32_t partitionId, std::unique_ptr<BlockPayload> blockPayload, bool, int64_t& evictBytes) {
  rawPartitionLengths_[partitionId] += blockPayload->rawSize();
  ScopedTimer timer(&spillTime_);
  ARROW_ASSIGN_OR_RAISE(auto buffer, blockPayload->readBufferAt(0));
  auto delta = rssClient_->pushPartitionData(partitionId, buffer->data_as<char>(), buffer->size());
  bytesEvicted_[partitionId] += delta;
  evictBytes += delta;
  return arrow::Status::OK();
}

arrow::Status RssPartitionWriter::doEvict(
    uint32_t partitionId,
    std::unique_ptr<InMemoryPayload> inMemoryPayload,
    int64_t& evictBytes) {
  rawPartitionLengths_[partitionId] += inMemoryPayload->rawSize();
  auto payloadType = codec_ ? Payload::Type::kCompressed : Payload::Type::kUncompressed;
  ARROW_ASSIGN_OR_RAISE(
      auto payload, inMemoryPayload->toBlockPayload(payloadType, payloadPool_.get(), codec_ ? codec_.get() : nullptr));
  // Copy payload to arrow buffered os.
  ARROW_ASSIGN_OR_RAISE(auto rssBufferOs, arrow::io::BufferOutputStream::Create(options_->pushBufferMaxSize));

  static constexpr uint8_t kRssBlock = static_cast<uint8_t>(BlockType::kPlainPayload);
  RETURN_NOT_OK(rssBufferOs->Write(&kRssBlock, sizeof(kRssBlock)));

  RETURN_NOT_OK(payload->serialize(rssBufferOs.get()));
  payload = nullptr; // Invalidate payload immediately.

  // Push.
  ScopedTimer timer(&spillTime_);
  ARROW_ASSIGN_OR_RAISE(auto buffer, rssBufferOs->Finish());
  auto delta = rssClient_->pushPartitionData(
      partitionId, reinterpret_cast<char*>(const_cast<uint8_t*>(buffer->data())), buffer->size());
  bytesEvicted_[partitionId] += delta;
  evictBytes += delta;
  return arrow::Status::OK();
}
} // namespace gluten

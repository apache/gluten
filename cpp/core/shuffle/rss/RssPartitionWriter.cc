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

#include <algorithm>
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
  spillTime_ -= compressTimeInSpill_;
  rssClient_->stop();

  auto totalBytesEvicted = std::accumulate(bytesEvicted_.begin(), bytesEvicted_.end(), 0LL);
  // Populate metrics.
  metrics->totalCompressTime += compressTime_;
  metrics->totalWriteTime += writeTime_;
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
      // Measured inside the `spillTime_` timer above, so it is tracked separately and subtracted
      // from `spillTime_` in `stop()`.
      compressTime_ += compressedOs_->compressTime();
      compressTimeInSpill_ += compressedOs_->compressTime();
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
  // Compression writes into a scratch buffer that is reused across payloads and only ever grows,
  // so the steady state costs no allocation at all. Reusing one buffer is what makes this path
  // viable, because the buffer has to satisfy two constraints that pull in opposite directions:
  //
  //   - It must not be reserved from `payloadPool_`. `doEvict` also runs inside the memory
  //     reclaim callback, and a reservation requested from there trips `OverAcquire#borrow`'s
  //     `overTarget.usedBytes() == 0` assertion. `LocalPartitionWriter` sidesteps this by
  //     emitting `kToBeCompressed` and letting the compression happen after the spill, against a
  //     buffer owned by the output stream. An RSS writer has to hand finished bytes to the client
  //     right here, so it needs to own the scratch space itself.
  //   - It must not be allocated and released per payload. That memory is invisible to Gluten's
  //     accounting, and on a spill-heavy stage the resulting allocator churn is enough to grow
  //     the container's RSS past its limit while Gluten still believes it is within budget.
  //
  // Allocating once from the unmanaged pool and growing it to the largest payload this writer
  // ever sees satisfies both: nothing is reserved on the reclaim path, and the untracked
  // footprint is one bounded buffer per writer instead of a stream of transient allocations.
  if (payloadType == Payload::Type::kCompressed) {
    RETURN_NOT_OK(ensureScratchBuffer(compressBuffer_));
  }
  ARROW_ASSIGN_OR_RAISE(
      auto payload,
      inMemoryPayload->toBlockPayload(
          payloadType, payloadPool_.get(), codec_ ? codec_.get() : nullptr, compressBuffer_));
  compressTime_ += payload->getCompressTime();

  static constexpr uint8_t kRssBlock = static_cast<uint8_t>(BlockType::kPlainPayload);

  // Serialize the payload into the reusable staging buffer. Its size is known up front, so the
  // buffer never has to grow while writing and the stream never over-allocates.
  RETURN_NOT_OK(reservePushBuffer(static_cast<int64_t>(sizeof(kRssBlock)) + payload->serializedSize()));
  int64_t pushLength = 0;
  {
    arrow::io::BufferOutputStream os(pushBuffer_);
    RETURN_NOT_OK(os.Write(&kRssBlock, sizeof(kRssBlock)));
    RETURN_NOT_OK(payload->serialize(&os));
    ARROW_ASSIGN_OR_RAISE(pushLength, os.Tell());
    // Closed explicitly rather than through the destructor, which would swallow the status. This
    // only trims the buffer's size down to what was written; with `shrink_to_fit=false` the
    // capacity - and therefore the allocation - is kept for the next payload.
    RETURN_NOT_OK(os.Close());
  }
  writeTime_ += payload->getWriteTime();
  payload = nullptr; // Invalidate payload immediately.

  // Push.
  ScopedTimer timer(&spillTime_);
  auto delta =
      rssClient_->pushPartitionData(partitionId, reinterpret_cast<char*>(pushBuffer_->mutable_data()), pushLength);
  bytesEvicted_[partitionId] += delta;
  evictBytes += delta;
  return arrow::Status::OK();
}

arrow::Status RssPartitionWriter::reservePushBuffer(int64_t size) {
  RETURN_NOT_OK(ensureScratchBuffer(pushBuffer_));
  // `BufferOutputStream` takes the buffer's *size* - not its capacity - as its own capacity, so the
  // size has to be set explicitly before handing the buffer over.
  //
  // One byte of slack is added on purpose: `BufferOutputStream::Write` grows the stream when
  // `position + nbytes >= capacity`, i.e. the write that exactly fills the buffer would still take
  // the grow path (which rounds the capacity up to at least 256 bytes). Sizing the buffer one byte
  // above what is actually written keeps every write on the fast path.
  //
  // `shrink_to_fit=false` means `PoolBuffer::Resize` only ever forwards to `Reserve`, which is a
  // no-op while the requested size fits in the current capacity. The buffer therefore grows to the
  // largest payload seen by this writer and then stays put: no allocation and no copy per push.
  return pushBuffer_->Resize(std::max<int64_t>(size + 1, kMinPushBufferSize), /*shrink_to_fit=*/false);
}

arrow::Status RssPartitionWriter::ensureScratchBuffer(std::shared_ptr<arrow::ResizableBuffer>& buffer) {
  if (buffer != nullptr) {
    return arrow::Status::OK();
  }
  // Deliberately taken from the unmanaged pool: both scratch buffers are reached from the memory
  // reclaim callback, where reserving from `payloadPool_` would trip `OverAcquire#borrow` - see
  // `doEvict`. They are allocated once per writer and then only grow, so the memory they hold is
  // bounded by the largest payload the writer handles rather than by how many payloads it pushes.
  ARROW_ASSIGN_OR_RAISE(
      auto allocated, arrow::AllocateResizableBuffer(kMinPushBufferSize, arrow::default_memory_pool()));
  buffer = std::move(allocated);
  return arrow::Status::OK();
}
} // namespace gluten

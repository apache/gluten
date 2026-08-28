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

#include "shuffle/VeloxSortShuffleWriter.h"

#include "memory/VeloxColumnarBatch.h"
#include "shuffle/RadixSort.h"
#include "utils/Common.h"
#include "utils/Timer.h"

#include <arrow/io/memory.h>

namespace gluten {
namespace {

constexpr uint32_t kMaskLower27Bits = (1 << 27) - 1;
constexpr uint64_t kMaskLower40Bits = (1UL << 40) - 1;
constexpr uint32_t kPartitionIdStartByteIndex = 5;
constexpr uint32_t kPartitionIdEndByteIndex = 7;
constexpr uint32_t kMaxPageNumber = (1 << 13) - 1; // 13-bit max = 8191

uint64_t toCompactRowId(uint32_t partitionId, uint32_t pageNumber, uint32_t offsetInPage) {
  // |63 partitionId(24) |39 inputIndex(13) |26 rowIndex(27) |
  return static_cast<uint64_t>(partitionId) << 40 | static_cast<uint64_t>(pageNumber) << 27 | offsetInPage;
}

uint32_t extractPartitionId(uint64_t compactRowId) {
  return static_cast<uint32_t>(compactRowId >> 40);
}

std::pair<uint32_t, uint32_t> extractPageNumberAndOffset(uint64_t compactRowId) {
  return {(compactRowId & kMaskLower40Bits) >> 27, compactRowId & kMaskLower27Bits};
}

} // namespace

arrow::Result<std::shared_ptr<VeloxShuffleWriter>> VeloxSortShuffleWriter::create(
    uint32_t numPartitions,
    const std::shared_ptr<PartitionWriter>& partitionWriter,
    const std::shared_ptr<ShuffleWriterOptions>& options,
    MemoryManager* memoryManager) {
  if (auto sortOptions = std::dynamic_pointer_cast<SortShuffleWriterOptions>(options)) {
    std::shared_ptr<VeloxSortShuffleWriter> writer(
        new VeloxSortShuffleWriter(numPartitions, partitionWriter, sortOptions, memoryManager));
    RETURN_NOT_OK(writer->init());
    return writer;
  }
  return arrow::Status::Invalid("Error casting ShuffleWriterOptions to SortShuffleWriterOptions.");
}

VeloxSortShuffleWriter::VeloxSortShuffleWriter(
    uint32_t numPartitions,
    const std::shared_ptr<PartitionWriter>& partitionWriter,
    const std::shared_ptr<SortShuffleWriterOptions>& options,
    MemoryManager* memoryManager)
    : VeloxShuffleWriter(numPartitions, partitionWriter, options, memoryManager),
      useRadixSort_(options->useRadixSort),
      initialSortBufferSize_(options->initialSortBufferSize),
      diskWriteBufferSize_(options->diskWriteBufferSize) {}

arrow::Status VeloxSortShuffleWriter::write(std::shared_ptr<ColumnarBatch> cb, int64_t memLimit) {
  writtenBytes_ = 0;
  ARROW_ASSIGN_OR_RAISE(auto rv, getPeeledRowVector(cb));
  initRowType(rv);
  RETURN_NOT_OK(insert(rv, memLimit));
  return arrow::Status::OK();
}

arrow::Status VeloxSortShuffleWriter::stop() {
  ARROW_RETURN_IF(evictState_ == EvictState::kUnevictable, arrow::Status::Invalid("Unevictable state in stop."));
  writtenBytes_ = 0;

  stopped_ = true;
  if (offset_ > 0) {
    RETURN_NOT_OK(evictAllPartitions());
  }
  array_.reset();
  sortedBuffer_.reset();
  pages_.clear();
  pageAddresses_.clear();
  RETURN_NOT_OK(partitionWriter_->stop(&metrics_, writtenBytes_));
  return arrow::Status::OK();
}

arrow::Status VeloxSortShuffleWriter::reclaimFixedSize(int64_t size, int64_t* actual) {
  if (evictState_ == EvictState::kUnevictable || offset_ == 0) {
    *actual = 0;
    return arrow::Status::OK();
  }
  auto beforeReclaim = veloxPool_->usedBytes();
  RETURN_NOT_OK(evictAllPartitions());
  *actual = beforeReclaim - veloxPool_->usedBytes();
  return arrow::Status::OK();
}

arrow::Status VeloxSortShuffleWriter::init() {
  ARROW_RETURN_IF(
      partitioning_ == Partitioning::kSingle,
      arrow::Status::Invalid("VeloxSortShuffleWriter doesn't support single partition."));
  allocateMinimalArray();
  // In Spark, sortedBuffer_ memory and compressionBuffer_ memory are pre-allocated and counted into executor
  // memory overhead. To align with Spark, we use arrow::default_memory_pool() to avoid counting these memory in Gluten.
  ARROW_ASSIGN_OR_RAISE(sortedBuffer_, arrow::AllocateBuffer(diskWriteBufferSize_, arrow::default_memory_pool()));
  sortedBufferPtr_ = sortedBuffer_->mutable_data();
  return arrow::Status::OK();
}

void VeloxSortShuffleWriter::initRowType(const facebook::velox::RowVectorPtr& rv) {
  if (UNLIKELY(!rowType_)) {
    rowType_ = rv->rowType();
    fixedRowSize_ = facebook::velox::row::CompactRow::fixedRowSize(rowType_);
  } else {
    VELOX_CHECK(rowType_->equivalent(*rv->rowType()));
  }
}

arrow::Result<facebook::velox::RowVectorPtr> VeloxSortShuffleWriter::getPeeledRowVector(
    const std::shared_ptr<ColumnarBatch>& cb) {
  if (partitioning_ == Partitioning::kRange) {
    auto veloxColumnBatch = VeloxColumnarBatch::from(veloxPool_.get(), cb);
    VELOX_CHECK_NOT_NULL(veloxColumnBatch);
    const int32_t numColumns = veloxColumnBatch->numColumns();
    VELOX_CHECK(numColumns >= 2);
    auto pidBatch = veloxColumnBatch->select(veloxPool_.get(), {0});
    auto pidArr = getFirstColumn(*(pidBatch->getRowVector()));
    RETURN_NOT_OK(partitioner_->compute(pidArr, pidBatch->numRows(), row2Partition_));

    std::vector<int32_t> range;
    range.reserve(numColumns);
    for (int32_t i = 1; i < numColumns; i++) {
      range.push_back(i);
    }
    auto rvBatch = veloxColumnBatch->select(veloxPool_.get(), range);
    return rvBatch->getFlattenedRowVector();
  } else {
    auto veloxColumnBatch = VeloxColumnarBatch::from(veloxPool_.get(), cb);
    VELOX_CHECK_NOT_NULL(veloxColumnBatch);
    auto rv = veloxColumnBatch->getFlattenedRowVector();
    if (partitioner_->hasPid()) {
      auto pidArr = getFirstColumn(*rv);
      RETURN_NOT_OK(partitioner_->compute(pidArr, rv->size(), row2Partition_));
      return getStrippedRowVector(*rv);
    } else {
      RETURN_NOT_OK(partitioner_->compute(nullptr, rv->size(), row2Partition_));
      return rv;
    }
  }
}

arrow::Status VeloxSortShuffleWriter::insert(const facebook::velox::RowVectorPtr& vector, int64_t memLimit) {
  ScopedTimer timer(&c2rTime_);
  auto inputRows = vector->size();
  VELOX_DCHECK_GT(inputRows, 0);

  facebook::velox::row::CompactRow row(vector);

  if (fixedRowSize_.has_value()) {
    rowSize_.resize(inputRows, fixedRowSize_.value());
  } else {
    rowSize_.resize(inputRows);
    rowSizePrefixSum_.resize(inputRows + 1);
    rowSizePrefixSum_[0] = 0;
    for (auto i = 0; i < inputRows; ++i) {
      rowSize_[i] = row.rowSize(i);
      rowSizePrefixSum_[i + 1] = rowSizePrefixSum_[i] + rowSize_[i] + sizeof(RowSizeType);
    }
  }

  facebook::velox::vector_size_t rowOffset = 0;
  while (rowOffset < inputRows) {
    auto remainingRows = inputRows - rowOffset;
    auto rows = maxRowsToInsert(rowOffset, remainingRows);
    if (rows == 0) {
      auto minSizeRequired =
          (fixedRowSize_.has_value() ? fixedRowSize_.value() : rowSize_[rowOffset]) + sizeof(RowSizeType);
      acquireNewBuffer(static_cast<uint64_t>(memLimit), minSizeRequired);
      rows = maxRowsToInsert(rowOffset, remainingRows);
      ARROW_RETURN_IF(
          rows == 0, arrow::Status::Invalid("Failed to insert rows. Remaining rows: " + std::to_string(remainingRows)));
    }
    // Spill to avoid offset_ overflow.
    RETURN_NOT_OK(maybeSpill(rows));
    // Allocate newArray can trigger spill.
    growArrayIfNecessary(rows);
    insertRows(row, rowOffset, rows);
    rowOffset += rows;
  }
  return arrow::Status::OK();
}

void VeloxSortShuffleWriter::insertRows(
    facebook::velox::row::CompactRow& compact,
    facebook::velox::vector_size_t offset,
    facebook::velox::vector_size_t size) {
  VELOX_CHECK(!pages_.empty());
  std::vector<size_t> offsets(size);
  for (auto i = 0; i < size; ++i) {
    auto row = offset + i;
    auto pid = row2Partition_[row];
    arrayPtr_[offset_++] = toCompactRowId(pid, pageNumber_, pageCursor_);
    // size(RowSize) | bytes
    memcpy(currentPage_ + pageCursor_, &rowSize_[row], sizeof(RowSizeType));
    offsets[i] = pageCursor_ + sizeof(RowSizeType);
    pageCursor_ += rowSize_[row] + sizeof(RowSizeType);
    VELOX_DCHECK_LE(pageCursor_, currenPageSize_);
  }
  compact.serialize(offset, size, offsets.data(), currentPage_);
}

arrow::Status VeloxSortShuffleWriter::maybeSpill(uint32_t nextRows) {
  if ((uint64_t)offset_ + nextRows > std::numeric_limits<uint32_t>::max() || pageNumber_ >= kMaxPageNumber) {
    RETURN_NOT_OK(evictAllPartitions());
  }
  return arrow::Status::OK();
}

arrow::Status VeloxSortShuffleWriter::evictAllPartitions() {
  VELOX_CHECK(offset_ > 0);
  EvictGuard evictGuard{evictState_};

  auto numRecords = offset_;
  // offset_ is used for checking spillable data.
  offset_ = 0;
  int32_t begin = 0;
  {
    ScopedTimer timer(&sortTime_);
    if (useRadixSort_) {
      begin = RadixSort::sort(arrayPtr_, arraySize_, numRecords, kPartitionIdStartByteIndex, kPartitionIdEndByteIndex);
    } else {
      std::sort(arrayPtr_, arrayPtr_ + numRecords);
    }
  }

  auto end = begin + numRecords;
  auto cur = begin;
  auto pid = extractPartitionId(arrayPtr_[begin]);
  while (++cur < end) {
    auto curPid = extractPartitionId(arrayPtr_[cur]);
    if (curPid != pid) {
      RETURN_NOT_OK(evictPartition(pid, begin, cur));
      pid = curPid;
      begin = cur;
    }
  }
  RETURN_NOT_OK(evictPartition(pid, begin, cur));

  if (!stopped_) {
    // Preserve the last page for use.
    auto numPages = pages_.size();
    while (--numPages) {
      pages_.pop_front();
    }
    auto& page = pages_.back();
    // Clear page for serialization.
    memset(page->asMutable<char>(), 0, page->size());
    // currentPage_ should always point to the last page.
    VELOX_CHECK(currentPage_ == page->asMutable<char>());

    pageAddresses_.resize(1);
    pageAddresses_[0] = currentPage_;
    pageNumber_ = 0;
    pageCursor_ = 0;

    // Reset and reallocate array_ to minimal size. Allocate array_ can trigger spill.
    allocateMinimalArray();
  }
  return arrow::Status::OK();
}

arrow::Status VeloxSortShuffleWriter::evictPartition(uint32_t partitionId, size_t begin, size_t end) {
  VELOX_DCHECK(begin < end);
  // Count copy row time into sortTime_.
  Timer sortTime{};
  // Serialize [begin, end)
  int64_t offset = 0;
  char* addr;
  uint32_t recordSize;

  auto index = begin;
  while (index < end) {
    auto pageIndex = extractPageNumberAndOffset(arrayPtr_[index]);
    addr = pageAddresses_[pageIndex.first] + pageIndex.second;
    recordSize = *(reinterpret_cast<RowSizeType*>(addr)) + sizeof(RowSizeType);
    if (offset + recordSize > diskWriteBufferSize_ && offset > 0) {
      sortTime.stop();
      RETURN_NOT_OK(evictPartitionInternal(partitionId, index - begin, sortedBufferPtr_, offset));
      sortTime.start();
      begin = index;
      offset = 0;
    }
    if (recordSize > static_cast<uint32_t>(diskWriteBufferSize_)) {
      // Split large rows.
      sortTime.stop();
      RowSizeType bytes = 0;
      auto* buffer = reinterpret_cast<uint8_t*>(addr);
      while (bytes < recordSize) {
        auto rawLength = std::min<RowSizeType>(static_cast<uint32_t>(diskWriteBufferSize_), recordSize - bytes);
        // Use numRows = 0 to represent a part of row.
        auto numRows = (bytes + rawLength == recordSize) ? 1 : 0;
        RETURN_NOT_OK(evictPartitionInternal(partitionId, numRows, buffer + bytes, rawLength));
        bytes += rawLength;
      }
      begin++;
      sortTime.start();
    } else {
      // Copy small rows.
      gluten::fastCopy(sortedBufferPtr_ + offset, addr, recordSize);
      offset += recordSize;
    }
    index++;
  }
  sortTime.stop();
  if (offset > 0) {
    VELOX_CHECK(index > begin);
    RETURN_NOT_OK(evictPartitionInternal(partitionId, index - begin, sortedBufferPtr_, offset));
  }
  sortTime_ += sortTime.realTimeUsed();
  return arrow::Status::OK();
}

arrow::Status VeloxSortShuffleWriter::evictPartitionInternal(
    uint32_t partitionId,
    uint32_t numRows,
    uint8_t* buffer,
    int64_t rawLength) {
  VELOX_CHECK(rawLength > 0);
  auto payload = std::make_unique<InMemoryPayload>(
      numRows,
      nullptr,
      nullptr,
      std::vector<std::shared_ptr<arrow::Buffer>>{std::make_shared<arrow::Buffer>(buffer, rawLength)});
  updateSpillMetrics(payload);
  RETURN_NOT_OK(partitionWriter_->sortEvict(partitionId, std::move(payload), stopped_, writtenBytes_));
  return arrow::Status::OK();
}

facebook::velox::vector_size_t VeloxSortShuffleWriter::maxRowsToInsert(
    facebook::velox::vector_size_t offset,
    facebook::velox::vector_size_t remainingRows) {
  // Check how many rows can be handled.
  if (pages_.empty()) {
    return 0;
  }
  auto remainingBytes = pages_.back()->size() - pageCursor_;
  if (fixedRowSize_.has_value()) {
    return std::min(
        static_cast<facebook::velox::vector_size_t>(remainingBytes / (fixedRowSize_.value() + sizeof(RowSizeType))),
        remainingRows);
  }
  auto beginIter = rowSizePrefixSum_.begin() + 1 + offset;
  auto bytesWritten = rowSizePrefixSum_[offset];
  auto iter = std::upper_bound(beginIter, rowSizePrefixSum_.end(), remainingBytes + bytesWritten);
  return (facebook::velox::vector_size_t)(iter - beginIter);
}

void VeloxSortShuffleWriter::acquireNewBuffer(uint64_t memLimit, uint64_t minSizeRequired) {
  DLOG_IF(INFO, !pages_.empty()) << "Acquire new buffer. current capacity: " << pages_.back()->capacity()
                                 << ", size: " << pages_.back()->size() << ", pageCursor: " << pageCursor_
                                 << ", unused: " << pages_.back()->capacity() - pageCursor_;
  auto size = std::max(
      std::min<uint64_t>(
          std::max<uint64_t>(memLimit >> 2, facebook::velox::AlignedBuffer::kPaddedSize), 64UL * 1024 * 1024) -
          facebook::velox::AlignedBuffer::kPaddedSize,
      minSizeRequired);
  // Allocating new buffer can trigger spill.
  auto newBuffer = facebook::velox::AlignedBuffer::allocate<char>(size, veloxPool_.get());
  DLOG(INFO) << "Allocated new buffer. capacity: " << newBuffer->capacity() << ", size: " << newBuffer->size();
  auto newBufferSize = newBuffer->capacity();
  newBuffer->setSize(newBufferSize);

  currentPage_ = newBuffer->asMutable<char>();
  currenPageSize_ = newBufferSize;
  memset(currentPage_, 0, newBufferSize);

  // If spill triggered, clear pages_.
  if (offset_ == 0 && pages_.size() > 0) {
    pageAddresses_.clear();
    pages_.clear();
  }
  pageAddresses_.emplace_back(currentPage_);
  pages_.emplace_back(std::move(newBuffer));

  pageCursor_ = 0;
  pageNumber_ = pages_.size() - 1;
}

void VeloxSortShuffleWriter::growArrayIfNecessary(uint32_t rows) {
  auto newSize = newArraySize(rows);
  if (newSize > arraySize_) {
    // May trigger spill.
    auto newSizeBytes = newSize * sizeof(uint64_t);
    auto newArray = facebook::velox::AlignedBuffer::allocate<char>(newSizeBytes, veloxPool_.get());
    // Check if already satisfies (spill has been triggered).
    if (newArraySize(rows) > arraySize_) {
      if (offset_ > 0) {
        gluten::fastCopy(newArray->asMutable<void>(), arrayPtr_, offset_ * sizeof(uint64_t));
      }
      setUpArray(std::move(newArray));
    }
  }
}

uint32_t VeloxSortShuffleWriter::newArraySize(uint32_t rows) {
  auto newSize = arraySize_;
  auto usableCapacity = useRadixSort_ ? newSize / 2 : newSize;
  while (offset_ + rows > usableCapacity) {
    newSize <<= 1;
    usableCapacity = useRadixSort_ ? newSize / 2 : newSize;
  }
  return newSize;
}

void VeloxSortShuffleWriter::setUpArray(facebook::velox::BufferPtr&& array) {
  array_.reset();
  array_ = std::move(array);
  // Capacity is a multiple of 8 (bytes).
  auto capacity = array_->capacity() & 0xfffffff8;
  array_->setSize(capacity);
  arraySize_ = capacity >> 3;
  arrayPtr_ = array_->asMutable<uint64_t>();
}

int64_t VeloxSortShuffleWriter::peakBytesAllocated() const {
  return veloxPool_->peakBytes();
}

int64_t VeloxSortShuffleWriter::totalSortTime() const {
  return sortTime_;
}

int64_t VeloxSortShuffleWriter::totalC2RTime() const {
  return c2rTime_;
}

void VeloxSortShuffleWriter::allocateMinimalArray() {
  auto array =
      facebook::velox::AlignedBuffer::allocate<char>(initialSortBufferSize_ * sizeof(uint64_t), veloxPool_.get());
  setUpArray(std::move(array));
}

void VeloxSortShuffleWriter::updateSpillMetrics(const std::unique_ptr<InMemoryPayload>& payload) {
  if (!stopped_) {
    metrics_.totalBytesToEvict += payload->rawSize();
  }
}

} // namespace gluten

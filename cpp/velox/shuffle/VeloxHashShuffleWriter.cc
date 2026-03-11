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

#include "shuffle/VeloxHashShuffleWriter.h"
#include "memory/ArrowMemory.h"
#include "memory/VeloxColumnarBatch.h"
#include "shuffle/Utils.h"
#include "utils/Common.h"
#include "utils/Macros.h"
#include "utils/VeloxArrowUtils.h"
#include "velox/buffer/Buffer.h"
#include "velox/common/base/Nulls.h"
#include "velox/type/HugeInt.h"
#include "velox/type/Timestamp.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"

#if defined(__x86_64__)
#include <immintrin.h>
#include <x86intrin.h>
#elif defined(__aarch64__)
#include <arm_neon.h>
#endif

namespace gluten {

#define VELOX_SHUFFLE_WRITER_LOG_FLAG 0

// macro to rotate left an 8-bit value 'x' given the shift 's' is a 32-bit integer
// (x is left shifted by 's' modulo 8) OR (x right shifted by (8 - 's' modulo 8))
#if !defined(__x86_64__)
#define rotateLeft(x, s) (x << (s - ((s >> 3) << 3)) | x >> (8 - (s - ((s >> 3) << 3))))
#endif

// on x86 machines, _MM_HINT_T0,T1,T2 are defined as 1, 2, 3
// equivalent mapping to __builtin_prefetch hints is 3, 2, 1
#if defined(__x86_64__)
#define PREFETCHT0(ptr) _mm_prefetch(ptr, _MM_HINT_T0)
#define PREFETCHT1(ptr) _mm_prefetch(ptr, _MM_HINT_T1)
#define PREFETCHT2(ptr) _mm_prefetch(ptr, _MM_HINT_T2)
#else
#define PREFETCHT0(ptr) __builtin_prefetch(ptr, 0, 3)
#define PREFETCHT1(ptr) __builtin_prefetch(ptr, 0, 2)
#define PREFETCHT2(ptr) __builtin_prefetch(ptr, 0, 1)
#endif

namespace {

bool vectorHasNull(const facebook::velox::VectorPtr& vp) {
  if (!vp->mayHaveNulls()) {
    return false;
  }
  return vp->countNulls(vp->nulls(), vp->size()) != 0;
}

class BinaryArrayResizeGuard {
 public:
  explicit BinaryArrayResizeGuard(BinaryArrayResizeState& state) : state_(state) {
    state_.inResize = true;
  }

  ~BinaryArrayResizeGuard() {
    state_.inResize = false;
  }

 private:
  BinaryArrayResizeState& state_;
};

template <facebook::velox::TypeKind kind>
arrow::Status collectFlatVectorBuffer(
    facebook::velox::BaseVector* vector,
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    arrow::MemoryPool* pool) {
  using T = typename facebook::velox::TypeTraits<kind>::NativeType;
  auto flatVector = dynamic_cast<const facebook::velox::FlatVector<T>*>(vector);
  buffers.emplace_back();
  ARROW_ASSIGN_OR_RAISE(buffers.back(), toArrowBuffer(flatVector->nulls(), pool));
  buffers.emplace_back();
  ARROW_ASSIGN_OR_RAISE(buffers.back(), toArrowBuffer(flatVector->values(), pool));
  return arrow::Status::OK();
}

arrow::Status collectFlatVectorBufferStringView(
    facebook::velox::BaseVector* vector,
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    arrow::MemoryPool* pool) {
  auto flatVector = dynamic_cast<const facebook::velox::FlatVector<facebook::velox::StringView>*>(vector);
  buffers.emplace_back();
  ARROW_ASSIGN_OR_RAISE(buffers.back(), toArrowBuffer(flatVector->nulls(), pool));

  auto rawValues = flatVector->rawValues();
  // last offset is the totalStringSize
  auto lengthBufferSize = sizeof(gluten::StringLengthType) * flatVector->size();
  ARROW_ASSIGN_OR_RAISE(auto lengthBuffer, arrow::AllocateResizableBuffer(lengthBufferSize, pool));
  auto* rawLength = reinterpret_cast<gluten::StringLengthType*>(lengthBuffer->mutable_data());
  uint64_t offset = 0;
  for (int32_t i = 0; i < flatVector->size(); i++) {
    auto length = rawValues[i].size();
    *rawLength++ = length;
    offset += length;
  }
  buffers.push_back(std::move(lengthBuffer));

  ARROW_ASSIGN_OR_RAISE(auto valueBuffer, arrow::AllocateResizableBuffer(offset, pool));
  auto raw = reinterpret_cast<char*>(valueBuffer->mutable_data());
  for (int32_t i = 0; i < flatVector->size(); i++) {
    gluten::fastCopy(raw, rawValues[i].data(), rawValues[i].size());
    raw += rawValues[i].size();
  }
  buffers.push_back(std::move(valueBuffer));
  return arrow::Status::OK();
}

template <>
arrow::Status collectFlatVectorBuffer<facebook::velox::TypeKind::UNKNOWN>(
    facebook::velox::BaseVector* vector,
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    arrow::MemoryPool* pool) {
  return arrow::Status::OK();
}

template <>
arrow::Status collectFlatVectorBuffer<facebook::velox::TypeKind::VARCHAR>(
    facebook::velox::BaseVector* vector,
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    arrow::MemoryPool* pool) {
  return collectFlatVectorBufferStringView(vector, buffers, pool);
}

template <>
arrow::Status collectFlatVectorBuffer<facebook::velox::TypeKind::VARBINARY>(
    facebook::velox::BaseVector* vector,
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    arrow::MemoryPool* pool) {
  return collectFlatVectorBufferStringView(vector, buffers, pool);
}

} // namespace

arrow::Result<std::shared_ptr<VeloxShuffleWriter>> VeloxHashShuffleWriter::create(
    uint32_t numPartitions,
    const std::shared_ptr<PartitionWriter>& partitionWriter,
    const std::shared_ptr<ShuffleWriterOptions>& options,
    MemoryManager* memoryManager) {
  if (auto hashOptions = std::dynamic_pointer_cast<HashShuffleWriterOptions>(options)) {
    std::shared_ptr<VeloxHashShuffleWriter> res(
        new VeloxHashShuffleWriter(numPartitions, partitionWriter, hashOptions, memoryManager));
    RETURN_NOT_OK(res->init());
    return res;
  }
  return arrow::Status::Invalid("Error casting ShuffleWriterOptions to HashShuffleWriterOptions. ");
} // namespace gluten

arrow::Status VeloxHashShuffleWriter::init() {
  // pre-allocated buffer size for each partition, unit is row count
  // when partitioner is SinglePart, partial variables don`t need init
  if (partitioning_ != Partitioning::kSingle) {
    partition2RowCount_.resize(numPartitions_);
    partitionBufferSize_.resize(numPartitions_);
    partition2RowOffsetBase_.resize(numPartitions_ + 1);
  }

  partitionBufferBase_.resize(numPartitions_);

  return arrow::Status::OK();
}

arrow::Status VeloxHashShuffleWriter::initPartitions() {
  auto simpleColumnCount = simpleColumnIndices_.size();

  partitionValidityAddrs_.resize(simpleColumnCount);
  std::for_each(partitionValidityAddrs_.begin(), partitionValidityAddrs_.end(), [this](std::vector<uint8_t*>& v) {
    v.resize(numPartitions_, nullptr);
  });

  partitionFixedWidthValueAddrs_.resize(fixedWidthColumnCount_);
  std::for_each(
      partitionFixedWidthValueAddrs_.begin(), partitionFixedWidthValueAddrs_.end(), [this](std::vector<uint8_t*>& v) {
        v.resize(numPartitions_, nullptr);
      });

  partitionBuffers_.resize(simpleColumnCount);
  std::for_each(partitionBuffers_.begin(), partitionBuffers_.end(), [this](auto& v) { v.resize(numPartitions_); });

  partitionBinaryAddrs_.resize(binaryColumnIndices_.size());
  std::for_each(partitionBinaryAddrs_.begin(), partitionBinaryAddrs_.end(), [this](std::vector<BinaryBuf>& v) {
    v.resize(numPartitions_);
  });

  return arrow::Status::OK();
}

void VeloxHashShuffleWriter::setPartitionBufferSize(uint32_t newSize) {
  splitBufferSize_ = newSize;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> VeloxHashShuffleWriter::generateComplexTypeBuffers(
    facebook::velox::RowVectorPtr vector) {
  auto arena = std::make_unique<facebook::velox::StreamArena>(veloxPool_.get());
  auto serializer =
      serde_.createIterativeSerializer(asRowType(vector->type()), vector->size(), arena.get(), &serdeOptions_);
  const facebook::velox::IndexRange allRows{0, vector->size()};
  serializer->append(vector, folly::Range(&allRows, 1));
  auto serializedSize = serializer->maxSerializedSize();
  auto flushBuffer = complexTypeFlushBuffer_[0];
  if (flushBuffer == nullptr) {
    ARROW_ASSIGN_OR_RAISE(flushBuffer, arrow::AllocateResizableBuffer(serializedSize, partitionBufferPool_.get()));
  } else if (serializedSize > flushBuffer->capacity()) {
    RETURN_NOT_OK(flushBuffer->Reserve(serializedSize));
  }

  auto valueBuffer = arrow::SliceMutableBuffer(flushBuffer, 0, serializedSize);
  auto output = std::make_shared<arrow::io::FixedSizeBufferWriter>(valueBuffer);
  facebook::velox::serializer::presto::PrestoOutputStreamListener listener;
  ArrowFixedSizeBufferOutputStream out(output, &listener);
  serializer->flush(&out);
  return valueBuffer;
}

arrow::Status VeloxHashShuffleWriter::write(std::shared_ptr<ColumnarBatch> cb, int64_t memLimit) {
  writtenBytes_ = 0;
  if (partitioning_ == Partitioning::kSingle) {
    auto veloxColumnBatch = VeloxColumnarBatch::from(veloxPool_.get(), cb);
    VELOX_CHECK_NOT_NULL(veloxColumnBatch);
    auto& rv = *veloxColumnBatch->getFlattenedRowVector();
    RETURN_NOT_OK(initFromRowVector(rv));
    std::vector<std::shared_ptr<arrow::Buffer>> buffers;
    std::vector<facebook::velox::VectorPtr> complexChildren;
    for (auto& child : rv.children()) {
      if (child->encoding() == facebook::velox::VectorEncoding::Simple::FLAT) {
        auto status = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
            collectFlatVectorBuffer, child->typeKind(), child.get(), buffers, partitionBufferPool_.get());
        RETURN_NOT_OK(status);
      } else {
        complexChildren.emplace_back(child);
      }
    }
    if (complexChildren.size() > 0) {
      auto rowVector = std::make_shared<facebook::velox::RowVector>(
          veloxPool_.get(),
          complexWriteType_,
          facebook::velox::BufferPtr(nullptr),
          rv.size(),
          std::move(complexChildren));
      buffers.emplace_back();
      ARROW_ASSIGN_OR_RAISE(buffers.back(), generateComplexTypeBuffers(rowVector));
    }
    RETURN_NOT_OK(evictBuffers(0, rv.size(), std::move(buffers), false));
  } else if (partitioning_ == Partitioning::kRange) {
    auto veloxColumnBatch = VeloxColumnarBatch::from(veloxPool_.get(), cb);
    VELOX_CHECK_NOT_NULL(veloxColumnBatch);
    const int32_t numColumns = veloxColumnBatch->numColumns();
    VELOX_CHECK(numColumns >= 2);
    auto pidBatch = veloxColumnBatch->select(veloxPool_.get(), {0});
    auto pidArr = getFirstColumn(*(pidBatch->getRowVector()));
    {
      SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingCompute]);
      std::fill(std::begin(partition2RowCount_), std::end(partition2RowCount_), 0);
      RETURN_NOT_OK(partitioner_->compute(pidArr, pidBatch->numRows(), row2Partition_));
      for (auto& pid : row2Partition_) {
        partition2RowCount_[pid]++;
      }
    }
    std::vector<int32_t> range;
    range.reserve(numColumns);
    for (int32_t i = 1; i < numColumns; i++) {
      range.push_back(i);
    }
    auto rvBatch = veloxColumnBatch->select(veloxPool_.get(), range);
    auto& rv = *rvBatch->getFlattenedRowVector();
    RETURN_NOT_OK(initFromRowVector(rv));
    RETURN_NOT_OK(doSplit(rv, memLimit));
  } else {
    auto veloxColumnBatch = VeloxColumnarBatch::from(veloxPool_.get(), cb);
    VELOX_CHECK_NOT_NULL(veloxColumnBatch);
    facebook::velox::RowVectorPtr rv;
    {
      SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingFlattenRV]);
      rv = veloxColumnBatch->getFlattenedRowVector();
    }
    if (isExtremelyLargeBatch(rv)) {
      auto numRows = rv->size();
      int32_t offset = 0;
      do {
        auto length = std::min(maxBatchSize_, numRows);
        auto slicedBatch = std::dynamic_pointer_cast<facebook::velox::RowVector>(rv->slice(offset, length));
        RETURN_NOT_OK(partitioningAndDoSplit(std::move(slicedBatch), memLimit));
        offset += length;
        numRows -= length;
      } while (numRows);
    } else {
      RETURN_NOT_OK(partitioningAndDoSplit(std::move(rv), memLimit));
    }
  }
  return arrow::Status::OK();
}

arrow::Status VeloxHashShuffleWriter::partitioningAndDoSplit(facebook::velox::RowVectorPtr rv, int64_t memLimit) {
  std::fill(std::begin(partition2RowCount_), std::end(partition2RowCount_), 0);
  if (partitioner_->hasPid()) {
    auto pidArr = getFirstColumn(*rv);
    {
      SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingCompute]);
      RETURN_NOT_OK(partitioner_->compute(pidArr, rv->size(), row2Partition_));
      for (auto& pid : row2Partition_) {
        partition2RowCount_[pid]++;
      }
    }
    auto strippedRv = getStrippedRowVector(*rv);
    RETURN_NOT_OK(initFromRowVector(*strippedRv));
    RETURN_NOT_OK(doSplit(*strippedRv, memLimit));
  } else {
    RETURN_NOT_OK(initFromRowVector(*rv));
    {
      SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingCompute]);
      RETURN_NOT_OK(partitioner_->compute(nullptr, rv->size(), row2Partition_));
      for (auto& pid : row2Partition_) {
        partition2RowCount_[pid]++;
      }
    }
    RETURN_NOT_OK(doSplit(*rv, memLimit));
  }
  return arrow::Status::OK();
}

arrow::Status VeloxHashShuffleWriter::stop() {
  writtenBytes_ = 0;
  setSplitState(SplitState::kStopEvict);
  if (partitioning_ != Partitioning::kSingle) {
    for (auto pid = 0; pid < numPartitions_; ++pid) {
      PartitionScopeGuard stopEvictGuard(partitionBufferInUse_, pid);
      RETURN_NOT_OK(evictPartitionBuffers(pid, false));
    }
  }
  {
    SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingStop]);
    setSplitState(SplitState::kStop);
    RETURN_NOT_OK(partitionWriter_->stop(&metrics_, writtenBytes_));
    partitionBuffers_.clear();
  }

  stat();

  return arrow::Status::OK();
}

arrow::Status VeloxHashShuffleWriter::buildPartition2Row(uint32_t rowNum) {
  SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingBuildPartition]);

  // calc partition2RowOffsetBase_
  partition2RowOffsetBase_[0] = 0;
  for (auto pid = 1; pid <= numPartitions_; ++pid) {
    partition2RowOffsetBase_[pid] = partition2RowOffsetBase_[pid - 1] + partition2RowCount_[pid - 1];
  }

  // calc rowOffset2RowId_
  rowOffset2RowId_.resize(rowNum);
  for (auto row = 0; row < rowNum; ++row) {
    auto pid = row2Partition_[row];
    rowOffset2RowId_[partition2RowOffsetBase_[pid]++] = row;
  }

  for (auto pid = 0; pid < numPartitions_; ++pid) {
    partition2RowOffsetBase_[pid] -= partition2RowCount_[pid];
  }

  // calc valid partition list
  partitionUsed_.clear();
  partitionUsed_.reserve(numPartitions_);
  for (auto pid = 0; pid != numPartitions_; ++pid) {
    if (partition2RowCount_[pid] > 0) {
      partitionUsed_.push_back(pid);
    }
  }

  printPartition2Row();

  return arrow::Status::OK();
}

arrow::Status VeloxHashShuffleWriter::updateInputHasNull(const facebook::velox::RowVector& rv) {
  SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingHasNull]);

  for (size_t col = 0; col < simpleColumnIndices_.size(); ++col) {
    if (!inputHasNull_[col]) {
      auto colIdx = simpleColumnIndices_[col];
      if (vectorHasNull(rv.childAt(colIdx))) {
        inputHasNull_[col] = true;
      }
    }
  }

  printInputHasNull();

  return arrow::Status::OK();
}

void VeloxHashShuffleWriter::setSplitState(SplitState state) {
  splitState_ = state;
}

arrow::Status VeloxHashShuffleWriter::doSplit(const facebook::velox::RowVector& rv, int64_t memLimit) {
  auto rowNum = rv.size();
  RETURN_NOT_OK(buildPartition2Row(rowNum));
  RETURN_NOT_OK(updateInputHasNull(rv));

  {
    SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingIteratePartitions]);
    setSplitState(SplitState::kPreAlloc);
    // Calculate buffer size based on available offheap memory, history average bytes per row and options_.bufferSize.
    auto preAllocBufferSize = calculatePartitionBufferSize(rv, memLimit);
    RETURN_NOT_OK(preAllocPartitionBuffers(preAllocBufferSize));
  }

  printPartitionBuffer();

  setSplitState(SplitState::kSplit);
  RETURN_NOT_OK(splitRowVector(rv));

  printPartitionBuffer();

  setSplitState(SplitState::kInit);
  return arrow::Status::OK();
}

arrow::Status VeloxHashShuffleWriter::splitRowVector(const facebook::velox::RowVector& rv) {
  SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingSplitRV]);

  // now start to split the RowVector
  RETURN_NOT_OK(splitFixedWidthValueBuffer(rv));
  RETURN_NOT_OK(splitValidityBuffer(rv));
  RETURN_NOT_OK(splitBinaryArray(rv));
  RETURN_NOT_OK(splitComplexType(rv));

  // update partition buffer base after split
  for (auto pid = 0; pid < numPartitions_; ++pid) {
    partitionBufferBase_[pid] += partition2RowCount_[pid];
  }

  return arrow::Status::OK();
}

arrow::Status VeloxHashShuffleWriter::splitFixedWidthValueBuffer(const facebook::velox::RowVector& rv) {
  for (auto col = 0; col < fixedWidthColumnCount_; ++col) {
    auto colIdx = simpleColumnIndices_[col];
    auto& column = rv.childAt(colIdx);
    const uint8_t* srcAddr = (const uint8_t*)column->valuesAsVoid();
    const auto& dstAddrs = partitionFixedWidthValueAddrs_[col];

    switch (arrow::bit_width(arrowColumnTypes_[colIdx]->id())) {
      case 0: // arrow::NullType::type_id:
        // No value buffer created for NullType.
        break;
      case 1: // arrow::BooleanType::type_id:
        splitBoolValueType(srcAddr, dstAddrs);
        break;
      case 8:
        RETURN_NOT_OK(splitFixedType<uint8_t>(srcAddr, dstAddrs));
        break;
      case 16:
        RETURN_NOT_OK(splitFixedType<uint16_t>(srcAddr, dstAddrs));
        break;
      case 32:
        RETURN_NOT_OK(splitFixedType<uint32_t>(srcAddr, dstAddrs));
        break;
      case 64: {
        if (column->type()->kind() == facebook::velox::TypeKind::TIMESTAMP) {
          RETURN_NOT_OK(splitTimestamp(srcAddr, dstAddrs));
        } else {
          RETURN_NOT_OK(splitFixedType<uint64_t>(srcAddr, dstAddrs));
        }
      } break;
      case 128: // arrow::Decimal128Type::type_id
        // too bad gcc generates movdqa even we use __m128i_u data type.
        // splitFixedType<__m128i_u>(srcAddr, dstAddrs);
        {
          if (column->type()->isShortDecimal()) {
            RETURN_NOT_OK(splitFixedType<int64_t>(srcAddr, dstAddrs));
          } else if (column->type()->isLongDecimal()) {
            // assume batch size = 32k; reducer# = 4K; row/reducer = 8
            RETURN_NOT_OK(splitFixedType<facebook::velox::int128_t>(srcAddr, dstAddrs));
          } else {
            return arrow::Status::Invalid(
                "Column type " + schema_->field(colIdx)->type()->ToString() + " is not supported.");
          }
        }
        break;
      default:
        return arrow::Status::Invalid(
            "Column type " + schema_->field(colIdx)->type()->ToString() + " is not fixed width");
    }
  }

  return arrow::Status::OK();
}

void VeloxHashShuffleWriter::splitBoolType(const uint8_t* srcAddr, const std::vector<uint8_t*>& dstAddrs) {
  // assume batch size = 32k; reducer# = 4K; row/reducer = 8
  for (auto& pid : partitionUsed_) {
    // set the last byte
    auto dstaddr = dstAddrs[pid];
    if (dstaddr != nullptr) {
      auto r = partition2RowOffsetBase_[pid]; /*8k*/
      auto size = partition2RowOffsetBase_[pid + 1];
      auto dstOffset = partitionBufferBase_[pid];
      auto dstOffsetInByte = (8 - (dstOffset & 0x7)) & 0x7;
      auto dstIdxByte = dstOffsetInByte;
      auto dst = dstaddr[dstOffset >> 3];

      for (; r < size && dstIdxByte > 0; r++, dstIdxByte--) {
        auto srcOffset = rowOffset2RowId_[r]; /*16k*/
        auto src = srcAddr[srcOffset >> 3];
        src = src >> (srcOffset & 7) | 0xfe; // get the bit in bit 0, other bits set to 1
#if defined(__x86_64__)
        src = __rolb(src, 8 - dstIdxByte);
#else
        src = rotateLeft(src, (8 - dstIdxByte));
#endif
        dst = dst & src; // only take the useful bit.
      }
      dstaddr[dstOffset >> 3] = dst;
      if (r == size) {
        continue;
      }
      dstOffset += dstOffsetInByte;
      // now dst_offset is 8 aligned
      for (; r + 8 < size; r += 8) {
        uint8_t src = 0;
        auto srcOffset = rowOffset2RowId_[r]; /*16k*/
        src = srcAddr[srcOffset >> 3];
        // PREFETCHT0((&(srcAddr)[(srcOffset >> 3) + 64]));
        dst = src >> (srcOffset & 7) | 0xfe; // get the bit in bit 0, other bits set to 1

        srcOffset = rowOffset2RowId_[r + 1]; /*16k*/
        src = srcAddr[srcOffset >> 3];
        dst &= src >> (srcOffset & 7) << 1 | 0xfd; // get the bit in bit 0, other bits set to 1

        srcOffset = rowOffset2RowId_[r + 2]; /*16k*/
        src = srcAddr[srcOffset >> 3];
        dst &= src >> (srcOffset & 7) << 2 | 0xfb; // get the bit in bit 0, other bits set to 1

        srcOffset = rowOffset2RowId_[r + 3]; /*16k*/
        src = srcAddr[srcOffset >> 3];
        dst &= src >> (srcOffset & 7) << 3 | 0xf7; // get the bit in bit 0, other bits set to 1

        srcOffset = rowOffset2RowId_[r + 4]; /*16k*/
        src = srcAddr[srcOffset >> 3];
        dst &= src >> (srcOffset & 7) << 4 | 0xef; // get the bit in bit 0, other bits set to 1

        srcOffset = rowOffset2RowId_[r + 5]; /*16k*/
        src = srcAddr[srcOffset >> 3];
        dst &= src >> (srcOffset & 7) << 5 | 0xdf; // get the bit in bit 0, other bits set to 1

        srcOffset = rowOffset2RowId_[r + 6]; /*16k*/
        src = srcAddr[srcOffset >> 3];
        dst &= src >> (srcOffset & 7) << 6 | 0xbf; // get the bit in bit 0, other bits set to 1

        srcOffset = rowOffset2RowId_[r + 7]; /*16k*/
        src = srcAddr[srcOffset >> 3];
        dst &= src >> (srcOffset & 7) << 7 | 0x7f; // get the bit in bit 0, other bits set to 1

        dstaddr[dstOffset >> 3] = dst;
        dstOffset += 8;
        //_mm_prefetch(dstaddr + (dst_offset >> 3) + 64, _MM_HINT_T0);
      }
      // last byte, set it to 0xff is ok
      dst = 0xff;
      dstIdxByte = 0;
      for (; r < size; r++, dstIdxByte++) {
        auto srcOffset = rowOffset2RowId_[r]; /*16k*/
        auto src = srcAddr[srcOffset >> 3];
        src = src >> (srcOffset & 7) | 0xfe; // get the bit in bit 0, other bits set to 1
#if defined(__x86_64__)
        src = __rolb(src, dstIdxByte);
#else
        src = rotateLeft(src, dstIdxByte);
#endif
        dst = dst & src; // only take the useful bit.
      }
      dstaddr[dstOffset >> 3] = dst;
    }
  }
}

arrow::Status VeloxHashShuffleWriter::splitValidityBuffer(const facebook::velox::RowVector& rv) {
  for (size_t col = 0; col < simpleColumnIndices_.size(); ++col) {
    auto colIdx = simpleColumnIndices_[col];
    auto& column = rv.childAt(colIdx);
    if (vectorHasNull(column)) {
      auto& dstAddrs = partitionValidityAddrs_[col];
      for (auto& pid : partitionUsed_) {
        if (dstAddrs[pid] == nullptr) {
          // Init bitmap if it's null.
          ARROW_ASSIGN_OR_RAISE(
              auto validityBuffer,
              arrow::AllocateResizableBuffer(
                  arrow::bit_util::BytesForBits(partitionBufferSize_[pid]), partitionBufferPool_.get()));
          dstAddrs[pid] = const_cast<uint8_t*>(validityBuffer->data());
          memset(validityBuffer->mutable_data(), 0xff, validityBuffer->capacity());
          partitionBuffers_[col][pid][kValidityBufferIndex] = std::move(validityBuffer);
        }
      }

      auto srcAddr = (const uint8_t*)(column->mutableRawNulls());
      splitBoolType(srcAddr, dstAddrs);
    } else {
      VsPrintLF(colIdx, " column hasn't null");
    }
  }
  return arrow::Status::OK();
}

arrow::Status VeloxHashShuffleWriter::splitBinaryType(
    uint32_t binaryIdx,
    const facebook::velox::FlatVector<facebook::velox::StringView>& src,
    std::vector<BinaryBuf>& dst) {
  const auto* srcRawValues = src.rawValues();
  const auto* srcRawNulls = src.rawNulls();

  for (auto& pid : partitionUsed_) {
    auto& binaryBuf = dst[pid];

    // use 32bit offset
    auto dstLengthBase = reinterpret_cast<StringLengthType*>(binaryBuf.lengthPtr) + partitionBufferBase_[pid];

    auto valueOffset = binaryBuf.valueOffset;
    auto dstValuePtr = binaryBuf.valuePtr + valueOffset;
    auto capacity = binaryBuf.valueCapacity;

    auto rowOffsetBase = partition2RowOffsetBase_[pid];
    auto numRows = partition2RowCount_[pid];
    auto multiply = 1;

    for (auto i = 0; i < numRows; i++) {
      auto rowId = rowOffset2RowId_[rowOffsetBase + i];
      auto& stringView = srcRawValues[rowId];
      size_t isNull = srcRawNulls && facebook::velox::bits::isBitNull(srcRawNulls, rowId);
      auto stringLen = (isNull - 1) & stringView.size();

      // 1. copy length, update offset.
      dstLengthBase[i] = stringLen;
      valueOffset += stringLen;

      // Resize if necessary.
      if (valueOffset >= capacity) {
        auto oldCapacity = capacity;
        (void)oldCapacity; // suppress warning
        capacity = capacity + std::max((capacity >> multiply), static_cast<uint64_t>(stringLen));
        multiply = std::min(3, multiply + 1);

        const auto& valueBuffer = partitionBuffers_[fixedWidthColumnCount_ + binaryIdx][pid][kBinaryValueBufferIndex];
        {
          binaryArrayResizeState_ = BinaryArrayResizeState{pid, binaryIdx};
          BinaryArrayResizeGuard guard(binaryArrayResizeState_);
          RETURN_NOT_OK(valueBuffer->Reserve(capacity));
        }

        binaryBuf.valuePtr = valueBuffer->mutable_data();
        binaryBuf.valueCapacity = capacity;
        dstValuePtr = binaryBuf.valuePtr + valueOffset - stringLen;
        // Need to update dstLengthBase because lengthPtr can be updated if Reserve triggers spill.
        dstLengthBase = reinterpret_cast<StringLengthType*>(binaryBuf.lengthPtr) + partitionBufferBase_[pid];
      }

      // 2. copy value
      gluten::fastCopy(dstValuePtr, stringView.data(), stringLen);

      dstValuePtr += stringLen;
    }

    binaryBuf.valueOffset = valueOffset;
  }
  return arrow::Status::OK();
}

arrow::Status VeloxHashShuffleWriter::splitBinaryArray(const facebook::velox::RowVector& rv) {
  for (auto col = fixedWidthColumnCount_; col < simpleColumnIndices_.size(); ++col) {
    auto binaryIdx = col - fixedWidthColumnCount_;
    auto& dstAddrs = partitionBinaryAddrs_[binaryIdx];
    auto colIdx = simpleColumnIndices_[col];
    auto column = rv.childAt(colIdx)->asFlatVector<facebook::velox::StringView>();
    RETURN_NOT_OK(splitBinaryType(binaryIdx, *column, dstAddrs));
  }
  return arrow::Status::OK();
}

arrow::Status VeloxHashShuffleWriter::splitComplexType(const facebook::velox::RowVector& rv) {
  if (complexColumnIndices_.size() == 0) {
    return arrow::Status::OK();
  }
  auto numRows = rv.size();
  std::vector<std::vector<facebook::velox::IndexRange>> rowIndexs;
  rowIndexs.resize(numPartitions_);
  // TODO: maybe an estimated row is more reasonable
  for (auto row = 0; row < numRows; ++row) {
    auto partition = row2Partition_[row];
    if (complexTypeData_[partition] == nullptr) {
      // TODO: maybe memory issue, copy many times
      if (arenas_[partition] == nullptr) {
        arenas_[partition] = std::make_unique<facebook::velox::StreamArena>(veloxPool_.get());
      }
      complexTypeData_[partition] = serde_.createIterativeSerializer(
          complexWriteType_, partition2RowCount_[partition], arenas_[partition].get(), &serdeOptions_);
    }
    rowIndexs[partition].emplace_back(facebook::velox::IndexRange{row, 1});
  }

  std::vector<facebook::velox::VectorPtr> children;
  children.reserve(complexColumnIndices_.size());
  for (size_t i = 0; i < complexColumnIndices_.size(); ++i) {
    auto colIdx = complexColumnIndices_[i];
    children.emplace_back(rv.childAt(colIdx));
  }
  auto rowVector = std::make_shared<facebook::velox::RowVector>(
      veloxPool_.get(), complexWriteType_, facebook::velox::BufferPtr(nullptr), rv.size(), std::move(children));

  for (auto& pid : partitionUsed_) {
    if (rowIndexs[pid].size() != 0) {
      auto old = arenas_[pid]->size();
      complexTypeData_[pid]->append(rowVector, folly::Range(rowIndexs[pid].data(), rowIndexs[pid].size()));
      complexTotalSizeBytes_ += arenas_[pid]->size() - old;
    }
  }

  return arrow::Status::OK();
}

arrow::Status VeloxHashShuffleWriter::initColumnTypes(const facebook::velox::RowVector& rv) {
  schema_ = toArrowSchema(rv.type(), veloxPool_.get());
  veloxColumnTypes_.reserve(rv.childrenSize());
  for (size_t i = 0; i < rv.childrenSize(); ++i) {
    veloxColumnTypes_.push_back(rv.childAt(i)->type());
  }

  VsPrintSplitLF("schema_", schema_->ToString());

  // get arrow_column_types_ from schema
  ARROW_ASSIGN_OR_RAISE(arrowColumnTypes_, toShuffleTypeId(schema_->fields()));

  std::vector<std::string> complexNames;
  std::vector<facebook::velox::TypePtr> complexChildrens;

  for (size_t i = 0; i < arrowColumnTypes_.size(); ++i) {
    switch (arrowColumnTypes_[i]->id()) {
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id: {
        binaryColumnIndices_.push_back(i);
        isValidityBuffer_.push_back(true);
        isValidityBuffer_.push_back(false);
        isValidityBuffer_.push_back(false);
      } break;
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::ListType::type_id: {
        complexColumnIndices_.push_back(i);
        complexNames.emplace_back(veloxColumnTypes_[i]->name());
        complexChildrens.emplace_back(veloxColumnTypes_[i]);
        hasComplexType_ = true;
      } break;
      case arrow::BooleanType::type_id: {
        simpleColumnIndices_.push_back(i);
        isValidityBuffer_.push_back(true);
        isValidityBuffer_.push_back(boolIsBit());
      } break;
      case arrow::NullType::type_id:
        break;
      default: {
        simpleColumnIndices_.push_back(i);
        isValidityBuffer_.push_back(true);
        isValidityBuffer_.push_back(false);
      } break;
    }
  }

  if (hasComplexType_) {
    isValidityBuffer_.push_back(false);
  }

  fixedWidthColumnCount_ = simpleColumnIndices_.size();

  simpleColumnIndices_.insert(simpleColumnIndices_.end(), binaryColumnIndices_.begin(), binaryColumnIndices_.end());

  printColumnsInfo();

  binaryArrayTotalSizeBytes_.resize(binaryColumnIndices_.size(), 0);

  inputHasNull_.resize(simpleColumnIndices_.size(), false);

  complexTypeData_.resize(numPartitions_);
  complexTypeFlushBuffer_.resize(numPartitions_);

  complexWriteType_ = std::make_shared<facebook::velox::RowType>(std::move(complexNames), std::move(complexChildrens));

  return arrow::Status::OK();
}

arrow::Status VeloxHashShuffleWriter::initFromRowVector(const facebook::velox::RowVector& rv) {
  if (veloxColumnTypes_.empty()) {
    RETURN_NOT_OK(initColumnTypes(rv));
    RETURN_NOT_OK(initPartitions());
    calculateSimpleColumnBytes();
  }
  return arrow::Status::OK();
}

inline bool VeloxHashShuffleWriter::beyondThreshold(uint32_t partitionId, uint32_t newSize) {
  auto currentBufferSize = partitionBufferSize_[partitionId];
  return newSize > (1 + splitBufferReallocThreshold_) * currentBufferSize ||
      newSize < (1 - splitBufferReallocThreshold_) * currentBufferSize;
}

void VeloxHashShuffleWriter::calculateSimpleColumnBytes() {
  fixedWidthBufferBytes_ = 0;
  for (size_t col = 0; col < fixedWidthColumnCount_; ++col) {
    auto colIdx = simpleColumnIndices_[col];
    // `bool(1) >> 3` gets 0, so +7
    fixedWidthBufferBytes_ += ((arrow::bit_width(arrowColumnTypes_[colIdx]->id()) + 7) >> 3);
  }
  fixedWidthBufferBytes_ += kSizeOfStringLength * binaryColumnIndices_.size();
}

uint32_t VeloxHashShuffleWriter::calculatePartitionBufferSize(const facebook::velox::RowVector& rv, int64_t memLimit) {
  auto bytesPerRow = fixedWidthBufferBytes_;

  SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingCalculateBufferSize]);
  auto numRows = rv.size();
  // Calculate average size bytes (bytes per row) for each binary array.
  std::vector<uint64_t> binaryArrayAvgBytesPerRow(binaryColumnIndices_.size());
  for (size_t i = 0; i < binaryColumnIndices_.size(); ++i) {
    uint64_t binarySizeBytes = 0;
    auto column = rv.childAt(binaryColumnIndices_[i])->asFlatVector<facebook::velox::StringView>();

    const auto* srcRawValues = column->rawValues();
    const auto* srcRawNulls = column->rawNulls();

    for (auto idx = 0; idx < numRows; idx++) {
      auto& stringView = srcRawValues[idx];
      size_t isNull = srcRawNulls && facebook::velox::bits::isBitNull(srcRawNulls, idx);
      auto stringLen = (isNull - 1) & stringView.size();
      binarySizeBytes += stringLen;
    }

    binaryArrayTotalSizeBytes_[i] += binarySizeBytes;
    binaryArrayAvgBytesPerRow[i] = binaryArrayTotalSizeBytes_[i] / (totalInputNumRows_ + numRows);
    bytesPerRow += binaryArrayAvgBytesPerRow[i];
  }

  VS_PRINT_VECTOR_MAPPING(binaryArrayAvgBytesPerRow);

  if (totalInputNumRows_ > 0) {
    bytesPerRow += complexTotalSizeBytes_ / totalInputNumRows_;
  }

  VS_PRINTLF(bytesPerRow);

  // Remove the cache memory size since it can be spilled.
  // The logic here is to keep the split buffer as large as possible, to get max batch size for reducer.
  memLimit += cachedPayloadSize();

  // make sure split buffer uses 128M memory at least, let's hardcode it here for now
  if (memLimit < kMinMemLimit) {
    memLimit = kMinMemLimit;
  }

  uint64_t preAllocRowCnt =
      memLimit > 0 && bytesPerRow > 0 ? memLimit / bytesPerRow / numPartitions_ >> 2 : splitBufferSize_;
  preAllocRowCnt = std::min(preAllocRowCnt, static_cast<uint64_t>(splitBufferSize_));

  DLOG(INFO) << "Calculated partition buffer size -  memLimit: " << memLimit << ", bytesPerRow: " << bytesPerRow
             << ", preAllocRowCnt: " << preAllocRowCnt << std::endl;

  VS_PRINTLF(preAllocRowCnt);

  totalInputNumRows_ += numRows;

  maxBatchSize_ = preAllocRowCnt == 0 ? numPartitions_ : preAllocRowCnt * numPartitions_;

  return (uint32_t)preAllocRowCnt;
}

arrow::Result<std::shared_ptr<arrow::ResizableBuffer>>
VeloxHashShuffleWriter::allocateValidityBuffer(uint32_t col, uint32_t partitionId, uint32_t newSize) {
  if (inputHasNull_[col]) {
    ARROW_ASSIGN_OR_RAISE(
        auto validityBuffer,
        arrow::AllocateResizableBuffer(arrow::bit_util::BytesForBits(newSize), partitionBufferPool_.get()));
    // initialize all true once allocated
    memset(validityBuffer->mutable_data(), 0xff, validityBuffer->capacity());
    partitionValidityAddrs_[col][partitionId] = validityBuffer->mutable_data();
    return validityBuffer;
  }
  partitionValidityAddrs_[col][partitionId] = nullptr;
  return nullptr;
}

arrow::Status VeloxHashShuffleWriter::updateValidityBuffers(uint32_t partitionId, uint32_t newSize) {
  for (auto i = 0; i < simpleColumnIndices_.size(); ++i) {
    // If the validity buffer is not yet allocated, allocate and fill 0xff based on inputHasNull_.
    if (partitionValidityAddrs_[i][partitionId] == nullptr) {
      ARROW_ASSIGN_OR_RAISE(
          partitionBuffers_[i][partitionId][kValidityBufferIndex], allocateValidityBuffer(i, partitionId, newSize));
    }
  }
  return arrow::Status::OK();
}

arrow::Status VeloxHashShuffleWriter::allocatePartitionBuffer(uint32_t partitionId, uint32_t newSize) {
  SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingAllocateBuffer]);

  for (auto i = 0; i < simpleColumnIndices_.size(); ++i) {
    auto columnType = schema_->field(simpleColumnIndices_[i])->type()->id();
    auto& buffers = partitionBuffers_[i][partitionId];

    std::shared_ptr<arrow::ResizableBuffer> validityBuffer{};
    ARROW_ASSIGN_OR_RAISE(validityBuffer, allocateValidityBuffer(i, partitionId, newSize));
    switch (columnType) {
      // binary types
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id: {
        auto binaryIdx = i - fixedWidthColumnCount_;

        std::shared_ptr<arrow::ResizableBuffer> lengthBuffer{};
        auto lengthBufferSize = newSize * kSizeOfStringLength;
        ARROW_ASSIGN_OR_RAISE(
            lengthBuffer, arrow::AllocateResizableBuffer(lengthBufferSize, partitionBufferPool_.get()));

        std::shared_ptr<arrow::ResizableBuffer> valueBuffer{};
        auto valueBufferSize = valueBufferSizeForBinaryArray(binaryIdx, newSize);
        ARROW_ASSIGN_OR_RAISE(valueBuffer, arrow::AllocateResizableBuffer(valueBufferSize, partitionBufferPool_.get()));

        partitionBinaryAddrs_[binaryIdx][partitionId] =
            BinaryBuf(valueBuffer->mutable_data(), lengthBuffer->mutable_data(), valueBufferSize);
        buffers = {std::move(validityBuffer), std::move(lengthBuffer), std::move(valueBuffer)};
        break;
      }
      case arrow::NullType::type_id: {
        break;
      }
      default: { // fixed-width types
        std::shared_ptr<arrow::ResizableBuffer> valueBuffer{};
        ARROW_ASSIGN_OR_RAISE(
            valueBuffer,
            arrow::AllocateResizableBuffer(valueBufferSizeForFixedWidthArray(i, newSize), partitionBufferPool_.get()));
        partitionFixedWidthValueAddrs_[i][partitionId] = valueBuffer->mutable_data();
        buffers = {std::move(validityBuffer), std::move(valueBuffer)};
        break;
      }
    }
  }
  partitionBufferSize_[partitionId] = newSize;
  return arrow::Status::OK();
}

arrow::Status VeloxHashShuffleWriter::evictBuffers(
    uint32_t partitionId,
    uint32_t numRows,
    std::vector<std::shared_ptr<arrow::Buffer>> buffers,
    bool reuseBuffers) {
  if (!buffers.empty()) {
    auto payload =
        std::make_unique<InMemoryPayload>(numRows, &isValidityBuffer_, schema_, std::move(buffers), hasComplexType_);
    RETURN_NOT_OK(partitionWriter_->hashEvict(partitionId, std::move(payload), Evict::kCache, reuseBuffers, writtenBytes_));
  }
  return arrow::Status::OK();
}

arrow::Status VeloxHashShuffleWriter::evictPartitionBuffers(uint32_t partitionId, bool reuseBuffers) {
  auto numRows = partitionBufferBase_[partitionId];
  if (numRows > 0) {
    ARROW_ASSIGN_OR_RAISE(auto buffers, assembleBuffers(partitionId, reuseBuffers));
    RETURN_NOT_OK(evictBuffers(partitionId, numRows, std::move(buffers), reuseBuffers));
  }
  return arrow::Status::OK();
}

arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>> VeloxHashShuffleWriter::assembleBuffers(
    uint32_t partitionId,
    bool reuseBuffers) {
  SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingCreateRbFromBuffer]);

  auto numRows = partitionBufferBase_[partitionId];
  auto fixedWidthIdx = 0;
  auto binaryIdx = 0;
  auto numFields = schema_->num_fields();

  std::vector<std::shared_ptr<arrow::Array>> arrays(numFields);
  std::vector<std::shared_ptr<arrow::Buffer>> allBuffers;
  // One column should have 2 buffers at least, string column has 3 column buffers.
  allBuffers.reserve(fixedWidthColumnCount_ * 2 + binaryColumnIndices_.size() * 3 + hasComplexType_);
  for (int i = 0; i < numFields; ++i) {
    switch (arrowColumnTypes_[i]->id()) {
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id: {
        const auto& buffers = partitionBuffers_[fixedWidthColumnCount_ + binaryIdx][partitionId];
        auto& binaryBuf = partitionBinaryAddrs_[binaryIdx][partitionId];
        // validity buffer
        if (buffers[kValidityBufferIndex] != nullptr) {
          auto validityBufferSize = arrow::bit_util::BytesForBits(numRows);
          if (reuseBuffers) {
            allBuffers.push_back(
                arrow::SliceBuffer(buffers[kValidityBufferIndex], 0, arrow::bit_util::BytesForBits(numRows)));
          } else {
            RETURN_NOT_OK(buffers[kValidityBufferIndex]->Resize(validityBufferSize, true));
            allBuffers.push_back(std::move(buffers[kValidityBufferIndex]));
          }
        } else {
          allBuffers.push_back(nullptr);
        }
        // Length buffer.
        auto lengthBufferSize = numRows * kSizeOfStringLength;
        ARROW_RETURN_IF(
            !buffers[kBinaryLengthBufferIndex], arrow::Status::Invalid("Offset buffer of binary array is null."));
        if (reuseBuffers) {
          allBuffers.push_back(arrow::SliceBuffer(buffers[kBinaryLengthBufferIndex], 0, lengthBufferSize));
        } else {
          RETURN_NOT_OK(buffers[kBinaryLengthBufferIndex]->Resize(lengthBufferSize, true));
          allBuffers.push_back(std::move(buffers[kBinaryLengthBufferIndex]));
        }

        // Value buffer.
        auto valueBufferSize = binaryBuf.valueOffset;
        ARROW_RETURN_IF(
            !buffers[kBinaryValueBufferIndex], arrow::Status::Invalid("Value buffer of binary array is null."));
        if (reuseBuffers) {
          allBuffers.push_back(arrow::SliceBuffer(buffers[kBinaryValueBufferIndex], 0, valueBufferSize));
        } else if (valueBufferSize > 0) {
          RETURN_NOT_OK(buffers[kBinaryValueBufferIndex]->Resize(valueBufferSize, true));
          allBuffers.push_back(std::move(buffers[kBinaryValueBufferIndex]));
        } else {
          // Binary value buffer size can be 0, in which case cannot be resized.
          allBuffers.push_back(zeroLengthNullBuffer());
        }

        if (reuseBuffers) {
          // Set the first value offset to 0.
          binaryBuf.valueOffset = 0;
        }
        binaryIdx++;
        break;
      }
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::ListType::type_id:
        break;
      case arrow::NullType::type_id: {
        break;
      }
      default: {
        auto& buffers = partitionBuffers_[fixedWidthIdx][partitionId];
        // validity buffer
        if (buffers[kValidityBufferIndex] != nullptr) {
          auto validityBufferSize = arrow::bit_util::BytesForBits(numRows);
          if (reuseBuffers) {
            allBuffers.push_back(
                arrow::SliceBuffer(buffers[kValidityBufferIndex], 0, arrow::bit_util::BytesForBits(numRows)));
          } else {
            RETURN_NOT_OK(buffers[kValidityBufferIndex]->Resize(validityBufferSize, true));
            allBuffers.push_back(std::move(buffers[kValidityBufferIndex]));
          }
        } else {
          allBuffers.push_back(nullptr);
        }
        // Value buffer.
        uint64_t valueBufferSize = 0;
        auto& valueBuffer = buffers[kFixedWidthValueBufferIndex];
        ARROW_RETURN_IF(!valueBuffer, arrow::Status::Invalid("Value buffer of fixed-width array is null."));
        if (arrowColumnTypes_[i]->id() == arrow::BooleanType::type_id) {
          valueBufferSize = valueBufferSizeForBool(numRows);
        } else if (veloxColumnTypes_[i]->isShortDecimal()) {
          valueBufferSize = numRows * (arrow::bit_width(arrow::Int64Type::type_id) >> 3);
        } else if (veloxColumnTypes_[i]->kind() == facebook::velox::TypeKind::TIMESTAMP) {
          valueBufferSize = valueBufferSizeForTimestamp(numRows);
        } else {
          valueBufferSize = numRows * (arrow::bit_width(arrowColumnTypes_[i]->id()) >> 3);
        }
        if (reuseBuffers) {
          auto slicedValueBuffer = arrow::SliceBuffer(valueBuffer, 0, valueBufferSize);
          allBuffers.push_back(std::move(slicedValueBuffer));
        } else {
          RETURN_NOT_OK(buffers[kFixedWidthValueBufferIndex]->Resize(valueBufferSize, true));
          allBuffers.push_back(std::move(buffers[kFixedWidthValueBufferIndex]));
        }
        fixedWidthIdx++;
        break;
      }
    }
  }
  if (hasComplexType_ && complexTypeData_[partitionId] != nullptr) {
    auto flushBuffer = complexTypeFlushBuffer_[partitionId];
    auto serializedSize = complexTypeData_[partitionId]->maxSerializedSize();
    if (flushBuffer == nullptr) {
      ARROW_ASSIGN_OR_RAISE(flushBuffer, arrow::AllocateResizableBuffer(serializedSize, partitionBufferPool_.get()));
    } else if (serializedSize > flushBuffer->capacity()) {
      RETURN_NOT_OK(flushBuffer->Reserve(serializedSize));
    }
    auto valueBuffer = arrow::SliceMutableBuffer(flushBuffer, 0, serializedSize);
    auto output = std::make_shared<arrow::io::FixedSizeBufferWriter>(valueBuffer);
    facebook::velox::serializer::presto::PrestoOutputStreamListener listener;
    ArrowFixedSizeBufferOutputStream out(output, &listener);
    complexTypeData_[partitionId]->flush(&out);
    allBuffers.emplace_back(valueBuffer);
    complexTypeData_[partitionId] = nullptr;
    arenas_[partitionId] = nullptr;
  }

  partitionBufferBase_[partitionId] = 0;
  if (!reuseBuffers) {
    RETURN_NOT_OK(resetPartitionBuffer(partitionId));
  }
  return allBuffers;
}

arrow::Status VeloxHashShuffleWriter::reclaimFixedSize(int64_t size, int64_t* actual) {
  if (evictState_ == EvictState::kUnevictable) {
    *actual = 0;
    return arrow::Status::OK();
  }
  EvictGuard evictGuard{evictState_};

  int64_t reclaimed = 0;
  if (reclaimed < size) {
    ARROW_ASSIGN_OR_RAISE(auto cached, evictCachedPayload(size - reclaimed));
    reclaimed += cached;
  }
  if (reclaimed < size && shrinkPartitionBuffersAfterSpill()) {
    ARROW_ASSIGN_OR_RAISE(auto shrunken, shrinkPartitionBuffersMinSize(size - reclaimed));
    reclaimed += shrunken;
  }
  if (reclaimed < size && evictPartitionBuffersAfterSpill()) {
    ARROW_ASSIGN_OR_RAISE(auto evicted, evictPartitionBuffersMinSize(size - reclaimed));
    reclaimed += evicted;
  }
  *actual = reclaimed;
  return arrow::Status::OK();
}

arrow::Result<int64_t> VeloxHashShuffleWriter::evictCachedPayload(int64_t size) {
  SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingEvictPartition]);
  int64_t actual;
  auto before = partitionBufferPool_->bytes_allocated();
  RETURN_NOT_OK(partitionWriter_->reclaimFixedSize(size, &actual));
  // Need to count the changes from partitionBufferPool as well.
  // When the evicted partition buffers are not copied, the merged ones
  // are resized from the original buffers thus allocated from partitionBufferPool.
  actual += before - partitionBufferPool_->bytes_allocated();

  DLOG(INFO) << "Evicted all cached payloads. " << std::to_string(actual) << " bytes released" << std::endl;
  return actual;
}

arrow::Status VeloxHashShuffleWriter::resetValidityBuffer(uint32_t partitionId) {
  std::for_each(partitionBuffers_.begin(), partitionBuffers_.end(), [partitionId](auto& bufs) {
    if (bufs[partitionId].size() != 0 && bufs[partitionId][kValidityBufferIndex] != nullptr) {
      // initialize all true once allocated
      auto validityBuffer = bufs[partitionId][kValidityBufferIndex];
      memset(validityBuffer->mutable_data(), 0xff, validityBuffer->capacity());
    }
  });
  return arrow::Status::OK();
}

arrow::Status VeloxHashShuffleWriter::resizePartitionBuffer(uint32_t partitionId, uint32_t newSize, bool preserveData) {
  for (auto i = 0; i < simpleColumnIndices_.size(); ++i) {
    auto columnType = schema_->field(simpleColumnIndices_[i])->type()->id();
    auto& buffers = partitionBuffers_[i][partitionId];

    // Handle validity buffer first.
    auto& validityBuffer = buffers[kValidityBufferIndex];
    if (!preserveData) {
      ARROW_ASSIGN_OR_RAISE(validityBuffer, allocateValidityBuffer(i, partitionId, newSize));
    } else if (buffers[kValidityBufferIndex]) {
      // Resize validity.
      auto filled = validityBuffer->capacity();
      RETURN_NOT_OK(validityBuffer->Resize(arrow::bit_util::BytesForBits(newSize)));
      partitionValidityAddrs_[i][partitionId] = validityBuffer->mutable_data();

      // If newSize is larger, fill 1 to the newly allocated bytes.
      if (validityBuffer->capacity() > filled) {
        memset(validityBuffer->mutable_data() + filled, 0xff, validityBuffer->capacity() - filled);
      }
    }

    // Resize value buffer if fixed-width, offset & value buffers if binary.
    switch (columnType) {
      // binary types
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id: {
        // Resize length buffer.
        auto binaryIdx = i - fixedWidthColumnCount_;
        auto& binaryBuf = partitionBinaryAddrs_[binaryIdx][partitionId];
        auto& lengthBuffer = buffers[kBinaryLengthBufferIndex];
        ARROW_RETURN_IF(!lengthBuffer, arrow::Status::Invalid("Offset buffer of binary array is null."));
        RETURN_NOT_OK(lengthBuffer->Resize(newSize * kSizeOfStringLength));

        // Skip Resize value buffer if the spill is triggered by resizing this split binary buffer.
        // Only update length buffer ptr.
        if (binaryArrayResizeState_.inResize && partitionId == binaryArrayResizeState_.partitionId &&
            binaryIdx == binaryArrayResizeState_.binaryIdx) {
          binaryBuf.lengthPtr = lengthBuffer->mutable_data();
          break;
        }

        // Resize value buffer.
        auto& valueBuffer = buffers[kBinaryValueBufferIndex];
        ARROW_RETURN_IF(!valueBuffer, arrow::Status::Invalid("Value buffer of binary array is null."));
        // Determine the new Size for value buffer.
        auto valueBufferSize = valueBufferSizeForBinaryArray(binaryIdx, newSize);
        // If shrink is triggered by spill, and binary new size is larger, do not resize the buffer to avoid issuing
        // another spill. Only update length buffer ptr.
        if (evictState_ == EvictState::kUnevictable && newSize <= partitionBufferSize_[partitionId] &&
            valueBufferSize >= valueBuffer->size()) {
          binaryBuf.lengthPtr = lengthBuffer->mutable_data();
          break;
        }
        auto valueOffset = 0;
        // If preserve data, the new valueBufferSize should not be smaller than the current offset.
        if (preserveData) {
          valueBufferSize = std::max(binaryBuf.valueOffset, valueBufferSize);
          valueOffset = binaryBuf.valueOffset;
        }
        RETURN_NOT_OK(valueBuffer->Resize(valueBufferSize));

        binaryBuf = BinaryBuf(valueBuffer->mutable_data(), lengthBuffer->mutable_data(), valueBufferSize, valueOffset);
        break;
      }
      case arrow::NullType::type_id:
        break;
      default: { // fixed-width types
        auto& valueBuffer = buffers[kFixedWidthValueBufferIndex];
        ARROW_RETURN_IF(!valueBuffer, arrow::Status::Invalid("Value buffer of fixed-width array is null."));
        RETURN_NOT_OK(valueBuffer->Resize(valueBufferSizeForFixedWidthArray(i, newSize)));
        partitionFixedWidthValueAddrs_[i][partitionId] = valueBuffer->mutable_data();
        break;
      }
    }
  }
  partitionBufferSize_[partitionId] = newSize;
  return arrow::Status::OK();
}

arrow::Status VeloxHashShuffleWriter::shrinkPartitionBuffer(uint32_t partitionId) {
  auto bufferSize = partitionBufferSize_[partitionId];
  if (bufferSize == 0) {
    return arrow::Status::OK();
  }

  ARROW_ASSIGN_OR_RAISE(auto newSize, partitionBufferSizeAfterShrink(partitionId));
  if (newSize > bufferSize) {
    std::stringstream invalid;
    invalid << "Cannot shrink to larger size. Partition: " << partitionId << ", before shrink: " << bufferSize
            << ", after shrink" << newSize;
    return arrow::Status::Invalid(invalid.str());
  }
  if (newSize == bufferSize) {
    // No space to shrink.
    return arrow::Status::OK();
  }
  if (newSize == 0) {
    return resetPartitionBuffer(partitionId);
  }
  return resizePartitionBuffer(partitionId, newSize, /*preserveData=*/true);
}

uint64_t VeloxHashShuffleWriter::valueBufferSizeForBinaryArray(uint32_t binaryIdx, uint32_t newSize) {
  return (binaryArrayTotalSizeBytes_[binaryIdx] + totalInputNumRows_ - 1) / totalInputNumRows_ * newSize + 1024;
}

uint64_t VeloxHashShuffleWriter::valueBufferSizeForBool(uint32_t newSize) {
  return arrow::bit_util::BytesForBits(newSize);
}

uint64_t VeloxHashShuffleWriter::valueBufferSizeForTimestamp(uint32_t newSize) {
  return facebook::velox::BaseVector::byteSize<facebook::velox::Timestamp>(newSize);
}

uint64_t VeloxHashShuffleWriter::valueBufferSizeForFixedWidthArray(uint32_t fixedWidthIndex, uint32_t newSize) {
  uint64_t valueBufferSize = 0;
  auto columnIdx = simpleColumnIndices_[fixedWidthIndex];
  if (arrowColumnTypes_[columnIdx]->id() == arrow::BooleanType::type_id) {
    valueBufferSize = valueBufferSizeForBool(newSize);
  } else if (veloxColumnTypes_[columnIdx]->isShortDecimal()) {
    valueBufferSize = newSize * (arrow::bit_width(arrow::Int64Type::type_id) >> 3);
  } else if (veloxColumnTypes_[columnIdx]->kind() == facebook::velox::TypeKind::TIMESTAMP) {
    valueBufferSize = valueBufferSizeForTimestamp(newSize);
  } else {
    valueBufferSize = newSize * (arrow::bit_width(arrowColumnTypes_[columnIdx]->id()) >> 3);
  }
  return valueBufferSize;
}

void VeloxHashShuffleWriter::stat() const {
#if VELOX_SHUFFLE_WRITER_LOG_FLAG
  for (int i = CpuWallTimingBegin; i != CpuWallTimingEnd; ++i) {
    std::ostringstream oss;
    auto& timing = cpuWallTimingList_[i];
    oss << "Velox shuffle writer stat:" << CpuWallTimingName((CpuWallTimingType)i);
    oss << " " << timing.toString();
    if (timing.count > 0) {
      oss << " wallNanos-avg:" << timing.wallNanos / timing.count;
      oss << " cpuNanos-avg:" << timing.cpuNanos / timing.count;
    }
    LOG(INFO) << oss.str();
  }
#endif
}

arrow::Status VeloxHashShuffleWriter::resetPartitionBuffer(uint32_t partitionId) {
  // Reset fixed-width partition buffers
  for (auto i = 0; i < fixedWidthColumnCount_; ++i) {
    partitionValidityAddrs_[i][partitionId] = nullptr;
    partitionFixedWidthValueAddrs_[i][partitionId] = nullptr;
    partitionBuffers_[i][partitionId].clear();
  }

  // Reset binary partition buffers
  for (auto i = 0; i < binaryColumnIndices_.size(); ++i) {
    auto binaryIdx = i + fixedWidthColumnCount_;
    partitionValidityAddrs_[binaryIdx][partitionId] = nullptr;
    partitionBinaryAddrs_[i][partitionId] = BinaryBuf();
    partitionBuffers_[binaryIdx][partitionId].clear();
  }

  partitionBufferSize_[partitionId] = 0;
  return arrow::Status::OK();
}

const uint64_t VeloxHashShuffleWriter::cachedPayloadSize() const {
  return partitionWriter_->cachedPayloadSize();
}

arrow::Result<int64_t> VeloxHashShuffleWriter::shrinkPartitionBuffersMinSize(int64_t size) {
  // Sort partition buffers by (partitionBufferSize_ - partitionBufferBase_)
  std::vector<std::pair<uint32_t, uint32_t>> pidToSize;
  for (auto pid = 0; pid < numPartitions_; ++pid) {
    if (partitionBufferInUse_.has_value() && *partitionBufferInUse_ == pid) {
      continue;
    }
    if (partitionBufferSize_[pid] > 0 && partitionBufferSize_[pid] > partitionBufferBase_[pid]) {
      pidToSize.emplace_back(pid, partitionBufferSize_[pid] - partitionBufferBase_[pid]);
    }
  }
  // No shrinkable partition buffer.
  if (pidToSize.empty()) {
    return 0;
  }

  std::sort(pidToSize.begin(), pidToSize.end(), [&](const auto& a, const auto& b) { return a.second > b.second; });

  auto beforeShrink = partitionBufferPool_->bytes_allocated();
  auto shrunken = 0;
  auto iter = pidToSize.begin();

  // Shrink in order to reclaim the largest amount of space with fewer resizes.
  do {
    RETURN_NOT_OK(shrinkPartitionBuffer(iter->first));
    shrunken = beforeShrink - partitionBufferPool_->bytes_allocated();
    iter++;
  } while (shrunken < size && iter != pidToSize.end());
  return shrunken;
}

arrow::Result<int64_t> VeloxHashShuffleWriter::evictPartitionBuffersMinSize(int64_t size) {
  // Evict partition buffers, only when splitState_ == SplitState::kInit, and space freed from
  // shrinking is not enough. In this case partitionBufferSize_ == partitionBufferBase_
  VELOX_CHECK(!partitionBufferInUse_);
  int64_t beforeEvict = partitionBufferPool_->bytes_allocated();
  int64_t evicted = 0;
  std::vector<std::pair<uint32_t, uint32_t>> pidToSize;
  for (auto pid = 0; pid < numPartitions_; ++pid) {
    if (partitionBufferSize_[pid] == 0) {
      continue;
    }
    pidToSize.emplace_back(pid, partitionBufferSize_[pid]);
  }
  std::sort(pidToSize.begin(), pidToSize.end(), [&](const auto& a, const auto& b) { return a.second > b.second; });
  if (!pidToSize.empty()) {
    for (auto& item : pidToSize) {
      auto pid = item.first;
      ARROW_ASSIGN_OR_RAISE(auto buffers, assembleBuffers(pid, false));
      auto payload = std::make_unique<InMemoryPayload>(
          item.second, &isValidityBuffer_, schema_, std::move(buffers), hasComplexType_);
      metrics_.totalBytesToEvict += payload->rawSize();
      RETURN_NOT_OK(partitionWriter_->hashEvict(pid, std::move(payload), Evict::kSpill, false, writtenBytes_));
      evicted = beforeEvict - partitionBufferPool_->bytes_allocated();
      if (evicted >= size) {
        break;
      }
    }
  }
  return evicted;
}

bool VeloxHashShuffleWriter::shrinkPartitionBuffersAfterSpill() const {
  // If OOM happens during SplitState::kSplit, it is triggered by binary buffers resize.
  // Or during SplitState::kInit, it is triggered by other operators.
  // Or during SplitState::kStopEvict, it is triggered by assembleBuffers allocating extra memory. In this case we use
  // PartitionBufferGuard to prevent the target partition from being shrunk.
  // The reclaim order is spill->shrink, because the partition buffers can be reused.
  // SinglePartitioning doesn't maintain partition buffers.
  return partitioning_ != Partitioning::kSingle &&
      (splitState_ == SplitState::kSplit || splitState_ == SplitState::kInit || splitState_ == SplitState::kStopEvict);
}

bool VeloxHashShuffleWriter::evictPartitionBuffersAfterSpill() const {
  // If OOM triggered by other operators, the splitState_ is SplitState::kInit.
  // The last resort is to evict the partition buffers to reclaim more space.
  return partitioning_ != Partitioning::kSingle && splitState_ == SplitState::kInit;
}

arrow::Result<uint32_t> VeloxHashShuffleWriter::partitionBufferSizeAfterShrink(uint32_t partitionId) const {
  if (splitState_ == SplitState::kSplit) {
    return partitionBufferBase_[partitionId] + partition2RowCount_[partitionId];
  }
  if (splitState_ == kInit || splitState_ == SplitState::kStopEvict) {
    return partitionBufferBase_[partitionId];
  }
  return arrow::Status::Invalid("Cannot shrink partition buffers in SplitState: " + std::to_string(splitState_));
}

arrow::Status VeloxHashShuffleWriter::preAllocPartitionBuffers(uint32_t preAllocBufferSize) {
  for (auto& pid : partitionUsed_) {
    auto newSize = std::max(preAllocBufferSize, partition2RowCount_[pid]);
    DLOG_IF(INFO, partitionBufferSize_[pid] != newSize)
        << "Actual partition buffer size - current: " << partitionBufferSize_[pid] << ", newSize: " << newSize
        << std::endl;
    // Make sure the size to be allocated is larger than the size to be filled.
    if (partitionBufferSize_[pid] == 0) {
      // Allocate buffer if it's not yet allocated.
      RETURN_NOT_OK(allocatePartitionBuffer(pid, newSize));
    } else if (beyondThreshold(pid, newSize)) {
      if (newSize <= partitionBufferBase_[pid]) {
        // If the newSize is smaller, cache the buffered data and reuse and shrink the buffer.
        RETURN_NOT_OK(evictPartitionBuffers(pid, true));
        RETURN_NOT_OK(resizePartitionBuffer(pid, newSize, /*preserveData=*/false));
      } else {
        // If the newSize is larger, check if alreadyFilled + toBeFilled <= newSize
        if (partitionBufferBase_[pid] + partition2RowCount_[pid] <= newSize) {
          // If so, keep the data in buffers and resize buffers.
          RETURN_NOT_OK(resizePartitionBuffer(pid, newSize, /*preserveData=*/true));
          // Because inputHasNull_ is updated every time split is called, and resizePartitionBuffer won't allocate
          // validity buffer.
          RETURN_NOT_OK(updateValidityBuffers(pid, newSize));
        } else {
          // Otherwise cache the buffered data.
          // If newSize <= allocated buffer size, reuse and shrink the buffer.
          // Else free and allocate new buffers.
          bool reuseBuffers = newSize <= partitionBufferSize_[pid];
          RETURN_NOT_OK(evictPartitionBuffers(pid, reuseBuffers));
          if (reuseBuffers) {
            RETURN_NOT_OK(resizePartitionBuffer(pid, newSize, /*preserveData=*/false));
          } else {
            RETURN_NOT_OK(allocatePartitionBuffer(pid, newSize));
          }
        }
      }
    } else if (partitionBufferBase_[pid] + partition2RowCount_[pid] > partitionBufferSize_[pid]) {
      // If the size to be filled + already filled > the buffer size, need to free current buffers and allocate new
      // buffer.
      if (newSize > partitionBufferSize_[pid]) {
        // If the partition size after split is already larger than allocated buffer size, need reallocate.
        RETURN_NOT_OK(evictPartitionBuffers(pid, false));
        RETURN_NOT_OK(allocatePartitionBuffer(pid, newSize));
      } else {
        // Partition size after split is smaller than buffer size. Reuse the buffers.
        RETURN_NOT_OK(evictPartitionBuffers(pid, true));
        // Reset validity buffer for reallocate.
        RETURN_NOT_OK(resetValidityBuffer(pid));
      }
    }
  }
  return arrow::Status::OK();
}

bool VeloxHashShuffleWriter::isExtremelyLargeBatch(facebook::velox::RowVectorPtr& rv) const {
  return (rv->size() > maxBatchSize_ && maxBatchSize_ > 0);
}

} // namespace gluten

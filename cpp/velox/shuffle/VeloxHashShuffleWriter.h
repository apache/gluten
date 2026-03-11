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

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "velox/common/time/CpuWallTimer.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/VectorStream.h"

#include <arrow/array/util.h>
#include <arrow/ipc/writer.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/type.h>

#include "VeloxShuffleWriter.h"
#include "memory/VeloxMemoryManager.h"
#include "shuffle/PartitionWriter.h"
#include "shuffle/Partitioner.h"
#include "shuffle/Utils.h"

#include "utils/Print.h"

namespace gluten {

// set 1 to open print
#define VELOX_SHUFFLE_WRITER_PRINT 0

#if VELOX_SHUFFLE_WRITER_PRINT

#define VsPrint Print
#define VsPrintLF PrintLF
#define VsPrintSplit PrintSplit
#define VsPrintSplitLF PrintSplitLF
#define VsPrintVectorRange PrintVectorRange
#define VS_PRINT PRINT
#define VS_PRINTLF PRINTLF
#define VS_PRINT_FUNCTION_NAME PRINT_FUNCTION_NAME
#define VS_PRINT_FUNCTION_SPLIT_LINE PRINT_FUNCTION_SPLIT_LINE
#define VS_PRINT_CONTAINER PRINT_CONTAINER
#define VS_PRINT_CONTAINER_TO_STRING PRINT_CONTAINER_TO_STRING
#define VS_PRINT_CONTAINER_2_STRING PRINT_CONTAINER_2_STRING
#define VS_PRINT_VECTOR_TO_STRING PRINT_VECTOR_TO_STRING
#define VS_PRINT_VECTOR_2_STRING PRINT_VECTOR_2_STRING
#define VS_PRINT_VECTOR_MAPPING PRINT_VECTOR_MAPPING

#else // VELOX_SHUFFLE_WRITER_PRINT

#define VsPrint(...) // NOLINT
#define VsPrintLF(...) // NOLINT
#define VsPrintSplit(...) // NOLINT
#define VsPrintSplitLF(...) // NOLINT
#define VsPrintVectorRange(...) // NOLINT
#define VS_PRINT(a)
#define VS_PRINTLF(a)
#define VS_PRINT_FUNCTION_NAME()
#define VS_PRINT_FUNCTION_SPLIT_LINE()
#define VS_PRINT_CONTAINER(c)
#define VS_PRINT_CONTAINER_TO_STRING(c)
#define VS_PRINT_CONTAINER_2_STRING(c)
#define VS_PRINT_VECTOR_TO_STRING(v)
#define VS_PRINT_VECTOR_2_STRING(v)
#define VS_PRINT_VECTOR_MAPPING(v)

#endif // end of VELOX_SHUFFLE_WRITER_PRINT

enum SplitState { kInit, kPreAlloc, kSplit, kStopEvict, kStop };

struct BinaryArrayResizeState {
  bool inResize;
  uint32_t partitionId;
  uint32_t binaryIdx;

  BinaryArrayResizeState() : inResize(false) {}
  BinaryArrayResizeState(uint32_t partitionId, uint32_t binaryIdx)
      : inResize(false), partitionId(partitionId), binaryIdx(binaryIdx) {}
};

class VeloxHashShuffleWriter : public VeloxShuffleWriter {
  enum {
    kValidityBufferIndex = 0,
    kFixedWidthValueBufferIndex = 1,
    kBinaryValueBufferIndex = 2,
    kBinaryLengthBufferIndex = kFixedWidthValueBufferIndex
  };

 public:
  struct BinaryBuf {
    BinaryBuf(uint8_t* value, uint8_t* length, uint64_t valueCapacityIn, uint64_t valueOffsetIn)
        : valuePtr(value), lengthPtr(length), valueCapacity(valueCapacityIn), valueOffset(valueOffsetIn) {}

    BinaryBuf(uint8_t* value, uint8_t* length, uint64_t valueCapacity) : BinaryBuf(value, length, valueCapacity, 0) {}

    BinaryBuf() : BinaryBuf(nullptr, nullptr, 0) {}

    uint8_t* valuePtr;
    uint8_t* lengthPtr;
    uint64_t valueCapacity;
    uint64_t valueOffset;
  };

  static arrow::Result<std::shared_ptr<VeloxShuffleWriter>> create(
      uint32_t numPartitions,
      const std::shared_ptr<PartitionWriter>& partitionWriter,
      const std::shared_ptr<ShuffleWriterOptions>& options,
      MemoryManager* memoryManager);

  arrow::Status write(std::shared_ptr<ColumnarBatch> cb, int64_t memLimit) override;

  arrow::Status stop() override;

  arrow::Status reclaimFixedSize(int64_t size, int64_t* actual) override;

  const uint64_t cachedPayloadSize() const override;

  arrow::Status evictPartitionBuffers(uint32_t partitionId, bool reuseBuffers) override;

  // For test only.
  void setPartitionBufferSize(uint32_t newSize) override;

  // for debugging
  void printColumnsInfo() const {
    VS_PRINT_FUNCTION_SPLIT_LINE();
    VS_PRINTLF(fixed_width_column_count_);

    VS_PRINT_CONTAINER(simple_column_indices_);
    VS_PRINT_CONTAINER(binary_column_indices_);
    VS_PRINT_CONTAINER(complex_column_indices_);

    VS_PRINT_VECTOR_2_STRING(velox_column_types_);
    VS_PRINT_VECTOR_TO_STRING(arrow_column_types_);
  }

  void printPartition() const {
    VS_PRINT_FUNCTION_SPLIT_LINE();
    // row ID -> partition ID
    VS_PRINT_VECTOR_MAPPING(row_2_partition_);

    // partition -> row count
    VS_PRINT_VECTOR_MAPPING(partition_2_row_count_);
  }

  void printPartitionBuffer() const {
    VS_PRINT_FUNCTION_SPLIT_LINE();
    VS_PRINT_VECTOR_MAPPING(partition_2_buffer_size_);
    VS_PRINT_VECTOR_MAPPING(partitionBufferBase_);
  }

  void printPartition2Row() const {
    VS_PRINT_FUNCTION_SPLIT_LINE();
    VS_PRINT_VECTOR_MAPPING(partition2RowOffsetBase_);

#if VELOX_SHUFFLE_WRITER_PRINT
    for (auto pid = 0; pid < numPartitions_; ++pid) {
      auto begin = partition2RowOffsetBase_[pid];
      auto end = partition2RowOffsetBase_[pid + 1];
      VsPrint("partition", pid);
      VsPrintVectorRange(rowOffset2RowId_, begin, end);
    }
#endif
  }

  void printInputHasNull() const {
    VS_PRINT_FUNCTION_SPLIT_LINE();
    VS_PRINT_CONTAINER(input_has_null_);
  }

 protected:
  virtual uint64_t valueBufferSizeForBool(uint32_t newSize);

  virtual void splitBoolValueType(const uint8_t* srcAddr, const std::vector<uint8_t*>& dstAddrs) {
    splitBoolType(srcAddr, dstAddrs);
  }

  virtual bool boolIsBit() {
    return true;
  }

  virtual uint64_t valueBufferSizeForTimestamp(uint32_t newSize);

  virtual arrow::Status splitTimestamp(const uint8_t* srcAddr, const std::vector<uint8_t*>& dstAddrs) {
    return splitFixedType<facebook::velox::int128_t>(srcAddr, dstAddrs);
  }

  arrow::Status init();

  VeloxHashShuffleWriter(
      uint32_t numPartitions,
      const std::shared_ptr<PartitionWriter>& partitionWriter,
      const std::shared_ptr<HashShuffleWriterOptions>& options,
      MemoryManager* memoryManager)
      : VeloxShuffleWriter(numPartitions, partitionWriter, options, memoryManager),
        splitBufferSize_(options->splitBufferSize),
        splitBufferReallocThreshold_(options->splitBufferReallocThreshold) {
    arenas_.resize(numPartitions);
  }

 private:
  arrow::Status initPartitions();

  arrow::Status initColumnTypes(const facebook::velox::RowVector& rv);

  arrow::Status splitRowVector(const facebook::velox::RowVector& rv);

  arrow::Status initFromRowVector(const facebook::velox::RowVector& rv);

  arrow::Status buildPartition2Row(uint32_t rowNum);

  arrow::Status updateInputHasNull(const facebook::velox::RowVector& rv);

  void setSplitState(SplitState state);

  arrow::Status doSplit(const facebook::velox::RowVector& rv, int64_t memLimit);

  bool beyondThreshold(uint32_t partitionId, uint32_t newSize);

  uint32_t calculatePartitionBufferSize(const facebook::velox::RowVector& rv, int64_t memLimit);

  arrow::Status preAllocPartitionBuffers(uint32_t preAllocBufferSize);

  arrow::Status updateValidityBuffers(uint32_t partitionId, uint32_t newSize);

  arrow::Result<std::shared_ptr<arrow::ResizableBuffer>>
  allocateValidityBuffer(uint32_t col, uint32_t partitionId, uint32_t newSize);

  arrow::Status allocatePartitionBuffer(uint32_t partitionId, uint32_t newSize);

  arrow::Status splitFixedWidthValueBuffer(const facebook::velox::RowVector& rv);

  arrow::Status splitValidityBuffer(const facebook::velox::RowVector& rv);

  arrow::Status splitBinaryArray(const facebook::velox::RowVector& rv);

  arrow::Status splitComplexType(const facebook::velox::RowVector& rv);

  arrow::Status evictBuffers(
      uint32_t partitionId,
      uint32_t numRows,
      std::vector<std::shared_ptr<arrow::Buffer>> buffers,
      bool reuseBuffers);

  arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>> assembleBuffers(uint32_t partitionId, bool reuseBuffers);

  // Split bool value type and validity buffer.
  void splitBoolType(const uint8_t* srcAddr, const std::vector<uint8_t*>& dstAddrs);

  template <typename T>
  arrow::Status splitFixedType(const uint8_t* srcAddr, const std::vector<uint8_t*>& dstAddrs) {
    for (auto& pid : partitionUsed_) {
      auto dstPidBase = reinterpret_cast<T*>(dstAddrs[pid] + partitionBufferBase_[pid] * sizeof(T));
      auto pos = partition2RowOffsetBase_[pid];
      auto end = partition2RowOffsetBase_[pid + 1];
      for (; pos < end; ++pos) {
        auto rowId = rowOffset2RowId_[pos];
        *dstPidBase++ = reinterpret_cast<const T*>(srcAddr)[rowId]; // copy
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status splitBinaryType(
      uint32_t binaryIdx,
      const facebook::velox::FlatVector<facebook::velox::StringView>& src,
      std::vector<BinaryBuf>& dst);

  arrow::Result<int64_t> evictCachedPayload(int64_t size);

  arrow::Result<std::shared_ptr<arrow::Buffer>> generateComplexTypeBuffers(facebook::velox::RowVectorPtr vector);

  arrow::Status resetValidityBuffer(uint32_t partitionId);

  arrow::Result<int64_t> shrinkPartitionBuffersMinSize(int64_t size);

  arrow::Result<int64_t> evictPartitionBuffersMinSize(int64_t size);

  arrow::Status shrinkPartitionBuffer(uint32_t partitionId);

  arrow::Status resetPartitionBuffer(uint32_t partitionId);

  // Resize the partition buffer to newSize. If preserveData is true, it will keep the data in buffer.
  // Note when preserveData is false, and newSize is larger, this function can introduce unnecessary memory copy.
  // In this case, use allocatePartitionBuffer to free current buffers and allocate new buffers instead.
  arrow::Status resizePartitionBuffer(uint32_t partitionId, uint32_t newSize, bool preserveData);

  uint64_t valueBufferSizeForBinaryArray(uint32_t binaryIdx, uint32_t newSize);

  uint64_t valueBufferSizeForFixedWidthArray(uint32_t fixedWidthIndex, uint32_t newSize);

  void calculateSimpleColumnBytes();

  void stat() const;

  bool shrinkPartitionBuffersAfterSpill() const;

  bool evictPartitionBuffersAfterSpill() const;

  arrow::Result<uint32_t> partitionBufferSizeAfterShrink(uint32_t partitionId) const;

  bool isExtremelyLargeBatch(facebook::velox::RowVectorPtr& rv) const;

  arrow::Status partitioningAndDoSplit(facebook::velox::RowVectorPtr rv, int64_t memLimit);

 protected:
  int32_t splitBufferSize_;
  double splitBufferReallocThreshold_;

  std::shared_ptr<arrow::Schema> schema_;

  // Column index, partition id, buffers.
  std::vector<std::vector<std::vector<std::shared_ptr<arrow::ResizableBuffer>>>> partitionBuffers_;

  BinaryArrayResizeState binaryArrayResizeState_{};

  bool hasComplexType_ = false;
  std::vector<bool> isValidityBuffer_;

  // Store arrow column types. Calculated once.
  std::vector<std::shared_ptr<arrow::DataType>> arrowColumnTypes_;

  // Store velox column types. Calculated once.
  std::vector<std::shared_ptr<const facebook::velox::Type>> veloxColumnTypes_;

  // How many fixed-width columns in the schema. Calculated once.
  uint32_t fixedWidthColumnCount_ = 0;

  // The column indices of all binary types in the schema.
  std::vector<uint32_t> binaryColumnIndices_;

  // The column indices of all fixed-width and binary columns in the schema.
  std::vector<uint32_t> simpleColumnIndices_;

  // The column indices of all complex types in the schema, including Struct, Map, List columns.
  std::vector<uint32_t> complexColumnIndices_;

  // Total bytes of fixed-width buffers of all simple columns. Including validity buffers, value buffers of
  // fixed-width types and length buffers of binary types.
  // Used for estimating pre-allocated partition buffer size. Calculated once.
  uint32_t fixedWidthBufferBytes_ = 0;

  // Used for calculating the average binary length.
  // Updated for each input RowVector.
  uint64_t totalInputNumRows_ = 0;
  std::vector<uint64_t> binaryArrayTotalSizeBytes_;
  size_t complexTotalSizeBytes_ = 0;

  // True if input column has null in any processed input RowVector.
  // In the order of fixed-width columns + binary columns.
  std::vector<bool> inputHasNull_;

  // Records which partitions are actually occurred in the current input RowVector.
  // Most of the loops can loop on this array to avoid visiting unused partition id.
  std::vector<uint32_t> partitionUsed_;

  // Row ID -> Partition ID
  // subscript: The index of row in the current input RowVector
  // value: Partition ID
  // Updated for each input RowVector.
  std::vector<uint32_t> row2Partition_;

  // Partition ID -> Row Count
  // subscript: Partition ID
  // value: How many rows does this partition have in the current input RowVector
  // Updated for each input RowVector.
  std::vector<uint32_t> partition2RowCount_;

  // Note: partition2RowOffsetBase_ and rowOffset2RowId_ are the optimization of flattening the 2-dimensional vector
  // into single dimension.
  // The first dimension is the partition id. The second dimension is the ith occurrence of this partition in the
  // input RowVector. The value is the index of the row in the input RowVector.
  // partition2RowOffsetBase_ records the offset of the first dimension.
  //
  // The index of the ith occurrence of a give partition `pid` in the input RowVector can be calculated via
  // rowOffset2RowId_[partition2RowOffsetBase_[pid] + i]
  // i is in the range of [0, partition2RowCount_[pid])

  // Partition ID -> Row offset, elements num: Partition num + 1
  // subscript: Partition ID
  // value: The base row offset of this Partition
  // Updated for each input RowVector.
  std::vector<uint32_t> partition2RowOffsetBase_;

  // Row offset -> Source row ID, elements num: input RowVector row num
  // subscript: Row offset
  // value: The index of row in the current input RowVector
  // Updated for each input RowVector.
  std::vector<uint32_t> rowOffset2RowId_;

  // Partition buffers are used for holding the intermediate data during split.
  // Partition ID -> Partition buffer size(unit is row)
  std::vector<uint32_t> partitionBufferSize_;

  // The write position of partition buffer. Updated after split. Reset when partition buffers are reallocated.
  std::vector<uint32_t> partitionBufferBase_;

  // Used by all simple types. Stores raw pointers of partition buffers.
  std::vector<std::vector<uint8_t*>> partitionValidityAddrs_;
  // Used by fixed-width types. Stores raw pointers of partition buffers.
  std::vector<std::vector<uint8_t*>> partitionFixedWidthValueAddrs_;
  // Used by binary types. Stores raw pointers and metadata of partition buffers.
  std::vector<std::vector<BinaryBuf>> partitionBinaryAddrs_;

  // Used by complex types.
  // Partition id -> Serialized complex data.
  std::vector<std::unique_ptr<facebook::velox::IterativeVectorSerializer>> complexTypeData_;
  std::vector<std::shared_ptr<arrow::ResizableBuffer>> complexTypeFlushBuffer_;
  std::shared_ptr<const facebook::velox::RowType> complexWriteType_;

  facebook::velox::serializer::presto::PrestoVectorSerde serde_;

  SplitState splitState_{kInit};

  std::optional<uint32_t> partitionBufferInUse_{std::nullopt};

  std::vector<std::unique_ptr<facebook::velox::StreamArena>> arenas_;
}; // class VeloxHashBasedShuffleWriter

} // namespace gluten

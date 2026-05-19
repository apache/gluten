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
#include <arrow/memory_pool.h>
#include <arrow/result.h>
#include <arrow/type.h>

#include "memory/VeloxMemoryManager.h"
#include "shuffle/Options.h"
#include "shuffle/PartitionWriter.h"
#include "shuffle/Partitioner.h"
#include "shuffle/ShuffleWriter.h"
#include "shuffle/Utils.h"

#include "utils/Print.h"

namespace gluten {

class VeloxShuffleWriter : public ShuffleWriter {
 public:
  static arrow::Result<std::shared_ptr<VeloxShuffleWriter>> create(
      ShuffleWriterType type,
      uint32_t numPartitions,
      const std::shared_ptr<PartitionWriter>& partitionWriter,
      const std::shared_ptr<ShuffleWriterOptions>& options,
      MemoryManager* memoryManager);

  facebook::velox::RowVectorPtr getStrippedRowVector(const facebook::velox::RowVector& rv) {
    // get new row type
    auto& rowType = rv.type()->asRow();
    auto typeChildren = rowType.children();
    typeChildren.erase(typeChildren.begin());
    auto newRowType = facebook::velox::ROW(std::move(typeChildren));

    // get length
    auto length = rv.size();

    // get children
    auto children = rv.children();
    children.erase(children.begin());

    return std::make_shared<facebook::velox::RowVector>(
        rv.pool(), newRowType, facebook::velox::BufferPtr(nullptr), length, std::move(children));
  }

  const int32_t* getFirstColumn(const facebook::velox::RowVector& rv) {
    VELOX_CHECK(rv.childrenSize() > 0, "RowVector missing partition id column.");

    auto& firstChild = rv.childAt(0);
    VELOX_CHECK(firstChild->isFlatEncoding(), "Partition id (field 0) is not flat encoding.");
    VELOX_CHECK(
        firstChild->type()->isInteger(),
        "Partition id (field 0) should be integer, but got {}",
        firstChild->type()->toString());

    // first column is partition key hash value or pid
    return firstChild->asFlatVector<int32_t>()->rawValues();
  }

  // For test only.
  virtual void setPartitionBufferSize(uint32_t newSize) {}

  virtual arrow::Status evictPartitionBuffers(uint32_t partitionId, bool reuseBuffers) {
    return arrow::Status::OK();
  }

  virtual arrow::Status evictRowVector(uint32_t partitionId) {
    return arrow::Status::OK();
  }

  virtual const uint64_t cachedPayloadSize() const {
    return 0;
  }

  int32_t maxBatchSize() const {
    return maxBatchSize_;
  }

  int64_t partitionBufferSize() const {
    return partitionBufferPool_->bytes_allocated();
  }

  int64_t peakBytesAllocated() const override {
    return partitionBufferPool_->max_memory() + veloxPool_->peakBytes();
  }

  // stat
  enum CpuWallTimingType {
    CpuWallTimingBegin = 0,
    CpuWallTimingCompute = CpuWallTimingBegin,
    CpuWallTimingBuildPartition,
    CpuWallTimingEvictPartition,
    CpuWallTimingHasNull,
    CpuWallTimingCalculateBufferSize,
    CpuWallTimingAllocateBuffer,
    CpuWallTimingCreateRbFromBuffer,
    CpuWallTimingMakeRB,
    CpuWallTimingCacheRB,
    CpuWallTimingFlattenRV,
    // Outer SplitRV bucket wraps `splitRowVector()` end-to-end. The four
    // entries below subdivide its body so reviewers / profilers can tell
    // which inner split path dominates when SplitRV does. Inner buckets
    // overlap the outer in time (the outer continues to run while each
    // inner is active); the outer minus the sum-of-inners is therefore
    // the time spent in `splitRowVector()` outside the four sub-paths
    // (mostly the post-split `partitionBufferBase_` update loop).
    CpuWallTimingSplitRV,
    CpuWallTimingSplitFixedWidth,
    CpuWallTimingSplitValidity,
    CpuWallTimingSplitBinary,
    CpuWallTimingSplitComplex,
    CpuWallTimingIteratePartitions,
    CpuWallTimingStop,
    CpuWallTimingEnd,
    CpuWallTimingNum = CpuWallTimingEnd - CpuWallTimingBegin
  };

  static std::string CpuWallTimingName(CpuWallTimingType type) {
    switch (type) {
      case CpuWallTimingCompute:
        return "CpuWallTimingCompute";
      case CpuWallTimingBuildPartition:
        return "CpuWallTimingBuildPartition";
      case CpuWallTimingEvictPartition:
        return "CpuWallTimingEvictPartition";
      case CpuWallTimingHasNull:
        return "CpuWallTimingHasNull";
      case CpuWallTimingCalculateBufferSize:
        return "CpuWallTimingCalculateBufferSize";
      case CpuWallTimingAllocateBuffer:
        return "CpuWallTimingAllocateBuffer";
      case CpuWallTimingCreateRbFromBuffer:
        return "CpuWallTimingCreateRbFromBuffer";
      case CpuWallTimingMakeRB:
        return "CpuWallTimingMakeRB";
      case CpuWallTimingCacheRB:
        return "CpuWallTimingCacheRB";
      case CpuWallTimingFlattenRV:
        return "CpuWallTimingFlattenRV";
      case CpuWallTimingSplitRV:
        return "CpuWallTimingSplitRV";
      case CpuWallTimingSplitFixedWidth:
        return "CpuWallTimingSplitFixedWidth";
      case CpuWallTimingSplitValidity:
        return "CpuWallTimingSplitValidity";
      case CpuWallTimingSplitBinary:
        return "CpuWallTimingSplitBinary";
      case CpuWallTimingSplitComplex:
        return "CpuWallTimingSplitComplex";
      case CpuWallTimingIteratePartitions:
        return "CpuWallTimingIteratePartitions";
      case CpuWallTimingStop:
        return "CpuWallTimingStop";
      default:
        return "CpuWallTimingUnknown";
    }
  }

  // Read-only access to a single per-stage timer. Useful for tests and for
  // anyone wanting to drive the existing `cpuWallTimingList_` data through a
  // channel other than the `VELOX_SHUFFLE_WRITER_LOG_FLAG` compile-time log.
  const facebook::velox::CpuWallTiming& cpuWallTiming(CpuWallTimingType type) const {
    return cpuWallTimingList_[type];
  }

 protected:
  VeloxShuffleWriter(
      uint32_t numPartitions,
      const std::shared_ptr<PartitionWriter>& partitionWriter,
      const std::shared_ptr<ShuffleWriterOptions>& options,
      MemoryManager* memoryManager)
      : ShuffleWriter(numPartitions, options->partitioning),
        partitionBufferPool_(memoryManager->getOrCreateArrowMemoryPool("VeloxShuffleWriter.partitionBufferPool")),
        veloxPool_(dynamic_cast<VeloxMemoryManager*>(memoryManager)->getLeafMemoryPool()),
        partitionWriter_(partitionWriter) {
    partitioner_ = Partitioner::make(options->partitioning, numPartitions_, options->startPartitionId);
    serdeOptions_.useLosslessTimestamp = true;
  }

  virtual ~VeloxShuffleWriter() = default;

  // Memory Pool used to track memory usage of partition buffers.
  // The actual allocation is delegated to options_.memoryPool.
  std::shared_ptr<arrow::MemoryPool> partitionBufferPool_;

  std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool_;

  // PartitionWriter must destruct before partitionBufferPool_, as it may hold buffers allocated by
  // partitionBufferPool_.
  std::shared_ptr<PartitionWriter> partitionWriter_;

  std::shared_ptr<Partitioner> partitioner_;

  facebook::velox::serializer::presto::PrestoVectorSerde::PrestoOptions serdeOptions_;

  int32_t maxBatchSize_{0};

  enum EvictState { kEvictable, kUnevictable };

  facebook::velox::CpuWallTiming cpuWallTimingList_[CpuWallTimingNum];

  EvictState evictState_{kEvictable};

  class EvictGuard {
   public:
    explicit EvictGuard(EvictState& evictState) : evictState_(evictState) {
      evictState_ = EvictState::kUnevictable;
    }

    ~EvictGuard() {
      evictState_ = EvictState::kEvictable;
    }

    // For safety and clarity.
    EvictGuard(const EvictGuard&) = delete;
    EvictGuard& operator=(const EvictGuard&) = delete;
    EvictGuard(EvictGuard&&) = delete;
    EvictGuard& operator=(EvictGuard&&) = delete;

   private:
    EvictState& evictState_;
  };
};

} // namespace gluten

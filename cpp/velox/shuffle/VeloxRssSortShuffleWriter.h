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
#include "memory/BufferOutputStream.h"
#include "memory/VeloxMemoryManager.h"
#include "shuffle/PartitionWriter.h"
#include "shuffle/Partitioner.h"
#include "shuffle/Utils.h"

#include "utils/Print.h"
#include "utils/VeloxArrowUtils.h"

namespace gluten {

enum RssSortState { kSortInit, kSort, kSortStop };

class VeloxRssSortShuffleWriter final : public VeloxShuffleWriter {
 public:
  static arrow::Result<std::shared_ptr<VeloxShuffleWriter>> create(
      uint32_t numPartitions,
      const std::shared_ptr<PartitionWriter>& partitionWriter,
      const std::shared_ptr<ShuffleWriterOptions>& options,
      MemoryManager* memoryManager);

  arrow::Status write(std::shared_ptr<ColumnarBatch> cb, int64_t memLimit) override;

  arrow::Status stop() override;

  arrow::Status reclaimFixedSize(int64_t size, int64_t* actual) override;

  arrow::Status evictRowVector(uint32_t partitionId) override;

  // Only for test.
  uint32_t getInputColumnBytes() const;

  // Public for test.
  void resetBatches();

 private:
  VeloxRssSortShuffleWriter(
      uint32_t numPartitions,
      const std::shared_ptr<PartitionWriter>& partitionWriter,
      const std::shared_ptr<RssSortShuffleWriterOptions>& options,
      MemoryManager* memoryManager)
      : VeloxShuffleWriter(numPartitions, partitionWriter, options, memoryManager),
        splitBufferSize_(options->splitBufferSize),
        sortBufferMaxSize_(options->sortBufferMaxSize),
        compressionKind_(arrowCompressionTypeToVelox(options->compressionType)) {}

  arrow::Status init();

  arrow::Status initFromRowVector(const facebook::velox::RowVector& rv);

  void setSortState(RssSortState state);

  arrow::Status doSort(facebook::velox::RowVectorPtr rv, int64_t /* memLimit */);

  arrow::Status evictBatch(uint32_t partitionId);

  void stat() const;

  void calculateBatchesSize(const facebook::velox::RowVectorPtr& vector);

  void deduplicateStrBuffer(const facebook::velox::VectorPtr& vector);

  int32_t splitBufferSize_;
  int64_t sortBufferMaxSize_;
  facebook::velox::common::CompressionKind compressionKind_;

  std::unique_ptr<facebook::velox::VectorStreamGroup> batch_;
  std::unique_ptr<BufferOutputStream> bufferOutputStream_;

  std::unique_ptr<facebook::velox::serializer::presto::PrestoVectorSerde> serde_ =
      std::make_unique<facebook::velox::serializer::presto::PrestoVectorSerde>();

  std::vector<facebook::velox::RowVectorPtr> batches_;

  std::unordered_map<int32_t, std::vector<int64_t>> rowVectorIndexMap_;

  uint32_t currentInputColumnBytes_ = 0;

  RssSortState sortState_{kSortInit};

  // The existing string buffers in the current batches.
  folly::F14FastSet<facebook::velox::Buffer*> stringBuffers_;

  bool stopped_{false};
}; // class VeloxSortBasedShuffleWriter

} // namespace gluten

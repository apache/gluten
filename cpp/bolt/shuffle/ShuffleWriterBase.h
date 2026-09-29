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

#include <arrow/ipc/writer.h>
#include <cstdint>
#include <numeric>
#include <utility>

#include "memory/ColumnarBatch.h"
#include "memory/Reclaimable.h"

namespace gluten {

class ShuffleWriterBase : public Reclaimable {
 public:
  virtual arrow::Status split(std::shared_ptr<ColumnarBatch> cb, int64_t memLimit) = 0;

  virtual arrow::Status stop() = 0;

  virtual int32_t numPartitions() const = 0;

  virtual int64_t partitionBufferSize() const = 0;

  virtual int64_t maxPartitionBufferSize() const = 0;

  virtual int64_t totalBytesWritten() const = 0;

  virtual int64_t totalBytesEvicted() const = 0;

  virtual int64_t totalWriteTime() const = 0;

  virtual int64_t totalEvictTime() const = 0;

  virtual int64_t totalCompressTime() const = 0;

  virtual int64_t avgPeallocSize() const = 0;

  virtual int64_t useV2() const = 0;

  virtual int64_t rowVectorModeCompress() const = 0;

  virtual int64_t combinedVectorNumber() const = 0;

  virtual int64_t combineVectorTimes() const = 0;

  virtual int64_t combineVectorCost() const = 0;

  virtual int64_t useRowBased() const = 0;

  virtual int64_t totalConvertTime() const = 0;

  virtual int64_t totalFlattenTime() const = 0;

  virtual int64_t totalComputePidTime() const = 0;

  virtual const std::vector<int64_t>& partitionLengths() const = 0;

  virtual const std::vector<int64_t>& rawPartitionLengths() const = 0;

  virtual const uint64_t cachedPayloadSize() const = 0;

 protected:
  virtual ~ShuffleWriterBase() = default;
};

} // namespace gluten

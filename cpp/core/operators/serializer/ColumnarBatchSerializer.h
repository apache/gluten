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

#include <arrow/c/abi.h>
#include <optional>
#include <vector>

#include "memory/ColumnarBatch.h"
#include "utils/Exception.h"

namespace gluten {

class ColumnarBatchSerializer {
 public:
  ColumnarBatchSerializer(arrow::MemoryPool* arrowPool) : arrowPool_(arrowPool) {}

  virtual ~ColumnarBatchSerializer() = default;

  virtual void append(const std::shared_ptr<ColumnarBatch>& batch) = 0;

  virtual int64_t maxSerializedSize() = 0;

  virtual void serializeTo(uint8_t* address, int64_t size) = 0;

  virtual std::shared_ptr<ColumnarBatch> deserialize(uint8_t* data, int32_t size) = 0;

  // V2: Backend-overridable framed serialization carrying per-column stats.
  // Layout: [magic=0xFECA5302 | statsLen | statsBlob | bytesLen | bytesBlob].
  // Default returns empty vector (not supported); callers fall back to legacy serialize().
  virtual std::vector<uint8_t> framedSerializeWithStats(const std::shared_ptr<ColumnarBatch>& /*batch*/) {
    return {};
  }

  // V3: Per-column framed serialization without stats (lazy deserialization support).
  // Layout: [magic=0xFECA5303 | statsLen=0 | numRows | numCols | per-col(colLen+colBytes)].
  // Default returns empty vector (not supported); callers detect and fall back.
  virtual std::vector<uint8_t> framedSerializeV3(const std::shared_ptr<ColumnarBatch>& /*batch*/) {
    return {};
  }

  // V3: Per-column framed serialization + stats (lazy deserialization + pruning support).
  // Layout: [magic=0xFECA5303 | statsLen | statsBlob | numRows | numCols | per-col(colLen+colBytes)].
  // Default returns empty vector (not supported); callers detect and fall back.
  virtual std::vector<uint8_t> framedSerializeWithStatsV3(const std::shared_ptr<ColumnarBatch>& /*batch*/) {
    return {};
  }

  // V3: Deserialize with column projection; returns M-column RowVector (only requested columns).
  // requestedColumns: nullopt=all columns, optional<vector{}>= zero columns, optional<vector{...}>=M cols.
  // Default throws GlutenException (not supported for non-Velox backends).
  virtual std::shared_ptr<ColumnarBatch>
  deserializeV3(uint8_t* /*data*/, int32_t /*size*/, const std::optional<std::vector<int32_t>>& /*requestedColumns*/) {
    throw GlutenException("deserializeV3 is not supported for this backend");
  }

 protected:
  arrow::MemoryPool* arrowPool_;
};

} // namespace gluten

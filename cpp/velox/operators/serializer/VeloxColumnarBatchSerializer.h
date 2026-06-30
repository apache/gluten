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

#include "memory/ColumnarBatch.h"
#include "operators/serializer/ColumnarBatchSerializer.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/Variant.h"

namespace gluten {

// Per-column min/max + nullCount carrier consumed by the JVM cache stats marshaling.
// Unsupported / NaN-poisoned columns emit hasLowerBound=hasUpperBound=false, which the JVM
// buildFilter treats as pass-through.
struct ColumnStats {
  bool hasLowerBound{false};
  bool hasUpperBound{false};
  facebook::velox::variant lowerBound;
  facebook::velox::variant upperBound;
  int64_t nullCount{0};
};

class VeloxColumnarBatchSerializer : public ColumnarBatchSerializer {
 public:
  VeloxColumnarBatchSerializer(
      arrow::MemoryPool* arrowPool,
      std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool,
      struct ArrowSchema* cSchema,
      const std::string& compressionKind = "none");

  void append(const std::shared_ptr<ColumnarBatch>& batch) override;

  int64_t maxSerializedSize() override;

  void serializeTo(uint8_t* address, int64_t size) override;

  std::shared_ptr<ColumnarBatch> deserialize(uint8_t* data, int32_t size) override;

  // Per-column min/max scan. Returns hasLowerBound/UpperBound=true for supported scalar types
  // (integer / decimal / float / double / bool / timestamp / varchar); other types degrade to
  // false so the JVM side falls back to pass-through in buildFilter.
  std::vector<ColumnStats> computeStats(facebook::velox::RowVectorPtr rowVector);

  // V2: Returns framed bytes [STATS_FRAMED_MAGIC=0x02: 4B][statsLen: u32 LE][statsBlob]
  // [bytesLen: u32 LE][bytesBlob]. statsBlob matches JVM CachedColumnarBatchKryoSerializer.
  std::vector<uint8_t> framedSerializeWithStats(const std::shared_ptr<ColumnarBatch>& batch) override;

  // V3: Per-column framed bytes [STATS_FRAMED_MAGIC_V3=0x03: 4B][statsLen=0: u32 LE]
  // [numRows: u32 LE][numCols: u32 LE][per-col: colLen(u32 LE) + colBytes].
  // Each colBytes is produced by PrestoVectorSerde::serializeSingleColumn (self-contained).
  std::vector<uint8_t> framedSerializeV3(const std::shared_ptr<ColumnarBatch>& batch) override;

  // V3: Per-column framed bytes [STATS_FRAMED_MAGIC_V3=0x03: 4B][statsLen: u32 LE][statsBlob]
  // [numRows: u32 LE][numCols: u32 LE][per-col: colLen(u32 LE) + colBytes].
  // Each colBytes is produced by PrestoVectorSerde::serializeSingleColumn (self-contained).
  std::vector<uint8_t> framedSerializeWithStatsV3(const std::shared_ptr<ColumnarBatch>& batch) override;

  // V3: Deserialize with column projection; returns M-column RowVector.
  // requestedColumns: nullopt=all columns, optional<vector{}>=zero cols, optional<vector{i,j}>=M cols.
  std::shared_ptr<ColumnarBatch>
  deserializeV3(uint8_t* data, int32_t size, const std::optional<std::vector<int32_t>>& requestedColumns) override;

 private:
  // Extract statsBlob from per-column stats (shared by V2 and V3 write paths).
  std::vector<uint8_t> buildStatsBlob(const std::vector<ColumnStats>& perCol, uint32_t numRows, uint32_t numCols);
  std::vector<uint8_t> framedSerializeV3Impl(const std::shared_ptr<ColumnarBatch>& batch, bool includeStats);

 protected:
  std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool_;
  std::unique_ptr<facebook::velox::StreamArena> arena_;
  std::unique_ptr<facebook::velox::IterativeVectorSerializer> serializer_;
  facebook::velox::RowTypePtr rowType_;
  std::unique_ptr<facebook::velox::serializer::presto::PrestoVectorSerde> serde_;
  facebook::velox::serializer::presto::PrestoVectorSerde::PrestoOptions options_;
};

} // namespace gluten

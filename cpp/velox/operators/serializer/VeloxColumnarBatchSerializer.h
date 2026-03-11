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

#include "memory/ColumnarBatch.h"
#include "operators/serializer/ColumnarBatchSerializer.h"
#include "velox/serializers/PrestoSerializer.h"

namespace gluten {

class VeloxColumnarBatchSerializer : public ColumnarBatchSerializer {
 public:
  VeloxColumnarBatchSerializer(
      arrow::MemoryPool* arrowPool,
      std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool,
      struct ArrowSchema* cSchema);

  void append(const std::shared_ptr<ColumnarBatch>& batch) override;

  int64_t maxSerializedSize() override;

  void serializeTo(uint8_t* address, int64_t size) override;

  std::shared_ptr<ColumnarBatch> deserialize(uint8_t* data, int32_t size) override;

 protected:
  std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool_;
  std::unique_ptr<facebook::velox::StreamArena> arena_;
  std::unique_ptr<facebook::velox::IterativeVectorSerializer> serializer_;
  facebook::velox::RowTypePtr rowType_;
  std::unique_ptr<facebook::velox::serializer::presto::PrestoVectorSerde> serde_;
  facebook::velox::serializer::presto::PrestoVectorSerde::PrestoOptions options_;
};

} // namespace gluten

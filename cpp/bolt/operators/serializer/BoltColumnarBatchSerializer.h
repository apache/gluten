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

#include <arrow/buffer.h>
#include <arrow/c/abi.h>

#include <vector>

#include "memory/ColumnarBatch.h"
#include "operators/serializer/ColumnarBatchSerializer.h"
#include "bolt/serializers/PrestoSerializer.h"

namespace gluten {

class BoltColumnarBatchSerializer final : public ColumnarBatchSerializer {
 public:
  BoltColumnarBatchSerializer(
      arrow::MemoryPool* arrowPool,
      std::shared_ptr<bytedance::bolt::memory::MemoryPool> boltPool,
      struct ArrowSchema* cSchema);

  void append(const std::shared_ptr<ColumnarBatch>& batch) override;

  int64_t maxSerializedSize() override;

  void serializeTo(uint8_t* address, int64_t size) override;

  std::shared_ptr<ColumnarBatch> deserialize(uint8_t* data, int32_t size) override;

 private:
  std::shared_ptr<arrow::Buffer> serializeColumnarBatches(const std::vector<std::shared_ptr<ColumnarBatch>>& batches);

  std::shared_ptr<bytedance::bolt::memory::MemoryPool> boltPool_;
  bytedance::bolt::RowTypePtr rowType_;
  std::unique_ptr<bytedance::bolt::serializer::presto::PrestoVectorSerde> serde_;
  bytedance::bolt::serializer::presto::PrestoVectorSerde::PrestoOptions options_;
  std::vector<std::shared_ptr<ColumnarBatch>> batches_;
  std::shared_ptr<arrow::Buffer> serializedBuffer_;
};

} // namespace gluten

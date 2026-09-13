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

// This File includes common helper functions with Arrow dependency.

#pragma once

#include "memory/ColumnarBatch.h"

#include "velox/buffer/Buffer.h"
#include "velox/common/compression/Compression.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/type/Type.h"
#include "velox/vector/arrow/Bridge.h"

#include <arrow/memory_pool.h>
#include <arrow/type.h>
#include <arrow/util/compression.h>

namespace gluten {

class ArrowUtils {
 public:
  static ArrowOptions getBridgeOptions(ArrowTypeLayout typeLayout = ArrowTypeLayout::kDefault) {
    ArrowOptions options;
    options.timestampUnit = static_cast<TimestampUnit>(6);
    if (typeLayout == ArrowTypeLayout::kStringView) {
      options.varTypeLayout = ArrowOptions::VarTypeLayout::kStringView;
    }
    return options;
  }
};

void toArrowSchema(
    const facebook::velox::TypePtr& rowType,
    facebook::velox::memory::MemoryPool* pool,
    struct ArrowSchema* out);

std::shared_ptr<arrow::Schema> toArrowSchema(
    const facebook::velox::TypePtr& rowType,
    facebook::velox::memory::MemoryPool* pool);

facebook::velox::TypePtr fromArrowSchema(const std::shared_ptr<arrow::Schema>& schema);

arrow::Result<std::shared_ptr<arrow::Buffer>> toArrowBuffer(facebook::velox::BufferPtr buffer, arrow::MemoryPool* pool);

/**
 * For testing.
 */
arrow::Result<std::shared_ptr<ColumnarBatch>> recordBatch2VeloxColumnarBatch(const arrow::RecordBatch& rb);

facebook::velox::common::CompressionKind arrowCompressionTypeToVelox(arrow::Compression::type type);
} // namespace gluten

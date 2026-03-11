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

#include "VeloxGpuColumnarBatchSerializer.h"

#include <arrow/buffer.h>

#include "memory/ArrowMemory.h"
#include "memory/VeloxColumnarBatch.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/arrow/Bridge.h"
#include "velox/experimental/cudf/exec/GpuResources.h"
#include "velox/experimental/cudf/exec/VeloxCudfInterop.h"
#include "velox/experimental/cudf/vector/CudfVector.h"

#include <iostream>

using namespace facebook::velox;

namespace gluten {

VeloxGpuColumnarBatchSerializer::VeloxGpuColumnarBatchSerializer(
    arrow::MemoryPool* arrowPool,
    std::shared_ptr<memory::MemoryPool> veloxPool,
    struct ArrowSchema* cSchema)
    : VeloxColumnarBatchSerializer(arrowPool, veloxPool, cSchema) {
}

std::shared_ptr<ColumnarBatch> VeloxGpuColumnarBatchSerializer::deserialize(uint8_t* data, int32_t size) {
  auto vb = VeloxColumnarBatchSerializer::deserialize(data, size);
  auto stream = cudf_velox::cudfGlobalStreamPool().get_stream();
  auto table = cudf_velox::with_arrow::toCudfTable(
    dynamic_pointer_cast<VeloxColumnarBatch>(vb)->getRowVector(), veloxPool_.get(), stream, cudf_velox::get_output_mr());
  stream.synchronize();
  auto vector = std::make_shared<cudf_velox::CudfVector>(
      veloxPool_.get(), rowType_, size, std::move(table), stream);
  return std::make_shared<VeloxColumnarBatch>(vector, vb->numColumns());
}

} // namespace gluten

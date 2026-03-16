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

#include "GpuBufferBatchResizer.h"
#include "cudf/GpuLock.h"
#include "utils/Timer.h"
#include "velox/experimental/cudf/exec/GpuResources.h"
#include "velox/experimental/cudf/exec/VeloxCudfInterop.h"
#include "velox/experimental/cudf/vector/CudfVector.h"
#include "velox/vector/FlatVector.h"

#include <arrow/buffer.h>

#include <cuda_runtime.h>
#include <cudf/column/column.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/null_mask.hpp>
#include <cudf/types.hpp>
#include <cudf/utilities/type_dispatcher.hpp>
#include <rmm/cuda_stream_view.hpp>
#include <rmm/device_buffer.hpp>

using namespace facebook::velox;

namespace gluten {

namespace {

struct DispatchColumn {
  rmm::cuda_stream_view stream;
  rmm::device_async_resource_ref mr;
  const std::vector<std::shared_ptr<arrow::Buffer>>& buffers;
  const int32_t numRows;
  int32_t bufferIdx = 0;

  std::unique_ptr<rmm::device_buffer> getMaskBuffer(const std::shared_ptr<arrow::Buffer>& buffer) {
    if (buffer == nullptr || buffer->size() == 0) {
      return std::make_unique<rmm::device_buffer>(0, stream, mr);
    }

    auto mask = std::make_unique<rmm::device_buffer>(buffer->size(), stream, mr);
    CUDF_CUDA_TRY(
        cudaMemcpyAsync(mask->data(), buffer->data(), buffer->size(), cudaMemcpyHostToDevice, stream.value()));
    return mask;
  }

  // For timestamp, it is cudf cudf::type_id::TIMESTAMP_NANOSECONDS, Velox uses int128_t while cudf uses int64_t to
  // represent it.
  template <TypeKind Kind, typename T = typename TypeTraits<Kind>::NativeType>
  std::unique_ptr<cudf::column> readFlatColumn(cudf::type_id typeId) {
    // === Step 1: get CPU buffers ===
    auto nulls = buffers[bufferIdx++];
    auto values = buffers[bufferIdx++];

    // === Step 2: allocate GPU device buffers and copy ===
    rmm::device_buffer dataBuf(values->size(), stream);
    CUDF_CUDA_TRY(
        cudaMemcpyAsync(dataBuf.data(), values->data(), values->size(), cudaMemcpyHostToDevice, stream.value()));

    auto nullBuf = getMaskBuffer(nulls);

    // === Step 3: create cudf::column ===
    cudf::data_type cudfType{typeId};
    size_t nullCount = nulls == nullptr || nulls->size() == 0
        ? 0
        : cudf::null_count(reinterpret_cast<const cudf::bitmask_type*>(nulls->data()), 0, numRows, stream);
    return std::make_unique<cudf::column>(cudfType, numRows, std::move(dataBuf), std::move(*nullBuf), nullCount);
  }

  /// We can optimize it in shuffle writer side, returns the offset buffer instead of length buffer.
  /// Then we don't need to recover the offsetBuf by rawLengths, also change the merge strategic, update the merge
  /// buffer offset from last offset.
  std::unique_ptr<cudf::column> getOffsetsColumn(const std::shared_ptr<arrow::Buffer>& offsets) {
    VELOX_CHECK_GT(numRows, 0);
    // --- 2. Copy offsets to GPU ---
    rmm::device_buffer offsetBuf(offsets->size(), stream, mr);
    CUDF_CUDA_TRY(
        cudaMemcpyAsync(offsetBuf.data(), offsets->data(), offsets->size(), cudaMemcpyHostToDevice, stream.value()));

    // --- 3. Empty null mask (no nulls in offset column) ---
    rmm::device_buffer nullBuf(0, stream, mr);

    // --- 4. Create cudf::column ---
    return std::make_unique<cudf::column>(
        cudf::data_type{cudf::type_id::INT32},
        static_cast<cudf::size_type>(numRows + 1),
        std::move(offsetBuf),
        std::move(nullBuf),
        0); // null_count = 0
  }

  std::unique_ptr<cudf::column> readFlatColumnStringView(cudf::type_id /*typeId*/) {
    auto nulls = buffers[bufferIdx++];
    auto offsets = buffers[bufferIdx++];
    auto valueBuffer = buffers[bufferIdx++];

    if (numRows == 0) {
      return make_empty_column(cudf::type_id::STRING);
    }

    auto mask = getMaskBuffer(nulls);

    // === Step 3: create cudf::column ===
    size_t nullCount = nulls == nullptr || nulls->size() == 0
        ? 0
        : cudf::null_count(reinterpret_cast<const cudf::bitmask_type*>(nulls->data()), 0, numRows, stream);

    auto offsetColumn = getOffsetsColumn(offsets);

    rmm::device_buffer chars(valueBuffer->size(), stream, mr);
    CUDF_CUDA_TRY(cudaMemcpyAsync(
        chars.data(), valueBuffer->data_as<uint8_t>(), chars.size(), cudaMemcpyDefault, stream.value()));
    return cudf::make_strings_column(
        numRows, std::move(offsetColumn), std::move(chars), nullCount, std::move(*mask.release()));
  }
};

template <>
std::unique_ptr<cudf::column> DispatchColumn::readFlatColumn<TypeKind::VARCHAR>(cudf::type_id typeId) {
  return readFlatColumnStringView(typeId);
}

template <>
std::unique_ptr<cudf::column> DispatchColumn::readFlatColumn<TypeKind::VARBINARY>(cudf::type_id typeId) {
  return readFlatColumnStringView(typeId);
}

std::shared_ptr<VeloxColumnarBatch> makeCudfTable(
    RowTypePtr type,
    int32_t numRows,
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    memory::MemoryPool* pool) {
  std::vector<std::unique_ptr<cudf::column>> cudfColumns;
  cudfColumns.reserve(type->size());

  auto stream = cudf_velox::cudfGlobalStreamPool().get_stream();
  DispatchColumn dispatch{stream, cudf::get_current_device_resource_ref(), buffers, numRows};
  for (const auto& colType : type->children()) {
    auto res = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
        dispatch.readFlatColumn, colType->kind(), cudf_velox::veloxToCudfTypeId(colType));
    cudfColumns.emplace_back(std::move(res));
  }
  auto cudfTable = std::make_unique<cudf::table>(std::move(cudfColumns));
  stream.synchronize();
  return std::make_shared<VeloxColumnarBatch>(
      std::make_shared<cudf_velox::CudfVector>(pool, type, numRows, std::move(cudfTable), stream), type->size());
}

} // namespace

GpuBufferBatchResizer::GpuBufferBatchResizer(
    arrow::MemoryPool* arrowPool,
    facebook::velox::memory::MemoryPool* pool,
    int32_t minOutputBatchSize,
    int64_t maxPrefetchSize,
    std::unique_ptr<ColumnarBatchIterator> in)
    : arrowPool_(arrowPool),
      pool_(pool),
      minOutputBatchSize_(minOutputBatchSize),
      maxPrefetchSize_(maxPrefetchSize),
      in_(std::move(in)) {
  VELOX_CHECK_GT(minOutputBatchSize_, 0, "minOutputBatchSize should be larger than 0");
  VELOX_CHECK_GT(maxPrefetchSize_, 0, "maxPrefetchSize should be larger than 0");
}

std::shared_ptr<GpuBufferColumnarBatch> GpuBufferBatchResizer::fetchAndComposeBatch() {
  std::vector<std::shared_ptr<GpuBufferColumnarBatch>> cachedBatches;
  int32_t cachedRows = 0;
  while (cachedRows < minOutputBatchSize_) {
    auto nextCb = in_->next();
    if (!nextCb) {
      break;
    }

    auto nextBatch = std::dynamic_pointer_cast<GpuBufferColumnarBatch>(nextCb);
    VELOX_CHECK_NOT_NULL(nextBatch);
    if (nextBatch->numRows() == 0) {
      continue;
    }

    cachedRows += nextBatch->numRows();
    cachedBatches.push_back(std::move(nextBatch));
  }

  if (cachedRows == 0) {
    return nullptr;
  }

  return GpuBufferColumnarBatch::compose(arrowPool_, cachedBatches, cachedRows);
}

std::shared_ptr<ColumnarBatch> GpuBufferBatchResizer::next() {
  // Ensure at least one batch is in the prefetch queue.
  if (prefetchQueue_.empty()) {
    auto batch = fetchAndComposeBatch();
    if (batch) {
      prefetchedBytes_ += batch->numBytes();
      prefetchQueue_.push_back(std::move(batch));
    }
  }

  if (prefetchQueue_.empty()) {
    return nullptr;
  }

  // Try to acquire the GPU lock non-blockingly. While we can't get it,
  // keep prefetching more batches into CPU memory within the budget.
  while (!tryLockGpu()) {
    if (prefetchedBytes_ < maxPrefetchSize_) {
      auto batch = fetchAndComposeBatch();
      if (batch) {
        prefetchedBytes_ += batch->numBytes();
        prefetchQueue_.push_back(std::move(batch));
      } else {
        // All the batches consumed.
        LOG(WARNING) << "Prefetched " << prefetchQueue_.size() << " batches (" << prefetchedBytes_ << " bytes) before blocking on GPU lock.";
        lockGpu();
        break;
      }
    } else {
      LOG(WARNING) << "Prefetched " << prefetchQueue_.size() << " batches (" << prefetchedBytes_ << " bytes) before blocking on GPU lock.";
      lockGpu();
      break;
    }
  }

  // GPU lock acquired. Pop one batch and convert to cuDF on GPU.
  auto batch = std::move(prefetchQueue_.front());
  prefetchQueue_.pop_front();
  prefetchedBytes_ -= batch->numBytes();

  return makeCudfTable(batch->getRowType(), batch->numRows(), batch->buffers(), pool_);
}

int64_t GpuBufferBatchResizer::spillFixedSize(int64_t size) {
  return in_->spillFixedSize(size);
}

} // namespace gluten

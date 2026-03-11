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

#include "RowVectorStream.h"
#include "memory/VeloxColumnarBatch.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"
#include "velox/vector/arrow/Bridge.h"

namespace {

class SuspendedSection {
 public:
  explicit SuspendedSection(facebook::velox::exec::Driver* driver) : driver_(driver) {
    if (driver_->task()->enterSuspended(driver->state()) != facebook::velox::exec::StopReason::kNone) {
      VELOX_FAIL("Terminate detected when entering suspended section");
    }
  }

  virtual ~SuspendedSection() {
    if (driver_->task()->leaveSuspended(driver_->state()) != facebook::velox::exec::StopReason::kNone) {
      LOG(WARNING) << "Terminate detected when leaving suspended section for driver " << driver_->driverCtx()->driverId
                   << " from task " << driver_->task()->taskId();
    }
  }

 private:
  facebook::velox::exec::Driver* const driver_;
};

} // namespace

namespace gluten {

bool RowVectorStream::hasNext() {
  if (finished_) {
    return false;
  }
  VELOX_DCHECK_NOT_NULL(iterator_);

  bool hasNext;
  {
    // We are leaving Velox task execution and are probably entering Spark code through JNI. Suspend the current
    // driver to make the current task open to spilling.
    //
    // When a task is getting spilled, it should have been suspended so has zero running threads, otherwise there's
    // possibility that this spill call hangs. See https://github.com/apache/gluten/issues/7243.
    // As of now, non-zero running threads usually happens when:
    // 1. Task A spills task B;
    // 2. Task A tries to grow buffers created by task B, during which spill is requested on task A again.
    const facebook::velox::exec::DriverThreadContext* driverThreadCtx =
    facebook::velox::exec::driverThreadContext();
    VELOX_CHECK_NOT_NULL(
        driverThreadCtx,
        "ExternalStreamDataSource::next() is not called "
        "from a driver thread");
    SuspendedSection ss(driverThreadCtx->driverCtx()->driver);
    hasNext = iterator_->hasNext();
  }
  if (!hasNext) {
    finished_ = true;
  }
  return hasNext;
}

std::shared_ptr<ColumnarBatch> RowVectorStream::nextInternal() {
  if (finished_) {
    return nullptr;
  }
  std::shared_ptr<ColumnarBatch> cb;
  {
    // We are leaving Velox task execution and are probably entering Spark code through JNI. Suspend the current
    // driver to make the current task open to spilling.
    const facebook::velox::exec::DriverThreadContext* driverThreadCtx =
    facebook::velox::exec::driverThreadContext();
    VELOX_CHECK_NOT_NULL(
        driverThreadCtx,
        "ExternalStreamDataSource::next() is not called "
        "from a driver thread");
    SuspendedSection ss(driverThreadCtx->driverCtx()->driver);
    cb = iterator_->next();
  }
  return cb;
}

facebook::velox::RowVectorPtr RowVectorStream::next() {
  auto cb = nextInternal();
  const std::shared_ptr<VeloxColumnarBatch>& vb = VeloxColumnarBatch::from(pool_, cb);
  auto vp = vb->getRowVector();
  VELOX_DCHECK(vp != nullptr);
  return std::make_shared<facebook::velox::RowVector>(
      vp->pool(), outputType_, facebook::velox::BufferPtr(0), vp->size(), vp->children());
}

ValueStreamDataSource::ValueStreamDataSource(
    const facebook::velox::RowTypePtr& outputType,
    const facebook::velox::connector::ConnectorTableHandlePtr& tableHandle,
    const facebook::velox::connector::ColumnHandleMap& columnHandles,
    facebook::velox::connector::ConnectorQueryCtx* connectorQueryCtx)
    : outputType_(outputType),
      pool_(connectorQueryCtx->memoryPool()),
      dynamicFilterEnabled_(
          std::dynamic_pointer_cast<const ValueStreamTableHandle>(tableHandle)->dynamicFilterEnabled()) {}

void ValueStreamDataSource::addSplit(std::shared_ptr<facebook::velox::connector::ConnectorSplit> split) {
  // Cast to IteratorConnectorSplit to extract the iterator
  auto iteratorSplit = std::dynamic_pointer_cast<const IteratorConnectorSplit>(split);
  if (!iteratorSplit) {
    throw std::runtime_error("Split is not an IteratorConnectorSplit");
  }
  
  auto iterator = iteratorSplit->iterator();
  if (!iterator) {
    throw std::runtime_error("IteratorConnectorSplit contains null iterator");
  }
  
  // Create RowVectorStream wrapper and add to pending queue
  auto rowVectorStream = std::make_shared<RowVectorStream>(pool_, iterator, outputType_);
  pendingIterators_.push_back(rowVectorStream);
}

std::optional<facebook::velox::RowVectorPtr> ValueStreamDataSource::next(
    uint64_t size,
    facebook::velox::ContinueFuture& future) {
  // Try to get current iterator if we don't have one
  while (!currentIterator_) {
    if (pendingIterators_.empty()) {
      // No more iterators to process
      return nullptr;
    }
    
    // Get next RowVectorStream from queue
    currentIterator_ = pendingIterators_.front();
    pendingIterators_.erase(pendingIterators_.begin());
  }

  // Check if current stream has more data
  if (!currentIterator_->hasNext()) {
    // Current stream exhausted, try next one
    currentIterator_ = nullptr;
    return next(size, future);  // Recursively try next stream
  }

  // Get next batch from current stream (RowVectorStream handles conversion)
  auto rowVector = currentIterator_->next();
  
  if (!rowVector) {
    currentIterator_ = nullptr;
    return next(size, future);  // Recursively try next stream
  }

  // Update metrics
  completedRows_ += rowVector->size();
  completedBytes_ += rowVector->estimateFlatSize();

  // Apply dynamic filters if any have been pushed down.
  if (!dynamicFilters_.empty()) {
    rowVector = applyDynamicFilters(rowVector);
    if (!rowVector) {
      // All rows filtered out, try next batch.
      return next(size, future);
    }
  }

  return rowVector;
}

facebook::velox::RowVectorPtr ValueStreamDataSource::applyDynamicFilters(
    const facebook::velox::RowVectorPtr& input) {
  using namespace facebook::velox;

  const auto numRows = input->size();
  if (numRows == 0) {
    return input;
  }

  SelectivityVector rows(numRows, true);

  for (const auto& [channel, filter] : dynamicFilters_) {
    if (!filter || channel >= input->childrenSize()) {
      continue;
    }
    applyFilterOnColumn(filter, input->childAt(channel), rows);
    if (!rows.hasSelections()) {
      dynamicFilterInputRows_ += numRows;
      return nullptr;
    }
  }

  const auto passedCount = rows.countSelected();
  if (passedCount == numRows) {
    return input;
  }

  dynamicFilterInputRows_ += numRows;

  BufferPtr indices = allocateIndices(passedCount, pool_);
  auto* rawIndices = indices->asMutable<vector_size_t>();
  vector_size_t idx = 0;
  rows.applyToSelected([&](auto row) { rawIndices[idx++] = row; });

  return exec::wrap(passedCount, std::move(indices), input);
}

void ValueStreamDataSource::applyFilterOnColumn(
    const std::shared_ptr<facebook::velox::common::Filter>& filter,
    const facebook::velox::VectorPtr& vector,
    facebook::velox::SelectivityVector& rows) {
  using namespace facebook::velox;

  DecodedVector decoded(*vector, rows);

  rows.applyToSelected([&](auto row) {
    if (decoded.isNullAt(row)) {
      if (!filter->testNull()) {
        rows.setValid(row, false);
      }
      return;
    }

    bool pass = false;
    switch (vector->typeKind()) {
      case TypeKind::BOOLEAN:
        pass = filter->testBool(decoded.valueAt<bool>(row));
        break;
      case TypeKind::TINYINT:
        pass = filter->testInt64(decoded.valueAt<int8_t>(row));
        break;
      case TypeKind::SMALLINT:
        pass = filter->testInt64(decoded.valueAt<int16_t>(row));
        break;
      case TypeKind::INTEGER:
        pass = filter->testInt64(decoded.valueAt<int32_t>(row));
        break;
      case TypeKind::BIGINT:
        pass = filter->testInt64(decoded.valueAt<int64_t>(row));
        break;
      case TypeKind::HUGEINT:
        pass = filter->testInt128(decoded.valueAt<int128_t>(row));
        break;
      case TypeKind::REAL:
        pass = filter->testFloat(decoded.valueAt<float>(row));
        break;
      case TypeKind::DOUBLE:
        pass = filter->testDouble(decoded.valueAt<double>(row));
        break;
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY: {
        auto sv = decoded.valueAt<StringView>(row);
        pass = filter->testBytes(sv.data(), sv.size());
        break;
      }
      case TypeKind::TIMESTAMP:
        pass = filter->testTimestamp(decoded.valueAt<Timestamp>(row));
        break;
      default:
        // For unsupported types, let the row pass through.
        pass = true;
        break;
    }
    if (!pass) {
      rows.setValid(row, false);
    }
  });
  rows.updateBounds();
}

} // namespace gluten
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
    // possibility that this spill call hangs. See https://github.com/apache/incubator-gluten/issues/7243.
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
      pool_(connectorQueryCtx->memoryPool()) {}

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

  return rowVector;
}

} // namespace gluten
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

#include "CudfVectorStream.h"
#include "memory/VeloxColumnarBatch.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"
#include "velox/exec/Task.h"

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
bool CudfVectorStreamBase::hasNext() {
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
    SuspendedSection ss(driverCtx_->driver);
    hasNext = iterator_->hasNext();
  }
  if (!hasNext) {
    finished_ = true;
  }
  return hasNext;
}

std::shared_ptr<ColumnarBatch> CudfVectorStreamBase::nextInternal() {
  if (finished_) {
    return nullptr;
  }
  std::shared_ptr<ColumnarBatch> cb;
  {
    // We are leaving Velox task execution and are probably entering Spark code through JNI. Suspend the current
    // driver to make the current task open to spilling.
    SuspendedSection ss(driverCtx_->driver);
    cb = iterator_->next();
  }
  return cb;
}

facebook::velox::RowVectorPtr CudfVectorStreamBase::next() {
  auto cb = nextInternal();
  const std::shared_ptr<VeloxColumnarBatch>& vb = VeloxColumnarBatch::from(pool_, cb);
  auto vp = vb->getRowVector();
  VELOX_DCHECK(vp != nullptr);
  return std::make_shared<facebook::velox::RowVector>(
      vp->pool(), outputType_, facebook::velox::BufferPtr(0), vp->size(), vp->children());
}
} // namespace gluten
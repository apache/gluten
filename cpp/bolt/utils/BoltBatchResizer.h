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

#include "memory/ColumnarBatchIterator.h"
#include "memory/BoltColumnarBatch.h"
#include "utils/Exception.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/vector/ComplexVector.h"

namespace gluten {

class BoltBatchResizer : public ColumnarBatchIterator {
 public:
  BoltBatchResizer(
      bytedance::bolt::memory::MemoryPool* pool,
      int32_t minOutputBatchSize,
      int32_t maxOutputBatchSize,
      std::unique_ptr<ColumnarBatchIterator> in);

  std::shared_ptr<ColumnarBatch> next() override;

  int64_t spillFixedSize(int64_t size) override;

 private:
  bytedance::bolt::memory::MemoryPool* pool_;
  const int32_t minOutputBatchSize_;
  const int32_t maxOutputBatchSize_;
  std::unique_ptr<ColumnarBatchIterator> in_;

  std::unique_ptr<ColumnarBatchIterator> next_ = nullptr;
};

} // namespace gluten

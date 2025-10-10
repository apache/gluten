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
#include "operators/r2c/RowToColumnar.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/type/Type.h"

namespace gluten {

class BoltRowToColumnarConverter final : public RowToColumnarConverter {
 public:
  BoltRowToColumnarConverter(
      struct ArrowSchema* cSchema,
      std::shared_ptr<bytedance::bolt::memory::MemoryPool> memoryPool);

  std::shared_ptr<ColumnarBatch> convert(int64_t numRows, int64_t* rowLength, uint8_t* memoryAddress);

 private:
  std::shared_ptr<ColumnarBatch> convertPrimitive(int64_t numRows, int64_t* rowLength, uint8_t* memoryAddress);
  bytedance::bolt::TypePtr rowType_;
  std::shared_ptr<bytedance::bolt::memory::MemoryPool> pool_;
};

} // namespace gluten

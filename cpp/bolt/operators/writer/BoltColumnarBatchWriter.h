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

#include "operators/writer/ColumnarBatchWriter.h"

#include "bolt/dwio/parquet/writer/Writer.h"

namespace gluten {

class BoltColumnarBatchWriter final : public ColumnarBatchWriter {
 public:
  BoltColumnarBatchWriter(
      const std::string& path,
      int64_t batchSize,
      std::shared_ptr<bytedance::bolt::memory::MemoryPool> pool);

  arrow::Status write(const std::shared_ptr<ColumnarBatch>& batch) override;

  arrow::Status close() override;

 private:
  arrow::Status initWriter(const bytedance::bolt::RowTypePtr& rowType);

  std::string path_;
  int64_t batchSize_;
  std::shared_ptr<bytedance::bolt::memory::MemoryPool> pool_;

  std::unique_ptr<bytedance::bolt::parquet::Writer> writer_{nullptr};
};

} // namespace gluten

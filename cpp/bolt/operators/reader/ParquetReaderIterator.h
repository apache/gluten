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

#include "memory/BoltColumnarBatch.h"
#include "operators/reader/FileReaderIterator.h"

#include "bolt/dwio/parquet/reader/ParquetReader.h"

#include <memory>

namespace gluten {

class ParquetReaderIterator : public FileReaderIterator {
 public:
  explicit ParquetReaderIterator(
      const std::string& path,
      int64_t batchSize,
      std::shared_ptr<bytedance::bolt::memory::MemoryPool> pool);

  bytedance::bolt::RowTypePtr getRowType() const {
    return rowType_;
  }

 protected:
  void createRowReader();

  std::shared_ptr<bytedance::bolt::memory::MemoryPool> pool_;

  bytedance::bolt::RowTypePtr rowType_;
  std::unique_ptr<bytedance::bolt::dwio::common::RowReader> rowReader_;

  int64_t batchSize_;
};

class ParquetStreamReaderIterator final : public ParquetReaderIterator {
 public:
  ParquetStreamReaderIterator(
      const std::string& path,
      int64_t batchSize,
      std::shared_ptr<bytedance::bolt::memory::MemoryPool> pool);

  std::shared_ptr<ColumnarBatch> next() override;
};

class ParquetBufferedReaderIterator final : public ParquetReaderIterator {
 public:
  explicit ParquetBufferedReaderIterator(
      const std::string& path,
      int64_t batchSize,
      std::shared_ptr<bytedance::bolt::memory::MemoryPool> pool);

  std::shared_ptr<ColumnarBatch> next() override;

 private:
  void collectBatches();

  std::vector<std::shared_ptr<BoltColumnarBatch>> batches_;
  std::vector<std::shared_ptr<BoltColumnarBatch>>::const_iterator iter_;
};

} // namespace gluten

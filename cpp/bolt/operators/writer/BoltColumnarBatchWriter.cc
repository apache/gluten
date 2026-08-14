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

#include "operators/writer/BoltColumnarBatchWriter.h"
#include "memory/BoltColumnarBatch.h"

#include "bolt/dwio/common/FileSink.h"
#include "bolt/dwio/parquet/writer/Writer.h"

namespace gluten {

BoltColumnarBatchWriter::BoltColumnarBatchWriter(
    const std::string& path,
    int64_t batchSize,
    std::shared_ptr<bytedance::bolt::memory::MemoryPool> pool)
    : path_(path), batchSize_(batchSize), pool_(std::move(pool)) {}

arrow::Status BoltColumnarBatchWriter::initWriter(const bytedance::bolt::RowTypePtr& rowType) {
  auto localWriteFile = std::make_unique<bytedance::bolt::LocalWriteFile>(path_, false, true);
  auto sink = std::make_unique<bytedance::bolt::dwio::common::WriteFileSink>(std::move(localWriteFile), path_);

  bytedance::bolt::parquet::WriterOptions writerOptions;
  writerOptions.memoryPool = pool_.get();
  writerOptions.compression = bytedance::bolt::common::CompressionKind::CompressionKind_SNAPPY;
  writerOptions.minBatchSize = batchSize_;

  writer_ = std::make_unique<bytedance::bolt::parquet::Writer>(std::move(sink), writerOptions, rowType);
  return arrow::Status::OK();
}

arrow::Status BoltColumnarBatchWriter::write(const std::shared_ptr<ColumnarBatch>& batch) {
  auto rowVector = BoltColumnarBatch::from(pool_.get(), batch)->getRowVector();
  if (!writer_) {
    RETURN_NOT_OK(initWriter(bytedance::bolt::asRowType(rowVector->type())));
  }

  writer_->write(rowVector);
  return arrow::Status::OK();
}

arrow::Status BoltColumnarBatchWriter::close() {
  writer_->close();
  return arrow::Status::OK();
}
} // namespace gluten

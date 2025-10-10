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

#include "memory/ColumnarBatch.h"

#include <arrow/ipc/message.h>
#include <arrow/ipc/options.h>

#include "compute/ResultIterator.h"
#include "bolt/shuffle/sparksql/BoltShuffleReader.h"

namespace gluten {

class BoltShuffleReaderIteratorWrapper : public ColumnarBatchIterator {
  using NextDeserializerFunc =
      std::function<std::unique_ptr<bytedance::bolt::shuffle::sparksql::BoltColumnarBatchDeserializer>()>;

 public:
  BoltShuffleReaderIteratorWrapper(NextDeserializerFunc nextDeserializer)
      : nextDeserializer_(std::move(nextDeserializer)), deserializer_(nullptr) {}

  ~BoltShuffleReaderIteratorWrapper() = default;

  // null means stream end
  virtual std::shared_ptr<ColumnarBatch> next() {
    if (deserializer_ == nullptr) {
      deserializer_ = nextDeserializer_();
      if (deserializer_ == nullptr) {
        return nullptr;
      }
    }
    do {
      auto nextBatch = deserializer_->next();
      if (nextBatch) {
        return std::make_shared<BoltColumnarBatch>(std::move(nextBatch));
      } else {
        deserializer_ = nextDeserializer_();
      }
    } while (deserializer_ != nullptr);
    return nullptr;
  }

  virtual int64_t spillFixedSize(int64_t size) {
    return 0L;
  }

 private:
  NextDeserializerFunc nextDeserializer_;
  std::unique_ptr<bytedance::bolt::shuffle::sparksql::BoltColumnarBatchDeserializer> deserializer_;
};

inline bytedance::bolt::shuffle::sparksql::ShuffleReaderOptions getOptionsFromInfo(const ShuffleReaderInfo& info) {
  // Convert codec string into lowercase.
  arrow::Compression::type compressionType;
  std::string comp = info.compression_type();
  if (comp.empty()) {
    compressionType = arrow::Compression::UNCOMPRESSED;
  } else {
    std::string lowerStr;
    std::transform(comp.begin(), comp.end(), std::back_inserter(lowerStr), ::tolower);
    GLUTEN_ASSIGN_OR_THROW(compressionType, arrow::util::Codec::GetCompressionType(lowerStr));
  }
  return bytedance::bolt::shuffle::sparksql::ShuffleReaderOptions{
      .compressionType = compressionType,
      .codecBackend = info.codec(),
      .batchSize = info.batch_size(),
      .shuffleBatchByteSize = info.shuffle_batch_byte_size(),
      .numPartitions = info.num_partitions(),
      .partitionShortName = info.partition_short_name(),
      .forceShuffleWriterType = info.forced_writer_type()};
}

class BoltShuffleReaderWrapper : public ShuffleReaderBase {
 public:
  explicit BoltShuffleReaderWrapper(
      std::shared_ptr<arrow::Schema> schema,
      const ShuffleReaderInfo& info,
      arrow::MemoryPool* pool,
      bytedance::bolt::memory::MemoryPool* boltPool)
      : shuffleReader_(schema, getOptionsFromInfo(info), pool, boltPool) {}

  virtual ~BoltShuffleReaderWrapper() {}

  virtual std::shared_ptr<ResultIterator> readStream(std::shared_ptr<arrow::io::InputStream> in) {
    return std::make_shared<ResultIterator>(
        std::make_unique<BoltShuffleReaderIteratorWrapper>([&]() { return shuffleReader_.readStream(in); }));
  }

  std::shared_ptr<ResultIterator> readStream(std::shared_ptr<StreamReader> streamReader) {
    return std::make_shared<ResultIterator>(std::make_unique<BoltShuffleReaderIteratorWrapper>([streamReader, this]() {
      auto stream = streamReader->readNextStream(getPool());
      return stream ? shuffleReader_.readStream(stream) : nullptr;
    }));
  }

  arrow::Status close() {
    return shuffleReader_.close();
  }

  int64_t getDecompressTime() const {
    return shuffleReader_.getDecompressTime();
  }

  int64_t getIpcTime() const {
    return shuffleReader_.getIpcTime();
  }

  int64_t getDeserializeTime() const {
    return shuffleReader_.getDeserializeTime();
  }

  arrow::MemoryPool* getPool() const {
    return shuffleReader_.getPool();
  }

 private:
  bytedance::bolt::shuffle::sparksql::BoltShuffleReader shuffleReader_;
};

} // namespace gluten

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
#include "shuffle_reader_info.pb.h"

namespace gluten {

class ShuffleReaderBase {
 public:
  virtual ~ShuffleReaderBase() = default;

  // FIXME iterator should be unique_ptr or un-copyable singleton
  virtual std::shared_ptr<ResultIterator> readStream(std::shared_ptr<arrow::io::InputStream> in) = 0;

  virtual arrow::Status close() = 0;

  virtual int64_t getDecompressTime() const = 0;

  virtual int64_t getIpcTime() const = 0;

  virtual int64_t getDeserializeTime() const = 0;

  virtual arrow::MemoryPool* getPool() const = 0;
};

class ReaderStreamIterator {
 public:
  virtual ~ReaderStreamIterator() = default;

  virtual ShuffleReaderInfo getShuffleReaderInfo() const = 0;

  virtual std::shared_ptr<arrow::io::InputStream> nextStream(arrow::MemoryPool* pool) = 0;

  virtual void close() = 0;

  virtual void updateMetrics(
      int64_t numRows,
      int64_t numBatches,
      int64_t decompressTime,
      int64_t deserializeTime,
      int64_t totalReadTime) = 0;
};
} // namespace gluten

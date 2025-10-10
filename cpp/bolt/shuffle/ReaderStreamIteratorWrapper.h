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

#include <mutex>

#include "shuffle/ShuffleReaderBase.h"
#include "bolt/shuffle/sparksql/ReaderStreamIterator.h"
#include "jni/JniCommon.h"
#include "compute/ResultIterator.h"

namespace gluten {

class ReaderStreamIteratorWrapper : public bytedance::bolt::shuffle::sparksql::ReaderStreamIterator {
 public:
  ReaderStreamIteratorWrapper(
      std::shared_ptr<ResultIterator> iteratorHolder,
      ShuffleReaderWrapperedIterator* readerWrapper)
      : iteratorHolder_(std::move(iteratorHolder)), readerWrapper_(readerWrapper) {
    GLUTEN_CHECK(readerWrapper_ != nullptr, "ShuffleReaderWrapperedIterator cannot be null");
  }
  ~ReaderStreamIteratorWrapper() override = default;

  virtual std::shared_ptr<arrow::io::InputStream> nextStream(arrow::MemoryPool* pool) override {
    std::lock_guard<std::mutex> lock{mutex_};
    auto streamReader = readerWrapper_->getStreamReader();
    GLUTEN_CHECK(streamReader != nullptr, "Shuffle stream reader cannot be null");
    return streamReader->readNextStream(pool);
  }

  virtual void close() override {
    std::lock_guard<std::mutex> lock{mutex_};
    readerWrapper_->markAsOffloaded();
  }

  virtual void updateMetrics(
      int64_t numRows,
      int64_t numBatches,
      int64_t decompressTime,
      int64_t deserializeTime,
      int64_t totalReadTime) override {
    std::lock_guard<std::mutex> lock{mutex_};
    readerWrapper_->updateMetrics(numRows, numBatches, decompressTime, deserializeTime, totalReadTime);
  }

 private:
  std::shared_ptr<ResultIterator> iteratorHolder_;
  ShuffleReaderWrapperedIterator* readerWrapper_;
  // [multi-thread spark]
  std::mutex mutex_;
};

} // namespace gluten

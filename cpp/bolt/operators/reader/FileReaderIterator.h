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

#include "compute/ResultIterator.h"
#include "memory/ColumnarBatchIterator.h"
#include "bolt/common/memory/MemoryPool.h"

namespace gluten {

enum FileReaderType { kBuffered, kStream, kNone };

class FileReaderIterator : public ColumnarBatchIterator {
 public:
  static std::shared_ptr<ResultIterator> getInputIteratorFromFileReader(
      FileReaderType readerType,
      const std::string& path,
      int64_t batchSize,
      std::shared_ptr<bytedance::bolt::memory::MemoryPool> pool);

  explicit FileReaderIterator(const std::string& path) : path_(path){};

  virtual ~FileReaderIterator() = default;

  int64_t getCollectBatchTime() const {
    return collectBatchTime_;
  }

 protected:
  int64_t collectBatchTime_ = 0;
  std::string path_;
};

} // namespace gluten

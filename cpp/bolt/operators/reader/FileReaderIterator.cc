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

#include "operators/reader/FileReaderIterator.h"
#include <filesystem>
#include "operators/reader/ParquetReaderIterator.h"

namespace gluten {
namespace {
const std::string kParquetSuffix = ".parquet";
}

std::shared_ptr<gluten::ResultIterator> FileReaderIterator::getInputIteratorFromFileReader(
    FileReaderType readerType,
    const std::string& path,
    int64_t batchSize,
    std::shared_ptr<bytedance::bolt::memory::MemoryPool> pool) {
  std::filesystem::path input{path};
  auto suffix = input.extension().string();
  if (suffix == kParquetSuffix) {
    if (readerType == FileReaderType::kStream) {
      return std::make_shared<gluten::ResultIterator>(
          std::make_unique<ParquetStreamReaderIterator>(path, batchSize, pool));
    }
    if (readerType == FileReaderType::kBuffered) {
      return std::make_shared<gluten::ResultIterator>(
          std::make_unique<ParquetBufferedReaderIterator>(path, batchSize, pool));
    }
  }
  throw new GlutenException("Unreachable.");
}

} // namespace gluten

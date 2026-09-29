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

namespace gluten {

class StreamReader {
 public:
  virtual ~StreamReader() = default;

  virtual std::shared_ptr<arrow::io::InputStream> readNextStream(arrow::MemoryPool* pool) = 0;
};

class ShuffleReader {
 public:
  enum class OutputType { kRowVector, kCudfTable };

  virtual ~ShuffleReader() = default;

  // FIXME iterator should be unique_ptr or un-copyable singleton
  virtual std::shared_ptr<ResultIterator> read(
      const std::shared_ptr<StreamReader>& streamReader,
      const OutputType& outputType) = 0;

  virtual int64_t getDecompressTime() const = 0;

  virtual int64_t getDeserializeTime() const = 0;

  virtual void stop() = 0;

  static OutputType getOutputType(int32_t executionModeId) {
    switch (executionModeId) {
      case 0:
        // Cpu execution mode.
        return OutputType::kRowVector;
      case 1:
        // Gpu execution mode.
        return OutputType::kCudfTable;
      default:
        throw GlutenException("Unsupported execution mode id: " + std::to_string(executionModeId));
    }
  }
};

} // namespace gluten

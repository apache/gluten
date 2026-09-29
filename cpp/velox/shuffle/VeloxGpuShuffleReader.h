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

#include "memory/VeloxMemoryManager.h"
#include "shuffle/Payload.h"
#include "shuffle/ShuffleReader.h"
#include "shuffle/VeloxShuffleReader.h"

#include "velox/type/Type.h"

namespace gluten {

/// Convert the buffers to cudf table.
/// Add a lock after reader produces the Vector, relase the lock after the thread processes all the batches.
/// After move the shuffle read operation to gpu, move the lock to start read.
class VeloxGpuHashShuffleReaderDeserializer final : public ShuffleReaderDeserializer {
 public:
  VeloxGpuHashShuffleReaderDeserializer(
      const std::shared_ptr<StreamReader>& streamReader,
      const std::shared_ptr<arrow::Schema>& schema,
      const std::shared_ptr<arrow::util::Codec>& codec,
      const facebook::velox::RowTypePtr& rowType,
      int64_t readerBufferSize,
      VeloxMemoryManager* memoryManager,
      int64_t& deserializeTime,
      int64_t& decompressTime);

  std::shared_ptr<ColumnarBatch> next();

  std::unique_ptr<ColumnarBatchIterator> deserializeStreams() override;

 private:
  bool resolveNextBlockType();

  void loadNextStream();

  std::shared_ptr<StreamReader> streamReader_;
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<arrow::util::Codec> codec_;
  facebook::velox::RowTypePtr rowType_;
  int64_t readerBufferSize_;
  VeloxMemoryManager* memoryManager_;

  int64_t& deserializeTime_;
  int64_t& decompressTime_;

  std::shared_ptr<arrow::io::InputStream> in_{nullptr};

  bool reachedEos_{false};
  bool blockTypeResolved_{false};
};
} // namespace gluten

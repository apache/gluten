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

#include <arrow/io/api.h>

#include "shuffle/PartitionWriter.h"
#include "shuffle/ShuffleWriter.h"
#include "utils/Macros.h"

namespace gluten {

class LocalPartitionWriter : public PartitionWriter {
 public:
  explicit LocalPartitionWriter(
      uint32_t numPartitions,
      std::unique_ptr<arrow::util::Codec> codec,
      MemoryManager* memoryManager,
      const std::shared_ptr<LocalPartitionWriterOptions>& options,
      const std::string& dataFile,
      std::vector<std::string> localDirs);

  arrow::Status hashEvict(
      uint32_t partitionId,
      std::unique_ptr<InMemoryPayload> inMemoryPayload,
      Evict::type evictType,
      bool reuseBuffers,
      int64_t& evictBytes) override;

  arrow::Status sortEvict(
      uint32_t partitionId,
      std::unique_ptr<InMemoryPayload> inMemoryPayload,
      bool isFinal,
      int64_t& evictBytes) override;

  // This code path is not used by LocalPartitionWriter, Not implement it by default.
  arrow::Status evict(uint32_t partitionId, std::unique_ptr<BlockPayload> blockPayload, bool stop, int64_t& evictBytes)
      override {
    return arrow::Status::NotImplemented("Invalid code path for local shuffle writer.");
  }

  bool enableTypeAwareCompress() const override {
    // Type-aware compression is not compatible with dictionary encoding
    // since it may change the buffer layout and types.
    return options_->enableTypeAwareCompress && !options_->enableDictionary;
  }

  /// The stop function performs several tasks:
  /// 1. Opens the final data file.
  /// 2. Iterates over each partition ID (pid) to:
  ///    a. Merge data from spilled files and write to the final file.
  ///    b. Write cached payloads to the final file.
  ///    c. Create the last payload from partition buffer, and write to the final file.
  ///    d. Optionally, write End of Stream (EOS) if any payload has been written.
  ///    e. Record the offset for each partition in the final file.
  /// 3. Closes and deletes all the spilled files.
  /// 4. Records various metrics such as total write time, bytes evicted, and bytes written.
  /// 5. Clears any buffered resources and closes the final file.
  ///
  /// Spill handling:
  /// Spill is allowed during stop().
  /// Among above steps, 1. and 2.c requires memory allocation and may trigger spill.
  /// If spill is triggered by 1., cached payloads of all partitions will be spilled.
  /// If spill is triggered by 2.c, cached payloads of the remaining unmerged partitions will be spilled.
  /// In both cases, if the cached payload size doesn't free enough memory,
  /// it will shrink partition buffers to free more memory.
  arrow::Status stop(ShuffleWriterMetrics* metrics, int64_t& evictBytes) override;

  // Spill source:
  // 1. Other op.
  // 2. PayloadMerger merging payloads or compressing merged payload.
  // 3. After stop() called,
  arrow::Status reclaimFixedSize(int64_t size, int64_t* actual) override;

 protected:
  class LocalSpiller;

  class PayloadMerger;

  class PayloadCache;

  void init();

  arrow::Status requestSpill(bool isFinal);

  arrow::Status finishSpill();

  arrow::Status finishMerger();

  std::string nextSpilledFileDir();

  arrow::Result<int64_t> mergeSpills(uint32_t partitionId, arrow::io::OutputStream* os);

  arrow::Status writeCachedPayloads(uint32_t partitionId, arrow::io::OutputStream* os) const;

  arrow::Status clearResource();

  arrow::Status populateMetrics(ShuffleWriterMetrics* metrics);

  std::shared_ptr<LocalPartitionWriterOptions> options_;
  std::string dataFile_;
  std::vector<std::string> localDirs_;

  bool stopped_{false};
  bool useSpillFileAsDataFile_{false};
  std::shared_ptr<LocalSpiller> spiller_{nullptr};
  std::shared_ptr<PayloadMerger> merger_{nullptr};
  std::shared_ptr<PayloadCache> payloadCache_{nullptr};
  std::list<std::shared_ptr<Spill>> spills_{};

  // configured local dirs for spilled file
  int32_t dirSelection_{0};
  std::vector<int32_t> subDirSelection_;
  std::shared_ptr<arrow::io::OutputStream> dataFileOs_{nullptr};

  int64_t totalBytesToEvict_{0};
  int64_t totalBytesEvicted_{0};
  int64_t totalBytesWritten_{0};
  std::vector<int64_t> partitionLengths_;
  std::vector<int64_t> rawPartitionLengths_;

  int32_t lastEvictPid_{-1};
};
} // namespace gluten

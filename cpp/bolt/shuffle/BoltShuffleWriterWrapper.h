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

#include <arrow/ipc/writer.h>
#include <cstdint>
#include <numeric>
#include <utility>

#include "memory/ColumnarBatch.h"
#include "shuffle_writer_info.pb.h"
#include "utils/StringUtil.h"
#include "shuffle/ShuffleWriterBase.h"
#include "shuffle/RssClientWrapper.h"
#include "bolt/shuffle/sparksql/BoltShuffleWriter.h"

namespace gluten {

class BoltShuffleWriterWrapper : public ShuffleWriterBase {
 public:
  static bytedance::bolt::shuffle::sparksql::ShuffleWriterOptions getOptionsFromInfo(const ShuffleWriterInfo& info, std::shared_ptr<RssClient> rssClient) {
    auto shuffleWriterOptions = bytedance::bolt::shuffle::sparksql::ShuffleWriterOptions{
        .bufferSize = info.buffer_size(),
        .bufferReallocThreshold = info.realloc_threshold(),
        .partitioning = bytedance::bolt::shuffle::sparksql::toPartitioning(info.partitioning_name()),
        .taskAttemptId = info.task_attempt_id(),
        .startPartitionId = info.start_partition_id(),
        .forceShuffleWriterType = info.forced_writer_type(),
        .useV2PreallocSizeThreshold = info.use_v2_prealloc_threshold(),
        .rowvectorModeCompressionMinColumns = info.row_compression_min_cols(),
        .rowvectorModeCompressionMaxBufferSize = info.row_compression_max_buffer(),
        .enableVectorCombination = info.enable_vector_combination(),
        .accumulateBatchMaxColumns = info.accumulate_batch_max_columns(),
        .accumulateBatchMaxBatches = info.accumulate_batch_max_batches(),
        .recommendedColumn2RowSize = info.recommended_c2r_size(),
        .shuffleCheckRatio = info.shuffle_check_ratio(),
    };

    // Convert codec string into lowercase.
    std::string codecLower;
    std::string codec = info.compression_codec();
    std::transform(codec.begin(), codec.end(), std::back_inserter(codecLower), ::tolower);
    GLUTEN_ASSIGN_OR_THROW(auto compressionType, arrow::util::Codec::GetCompressionType(codecLower));

    auto partitionWriterOptions = bytedance::bolt::shuffle::sparksql::PartitionWriterOptions{
        .numPartitions = info.num_partitions(),
        .mergeBufferSize = info.merge_buffer_size(),
        .mergeThreshold = info.merge_threshold(),
        .compressionThreshold = info.compression_threshold(),
        .compressionType = compressionType,
        .codecBackend = info.compression_backend(),
        .compressionLevel = info.compression_level(),
        .compressionMode = info.compression_mode(),
        .bufferedWrite = true,
        .numSubDirs = info.num_sub_dirs(),
        .pushBufferMaxSize =
            info.push_buffer_max_size() > 0 ? info.push_buffer_max_size() : bytedance::bolt::shuffle::sparksql::kDefaultShuffleWriterBufferSize,
	.shuffleBufferSize = info.shuffle_batch_byte_size(),
        .rowvectorModeCompressionMinColumns = info.row_compression_min_cols(),
        .rowvectorModeCompressionMaxBufferSize = info.row_compression_max_buffer(),

        .partitionWriterType = bytedance::bolt::shuffle::sparksql::getPartitionWriterType(info.writer_type()),
        .dataFile = info.data_file(),
        .configuredDirs = gluten::splitPaths(info.local_dirs()),
        .rssClient = std::make_shared<RssClientWrapper>(rssClient),
    };
    shuffleWriterOptions.partitionWriterOptions = partitionWriterOptions;
    return shuffleWriterOptions;
  }

  static ShuffleWriterResult getResultFromMetrics(const bytedance::bolt::shuffle::sparksql::ShuffleWriterMetrics& m) {
    ShuffleWriterResult result;
    for (auto length : m.partitionLengths) {
      result.add_partitionlengths(length);
    }
    auto metrics = result.mutable_metrics();
    metrics->set_input_row_number(m.totalInputRowNumber);
    metrics->set_input_batches(m.totalInputBatches);
    metrics->set_split_time(m.splitTime);
    metrics->set_spill_time(m.totalEvictTime);
    metrics->set_spill_bytes(m.totalBytesEvicted);
    metrics->set_split_buffer_size(m.maxPartitionBufferSize);
    metrics->set_prealloc_size(m.avgPreallocSize);
    metrics->set_row_vector_mode_compress(m.rowVectorModeCompress);
    metrics->set_combined_vector_number(m.combinedVectorNumber);
    metrics->set_combined_vector_times(m.combineVectorTimes);
    metrics->set_compute_pid_time(m.computePidTime);
    metrics->set_compress_time(m.totalCompressTime);
    metrics->set_use_v2(m.useV2);
    metrics->set_convert_time(m.convertTime);
    metrics->set_flatten_time(m.flattenTime);
    metrics->set_data_size(m.dataSize);
    metrics->set_use_row_based(m.useRowBased);
    metrics->set_total_bytes_written(m.totalBytesWritten);
    metrics->set_total_write_time(m.totalWriteTime);
    metrics->set_shuffle_write_time(m.shuffleWriteTime);
    return result;
  }

  BoltShuffleWriterWrapper(
      const ShuffleWriterInfo& info,
      std::shared_ptr<RssClient> rssClient,
      int32_t numColumnsExcludePid,
      int64_t firstBatchRowNumber,
      int64_t firstBatchFlatSize,
      bytedance::bolt::memory::MemoryPool* boltPool,
      arrow::MemoryPool* pool)
      : shuffleWriter_(
            bytedance::bolt::shuffle::sparksql::BoltShuffleWriter::create(
                getOptionsFromInfo(info, rssClient),
                numColumnsExcludePid,
                firstBatchRowNumber,
                firstBatchFlatSize,
                info.mem_limit(),
                boltPool,
                pool)) {}

  BoltShuffleWriterWrapper(
      std::shared_ptr<bytedance::bolt::shuffle::sparksql::BoltShuffleWriter> shuffleWriter) : shuffleWriter_(std::move(shuffleWriter)) {}

  virtual arrow::Status split(std::shared_ptr<ColumnarBatch> cb, int64_t memLimit) {
    // Note: if cb is a CompositeColumnarBatch, it will combine into one RowVector here.
    auto boltColumnBatch = BoltColumnarBatch::from(shuffleWriter_->boltPool(), cb);
    BOLT_CHECK_NOT_NULL(boltColumnBatch);
    return shuffleWriter_->split(boltColumnBatch->getRowVector(), memLimit);
  }

  virtual arrow::Status stop() {
    return shuffleWriter_->stop();
  }

  virtual int32_t numPartitions() const {
    return shuffleWriter_->numPartitions();
  }

  virtual int64_t partitionBufferSize() const {
    return shuffleWriter_->partitionBufferSize();
  }

  virtual int64_t maxPartitionBufferSize() const {
    return shuffleWriter_->maxPartitionBufferSize();
  }

  virtual int64_t totalBytesWritten() const {
    return shuffleWriter_->totalBytesWritten();
  }

  virtual int64_t totalBytesEvicted() const {
    return shuffleWriter_->totalBytesEvicted();
  }

  virtual int64_t totalWriteTime() const {
    return shuffleWriter_->totalWriteTime();
  }

  virtual int64_t totalEvictTime() const {
    return shuffleWriter_->totalEvictTime();
  }

  virtual int64_t totalCompressTime() const {
    return shuffleWriter_->totalCompressTime();
  }

  virtual int64_t avgPeallocSize() const {
    return shuffleWriter_->avgPeallocSize();
  }

  virtual int64_t useV2() const {
    return shuffleWriter_->useV2();
  }

  virtual int64_t rowVectorModeCompress() const {
    return shuffleWriter_->rowVectorModeCompress();
  }

  virtual int64_t combinedVectorNumber() const {
    return shuffleWriter_->combinedVectorNumber();
  }

  virtual int64_t combineVectorTimes() const {
    return shuffleWriter_->combineVectorTimes();
  }

  virtual int64_t combineVectorCost() const {
    return shuffleWriter_->combineVectorCost();
  }

  virtual int64_t useRowBased() const {
    return shuffleWriter_->useRowBased();
  }

  virtual int64_t totalConvertTime() const {
    return shuffleWriter_->totalConvertTime();
  }

  virtual int64_t totalFlattenTime() const {
    return shuffleWriter_->totalFlattenTime();
  }

  virtual int64_t totalComputePidTime() const {
    return shuffleWriter_->totalComputePidTime();
  }

  virtual const std::vector<int64_t>& partitionLengths() const {
    return shuffleWriter_->partitionLengths();
  }

  virtual const std::vector<int64_t>& rawPartitionLengths() const {
    return shuffleWriter_->rawPartitionLengths();
  }

  virtual const uint64_t cachedPayloadSize() const {
    return shuffleWriter_->cachedPayloadSize();
  }

  virtual arrow::Status reclaimFixedSize(int64_t size, int64_t* actual) {
    return shuffleWriter_->reclaimFixedSize(size, actual);
  }

  virtual ~BoltShuffleWriterWrapper() {}

 private:
  std::shared_ptr<bytedance::bolt::shuffle::sparksql::BoltShuffleWriter> shuffleWriter_;
};

} // namespace gluten

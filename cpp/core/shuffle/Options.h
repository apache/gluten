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

#include "shuffle/Partitioning.h"
#include "utils/Compression.h"
#include "utils/Macros.h"

#include <arrow/ipc/options.h>
#include <arrow/util/compression.h>

namespace gluten {

static constexpr int16_t kDefaultBatchSize = 4096;
static constexpr int32_t kDefaultShuffleWriterBufferSize = 4096;
static constexpr int64_t kDefaultSortBufferThreshold = 64 << 20;
static constexpr int64_t kDefaultPushMemoryThreshold = 4096;
static constexpr int32_t kDefaultNumSubDirs = 64;
static constexpr int32_t kDefaultCompressionThreshold = 100;
static constexpr int32_t kDefaultCompressionBufferSize = 32 * 1024;
static constexpr int32_t kDefaultDiskWriteBufferSize = 1024 * 1024;
static constexpr double kDefaultSplitBufferReallocThreshold = 0.25;
static constexpr double kDefaultMergeBufferThreshold = 0.25;
static constexpr bool kDefaultUseRadixSort = true;
static constexpr int32_t kDefaultSortBufferSize = 4096;
static constexpr int64_t kDefaultReadBufferSize = 1 << 20;
static constexpr int64_t kDefaultDeserializerBufferSize = 1 << 20;
static constexpr int64_t kDefaultShuffleFileBufferSize = 32 << 10;
static constexpr bool kDefaultEnableDictionary = false;
static constexpr bool kDefaultEnableTypeAwareCompress = false;

enum class ShuffleWriterType { kHashShuffle, kSortShuffle, kRssSortShuffle, kGpuHashShuffle };

enum class PartitionWriterType { kLocal, kRss };

struct ShuffleReaderOptions {
  ShuffleWriterType shuffleWriterType = ShuffleWriterType::kHashShuffle;

  // Compression options.
  arrow::Compression::type compressionType = arrow::Compression::type::LZ4_FRAME;
  CodecBackend codecBackend = CodecBackend::NONE;

  // Output batch size.
  int32_t batchSize = kDefaultBatchSize;

  // Buffer size when reading data from the input stream.
  int64_t readerBufferSize = kDefaultReadBufferSize;

  // Buffer size when deserializing rows into columnar batches. Only used for sort-based shuffle.
  int64_t deserializerBufferSize = kDefaultDeserializerBufferSize;
};

struct ShuffleWriterOptions {
  ShuffleWriterType shuffleWriterType;
  Partitioning partitioning = Partitioning::kRoundRobin;
  int32_t startPartitionId = 0;

  ShuffleWriterOptions(ShuffleWriterType shuffleWriterType) : shuffleWriterType(shuffleWriterType) {}

  ShuffleWriterOptions(ShuffleWriterType shuffleWriterType, Partitioning partitioning, int32_t startPartitionId)
      : shuffleWriterType(shuffleWriterType), partitioning(partitioning), startPartitionId(startPartitionId) {}

  virtual ~ShuffleWriterOptions() = default;
};

struct HashShuffleWriterOptions : ShuffleWriterOptions {
  int32_t splitBufferSize = kDefaultShuffleWriterBufferSize;
  double splitBufferReallocThreshold = kDefaultSplitBufferReallocThreshold;

  HashShuffleWriterOptions() : ShuffleWriterOptions(ShuffleWriterType::kHashShuffle) {}

  HashShuffleWriterOptions(
      Partitioning partitioning,
      int32_t startPartitionId,
      int32_t partitionBufferSize,
      double partitionBufferReallocThreshold)
      : ShuffleWriterOptions(ShuffleWriterType::kHashShuffle, partitioning, startPartitionId),
        splitBufferSize(partitionBufferSize),
        splitBufferReallocThreshold(partitionBufferReallocThreshold) {}

 protected:
  HashShuffleWriterOptions(ShuffleWriterType shuffleWriterType) : ShuffleWriterOptions(shuffleWriterType) {}

  HashShuffleWriterOptions(
      ShuffleWriterType shuffleWriterType,
      Partitioning partitioning,
      int32_t startPartitionId,
      int32_t partitionBufferSize,
      double partitionBufferReallocThreshold)
      : ShuffleWriterOptions(shuffleWriterType, partitioning, startPartitionId),
        splitBufferSize(partitionBufferSize),
        splitBufferReallocThreshold(partitionBufferReallocThreshold) {}
};

struct SortShuffleWriterOptions : ShuffleWriterOptions {
  int32_t initialSortBufferSize = kDefaultSortBufferSize; // spark.shuffle.sort.initialBufferSize
  int32_t diskWriteBufferSize = kDefaultDiskWriteBufferSize; // spark.shuffle.spill.diskWriteBufferSize
  bool useRadixSort = kDefaultUseRadixSort; // spark.shuffle.sort.useRadixSort

  SortShuffleWriterOptions() : ShuffleWriterOptions(ShuffleWriterType::kSortShuffle) {}

  SortShuffleWriterOptions(
      Partitioning partitioning,
      int32_t startPartitionId,
      int32_t initialSortBufferSize,
      int32_t diskWriteBufferSize,
      bool useRadixSort)
      : ShuffleWriterOptions(ShuffleWriterType::kSortShuffle, partitioning, startPartitionId),
        initialSortBufferSize(initialSortBufferSize),
        diskWriteBufferSize(diskWriteBufferSize),
        useRadixSort(useRadixSort) {}
};

struct RssSortShuffleWriterOptions : ShuffleWriterOptions {
  int32_t splitBufferSize = kDefaultShuffleWriterBufferSize;
  int64_t sortBufferMaxSize = kDefaultSortBufferThreshold;
  arrow::Compression::type compressionType = arrow::Compression::type::LZ4_FRAME;

  RssSortShuffleWriterOptions() : ShuffleWriterOptions(ShuffleWriterType::kRssSortShuffle) {}

  RssSortShuffleWriterOptions(
      Partitioning partitioning,
      int32_t startPartitionId,
      int32_t splitBufferSize,
      int64_t sortBufferMaxSize,
      arrow::Compression::type compressionType)
      : ShuffleWriterOptions(ShuffleWriterType::kRssSortShuffle, partitioning, startPartitionId),
        splitBufferSize(splitBufferSize),
        sortBufferMaxSize(sortBufferMaxSize),
        compressionType(compressionType) {}
};

struct GpuHashShuffleWriterOptions : HashShuffleWriterOptions {
  int32_t splitBufferSize = kDefaultShuffleWriterBufferSize;
  double splitBufferReallocThreshold = kDefaultSplitBufferReallocThreshold;

  GpuHashShuffleWriterOptions() : HashShuffleWriterOptions(ShuffleWriterType::kGpuHashShuffle) {}

  GpuHashShuffleWriterOptions(
      Partitioning partitioning,
      int32_t startPartitionId,
      int32_t partitionBufferSize,
      double partitionBufferReallocThreshold)
      : HashShuffleWriterOptions(
            ShuffleWriterType::kGpuHashShuffle,
            partitioning,
            startPartitionId,
            partitionBufferSize,
            partitionBufferReallocThreshold) {}
};

struct LocalPartitionWriterOptions {
  int64_t shuffleFileBufferSize = kDefaultShuffleFileBufferSize; // spark.shuffle.file.buffer
  int32_t compressionBufferSize =
      kDefaultCompressionBufferSize; // spark.io.compression.lz4.blockSize,spark.io.compression.zstd.bufferSize

  int32_t compressionThreshold = kDefaultCompressionThreshold;
  int32_t mergeBufferSize = kDefaultShuffleWriterBufferSize;
  double mergeThreshold = kDefaultMergeBufferThreshold;

  int32_t numSubDirs = kDefaultNumSubDirs; // spark.diskStore.subDirectories

  bool enableDictionary = kDefaultEnableDictionary;
  bool enableTypeAwareCompress = kDefaultEnableTypeAwareCompress;

  LocalPartitionWriterOptions() = default;

  LocalPartitionWriterOptions(
      int64_t shuffleFileBufferSize,
      int32_t compressionBufferSize,
      int64_t compressionThreshold,
      int32_t mergeBufferSize,
      double mergeThreshold,
      int32_t numSubDirs,
      bool enableDictionary,
      bool enableTypeAwareCompress = kDefaultEnableTypeAwareCompress)
      : shuffleFileBufferSize(shuffleFileBufferSize),
        compressionBufferSize(compressionBufferSize),
        compressionThreshold(compressionThreshold),
        mergeBufferSize(mergeBufferSize),
        mergeThreshold(mergeThreshold),
        numSubDirs(numSubDirs),
        enableDictionary(enableDictionary),
        enableTypeAwareCompress(enableTypeAwareCompress) {}
};

struct RssPartitionWriterOptions {
  int32_t compressionBufferSize =
      kDefaultCompressionBufferSize; // spark.io.compression.lz4.blockSize,spark.io.compression.zstd.bufferSize
  int64_t pushBufferMaxSize = kDefaultPushMemoryThreshold;
  int64_t sortBufferMaxSize = kDefaultSortBufferThreshold;

  RssPartitionWriterOptions() = default;

  RssPartitionWriterOptions(int32_t compressionBufferSize, int64_t pushBufferMaxSize, int64_t sortBufferMaxSize)
      : compressionBufferSize(compressionBufferSize),
        pushBufferMaxSize(pushBufferMaxSize),
        sortBufferMaxSize(sortBufferMaxSize) {}
};

struct ShuffleWriterMetrics {
  int64_t totalBytesWritten{0};
  int64_t totalBytesEvicted{0};
  int64_t totalBytesToEvict{0};
  int64_t totalWriteTime{0};
  int64_t totalEvictTime{0};
  int64_t totalCompressTime{0};
  double avgDictionaryFields{0};
  int64_t dictionarySize{0};
  std::vector<int64_t> partitionLengths{};
  std::vector<int64_t> rawPartitionLengths{}; // Uncompressed size.
};
} // namespace gluten

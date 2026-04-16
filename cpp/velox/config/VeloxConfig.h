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

#include "config/GlutenConfig.h"

namespace gluten {
// memory
const std::string kSpillStrategy = "spark.gluten.sql.columnar.backend.velox.spillStrategy";
const std::string kSpillStrategyDefaultValue = "auto";
const std::string kSpillThreadNum = "spark.gluten.sql.columnar.backend.velox.spillThreadNum";
const uint32_t kSpillThreadNumDefaultValue = 0;
const std::string kAggregationSpillEnabled = "spark.gluten.sql.columnar.backend.velox.aggregationSpillEnabled";
const std::string kJoinSpillEnabled = "spark.gluten.sql.columnar.backend.velox.joinSpillEnabled";
const std::string kOrderBySpillEnabled = "spark.gluten.sql.columnar.backend.velox.orderBySpillEnabled";
const std::string kWindowSpillEnabled = "spark.gluten.sql.columnar.backend.velox.windowSpillEnabled";

// spill config
// refer to
// https://github.com/facebookincubator/velox/blob/95f3e80e77d046c12fbc79dc529366be402e9c2b/velox/docs/configs.rst#spilling
const std::string kMaxSpillLevel = "spark.gluten.sql.columnar.backend.velox.maxSpillLevel";
const std::string kMaxSpillFileSize = "spark.gluten.sql.columnar.backend.velox.maxSpillFileSize";
const std::string kSpillStartPartitionBit = "spark.gluten.sql.columnar.backend.velox.spillStartPartitionBit";
const std::string kSpillPartitionBits = "spark.gluten.sql.columnar.backend.velox.spillPartitionBits";
const std::string kMaxSpillRunRows = "spark.gluten.sql.columnar.backend.velox.MaxSpillRunRows";
const std::string kMaxSpillBytes = "spark.gluten.sql.columnar.backend.velox.MaxSpillBytes";
const std::string kSpillReadBufferSize = "spark.unsafe.sorter.spill.reader.buffer.size";
const uint64_t kMaxSpillFileSizeDefault = 1L * 1024 * 1024 * 1024;

const std::string kSpillableReservationGrowthPct =
    "spark.gluten.sql.columnar.backend.velox.spillableReservationGrowthPct";
const std::string kSpillPrefixSortEnabled = "spark.gluten.sql.columnar.backend.velox.spillPrefixsortEnabled";
// Whether to compress data spilled. Compression will use spark.io.compression.codec or kSpillCompressionKind.
const std::string kSparkShuffleSpillCompress = "spark.shuffle.spill.compress";
const std::string kCompressionKind = "spark.io.compression.codec";
/// The compression codec to use for spilling. Use kCompressionKind if not set.
const std::string kSpillCompressionKind = "spark.gluten.sql.columnar.backend.velox.spillCompressionCodec";
const std::string kMaxPartialAggregationMemoryRatio =
    "spark.gluten.sql.columnar.backend.velox.maxPartialAggregationMemoryRatio";
const std::string kMaxPartialAggregationMemory = "spark.gluten.sql.columnar.backend.velox.maxPartialAggregationMemory";
const std::string kMaxExtendedPartialAggregationMemoryRatio =
    "spark.gluten.sql.columnar.backend.velox.maxExtendedPartialAggregationMemoryRatio";
const std::string kMaxExtendedPartialAggregationMemory =
    "spark.gluten.sql.columnar.backend.velox.maxExtendedPartialAggregationMemory";
const std::string kAbandonPartialAggregationMinPct =
    "spark.gluten.sql.columnar.backend.velox.abandonPartialAggregationMinPct";
const std::string kAbandonPartialAggregationMinRows =
    "spark.gluten.sql.columnar.backend.velox.abandonPartialAggregationMinRows";

// hashmap build
const std::string kMinTableRowsForParallelJoinBuild = "spark.gluten.velox.minTableRowsForParallelJoinBuild";
const uint32_t kMinTableRowsForParallelJoinBuildDefault = 1'000;

const std::string kJoinBuildVectorHasherMaxNumDistinct = "spark.gluten.velox.joinBuildVectorHasherMaxNumDistinct";
const uint32_t kJoinBuildVectorHasherMaxNumDistinctDefault = 1'000'000;

const std::string kAbandonDedupHashMapMinRows = "spark.gluten.velox.abandonDedupHashMap.minRows";
const uint32_t kAbandonDedupHashMapMinRowsDefault = 100'000;
const std::string kAbandonDedupHashMapMinPct = "spark.gluten.velox.abandonDedupHashMap.minPct";
const uint32_t kAbandonDedupHashMapMinPctDefault = 0;

// execution
const std::string kSparkBloomFilterExpectedNumItems = "spark.sql.optimizer.runtime.bloomFilter.expectedNumItems";
const std::string kSparkBloomFilterNumBits = "spark.sql.optimizer.runtime.bloomFilter.numBits";
const std::string kSparkBloomFilterMaxNumBits = "spark.sql.optimizer.runtime.bloomFilter.maxNumBits";
const std::string kSparkBloomFilterMaxNumItems = "spark.sql.optimizer.runtime.bloomFilter.maxNumItems";
const std::string kBloomFilterExpectedNumItems = "spark.gluten.sql.columnar.backend.velox.bloomFilter.expectedNumItems";
const std::string kBloomFilterNumBits = "spark.gluten.sql.columnar.backend.velox.bloomFilter.numBits";
const std::string kBloomFilterMaxNumBits = "spark.gluten.sql.columnar.backend.velox.bloomFilter.maxNumBits";
const std::string kVeloxSplitPreloadPerDriver = "spark.gluten.sql.columnar.backend.velox.SplitPreloadPerDriver";

const std::string kHashProbeDynamicFilterPushdownEnabled =
    "spark.gluten.sql.columnar.backend.velox.hashProbe.dynamicFilterPushdown.enabled";

const std::string kHashProbeBloomFilterPushdownMaxSize =
    "spark.gluten.sql.columnar.backend.velox.hashProbe.bloomFilterPushdown.maxSize";

const std::string kValueStreamDynamicFilterEnabled =
    "spark.gluten.sql.columnar.backend.velox.valueStream.dynamicFilter.enabled";
const bool kValueStreamDynamicFilterEnabledDefault = false;

const std::string kShowTaskMetricsWhenFinished = "spark.gluten.sql.columnar.backend.velox.showTaskMetricsWhenFinished";
const bool kShowTaskMetricsWhenFinishedDefault = false;

const std::string kTaskMetricsToEventLogThreshold =
    "spark.gluten.sql.columnar.backend.velox.taskMetricsToEventLog.threshold";
const int64_t kTaskMetricsToEventLogThresholdDefault = -1;

const std::string kEnableUserExceptionStacktrace =
    "spark.gluten.sql.columnar.backend.velox.enableUserExceptionStacktrace";
const bool kEnableUserExceptionStacktraceDefault = true;

const std::string kEnableSystemExceptionStacktrace =
    "spark.gluten.sql.columnar.backend.velox.enableSystemExceptionStacktrace";
const bool kEnableSystemExceptionStacktraceDefault = true;

const std::string kMemoryUseHugePages = "spark.gluten.sql.columnar.backend.velox.memoryUseHugePages";
const bool kMemoryUseHugePagesDefault = false;

const std::string kVeloxMemInitCapacity = "spark.gluten.sql.columnar.backend.velox.memInitCapacity";
const uint64_t kVeloxMemInitCapacityDefault = 8 << 20;

const std::string kVeloxMemReclaimMaxWaitMs = "spark.gluten.sql.columnar.backend.velox.reclaimMaxWaitMs";
const uint64_t kVeloxMemReclaimMaxWaitMsDefault = 3600000; // 60min

const std::string kHiveConnectorId = "test-hive";
const std::string kVeloxCacheEnabled = "spark.gluten.sql.columnar.backend.velox.cacheEnabled";

const std::string kExprMaxCompiledRegexes = "spark.gluten.sql.columnar.backend.velox.maxCompiledRegexes";

// memory cache
const std::string kVeloxMemCacheSize = "spark.gluten.sql.columnar.backend.velox.memCacheSize";
const uint64_t kVeloxMemCacheSizeDefault = 1073741824; // 1G

// ssd cache
const std::string kVeloxSsdCacheSize = "spark.gluten.sql.columnar.backend.velox.ssdCacheSize";
const uint64_t kVeloxSsdCacheSizeDefault = 1073741824; // 1G
const std::string kVeloxSsdCachePath = "spark.gluten.sql.columnar.backend.velox.ssdCachePath";
const std::string kVeloxSsdCachePathDefault = "/tmp/";
const std::string kVeloxSsdCacheShards = "spark.gluten.sql.columnar.backend.velox.ssdCacheShards";
const uint32_t kVeloxSsdCacheShardsDefault = 1;
const std::string kVeloxSsdCacheIOThreads = "spark.gluten.sql.columnar.backend.velox.ssdCacheIOThreads";
const uint32_t kVeloxSsdCacheIOThreadsDefault = 1;
const std::string kVeloxSsdODirectEnabled = "spark.gluten.sql.columnar.backend.velox.ssdODirect";
const std::string kVeloxSsdCheckpointIntervalBytes =
    "spark.gluten.sql.columnar.backend.velox.ssdCheckpointIntervalBytes";
const std::string kVeloxSsdDisableFileCow = "spark.gluten.sql.columnar.backend.velox.ssdDisableFileCow";
const std::string kVeloxSsdCheckSumEnabled = "spark.gluten.sql.columnar.backend.velox.ssdChecksumEnabled";
const std::string kVeloxSsdCheckSumReadVerificationEnabled =
    "spark.gluten.sql.columnar.backend.velox.ssdChecksumReadVerificationEnabled";

// async
const std::string kVeloxIOThreads = "spark.gluten.sql.columnar.backend.velox.IOThreads";
const uint32_t kVeloxIOThreadsDefault = 0;
const std::string kVeloxAsyncTimeoutOnTaskStopping =
    "spark.gluten.sql.columnar.backend.velox.asyncTimeoutOnTaskStopping";
const int32_t kVeloxAsyncTimeoutOnTaskStoppingDefault = 30000; // 30s

// udf
const std::string kVeloxUdfLibraryPaths = "spark.gluten.sql.columnar.backend.velox.internal.udfLibraryPaths";

// VeloxShuffleReader print flag.
const std::string kVeloxShuffleReaderPrintFlag = "spark.gluten.velox.shuffleReaderPrintFlag";

const std::string kVeloxFileHandleCacheEnabled = "spark.gluten.sql.columnar.backend.velox.fileHandleCacheEnabled";
const bool kVeloxFileHandleCacheEnabledDefault = false;

/* configs for file read in velox*/
const std::string kDirectorySizeGuess = "spark.gluten.sql.columnar.backend.velox.directorySizeGuess";
const std::string kFooterEstimatedSize = "spark.gluten.sql.columnar.backend.velox.footerEstimatedSize";
const std::string kFilePreloadThreshold = "spark.gluten.sql.columnar.backend.velox.filePreloadThreshold";
const std::string kPrefetchRowGroups = "spark.gluten.sql.columnar.backend.velox.prefetchRowGroups";
const std::string kLoadQuantum = "spark.gluten.sql.columnar.backend.velox.loadQuantum";
const std::string kMaxCoalescedDistance = "spark.gluten.sql.columnar.backend.velox.maxCoalescedDistance";
const std::string kMaxCoalescedBytes = "spark.gluten.sql.columnar.backend.velox.maxCoalescedBytes";
const std::string kCachePrefetchMinPct = "spark.gluten.sql.columnar.backend.velox.cachePrefetchMinPct";
const std::string kMemoryPoolCapacityTransferAcrossTasks =
    "spark.gluten.sql.columnar.backend.velox.memoryPoolCapacityTransferAcrossTasks";
const std::string kOrcUseColumnNames = "spark.gluten.sql.columnar.backend.velox.orcUseColumnNames";
const std::string kParquetUseColumnNames = "spark.gluten.sql.columnar.backend.velox.parquetUseColumnNames";
const std::string kAllowInt32Narrowing = "spark.gluten.sql.columnar.backend.velox.allowInt32Narrowing";

// write fies
const std::string kMaxPartitions = "spark.gluten.sql.columnar.backend.velox.maxPartitionsPerWritersSession";
const std::string kMaxTargetFileSize = "spark.gluten.sql.columnar.backend.velox.maxTargetFileSize";

const std::string kGlogVerboseLevel = "spark.gluten.sql.columnar.backend.velox.glogVerboseLevel";
const uint32_t kGlogVerboseLevelDefault = 0;
const uint32_t kGlogVerboseLevelMaximum = 99;
const std::string kGlogSeverityLevel = "spark.gluten.sql.columnar.backend.velox.glogSeverityLevel";
const uint32_t kGlogSeverityLevelDefault = 1;

// Query trace
/// Enable query tracing flag.
const std::string kQueryTraceEnabled = "spark.gluten.sql.columnar.backend.velox.queryTraceEnabled";
/// Base dir of a query to store tracing data.
const std::string kQueryTraceDir = "spark.gluten.sql.columnar.backend.velox.queryTraceDir";
/// The max trace bytes limit. Tracing is disabled if zero.
const std::string kQueryTraceMaxBytes = "spark.gluten.sql.columnar.backend.velox.queryTraceMaxBytes";
/// The regexp of traced task id. We only enable trace on a task if its id
/// matches.
const std::string kQueryTraceTaskRegExp = "spark.gluten.sql.columnar.backend.velox.queryTraceTaskRegExp";
/// Config used to create operator trace directory. This config is provided to
/// underlying file system and the config is free form. The form should be
/// defined by the underlying file system.
const std::string kOpTraceDirectoryCreateConfig =
    "spark.gluten.sql.columnar.backend.velox.opTraceDirectoryCreateConfig";

// Cudf config.
// GPU RMM memory resource
const std::string kCudfMemoryResource = "spark.gluten.sql.columnar.backend.velox.cudf.memoryResource";
const std::string kCudfMemoryResourceDefault =
    "async"; // Allowed: "cuda", "pool", "async", "arena", "managed", "managed_pool"

// Initial percent of GPU memory to allocate for memory resource for one thread
const std::string kCudfMemoryPercent = "spark.gluten.sql.columnar.backend.velox.cudf.memoryPercent";
const std::string kCudfMemoryPercentDefault = "50";

/// Preferred size of batches in bytes to be returned by operators.
const std::string kVeloxPreferredBatchBytes = "spark.gluten.sql.columnar.backend.velox.preferredBatchBytes";

/// cudf
const std::string kCudfEnableTableScan = "spark.gluten.sql.columnar.backend.velox.cudf.enableTableScan";
const bool kCudfEnableTableScanDefault = false;
const std::string kCudfHiveConnectorId = "cudf-hive";
const std::string kCudfShuffleMaxPrefetchBytes = "spark.gluten.sql.columnar.backend.velox.cudf.shuffleMaxPrefetchBytes";
const int64_t kCudfShuffleMaxPrefetchBytesDefault = 1028L * 1024 * 1024; // 1028MB

const std::string kStaticBackendConfPrefix = "spark.gluten.velox.";
const std::string kDynamicBackendConfPrefix = "spark.gluten.sql.columnar.backend.velox.";

} // namespace gluten

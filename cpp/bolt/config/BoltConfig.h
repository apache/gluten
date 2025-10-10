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
const std::string kSpillStrategy = "spark.gluten.sql.columnar.backend.bolt.spillStrategy";
const std::string kSpillStrategyDefaultValue = "auto";
const std::string kSpillThreadNum = "spark.gluten.sql.columnar.backend.bolt.spillThreadNum";
const uint32_t kSpillThreadNumDefaultValue = 0;
const std::string kAggregationSpillEnabled = "spark.gluten.sql.columnar.backend.bolt.aggregationSpillEnabled";
const std::string kJoinSpillEnabled = "spark.gluten.sql.columnar.backend.bolt.joinSpillEnabled";
const std::string kOrderBySpillEnabled = "spark.gluten.sql.columnar.backend.bolt.orderBySpillEnabled";
const std::string kWindowSpillEnabled = "spark.gluten.sql.columnar.backend.bolt.windowSpillEnabled";

// operation fusion
const std::string kAggOutputCompositeVector = "aggregationOutputCompositeVector";
const std::string kHashAggregationCompositeOutputEnabled =
    "spark.gluten.sql.columnar.backend.bolt.hashAggregationCompositeOutputEnabled";
const std::string kHashAggregationCompositeOutputAccumulatorRatio =
    "spark.gluten.sql.columnar.backend.bolt.hashAggregationCompositeOutputAccumulatorRatio";
const std::string kHashAggregationUniqueRowOpt =
    "spark.gluten.sql.columnar.backend.bolt.hashAggregationUniqueRowOptEnabled";
// testing only
const std::string kTestingSpillPct =
    "spark.gluten.sql.columnar.backend.bolt.testingSpillPct";

// spill config
const std::string kMaxSpillLevel = "spark.gluten.sql.columnar.backend.bolt.maxSpillLevel";
const std::string kMaxSpillFileSize = "spark.gluten.sql.columnar.backend.bolt.maxSpillFileSize";
const std::string kSpillStartPartitionBit = "spark.gluten.sql.columnar.backend.bolt.spillStartPartitionBit";
const std::string kSpillPartitionBits = "spark.gluten.sql.columnar.backend.bolt.spillPartitionBits";
const std::string kMaxSpillRunRows = "spark.gluten.sql.columnar.backend.bolt.MaxSpillRunRows";
const std::string kMaxSpillBytes = "spark.gluten.sql.columnar.backend.bolt.MaxSpillBytes";
const std::string kSpillReadBufferSize = "spark.unsafe.sorter.spill.reader.buffer.size";
const uint64_t kMaxSpillFileSizeDefault = 1L * 1024 * 1024 * 1024;

const std::string kSpillableReservationGrowthPct =
    "spark.gluten.sql.columnar.backend.bolt.spillableReservationGrowthPct";
const std::string kSpillPrefixSortEnabled = "spark.gluten.sql.columnar.backend.bolt.spillPrefixsortEnabled";
// Whether to compress data spilled. Compression will use spark.io.compression.codec or kSpillCompressionKind.
const std::string kSparkShuffleSpillCompress = "spark.shuffle.spill.compress";
const std::string kCompressionKind = "spark.io.compression.codec";
/// The compression codec to use for spilling. Use kCompressionKind if not set.
const std::string kSpillCompressionKind = "spark.gluten.sql.columnar.backend.bolt.spillCompressionCodec";
const std::string kMaxPartialAggregationMemoryRatio =
    "spark.gluten.sql.columnar.backend.bolt.maxPartialAggregationMemoryRatio";
const std::string kMaxPartialAggregationMemory = "spark.gluten.sql.columnar.backend.bolt.maxPartialAggregationMemory";
const std::string kMaxExtendedPartialAggregationMemoryRatio =
    "spark.gluten.sql.columnar.backend.bolt.maxExtendedPartialAggregationMemoryRatio";
const std::string kAbandonPartialAggregationMinPct =
    "spark.gluten.sql.columnar.backend.bolt.abandonPartialAggregationMinPct";
const std::string kAbandonPartialAggregationMinRows =
    "spark.gluten.sql.columnar.backend.bolt.abandonPartialAggregationMinRows";

// hashmap build
const std::string kAbandonBuildNoDupHashMinRows = "spark.gluten.sql.columnar.backend.bolt.abandonbuild.noduphashminrows";
const std::string kAbandonBuildNoDupHashMinPct = "spark.gluten.sql.columnar.backend.bolt.abandonbuild.noduphashminpct";

// execution
const std::string kBloomFilterExpectedNumItems = "spark.gluten.sql.columnar.backend.bolt.bloomFilter.expectedNumItems";
const std::string kBloomFilterNumBits = "spark.gluten.sql.columnar.backend.bolt.bloomFilter.numBits";
const std::string kBloomFilterMaxNumBits = "spark.gluten.sql.columnar.backend.bolt.bloomFilter.maxNumBits";
const std::string kBoltSplitPreloadPerDriver = "spark.gluten.sql.columnar.backend.bolt.SplitPreloadPerDriver";

const std::string kShowTaskMetricsWhenFinished = "spark.gluten.sql.columnar.backend.bolt.showTaskMetricsWhenFinished";
const bool kShowTaskMetricsWhenFinishedDefault = false;

const std::string kTaskMetricsToEventLogThreshold =
    "spark.gluten.sql.columnar.backend.bolt.taskMetricsToEventLog.threshold";
const int64_t kTaskMetricsToEventLogThresholdDefault = -1;

const std::string kEnableUserExceptionStacktrace =
    "spark.gluten.sql.columnar.backend.bolt.enableUserExceptionStacktrace";
const bool kEnableUserExceptionStacktraceDefault = true;

const std::string kEnableSystemExceptionStacktrace =
    "spark.gluten.sql.columnar.backend.bolt.enableSystemExceptionStacktrace";
const bool kEnableSystemExceptionStacktraceDefault = true;

const std::string kMemoryUseHugePages = "spark.gluten.sql.columnar.backend.bolt.memoryUseHugePages";
const bool kMemoryUseHugePagesDefault = false;

const std::string kBoltMemInitCapacity = "spark.gluten.sql.columnar.backend.bolt.memInitCapacity";
const uint64_t kBoltMemInitCapacityDefault = 8 << 20;

const std::string kBoltMemReclaimMaxWaitMs = "spark.gluten.sql.columnar.backend.bolt.reclaimMaxWaitMs";
const uint64_t kBoltMemReclaimMaxWaitMsDefault = 3600000; // 60min

const std::string kHiveConnectorId = "test-hive";
const std::string kBoltCacheEnabled = "spark.gluten.sql.columnar.backend.bolt.cacheEnabled";

const std::string kExprMaxCompiledRegexes = "spark.gluten.sql.columnar.backend.bolt.maxCompiledRegexes";

// memory cache
const std::string kBoltMemCacheSize = "spark.gluten.sql.columnar.backend.bolt.memCacheSize";
const uint64_t kBoltMemCacheSizeDefault = 1073741824; // 1G

// ssd cache
const std::string kBoltSsdCacheSize = "spark.gluten.sql.columnar.backend.bolt.ssdCacheSize";
const uint64_t kBoltSsdCacheSizeDefault = 1073741824; // 1G
const std::string kBoltSsdCachePath = "spark.gluten.sql.columnar.backend.bolt.ssdCachePath";
const std::string kBoltSsdCachePathDefault = "/tmp/";
const std::string kBoltSsdCacheShards = "spark.gluten.sql.columnar.backend.bolt.ssdCacheShards";
const uint32_t kBoltSsdCacheShardsDefault = 1;
const std::string kBoltSsdCacheIOThreads = "spark.gluten.sql.columnar.backend.bolt.ssdCacheIOThreads";
const uint32_t kBoltSsdCacheIOThreadsDefault = 1;
const std::string kBoltSsdODirectEnabled = "spark.gluten.sql.columnar.backend.bolt.ssdODirect";
const std::string kBoltSsdCheckpointIntervalBytes =
    "spark.gluten.sql.columnar.backend.bolt.ssdCheckpointIntervalBytes";
const std::string kBoltSsdDisableFileCow = "spark.gluten.sql.columnar.backend.bolt.ssdDisableFileCow";
const std::string kBoltSsdCheckSumEnabled = "spark.gluten.sql.columnar.backend.bolt.ssdChecksumEnabled";
const std::string kBoltSsdCheckSumReadVerificationEnabled =
    "spark.gluten.sql.columnar.backend.bolt.ssdChecksumReadVerificationEnabled";

// async
const std::string kBoltIOThreads = "spark.gluten.sql.columnar.backend.bolt.IOThreads";
const uint32_t kBoltIOThreadsDefault = 16;
const std::string kBoltAsyncTimeoutOnTaskStopping =
    "spark.gluten.sql.columnar.backend.bolt.asyncTimeoutOnTaskStopping";
const int32_t kBoltAsyncTimeoutOnTaskStoppingDefault = 30000; // 30s

// 0 for disable, 1 for adaptive enable, -1 for force enable
const std::string kPreloadEnabled = "spark.gluten.sql.columnar.backend.bolt.preload.enabled";
const int32_t kPreloadEnabledDefault = -1;

// udf
const std::string kBoltUdfLibraryPaths = "spark.gluten.sql.columnar.backend.bolt.internal.udfLibraryPaths";

// BoltShuffleReader print flag.
const std::string kBoltShuffleReaderPrintFlag = "spark.gluten.bolt.shuffleReaderPrintFlag";

const std::string kBoltFileHandleCacheEnabled = "spark.gluten.sql.columnar.backend.bolt.fileHandleCacheEnabled";
const bool kBoltFileHandleCacheEnabledDefault = false;

/* configs for file read in bolt*/
const std::string kDirectorySizeGuess = "spark.gluten.sql.columnar.backend.bolt.directorySizeGuess";
const std::string kFooterEstimatedSize = "spark.gluten.sql.columnar.backend.bolt.footerEstimatedSize";
const std::string kFilePreloadThreshold = "spark.gluten.sql.columnar.backend.bolt.filePreloadThreshold";
const std::string kPrefetchRowGroups = "spark.gluten.sql.columnar.backend.bolt.prefetchRowGroups";
const std::string kLoadQuantum = "spark.gluten.sql.columnar.backend.bolt.loadQuantum";
const std::string kMaxCoalescedDistance = "spark.gluten.sql.columnar.backend.bolt.maxCoalescedDistance";
const std::string kMaxCoalescedBytes = "spark.gluten.sql.columnar.backend.bolt.maxCoalescedBytes";
const std::string kCachePrefetchMinPct = "spark.gluten.sql.columnar.backend.bolt.cachePrefetchMinPct";
const std::string kMemoryPoolCapacityTransferAcrossTasks =
    "spark.gluten.sql.columnar.backend.bolt.memoryPoolCapacityTransferAcrossTasks";

const std::string kOrcUseColumnNames = "spark.gluten.sql.columnar.backend.bolt.orcUseColumnNames";
const std::string kParquetUseColumnNames = "spark.gluten.sql.columnar.backend.bolt.parquetUseColumnNames";

// write fies
const std::string kMaxPartitions = "spark.gluten.sql.columnar.backend.bolt.maxPartitionsPerWritersSession";

const std::string kGlogVerboseLevel = "spark.gluten.sql.columnar.backend.bolt.glogVerboseLevel";
const uint32_t kGlogVerboseLevelDefault = 0;
const uint32_t kGlogVerboseLevelMaximum = 99;
const std::string kGlogSeverityLevel = "spark.gluten.sql.columnar.backend.bolt.glogSeverityLevel";
const uint32_t kGlogSeverityLevelDefault = 1;

// Query trace
/// Enable query tracing flag.
const std::string kQueryTraceEnabled = "spark.gluten.sql.columnar.backend.bolt.queryTraceEnabled";
/// Base dir of a query to store tracing data.
const std::string kQueryTraceDir = "spark.gluten.sql.columnar.backend.bolt.queryTraceDir";
/// The max trace bytes limit. Tracing is disabled if zero.
const std::string kQueryTraceMaxBytes = "spark.gluten.sql.columnar.backend.bolt.queryTraceMaxBytes";
/// The regexp of traced task id. We only enable trace on a task if its id
/// matches.
const std::string kQueryTraceTaskRegExp = "spark.gluten.sql.columnar.backend.bolt.queryTraceTaskRegExp";
/// Config used to create operator trace directory. This config is provided to
/// underlying file system and the config is free form. The form should be
/// defined by the underlying file system.
const std::string kOpTraceDirectoryCreateConfig =
    "spark.gluten.sql.columnar.backend.bolt.opTraceDirectoryCreateConfig";

// Cudf config.
// GPU RMM memory resource
const std::string kCudfMemoryResource = "spark.gluten.sql.columnar.backend.bolt.cudf.memoryResource";
const std::string kCudfMemoryResourceDefault =
    "async"; // Allowed: "cuda", "pool", "async", "arena", "managed", "managed_pool"

// Initial percent of GPU memory to allocate for memory resource for one thread
const std::string kCudfMemoryPercent = "spark.gluten.sql.columnar.backend.bolt.cudf.memoryPercent";
const std::string kCudfMemoryPercentDefault = "50";

/// Preferred size of batches in bytes to be returned by operators.
const std::string kBoltPreferredBatchBytes = "spark.gluten.sql.columnar.backend.bolt.preferredBatchBytes";

/// cudf
const std::string kCudfEnableTableScan = "spark.gluten.sql.columnar.backend.bolt.cudf.enableTableScan";
const bool kCudfEnableTableScanDefault = false;
const std::string kCudfHiveConnectorId = "cudf-hive";

// dynamic concurrency adjustment at runtime
const std::string kDynamicConcurrencyAdjustmentEnabled =
    "spark.gluten.sql.columnar.backend.bolt.dynamicConcurrencyAdjustment.enabled";
const bool kDynamicConcurrencyAdjustmentEnabledDefault = false;
const std::string kDynamicConcurrencyDefaultValue = "spark.gluten.sql.columnar.backend.bolt.dynamicDefaultConcurrency";
const std::string kBoltTaskSchedulingEnabled = "spark.gluten.sql.columnar.backend.bolt.boltTaskScheduling.enabled";
const std::string kDynamicConcurrency = "dynamicConcurrency";

const std::string kBoltJitEnabled = "spark.gluten.sql.columnar.backend.bolt.jit.enabled";
const std::string kThrowExceptionWhenEncounterBadTimestamp = "spark.gluten.sql.columnar.backend.bolt.timestamp.throwExceptionWhenEncounterBadTimestamp";
const std::string kRegexMatchDanglingRightBrackets = "spark.gluten.sql.columnar.backend.bolt.regex.regexMatchDanglingRightBrackets";

const std::string kEstimateRowSizeBasedOnSampleEnabled = "spark.gluten.sql.columnar.backend.bolt.estimateRowSizeBasedOnSample";
const std::string kAbandonPartialAggregationMinFinalPct = "spark.gluten.sql.columnar.backend.bolt.abandonPartialAggregationMinFinalPct";
const std::string kPartialAggregationSpillMaxPct = "spark.gluten.sql.columnar.backend.bolt.partialAggregationSpillMaxPct";
const std::string kPreferPartialAggregationSpill = "spark.gluten.sql.columnar.backend.bolt.preferPartialAggregationSpill";
const std::string kAdaptiveSkippedDataSizeThreshold = "spark.gluten.sql.columnar.backend.bolt.partialAggregationAdaptiveSkippedSize";
const std::string kMaxHashTableSize = "spark.gluten.sql.columnar.backend.bolt.hashTableMaxSize";

// spill related config
const std::string kRowBasedSpillMode = "spark.gluten.sql.columnar.backend.bolt.rowBasedSpillMode";
const std::string kSpillUringEnabled = "spark.gluten.sql.columnar.backend.bolt.spillUringEnabled";
const std::string kSpilledAggregationBypassHTRatio = "spark.gluten.sql.columnar.backend.bolt.spilledAggregationBypassHashTableRatio";
const std::string kParquetDecodeRepDefPageCount = "spark.gluten.sql.columnar.backend.bolt.parquetDecodeRepDefPageCount";
const std::string kParquetRepDefMemoryRatio = "spark.gluten.sql.columnar.backend.bolt.parquetRepDefMemoryRatio";

// json related config
const std::string kUseSonicJson = "spark.gluten.sql.columnar.backend.bolt.json.useSonicJson";
const std::string kThrowExceptionWhenEncounterBadJson = "spark.gluten.sql.columnar.backend.bolt.json.throwExceptionWhenEncounterBadJson";
const std::string kOrderBySpillInOutputStageEnabled = "spark.gluten.sql.columnar.backend.bolt.orderBySpillInOutputStageEnabled";
const std::string kUseDOMParserInGetJsonObject = "spark.gluten.sql.columnar.backend.bolt.json.useDOMParserInGetJsonObject";
const std::string kEnableSonicIsJsonScalar =  "spark.gluten.sql.columnar.backend.bolt.sonic.is_json_scalar";
const std::string kEnableSonicJsonArrayContains = "spark.gluten.sql.columnar.backend.bolt.sonic.json_array_contains";
const std::string kEnableSonicJsonArrayLength = "spark.gluten.sql.columnar.backend.bolt.sonic.json_array_length";
const std::string kEnableSonicJsonExtractScalar = "spark.gluten.sql.columnar.backend.bolt.sonic.json_extract_scalar";
const std::string kEnableSonicJsonExtract = "spark.gluten.sql.columnar.backend.bolt.sonic.json_extract";
const std::string kEnableSonicJsonSize = "spark.gluten.sql.columnar.backend.bolt.sonic.json_size";
const std::string kEnableSonicJsonSplit = "spark.gluten.sql.columnar.backend.bolt.sonic.json_split";
const std::string kEnableSonicJsonParse = "spark.gluten.sql.columnar.backend.bolt.sonic.json_parse";
const std::string kEnableSonicJsonToMap = "spark.gluten.sql.columnar.backend.bolt.sonic.json_to_map";

const std::string kLegacyCastComplexTypesToStringEnabled = "spark.sql.legacy.castComplexTypesToString.enabled";
const std::string kIgnoreCorruptFiles = "spark.sql.files.ignoreCorruptFiles";

const std::string kParquetRowNumInEachBlock = "parquet.block.rowNumInEachBlock";

const std::string kParquetWriterBufferGrowRatio = "spark.gluten.sql.parquet.writer.bufferGrowRatio";

const std::string kParquetWriterBufferReserveRatio = "spark.gluten.sql.parquet.writer.bufferReserveRatio";

const std::string kParquetWriteLegacyFormat = "spark.sql.parquet.writeLegacyFormat";

const std::string kParquetWriterMultithreadingEnabled = "spark.gluten.sql.native.writer.multithreading.enabled";

const std::string kNativeWriterParquetVersion = "spark.gluten.sql.native.writer.parquet.version";
const std::string kNativeWriterParquetSplitMinBatchSize = "spark.gluten.sql.native.writer.parquet.split.minBatchSize";
const std::string kNativeWriterParquetSplitBatchBytes = "spark.gluten.sql.native.writer.parquet.split.BatchBytes";
} // namespace gluten

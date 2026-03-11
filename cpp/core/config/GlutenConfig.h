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

#include <jni.h>
#include <cstdint>
#include <string>
#include <unordered_map>

namespace gluten {

// store configurations that are general to all backend types
const std::string kDebugModeEnabled = "spark.gluten.sql.debug";

const std::string kGlutenSaveDir = "spark.gluten.saveDir";

const std::string kCaseSensitive = "spark.sql.caseSensitive";

const std::string kAnsiEnabled = "spark.sql.ansi.enabled";

const std::string kSessionTimezone = "spark.sql.session.timeZone";

const std::string kAllowPrecisionLoss = "spark.sql.decimalOperations.allowPrecisionLoss";

const std::string kIgnoreMissingFiles = "spark.sql.files.ignoreMissingFiles";

const std::string kSparkOverheadMemory = "spark.gluten.memoryOverhead.size.in.bytes";

const std::string kSparkOffHeapMemory = "spark.gluten.memory.offHeap.size.in.bytes";

const std::string kSparkTaskOffHeapMemory = "spark.gluten.memory.task.offHeap.size.in.bytes";

const std::string kMemoryReservationBlockSize = "spark.gluten.memory.reservationBlockSize";
const uint64_t kMemoryReservationBlockSizeDefault = 8 << 20;

const std::string kCheckUsageLeak = "spark.gluten.sql.columnar.backend.velox.checkUsageLeak";
const bool kCheckUsageLeakDefault = true;

const std::string kSparkBatchSize = "spark.gluten.sql.columnar.maxBatchSize";

const std::string kParquetBlockSize = "parquet.block.size";

const std::string kParquetBlockRows = "parquet.block.rows";

const std::string kParquetGzipWindowSize = "parquet.gzip.windowSize";
const std::string kGzipWindowSize4k = "4096";

const std::string kParquetZSTDCompressionLevel = "parquet.compression.codec.zstd.level";

const std::string kParquetDataPageSize = "parquet.page.size";

const std::string kParquetEnableDictionary = "parquet.enable.dictionary";

const std::string kParquetWriterVersion = "parquet.writer.version";

const std::string kParquetCompressionCodec = "spark.sql.parquet.compression.codec";

const std::string kColumnarToRowMemoryThreshold = "spark.gluten.sql.columnarToRowMemoryThreshold";

const std::string kUGIUserName = "spark.gluten.ugi.username";
const std::string kUGITokens = "spark.gluten.ugi.tokens";

const std::string kShuffleCompressionCodec = "spark.gluten.sql.columnar.shuffle.codec";
const std::string kShuffleCompressionCodecBackend = "spark.gluten.sql.columnar.shuffle.codecBackend";
const std::string kShuffleSpillDiskWriteBufferSize = "spark.shuffle.spill.diskWriteBufferSize";
const std::string kSortShuffleReaderDeserializerBufferSize =
    "spark.gluten.sql.columnar.shuffle.sort.deserializerBufferSize";
const std::string kQatBackendName = "qat";

const std::string kSparkRedactionRegex = "spark.redaction.regex";
const std::string kSparkRedactionString = "*********(redacted)";

const std::string kSparkLegacyTimeParserPolicy = "spark.sql.legacy.timeParserPolicy";
const std::string kShuffleFileBufferSize = "spark.shuffle.file.buffer";
const std::string kSparkMapKeyDedupPolicy = "spark.sql.mapKeyDedupPolicy";

const std::string kSparkLegacyStatisticalAggregate = "spark.sql.legacy.statisticalAggregate";

const std::string kSparkJsonIgnoreNullFields = "spark.sql.jsonGenerator.ignoreNullFields";

// cudf
const std::string kCudfEnabled = "spark.gluten.sql.columnar.cudf";
constexpr bool kCudfEnabledDefault = false;
const std::string kDebugCudf = "spark.gluten.sql.debug.cudf";
const std::string kDebugCudfDefault = "false";

std::unordered_map<std::string, std::string>
parseConfMap(JNIEnv* env, const uint8_t* planData, const int32_t planDataLength);

std::string printConfig(const std::unordered_map<std::string, std::string>& conf);
} // namespace gluten

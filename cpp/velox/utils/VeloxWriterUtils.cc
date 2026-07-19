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

#include "utils/VeloxWriterUtils.h"

#include <boost/algorithm/string.hpp>

#include "config/GlutenConfig.h"
#include "utils/ConfigExtractor.h"
#include "utils/Exception.h"

#include "memory/VeloxMemoryManager.h"
#include "velox/common/compression/Compression.h"
#include "velox/type/Type.h"

namespace gluten {

using namespace facebook::velox;
using namespace facebook::velox::parquet;
using namespace facebook::velox::common;

namespace {
const int32_t kGzipWindowBits4k = 12;
const int32_t kZSTDDefaultCompressionLevel = 3;
} // namespace

std::shared_ptr<facebook::velox::dwio::common::WriterOptions> makeParquetWriteOption(
    const std::unordered_map<std::string, std::string>& sparkConfs) {
  int64_t maxRowGroupBytes = 134217728; // 128MB
  int64_t maxRowGroupRows = 100000000; // 100M
  if (auto it = sparkConfs.find(kParquetBlockSize); it != sparkConfs.end()) {
    maxRowGroupBytes = std::stoll(it->second);
  }
  if (auto it = sparkConfs.find(kParquetBlockRows); it != sparkConfs.end()) {
    maxRowGroupRows = std::stoll(it->second);
  }

  auto writeOption = std::make_shared<facebook::velox::dwio::common::WriterOptions>();
  auto parquetOptions = std::make_shared<ParquetWriterOptions>();
  parquetOptions->parquetWriteTimestampUnit = TimestampPrecision::kMicroseconds /*micro*/;
  bool writeLegacyParquetFormat = false;
  if (auto it = sparkConfs.find(kParquetStoreDecimalAsInteger); it != sparkConfs.end()) {
    writeLegacyParquetFormat = boost::iequals(it->second, "true");
  }
  // Maps spark.sql.parquet.writeLegacyFormat to Velox enableStoreDecimalAsInteger (inverted).
  // Controls how DECIMAL values are stored by the Writer:
  // - true (default when legacy format is off): store as integer using INT32/INT64 for short
  //   DECIMAL precisions; higher precisions use FIXED_LEN_BYTE_ARRAY.
  // - false (when spark.sql.parquet.writeLegacyFormat is true): store as FIXED_LEN_BYTE_ARRAY
  //   regardless of precision (Spark legacy Parquet decimal layout).
  // TODO: Only DECIMAL is handled here. Spark's writeLegacyFormat also changes ARRAY (bag/array
  // vs list/element) and MAP (map vs key_value) physical layouts, which are not yet supported
  // in Gluten native Parquet write.
  parquetOptions->enableStoreDecimalAsInteger = !writeLegacyParquetFormat;
  auto compressionCodec = CompressionKind::CompressionKind_SNAPPY;
  if (auto it = sparkConfs.find(kParquetCompressionCodec); it != sparkConfs.end()) {
    auto compressionCodecStr = it->second;
    // spark support none, uncompressed, snappy, gzip, lzo, brotli, lz4, zstd.
    if (boost::iequals(compressionCodecStr, "snappy")) {
      compressionCodec = CompressionKind::CompressionKind_SNAPPY;
    } else if (boost::iequals(compressionCodecStr, "gzip")) {
      compressionCodec = CompressionKind::CompressionKind_GZIP;
      if (sparkConfs.find(kParquetGzipWindowSize) != sparkConfs.end()) {
        auto parquetGzipWindowSizeStr = sparkConfs.find(kParquetGzipWindowSize)->second;
        if (parquetGzipWindowSizeStr == kGzipWindowSize4k) {
          auto codecOptions = std::make_shared<parquet::arrow::util::GZipCodecOptions>();
          codecOptions->windowBits = kGzipWindowBits4k;
          parquetOptions->codecOptions = std::move(codecOptions);
        }
      }
    } else if (boost::iequals(compressionCodecStr, "lzo")) {
      compressionCodec = CompressionKind::CompressionKind_LZO;
    } else if (boost::iequals(compressionCodecStr, "brotli")) {
      // please make sure `brotli` is enabled when compiling
      throw GlutenException("Gluten+velox does not support write parquet using brotli.");
    } else if (boost::iequals(compressionCodecStr, "lz4")) {
      compressionCodec = CompressionKind::CompressionKind_LZ4_HADOOP;
    } else if (boost::iequals(compressionCodecStr, "zstd")) {
      compressionCodec = CompressionKind::CompressionKind_ZSTD;
      auto codecOptions = std::make_shared<parquet::arrow::util::CodecOptions>();
      auto it = sparkConfs.find(kParquetZSTDCompressionLevel);
      auto compressionLevel = it != sparkConfs.end() ? std::stoi(it->second) : kZSTDDefaultCompressionLevel;
      codecOptions->compressionLevel = compressionLevel;
      parquetOptions->codecOptions = std::move(codecOptions);
    } else if (boost::iequals(compressionCodecStr, "uncompressed")) {
      compressionCodec = CompressionKind::CompressionKind_NONE;
    } else if (boost::iequals(compressionCodecStr, "none")) {
      compressionCodec = CompressionKind::CompressionKind_NONE;
    }
  }
  writeOption->compressionKind = compressionCodec;
  writeOption->flushPolicyFactory = [maxRowGroupRows, maxRowGroupBytes]() {
    return std::make_unique<LambdaFlushPolicy>(maxRowGroupRows, maxRowGroupBytes, [&]() { return false; });
  };
  if (auto it = sparkConfs.find(kSessionTimezone); it != sparkConfs.end()) {
    parquetOptions->parquetWriteTimestampTimeZone = normalizeSessionTimezone(it->second);
  }
  parquetOptions->arrowMemoryPool =
      getDefaultMemoryManager()->getOrCreateArrowMemoryPool("VeloxParquetWrite.ArrowMemoryPool");
  if (auto it = sparkConfs.find(kParquetDataPageSize); it != sparkConfs.end()) {
    auto dataPageSize = std::stoll(it->second);
    parquetOptions->dataPageSize = dataPageSize;
  }
  if (auto it = sparkConfs.find(kParquetWriterVersion); it != sparkConfs.end()) {
    auto parquetVersion = it->second;
    if (boost::iequals(parquetVersion, "v2")) {
      parquetOptions->useParquetDataPageV2 = true;
    }
  }
  if (auto it = sparkConfs.find(kParquetEnableDictionary); it != sparkConfs.end()) {
    auto enableDictionary = it->second;
    if (boost::iequals(enableDictionary, "true")) {
      parquetOptions->enableDictionary = true;
    } else {
      parquetOptions->enableDictionary = false;
    }
  }
  writeOption->formatSpecificOptions = std::move(parquetOptions);
  return writeOption;
}

} // namespace gluten

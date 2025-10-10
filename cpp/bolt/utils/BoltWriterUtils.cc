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

#include "utils/BoltWriterUtils.h"

#include <boost/algorithm/string.hpp>

#include "config/GlutenConfig.h"
#include "utils/ConfigExtractor.h"
#include "utils/Exception.h"

#include "memory/BoltMemoryManager.h"
#include "bolt/common/compression/Compression.h"
#include "bolt/type/Type.h"

namespace gluten {

using namespace bytedance::bolt;
using namespace bytedance::bolt::parquet;
using namespace bytedance::bolt::common;

namespace {
const int32_t kGzipWindowBits4k = 12;
}

std::unique_ptr<WriterOptions> makeParquetWriteOption(const std::unordered_map<std::string, std::string>& sparkConfs) {
  int64_t maxRowGroupBytes = 134217728; // 128MB
  int64_t maxRowGroupRows = 100000000; // 100M
  if (auto it = sparkConfs.find(kParquetBlockSize); it != sparkConfs.end()) {
    maxRowGroupBytes = static_cast<int64_t>(stoi(it->second));
  }
  if (auto it = sparkConfs.find(kParquetBlockRows); it != sparkConfs.end()) {
    maxRowGroupRows = static_cast<int64_t>(stoi(it->second));
  }
  auto writeOption = std::make_unique<WriterOptions>();
  // writeOption->parquetWriteTimestampUnit = TimestampPrecision::kMicroseconds /*micro*/;
  writeOption->parquetWriteTimestampUnit = TimestampUnit::kMicro;
  // set default compression to snappy
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
          auto codecOptions = std::make_shared<bytedance::bolt::parquet::arrow::util::GZipCodecOptions>();
          codecOptions->window_bits = kGzipWindowBits4k;
          writeOption->codecOptions = std::move(codecOptions);
        }
      }
    } else if (boost::iequals(compressionCodecStr, "lzo")) {
      compressionCodec = CompressionKind::CompressionKind_LZO;
    } else if (boost::iequals(compressionCodecStr, "brotli")) {
      // please make sure `brotli` is enabled when compiling
      throw GlutenException("Gluten+bolt does not support write parquet using brotli.");
    } else if (boost::iequals(compressionCodecStr, "lz4")) {
      compressionCodec = CompressionKind::CompressionKind_LZ4;
    } else if (boost::iequals(compressionCodecStr, "zstd")) {
      compressionCodec = CompressionKind::CompressionKind_ZSTD;
      if (auto it = sparkConfs.find(kParquetZSTDCompressionLevel); it != sparkConfs.end()) {
        auto compressionLevel = std::stoi(it->second);
        auto codecOptions = std::make_shared<bytedance::bolt::parquet::arrow::util::CodecOptions>();
        codecOptions->compression_level = compressionLevel;
        writeOption->codecOptions = std::move(codecOptions);
      }
    } else if (boost::iequals(compressionCodecStr, "uncompressed")) {
      compressionCodec = CompressionKind::CompressionKind_NONE;
    } else if (boost::iequals(compressionCodecStr, "none")) {
      compressionCodec = CompressionKind::CompressionKind_NONE;
    }
  }
  writeOption->compression = compressionCodec;
  writeOption->flushPolicyFactory = [maxRowGroupRows, maxRowGroupBytes]() {
    return std::make_unique<LambdaFlushPolicy>(maxRowGroupRows, maxRowGroupBytes, [&]() { return false; });
  };
  writeOption->parquetWriteTimestampTimeZone = getConfigValue(sparkConfs, kSessionTimezone, std::nullopt);
  // writeOption->arrowPool_ =
      // getDefaultMemoryManager()->getOrCreateArrowMemoryPool("BoltParquetWrite.ArrowMemoryPool");
  if (auto it = sparkConfs.find(kParquetDataPageSize); it != sparkConfs.end()) {
    auto dataPageSize = std::stoi(it->second);
    writeOption->dataPageSize = dataPageSize;
  }
  if (auto it = sparkConfs.find(kParquetWriterVersion); it != sparkConfs.end()) {
    auto parquetVersion = it->second;
    if (boost::iequals(parquetVersion, "v2")) {
      // writeOption->useParquetDataPageV2 = true;
      writeOption->dataPageVersion = bytedance::bolt::parquet::arrow::ParquetDataPageVersion::V2;
    }
  }
  if (auto it = sparkConfs.find(kParquetEnableDictionary); it != sparkConfs.end()) {
    auto enableDictionary = it->second;
    if (boost::iequals(enableDictionary, "true")) {
      writeOption->enableDictionary = true;
    } else {
      writeOption->enableDictionary = false;
    }
  }
  return writeOption;
}

} // namespace gluten

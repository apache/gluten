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

#include <cctype>
#include <string_view>

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

class ParquetFieldIdParser {
 public:
  explicit ParquetFieldIdParser(std::string_view value) : value_(value) {}

  std::vector<ParquetFieldId> parse() {
    skipWhitespace();
    if (atEnd()) {
      return {};
    }
    auto fieldIds = parseList('\0');
    skipWhitespace();
    if (!atEnd()) {
      throw GlutenException("Invalid parquet field id tree: unexpected trailing input.");
    }
    return fieldIds;
  }

 private:
  std::vector<ParquetFieldId> parseList(char terminator) {
    std::vector<ParquetFieldId> fieldIds;
    while (true) {
      skipWhitespace();
      if (atEnd()) {
        if (terminator != '\0') {
          throw GlutenException("Invalid parquet field id tree: missing closing delimiter.");
        }
        return fieldIds;
      }
      if (terminator != '\0' && value_[position_] == terminator) {
        ++position_;
        return fieldIds;
      }

      fieldIds.push_back(parseNode());
      skipWhitespace();
      if (atEnd()) {
        if (terminator != '\0') {
          throw GlutenException("Invalid parquet field id tree: missing closing delimiter.");
        }
        return fieldIds;
      }
      if (value_[position_] == ',') {
        ++position_;
      } else if (terminator != '\0' && value_[position_] == terminator) {
        ++position_;
        return fieldIds;
      } else {
        throw GlutenException("Invalid parquet field id tree: expected ',' or closing delimiter.");
      }
    }
  }

  ParquetFieldId parseNode() {
    ParquetFieldId fieldId;
    fieldId.fieldId = parseInteger();
    skipWhitespace();
    if (!atEnd() && value_[position_] == '(') {
      ++position_;
      fieldId.children = parseList(')');
    }
    return fieldId;
  }

  int32_t parseInteger() {
    skipWhitespace();
    if (atEnd()) {
      throw GlutenException("Invalid parquet field id tree: expected integer.");
    }

    int sign = 1;
    if (value_[position_] == '-') {
      sign = -1;
      ++position_;
    }
    if (atEnd() || !std::isdigit(static_cast<unsigned char>(value_[position_]))) {
      throw GlutenException("Invalid parquet field id tree: expected integer.");
    }

    int64_t value = 0;
    while (!atEnd() && std::isdigit(static_cast<unsigned char>(value_[position_]))) {
      value = value * 10 + value_[position_] - '0';
      ++position_;
    }
    return static_cast<int32_t>(sign * value);
  }

  void skipWhitespace() {
    while (!atEnd() && std::isspace(static_cast<unsigned char>(value_[position_]))) {
      ++position_;
    }
  }

  bool atEnd() const {
    return position_ >= value_.size();
  }

  std::string_view value_;
  size_t position_{0};
};

std::vector<ParquetFieldId> parseParquetFieldIds(std::string_view value) {
  return ParquetFieldIdParser(value).parse();
}
} // namespace

std::unique_ptr<WriterOptions> makeParquetWriteOption(const std::unordered_map<std::string, std::string>& sparkConfs) {
  int64_t maxRowGroupBytes = 134217728; // 128MB
  int64_t maxRowGroupRows = 100000000; // 100M
  if (auto it = sparkConfs.find(kParquetBlockSize); it != sparkConfs.end()) {
    maxRowGroupBytes = std::stoll(it->second);
  }
  if (auto it = sparkConfs.find(kParquetBlockRows); it != sparkConfs.end()) {
    maxRowGroupRows = std::stoll(it->second);
  }
  auto writeOption = std::make_unique<WriterOptions>();
  writeOption->parquetWriteTimestampUnit = TimestampPrecision::kMicroseconds /*micro*/;
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
          writeOption->codecOptions = std::move(codecOptions);
        }
      }
    } else if (boost::iequals(compressionCodecStr, "lzo")) {
      compressionCodec = CompressionKind::CompressionKind_LZO;
    } else if (boost::iequals(compressionCodecStr, "brotli")) {
      // please make sure `brotli` is enabled when compiling
      throw GlutenException("Gluten+velox does not support write parquet using brotli.");
    } else if (boost::iequals(compressionCodecStr, "lz4")) {
      compressionCodec = CompressionKind::CompressionKind_LZ4;
    } else if (boost::iequals(compressionCodecStr, "zstd")) {
      compressionCodec = CompressionKind::CompressionKind_ZSTD;
      auto codecOptions = std::make_shared<parquet::arrow::util::CodecOptions>();
      auto it = sparkConfs.find(kParquetZSTDCompressionLevel);
      auto compressionLevel = it != sparkConfs.end() ? std::stoi(it->second) : kZSTDDefaultCompressionLevel;
      codecOptions->compressionLevel = compressionLevel;
      writeOption->codecOptions = std::move(codecOptions);
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
    writeOption->parquetWriteTimestampTimeZone = normalizeSessionTimezone(it->second);
  }
  writeOption->arrowMemoryPool =
      getDefaultMemoryManager()->getOrCreateArrowMemoryPool("VeloxParquetWrite.ArrowMemoryPool");
  if (auto it = sparkConfs.find(kParquetDataPageSize); it != sparkConfs.end()) {
    auto dataPageSize = std::stoll(it->second);
    writeOption->dataPageSize = dataPageSize;
  }
  if (auto it = sparkConfs.find(kParquetWriterVersion); it != sparkConfs.end()) {
    auto parquetVersion = it->second;
    if (boost::iequals(parquetVersion, "v2")) {
      writeOption->useParquetDataPageV2 = true;
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
  if (auto it = sparkConfs.find(kParquetFieldIds); it != sparkConfs.end()) {
    writeOption->parquetFieldIds = parseParquetFieldIds(it->second);
  }
  return writeOption;
}

} // namespace gluten
